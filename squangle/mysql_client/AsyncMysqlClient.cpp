/*
 *  Copyright (c) 2015, Facebook, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */

#include "squangle/mysql_client/AsyncMysqlClient.h"
#include "squangle/mysql_client/Operation.h"
#include "squangle/mysql_client/FutureAdapter.h"

#include <vector>

#include <folly/Singleton.h>
#include <folly/ThreadName.h>
#include <folly/Memory.h>
#include <folly/io/async/EventBaseManager.h>

#include <mysql.h>

#include <unistd.h>
#include <fcntl.h>

#ifndef NO_LIB_GFLAGS
#include "common/config/Flags.h" // nolint
DECLARE_int64(async_mysql_timeout_micros);
DECLARE_bool(ssl_session_cache_enabled);
DECLARE_int32(ssl_session_cache_size);
#endif

namespace {
class InitMysqlLibrary {
 public:
  InitMysqlLibrary() { mysql_library_init(-1, nullptr, nullptr); }
};
}

namespace facebook {
namespace common {
namespace mysql_client {

#ifdef NO_LIB_GFLAGS
extern int64_t FLAGS_async_mysql_timeout_micros;
extern int64_t FLAGS_ssl_session_cache_enabled;
extern int64_t FLAGS_ssl_session_cache_size;
#endif

ConnectionOptions::ConnectionOptions()
    : connection_timeout_(FLAGS_async_mysql_timeout_micros),
      total_timeout_(FLAGS_async_mysql_timeout_micros),
      query_timeout_(FLAGS_async_mysql_timeout_micros) {}

namespace {
folly::Singleton<AsyncMysqlClient> client;
}

std::shared_ptr<AsyncMysqlClient> AsyncMysqlClient::defaultClient() {
  return folly::Singleton<AsyncMysqlClient>::try_get();
}

AsyncMysqlClient::AsyncMysqlClient()
    : client_stats_(new db::SimpleDbCounter()),
      pools_conn_limit_(std::numeric_limits<uint64_t>::max()),
      ssl_session_cache_(FLAGS_ssl_session_cache_enabled
                             ? folly::make_unique<SSLSessionCache>(
                                   FLAGS_ssl_session_cache_size)
                             : nullptr) {
  init();
}

AsyncMysqlClient::AsyncMysqlClient(
    std::unique_ptr<db::SquangleLoggerBase> db_logger,
    std::unique_ptr<db::DBCounterBase> db_stats)
    : db_logger_(std::move(db_logger)),
      client_stats_(std::move(db_stats)),
      pools_conn_limit_(std::numeric_limits<uint64_t>::max()),
      ssl_session_cache_(FLAGS_ssl_session_cache_enabled
                             ? folly::make_unique<SSLSessionCache>(
                                   FLAGS_ssl_session_cache_size)
                             : nullptr) {
  init();
}

void AsyncMysqlClient::init() {
  static InitMysqlLibrary unused;
  thread_ = std::thread([this]() {
#ifdef __GLIBC__
    folly::setThreadName(pthread_self(), "async-mysql");
#endif
    folly::EventBaseManager::get()->setEventBase(this->getEventBase(), false);
    tevent_base_.loopForever();
    mysql_thread_end();
  });
  tevent_base_.waitUntilRunning();
}


bool AsyncMysqlClient::runInThread(const folly::Cob& fn) {
  auto scheduleTime = std::chrono::high_resolution_clock::now();
  if (!tevent_base_.runInEventBaseThread(
          [ fn, scheduleTime, this ]() {
            auto delay = std::chrono::duration_cast<std::chrono::microseconds>(
                             std::chrono::high_resolution_clock::now() -
                             scheduleTime).count();
            callbackDelayAvg_.addSample(delay);
            fn();
          })) {
    LOG(DFATAL) << "folly::EventBase::runInEventBase failed";
    return false;
  }
  return true;
}

void AsyncMysqlClient::drain(bool also_shutdown) {
  {
    std::unique_lock<std::mutex> lock(pending_operations_mutex_);
    shutting_down_ = also_shutdown;

    auto it = pending_operations_.begin();
    // Clean out any unstarted operations.
    while (it != pending_operations_.end()) {
      // So here the Operation `run` was not called
      // We don't need to lock the state change in the operation here since the
      // cancelling process is going to fire not matter in which part it is.
      if ((*it)->state() == OperationState::Unstarted) {
        (*it)->cancel();
        it = pending_operations_.erase(it);
      } else {
      ++it;
      }
    }
  }

  // Now wait for any started operations to complete.
  std::unique_lock<std::mutex> counter_lock(this->counters_mutex_);
  active_connections_closed_cv_.wait(counter_lock,
      [&also_shutdown, this] {
      if (also_shutdown) {
        VLOG(11) << "Waiting for " << this->active_connection_counter_
                 << " connections to be released before shutting client down";
      }
      return this->active_connection_counter_ == 0;
    }
  );
}

AsyncMysqlClient::~AsyncMysqlClient() {
  // Drain anything we currently have, and if those operations make
  // new operations, that's okay.
  drain(false);
  // Once that pass is done, finish anything that happened to sneak
  // in, but guarantee no new operations will come along.
  drain(true);

  CHECK_EQ(numStartedAndOpenConnections(), 0);

  DCHECK(connection_references_.size() == 0);

  // TODO: Maybe add here a runInThread to cancel the AsyncTimeout

  // All operations are done.  Shut the thread down.
  tevent_base_.terminateLoopSoon();
  thread_.join();
  VLOG(2) << "AsyncMysqlClient finished destructor";
}


void AsyncMysqlClient::setDBLoggerForTesting(
    std::unique_ptr<db::SquangleLoggerBase> dbLogger) {
  db_logger_ = std::move(dbLogger);
}

void AsyncMysqlClient::setDBCounterForTesting(
    std::unique_ptr<db::DBCounterBase> dbCounter) {
  client_stats_ = std::move(dbCounter);
}

void AsyncMysqlClient::logQuerySuccess(Duration dur,
                                       db::QueryType type,
                                       int queries_executed,
                                       const folly::fbstring& query,
                                       const Connection& conn) {
  CHECK_EQ(threadId(), std::this_thread::get_id());
  stats()->incrSucceededQueries();
  if (db_logger_) {
    db_logger_->logQuerySuccess(
        dur,
        type,
        queries_executed,
        query.toStdString(),
        makeSquangleLoggingData(conn.getKey(), conn.getConnectionContext()));
  }
}

void AsyncMysqlClient::logQueryFailure(db::FailureReason reason,
                                       Duration duration,
                                       db::QueryType type,
                                       int queries_executed,
                                       const folly::fbstring& query,
                                       const Connection& conn) {
  CHECK_EQ(threadId(), std::this_thread::get_id());
  stats()->incrFailedQueries();
  if (db_logger_) {
    db_logger_->logQueryFailure(
        reason,
        duration,
        type,
        queries_executed,
        query.toStdString(),
        conn.mysql(),
        makeSquangleLoggingData(conn.getKey(), conn.getConnectionContext()));
  }
}

void AsyncMysqlClient::logConnectionSuccess(
    Duration duration,
    const ConnectionKey& conn_key,
    const db::ConnectionContextBase* connection_context) {
  CHECK_EQ(threadId(), std::this_thread::get_id());
  if (db_logger_) {
    db_logger_->logConnectionSuccess(
        duration, makeSquangleLoggingData(&conn_key, connection_context));
  }
}

void AsyncMysqlClient::logConnectionFailure(
    db::FailureReason reason,
    Duration duration,
    const ConnectionKey& conn_key,
    MYSQL* mysql,
    const db::ConnectionContextBase* connection_context) {
  CHECK_EQ(threadId(), std::this_thread::get_id());
  stats()->incrFailedConnections();
  if (db_logger_) {
    db_logger_->logConnectionFailure(
        reason,
        duration,
        mysql,
        makeSquangleLoggingData(&conn_key, connection_context));
  }
}

db::SquangleLoggingData AsyncMysqlClient::makeSquangleLoggingData(
    const ConnectionKey* connKey,
    const db::ConnectionContextBase* connContext) {
  db::SquangleLoggingData ret(connKey, connContext);
  ret.clientPerfStats = collectPerfStats();
  return ret;
}

void AsyncMysqlClient::cleanupCompletedOperations() {
  std::unique_lock<std::mutex> lock(pending_operations_mutex_);
  size_t num_erased = 0, before = pending_operations_.size();

  VLOG(11) << "removing pending operations";
  for (auto& op : operations_to_remove_) {
    if (pending_operations_.erase(op) > 0) {
      ++num_erased;
    } else {
      LOG(DFATAL) << "asked to remove non-pending operation";
    }
  }

  operations_to_remove_.clear();

  VLOG(11) << "erased: " << num_erased << ", before: " << before
           << ", after: " << pending_operations_.size();
}

folly::Future<ConnectResult> AsyncMysqlClient::connectFuture(
    const string& host,
    int port,
    const string& database_name,
    const string& user,
    const string& password,
    const ConnectionOptions& conn_opts) {

  return toFuture(beginConnection(host, port, database_name, user, password)
                      ->setConnectionOptions(conn_opts));
}

std::unique_ptr<Connection> AsyncMysqlClient::connect(
    const string& host,
    int port,
    const string& database_name,
    const string& user,
    const string& password,
    const ConnectionOptions& conn_opts) {
  auto op = beginConnection(host, port, database_name, user, password);
  op->setConnectionOptions(conn_opts);
  // This will throw (intended behavour) in case the operation didn't succeed
  auto conn = blockingConnectHelper(op);
  return conn;
}

std::shared_ptr<ConnectOperation> AsyncMysqlClient::beginConnection(
    const string& host,
    int port,
    const string& database_name,
    const string& user,
    const string& password) {
  return beginConnection(
      ConnectionKey(host, port, database_name, user, password));
}

std::shared_ptr<ConnectOperation> AsyncMysqlClient::beginConnection(
    ConnectionKey conn_key) {
  auto ret = std::make_shared<ConnectOperation>(this, conn_key);
  addOperation(ret);
  return ret;
}

std::unique_ptr<Connection> AsyncMysqlClient::adoptConnection(
    MYSQL* raw_conn,
    const string& host,
    int port,
    const string& database_name,
    const string& user,
    const string& password) {
  CHECK_THROW(raw_conn->async_op_status == ASYNC_OP_UNSET,
              InvalidConnectionException);
  auto conn = folly::make_unique<Connection>(
      this, ConnectionKey(host, port, database_name, user, password), raw_conn);
  conn->associateWithClientThread();
  conn->socketHandler()->changeHandlerFD(mysql_get_file_descriptor(raw_conn));
  return conn;
}

Connection::Connection(AsyncMysqlClient* async_client,
                       ConnectionKey conn_key,
                       MYSQL* existing_connection)
    : conn_key_(conn_key),
      async_client_(async_client),
      socket_handler_(async_client_->getEventBase()),
      initialized_(false) {
  if (existing_connection) {
    mysql_connection_ = folly::make_unique<MysqlConnectionHolder>(
        async_client_, existing_connection, conn_key);
  }
}

void Connection::associateWithClientThread() {
  CHECK_EQ(mysql_operation_thread_id_, std::thread::id());
  mysql_operation_thread_id_ = async_client_->threadId();
  initialized_ = true;
}

void Connection::initMysqlOnly() {
  // Thread must be already associated
  CHECK_EQ(mysql_operation_thread_id_, std::this_thread::get_id());
  CHECK_THROW(mysql_connection_ == nullptr, InvalidConnectionException);
  mysql_connection_ = folly::make_unique<MysqlConnectionHolder>(
      async_client_, mysql_init(nullptr), conn_key_);
  mysql_connection_->mysql()->options.client_flag &= ~CLIENT_LOCAL_FILES;
}

void Connection::initialize() {
  associateWithClientThread();
  initMysqlOnly();
}

Connection::~Connection() {
  if (mysql_connection_ && conn_dying_callback_) {
    // Recycle connection, if not needed the client will throw it away
    conn_dying_callback_(std::move(mysql_connection_));
  }
}

template <>
std::shared_ptr<QueryOperation> Connection::beginQuery(
    std::unique_ptr<Connection> conn, Query&& query) {
  return beginAnyQuery<QueryOperation>(
    folly::make_unique<Operation::OwnedConnection>(std::move(conn)),
    std::move(query));
}

template <>
std::shared_ptr<MultiQueryOperation> Connection::beginMultiQuery(
    std::unique_ptr<Connection> conn, std::vector<Query>&& queries) {
  return beginAnyQuery<MultiQueryOperation>(
      folly::make_unique<Operation::OwnedConnection>(std::move(conn)),
      std::move(queries));
}

template <typename QueryType, typename QueryArg>
std::shared_ptr<QueryType> Connection::beginAnyQuery(
    std::unique_ptr<Operation::ConnectionProxy> conn_ptr, QueryArg&& query) {
  CHECK_THROW(conn_ptr->get(), InvalidConnectionException);
  CHECK_THROW(conn_ptr->get()->ok(), InvalidConnectionException);
  conn_ptr->get()->checkOperationInProgress();
  auto ret = std::make_shared<QueryType>(
      std::move(conn_ptr),
      std::move(query));
  Duration timeout = ret->connection()->default_query_timeout_;
  if (timeout.count() > 0) {
    ret->setTimeout(timeout);
  }

  ret->connection()->async_client_->addOperation(ret);
  ret->connection()->socket_handler_.setOperation(ret.get());
  return ret;
}

// A query might already be semicolon-separated, so we allow this to
// be a MultiQuery.  Or it might just be one query; that's okay, too.
template <>
std::shared_ptr<MultiQueryOperation> Connection::beginMultiQuery(
    std::unique_ptr<Connection> conn, Query&& query) {
  return Connection::beginMultiQuery(std::move(conn),
                                     std::vector<Query>{std::move(query)});
}

template <>
folly::Future<DbQueryResult> Connection::queryFuture(
    std::unique_ptr<Connection> conn, Query&& query) {
  auto op = beginQuery(std::move(conn), std::move(query));
  return toFuture(op);
}

template <typename... Args>
folly::Future<DbMultiQueryResult> Connection::multiQueryFuture(
    std::unique_ptr<Connection> conn, Args&&... args) {
  auto op = beginMultiQuery(std::move(conn), std::forward<Args>(args)...);
  return toFuture(op);
}

template <>
folly::Future<DbMultiQueryResult> Connection::multiQueryFuture(
    std::unique_ptr<Connection> conn, Query&& args) {
  auto op = beginMultiQuery(std::move(conn), std::move(args));
  return toFuture(op);
}

template <>
folly::Future<DbMultiQueryResult> Connection::multiQueryFuture(
        std::unique_ptr<Connection> conn, vector<Query>&& args) {
  auto op = beginMultiQuery(std::move(conn), std::move(args));
  return toFuture(op);
}


template <>
DbQueryResult Connection::query(Query&& query) {
  auto op = beginAnyQuery<QueryOperation>(
      folly::make_unique<Operation::ReferencedConnection>(this),
      std::move(query));
  folly::ScopeGuard guard =
      folly::makeGuard([&] { sync_operation_in_progress_ = false; });
  sync_operation_in_progress_ = true;

  op->run()->wait();

  if (!op->ok()) {
    throw QueryException(op->numQueriesExecuted(),
                         op->result(),
                         op->mysql_errno(),
                         op->mysql_error(),
                         *getKey(),
                         op->elapsed());
  }
  auto conn_key = *op->connection()->getKey();
  DbQueryResult result(std::move(op->stealQueryResult()),
                       op->numQueriesExecuted(),
                       nullptr,
                       op->result(),
                       conn_key,
                       op->elapsed());
  return result;
}

template <>
DbMultiQueryResult Connection::multiQuery(std::vector<Query>&& queries) {
  auto op = beginAnyQuery<MultiQueryOperation>(
      folly::make_unique<Operation::ReferencedConnection>(this),
      std::move(queries));
  folly::ScopeGuard guard =
      folly::makeGuard([&] { sync_operation_in_progress_ = false; });

  sync_operation_in_progress_ = true;
  op->run()->wait();

  if (!op->ok()) {
    throw QueryException(op->numQueriesExecuted(),
                         op->result(),
                         op->mysql_errno(),
                         op->mysql_error(),
                         *getKey(),
                         op->elapsed());
  }

  auto conn_key = *op->connection()->getKey();
  DbMultiQueryResult result(std::move(op->stealQueryResults()),
                            op->numQueriesExecuted(),
                            nullptr,
                            op->result(),
                            conn_key,
                            op->elapsed());
  return result;
}

template <>
DbMultiQueryResult Connection::multiQuery(Query&& query) {
  return multiQuery(std::vector<Query>{std::move(query)});
}

template <typename... Args>
DbMultiQueryResult Connection::multiQuery(Args&&... args) {
  return multiQuery(std::vector<Query>{std::forward<Args>(args)...});
}

std::shared_ptr<QueryOperation> Connection::beginTransaction(
    std::unique_ptr<Connection> conn) {
  return beginQuery(std::move(conn), "BEGIN");
}

std::shared_ptr<QueryOperation> Connection::commitTransaction(
    std::unique_ptr<Connection> conn) {
  return beginQuery(std::move(conn), "COMMIT");
}

std::shared_ptr<QueryOperation> Connection::rollbackTransaction(
    std::unique_ptr<Connection> conn) {
  return beginQuery(std::move(conn), "ROLLBACK");
}

std::shared_ptr<QueryOperation> Connection::beginTransaction(
    std::shared_ptr<QueryOperation>& op) {
  return beginQuery(op, "BEGIN");
}

std::shared_ptr<QueryOperation> Connection::commitTransaction(
    std::shared_ptr<QueryOperation>& op) {
  return beginQuery(op, "COMMIT");
}

std::shared_ptr<QueryOperation> Connection::rollbackTransaction(
    std::shared_ptr<QueryOperation>& op) {
  return beginQuery(op, "ROLLBACK");
}

ConnectionSocketHandler::ConnectionSocketHandler(folly::EventBase* base)
    : EventHandler(base), AsyncTimeout(base), op_(nullptr) {}

void ConnectionSocketHandler::timeoutExpired() noexcept {
  op_->timeoutTriggered();
}

void ConnectionSocketHandler::handlerReady(uint16_t events) noexcept {
  CHECK_EQ(std::this_thread::get_id(), op_->async_client()->threadId());
  CHECK_THROW(op_->state_ != OperationState::Completed &&
                  op_->state_ != OperationState::Unstarted,
              OperationStateException);

  if (op_->state() == OperationState::Cancelling) {
    op_->cancel();
  } else {
    op_->socketActionable();
  }
}
}
}
} // namespace facebook::common::mysql_client
