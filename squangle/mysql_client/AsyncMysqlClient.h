/*
 *  Copyright (c) 2016, Facebook, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */
//
// The Asynchronous MySQL Client, a high-performance, nonblocking
// client for MySQL.
//
// This client exposes a fully asynchronous MySQL interface.  With it,
// you can connect and run queries simultaneously across multiple
// databases without creating threads.
//
// The interface itself is split across multiple classes:
//
// AsyncMysqlClient - the client itself.  This client manages connections
//   to *multiple* databases.  In general, one needs only one client,
//   regardless of the number of databases connected to.  When in doubt,
//   simply use AsyncMysqlClient::defaultClient rather than constructing
//   your own.  All methods of AsyncMysqlClient are thread safe; however,
//   resulting Operations should not be shared across threads.
//
// Connection - a representation of a living, active MySQL connection.
//   Returned by a successful ConnectOperation (see below).
//
// Operation / ConnectOperation / QueryOperation / MultiQueryOperation
//   - these are the  primary ways of interacting with MySQL databases.
//   Operations represent a pending or completed MySQL action such as
//   connecting or performing a query.  Operations are returned when
//   queries or connections are begun, and can be waited for.  Alternatively,
//   callbacks can be associated with operations.
//
// QueryResult - holds the result data of a query and provides simple ways to
//   to process it.
//
// RowBlock - this is the buffer rows are returned in.  Rather than a
//   row at a time, data from MySQL comes in blocks.  RowBlock is an
//   efficient representation of this, and exposes methods to interact
//   with the contained rows and columns.
//
// For more detail and examples, please see the README file.

#ifndef COMMON_ASYNC_MYSQL_CLIENT_H
#define COMMON_ASYNC_MYSQL_CLIENT_H

#include "squangle/mysql_client/Operation.h"
#include "squangle/mysql_client/Row.h"
#include "squangle/mysql_client/Query.h"
#include "squangle/mysql_client/DbResult.h"
#include "squangle/mysql_client/Connection.h"
#include "squangle/logger/DBEventCounter.h"
#include "squangle/logger/DBEventLogger.h"

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>

#include <folly/Exception.h>
#include <folly/fibers/Baton.h>
#include <folly/futures/Future.h>
#include <folly/io/async/EventBase.h>
#include <folly/Portability.h>
#include <folly/Singleton.h>
#include <wangle/client/ssl/SSLSession.h>

namespace facebook {
namespace common {
namespace mysql_client {

using std::string;
using std::unordered_map;
using facebook::db::InvalidConnectionException;

class AsyncMysqlClient;
class Operation;
class ConnectOperation;
class ConnectionKey;
class MysqlConnectionHolder;

typedef std::function<void(std::unique_ptr<MysqlConnectionHolder>)>
ConnectionDyingCallback;

// The client itself.  As mentioned above, in general, it isn't
// necessary to create a client; instead, simply call defaultClient()
// and use the client it returns, which is shared process-wide.
class AsyncMysqlClient {
 public:
  AsyncMysqlClient();
  virtual ~AsyncMysqlClient();

  static void deleter(AsyncMysqlClient* client) {
    // If we are dying in the thread we own, spin up a new thread to
    // call delete. This allows the asyncmysql thread to terminate safely
    if (std::this_thread::get_id() == client->threadId()) {
      std::thread myThread{[client]() { delete client; }};
      myThread.detach();
    } else {
      delete client;
    }
  }

  static std::shared_ptr<AsyncMysqlClient> defaultClient();

  // Initiate a connection to a database.  This is the main entrypoint.
  std::shared_ptr<ConnectOperation> beginConnection(const string& host,
                                                    int port,
                                                    const string& database_name,
                                                    const string& user,
                                                    const string& password);

  std::shared_ptr<ConnectOperation> beginConnection(ConnectionKey conn_key);

  std::unique_ptr<Connection> adoptConnection(MYSQL* conn,
                                              const string& host,
                                              int port,
                                              const string& database_name,
                                              const string& user,
                                              const string& password);

  folly::Future<ConnectResult> connectFuture(
      const string& host,
      int port,
      const string& database_name,
      const string& user,
      const string& password,
      const ConnectionOptions& conn_opts = ConnectionOptions());

  // Synchronous call to acquire a connection, the caller thread will be blocked
  // until the operation has finished.
  // In case the we fail to acquire the connection, MysqlException will be
  // thrown.
  std::unique_ptr<Connection> connect(
      const string& host,
      int port,
      const string& database_name,
      const string& user,
      const string& password,
      const ConnectionOptions& conn_opts = ConnectionOptions());

  // Stop accepting new queries and connections.
  void blockIncomingOperations() {
    std::unique_lock<std::mutex> l(pending_operations_mutex_);
    block_operations_ = true;
  }

  // Do not call this from inside the AsyncMysql thread
  void shutdownClient();

  // Drain any remaining operations.  If also_block_operations is true, then
  // any attempt to add operations during or after this drain will
  // fail harshly.
  void drain(bool also_block_operations);

  folly::EventBase* getEventBase() { return &tevent_base_; }

  const std::thread::id threadId() const { return thread_.get_id(); }

  // For testing only; trigger a delicate connection failure to enable testing
  // of an error codepath.
  void triggerDelicateConnectionFailures() {
    delicate_connection_failure_ = true;
  }

  void logQuerySuccess(
      db::OperationType operation_name,
      Duration dur,
      int queries_executed,
      const folly::fbstring& query,
      const Connection& conn);

  void logQueryFailure(
      db::OperationType operation_name,
      db::FailureReason reason,
      Duration duration,
      int queries_executed,
      const folly::fbstring& query,
      const Connection& conn);

  void logConnectionSuccess(
      db::OperationType operation_name,
      Duration dur,
      const ConnectionKey& conn_key,
      const db::ConnectionContextBase* extra_logging_data);

  void logConnectionFailure(
      db::OperationType operation_name,
      db::FailureReason reason,
      Duration dur,
      const ConnectionKey& conn_key,
      MYSQL* mysql,
      const db::ConnectionContextBase* extra_logging_data);

  db::SquangleLoggerBase* dbLogger() { return db_logger_.get(); }
  db::DBCounterBase* stats() { return client_stats_.get(); }

  db::SquangleLoggingData makeSquangleLoggingData(
      const ConnectionKey* connKey,
      const db::ConnectionContextBase* connContext);

  // For internal use only
  void setDBLoggerForTesting(std::unique_ptr<db::SquangleLoggerBase> dbLogger);
  void setDBCounterForTesting(std::unique_ptr<db::DBCounterBase> dbCounter);

  void setPoolsConnectionLimit(uint64_t limit) {
    pools_conn_limit_.store(limit, std::memory_order_relaxed);
  }

  uint64_t getPoolsConnectionLimit() {
    return pools_conn_limit_.load(std::memory_order_relaxed);
  }

  db::ClientPerfStats collectPerfStats() {
    db::ClientPerfStats ret;
    ret.callbackDelayMicrosAvg = callbackDelayAvg_.value();
    ret.ioEventLoopMicrosAvg = tevent_base_.getAvgLoopTime();
    return ret;
  }

 protected:
  AsyncMysqlClient(std::unique_ptr<db::SquangleLoggerBase> db_logger,
                   std::unique_ptr<db::DBCounterBase> db_stats);

 private:
  // Private methods, primarily used by Operations and its subclasses.
  friend class Connection;
  friend class Operation;
  friend class ConnectOperation;
  friend class ConnectPoolOperation;
  friend class FetchOperation;
  friend class MysqlConnectionHolder;
  friend class AsyncConnectionPool;

  void init();

  bool runInThread(const folly::Cob& fn);

  // Gives the number of connections being created (started) and the ones that
  // are already open for a ConnectionKey
  uint32_t numStartedAndOpenConnections(const ConnectionKey* conn_key) {
    std::unique_lock<std::mutex> l(counters_mutex_);
    return connection_references_[*conn_key];
  }

  // Similar to the above function, but returns the total number of connections
  // being and already opened.
  uint32_t numStartedAndOpenConnections() {
    std::unique_lock<std::mutex> l(counters_mutex_);
    return active_connection_counter_;
  }

  void activeConnectionAdded(const ConnectionKey* key) {
    std::unique_lock<std::mutex> l(counters_mutex_);
    ++active_connection_counter_;
    ++connection_references_[*key];
  }

  // Called in MysqlConnectionHolder and ConnectOperation. The ref count should
  // be in incremented when a connection exists or is about to exist.
  // ConnectOperation decrements it when the connection is acquired.
  // MysqlConnectionHolder is counted during its lifetime.
  void activeConnectionRemoved(const ConnectionKey* key) {
    std::unique_lock<std::mutex> l(counters_mutex_);
    // Sanity check, if the old value was 0, then the counter overflowed
    DCHECK(active_connection_counter_ != 0);
    --active_connection_counter_;
    if (active_connection_counter_ == 0) {
      active_connections_closed_cv_.notify_one();
    }

    auto ref_iter = connection_references_.find(*key);
    DCHECK(ref_iter != connection_references_.end());

    if (--ref_iter->second == 0) {
      connection_references_.erase(ref_iter);
    }
  }

  // Add a pending operation to the client.
  void addOperation(std::shared_ptr<Operation> op) {
    std::unique_lock<std::mutex> l(pending_operations_mutex_);
    if (block_operations_) {
      LOG(ERROR) << "Attempt to start operation when client is shutting down";
      op->cancel();
    }
    pending_operations_.insert(op);
  }

  // We remove operations from pending_operations_ after an iteration
  // of the event loop to ensure we don't delete an object that is
  // executing one of its methods (ie handling an event or cancel
  // call).
  void deferRemoveOperation(Operation* op) {
    std::unique_lock<std::mutex> l(pending_operations_mutex_);
    // If the queue to remove is empty, schedule a cleanup to occur after
    // this pass through the event loop.
    if (operations_to_remove_.empty()) {
      if (!runInThread([this]() { cleanupCompletedOperations(); })) {
        LOG(DFATAL) << "Operation could not be cleaned: error in folly::EventBase";
      }
    }
    operations_to_remove_.push_back(op->getSharedPointer());
  }

  void cleanupCompletedOperations();

  // thread_ is where loop() runs and most of the class does its work.
  std::thread thread_;

  // pending_operations_mutex_ protects pending_operations_ and
  // blockIncomingOperations(),
  // this mutex is meant for external operations on the client. For example,
  // when the user wants to begin an operation.
  std::mutex pending_operations_mutex_;

  // The client must keep a reference (via a shared_ptr) to any active
  // Operation as the op's creator may have released their reference.
  // We do this via a map of shared_ptr's, where the keys are raw
  // pointers.
  std::unordered_set<std::shared_ptr<Operation>> pending_operations_;

  // See comment for deferRemoveOperation.
  std::vector<std::shared_ptr<Operation>> operations_to_remove_;

  // Our event loop.
  folly::EventBase tevent_base_;

  // Are we accepting new connections
  bool block_operations_ = false;
  // Used to guard thread destruction
  std::atomic<bool> is_shutdown_{false};

  // We count the number of references we have from Connections and
  // ConnectionOperations.  This is used for draining and destruction;
  // ~AsyncMysqlClient blocks until this value becomes zero.
  uint32_t active_connection_counter_ = 0;
  unordered_map<ConnectionKey, uint32_t> connection_references_;
  // Protects the look ups and writes to both counters
  std::mutex counters_mutex_;
  std::condition_variable active_connections_closed_cv_;

  // For testing purposes
  bool delicate_connection_failure_ = false;

  // Using unique pointer due inheritance virtual calls
  std::unique_ptr<db::SquangleLoggerBase> db_logger_;
  std::unique_ptr<db::DBCounterBase> client_stats_;

  // This only works if you are using AsyncConnectionPool
  std::atomic<uint64_t> pools_conn_limit_;

  // Average time between a callback being scheduled in the IO Thread and the
  // time it runs
  db::ExponentialMovingAverage callbackDelayAvg_{1.0 / 16.0};

  AsyncMysqlClient(const AsyncMysqlClient&) = delete;
  AsyncMysqlClient& operator=(const AsyncMysqlClient&) = delete;
};

// A helper class to interface with the EventBase.  Each connection
// has an instance of this class and this class is what is invoked
// when sockets become readable/writable or when a timeout occurs.
// This is a separate class to avoid polluting the class hierarchy.
class ConnectionSocketHandler : public folly::EventHandler,
                                public folly::AsyncTimeout {
 public:
  explicit ConnectionSocketHandler(folly::EventBase* base);
  virtual void timeoutExpired() noexcept;
  void handlerReady(uint16_t events) noexcept;
  void setOperation(Operation* op) { op_ = op; }

 private:
  Operation* op_;

  ConnectionSocketHandler() = delete;
  ConnectionSocketHandler(const ConnectionSocketHandler&) = delete;
};

// Connection is a thin wrapper around a MYSQL object, associating it
// with an AsyncMysqlClient.  Its primary purpose is to manage that
// connection and initiate queries.
//
// It also holds a notification descriptor, used across queries, to
// signal their completion.  Operation::wait blocks on this fd.
class Connection {
 public:
  Connection(AsyncMysqlClient* async_client,
             ConnectionKey conn_key,
             MYSQL* existing_connection);

  ~Connection();

  // Like beginConnection, this is how you start a query.  Note that
  // ownership of the Connection is passed into this function; the
  // returned QueryOperation allows access to it (once the query
  // completes).  This is a limitation of MySQL as you cannot perform
  // operations while a query is in progress.  We use unique_ptr to
  // represent this connection-level statefulness.
  //
  // To run subsequent queries, after query_op->wait() returns, you
  // can call query_op->releaseConnection() to retrieve the connection
  // itself and run further queries.
  //
  // The query itself is constructed from args....  If args... is a single
  // Query object, it is used directly; otherwise a Query object is
  // constructed via Query(args...) and that is used for the query.
  template <typename... Args>
  static std::shared_ptr<QueryOperation> beginQuery(
      std::unique_ptr<Connection> conn, Args&&... args);

  template <typename... Args>
  static std::shared_ptr<MultiQueryOperation> beginMultiQuery(
      std::unique_ptr<Connection> conn, Args&&... args);

  template <typename... Args>
  static folly::Future<DbQueryResult> queryFuture(
      std::unique_ptr<Connection> conn, Args&&... args);

  template <typename... Args>
  static folly::Future<DbMultiQueryResult> multiQueryFuture(
      std::unique_ptr<Connection> conn, Args&&... args);

  // An alternate interface that allows for easier re-use of an
  // existing query_op, moving the Connection from the old op and into
  // the new one.  See details above for what args... are.
  template <typename... Args>
  static std::shared_ptr<QueryOperation> beginQuery(
      std::shared_ptr<QueryOperation>& op, Args&&... args) {
    CHECK_THROW(op->done(), OperationStateException);
    auto conn = std::move(op->releaseConnection());
    op = beginQuery(std::move(conn), std::forward<Args>(args)...);
    return op;
  }

  // Experimental
  template <typename... Args>
  static std::shared_ptr<MultiQueryStreamOperation> beginMultiQueryStreaming(
      std::unique_ptr<Connection> conn,
      Args&&... args);

  // Synchronous calls
  template <typename... Args>
  DbQueryResult query(Args&&... args);

  template <typename... Args>
  DbMultiQueryResult multiQuery(Args&&... args);

  // EXPERIMENTAL

  // StreamResultHandler
  static std::unique_ptr<MultiQueryStreamHandler> streamMultiQuery(
      std::unique_ptr<Connection> connection,
      std::vector<Query> queries);

  // variant that takes a QueryOperation for more convenient chaining of
  // queries.
  //
  // These return QueryOperations that are used to verify success or
  // failure.
  static std::shared_ptr<QueryOperation> beginTransaction(
      std::unique_ptr<Connection> conn);
  static std::shared_ptr<QueryOperation> rollbackTransaction(
      std::unique_ptr<Connection> conn);
  static std::shared_ptr<QueryOperation> commitTransaction(
      std::unique_ptr<Connection> conn);

  static std::shared_ptr<QueryOperation> beginTransaction(
      std::shared_ptr<QueryOperation>& op);
  static std::shared_ptr<QueryOperation> rollbackTransaction(
      std::shared_ptr<QueryOperation>& op);
  static std::shared_ptr<QueryOperation> commitTransaction(
      std::shared_ptr<QueryOperation>& op);

  // It's going to make the association with the client thread to avoid mysql
  // operations happening in a different thread.
  void associateWithClientThread();

  // Called in the libevent thread to create the MYSQL* client.
  void initMysqlOnly();
  void initialize();

  bool hasInitialized() const { return initialized_; }

  bool ok() const { return mysql_connection_ != nullptr; }

  void close() {
    if (mysql_connection_) {
      mysql_connection_.reset();
    }
  }

  // Default timeout for queries created by this client.
  void setDefaultQueryTimeout(Duration t) { conn_options_.setQueryTimeout(t); }
  // TODO #9834064
  void setQueryTimeout(Duration t) { conn_options_.setQueryTimeout(t); }

  // set last successful query time to MysqlConnectionHolder
  void setLastActivityTime(Timepoint last_activity_time) {
    CHECK_THROW(mysql_connection_ != nullptr, InvalidConnectionException);
    mysql_connection_->setLastActivityTime(last_activity_time);
  }

  Timepoint getLastActivityTime() const {
    CHECK_THROW(mysql_connection_ != nullptr, InvalidConnectionException);
    return mysql_connection_->getLastActivityTime();
  }

  // Returns the MySQL server version. If the connection has been closed
  // an error is generated.
  const string serverInfo() const {
    CHECK_THROW(mysql_connection_ != nullptr, InvalidConnectionException);
    auto ret = mysql_get_server_info(mysql_connection_->mysql());
    return string(ret);
  }

  // Returns whether or not the SSL session was reused from a previous
  // connection.
  // If the connection isn't SSL, it will return false as well.
  bool sslSessionReused() const {
    CHECK_THROW(mysql_connection_ != nullptr, InvalidConnectionException);
    return mysql_get_ssl_session_reused(mysql_connection_->mysql());
  }

  // Checks if `client_flag` is set for SSL.
  bool isSSL() const;

  // Escape the provided string using mysql_real_escape_string(). You almost
  // certainly don't want to use this - look at the Query class instead.
  //
  // This is provided so that non-Facebook users of the HHVM extension have
  // a familiar API.
  const string escapeString(const string& unescaped) {
    CHECK_THROW(mysql_connection_ != nullptr, InvalidConnectionException);
    return Query::escapeString(mysql_connection_->mysql(), unescaped);
  }

  // Returns the number of errors, warnings, and notes generated during
  // execution of the previous SQL statement
  const int warningCount() const {
    CHECK_THROW(mysql_connection_ != nullptr, InvalidConnectionException);
    return mysql_warning_count(mysql_connection_->mysql());
  }

  const string& host() const { return conn_key_.host; }
  int port() const { return conn_key_.port; }
  const string& user() const { return conn_key_.user; }
  const string& database() const { return conn_key_.db_name; }
  const string& password() const { return conn_key_.password; }

  AsyncMysqlClient* client() const { return async_client_; }

  MYSQL* stealMysql() {
    if (mysql_connection_) {
      auto ret = mysql_connection_->stealMysql();
      mysql_connection_.reset();
      return ret;
    } else {
      return nullptr;
    }
  }

  MysqlConnectionHolder* mysql_for_testing_only() const {
    return mysql_connection_.get();
  }

  std::unique_ptr<MysqlConnectionHolder> stealMysqlConnectionHolder() {
    CHECK_EQ(mysql_operation_thread_id_, std::this_thread::get_id());
    return std::move(mysql_connection_);
  }

  const ConnectionKey* getKey() const { return &conn_key_; }

  void setReusable(bool reusable) {
    if (mysql_connection_) {
      mysql_connection_->setReusable(reusable);
    }
  }

  bool isReusable() {
    if (mysql_connection_) {
      return mysql_connection_->isReusable();
    }
    return false;
  }

  const ConnectionOptions& getConnectionOptions() const {
    return conn_options_;
  }

  void setConnectionOptions(const ConnectionOptions& conn_options) {
    conn_options_ = conn_options;
  }

  void setConnectionDyingCallback(ConnectionDyingCallback callback) {
    conn_dying_callback_ = callback;
  }

 private:
  // Methods primarily invoked by Operations and AsyncMysqlClient.
  friend class AsyncMysqlClient;
  friend class Operation;
  friend class ConnectOperation;
  friend class ConnectPoolOperation;
  friend class FetchOperation;
  friend class QueryOperation;
  friend class MultiQueryOperation;
  friend class MultiQueryStreamOperation;

  ConnectionSocketHandler* socketHandler() { return &socket_handler_; }

  MYSQL* mysql() const {
    CHECK_EQ(mysql_operation_thread_id_, std::this_thread::get_id());
    if (mysql_connection_) {
      return mysql_connection_->mysql();
    } else {
      return nullptr;
    }
  }

  MysqlConnectionHolder* mysqlConnection() const {
    CHECK_EQ(mysql_operation_thread_id_, std::this_thread::get_id());
    return mysql_connection_.get();
  }

  void setMysqlConnectionHolder(
      std::unique_ptr<MysqlConnectionHolder> mysql_connection) {
    CHECK_THROW(mysql_connection_ == nullptr, InvalidConnectionException);
    CHECK_THROW(conn_key_ == *mysql_connection->getKey(),
                InvalidConnectionException);
    mysql_connection_ = std::move(mysql_connection);
  }

  // Operations call these methods as the operation becomes unblocked, as
  // callers want to wait for completion, etc.
  void notify() {
    if (actionableBaton_.try_wait()) {
      LOG(DFATAL) << "asked to notify already-actionable operation";
    }
    actionableBaton_.post();
  }

  void wait() {
    actionableBaton_.wait();
  }

  // Called when a new operation is being started.
  void resetActionable() {
    actionableBaton_.reset();
  }

  // Helper function that will begin multiqueries or single queries depending
  // on the specified in the templates. Being used to avoid duplicated code
  // that both need to do.
  template <typename QueryType, typename QueryArg>
  static std::shared_ptr<QueryType> beginAnyQuery(
      std::unique_ptr<Operation::ConnectionProxy> conn_ptr, QueryArg&& query);

  void checkOperationInProgress() {
    if (sync_operation_in_progress_) {
      throw InvalidConnectionException(
          "Attempting to run parallel queries in same connection");
    }
  }

  void setConnectionContext(std::unique_ptr<db::ConnectionContextBase>&& e) {
    connection_context_ = std::move(e);
  }

  const db::ConnectionContextBase* getConnectionContext() const {
    return connection_context_.get();
  }

  std::unique_ptr<MysqlConnectionHolder> mysql_connection_;

  ConnectionKey conn_key_;
  ConnectionOptions conn_options_;
  std::thread::id mysql_operation_thread_id_;

  // Context information for logging purposes.
  std::unique_ptr<db::ConnectionContextBase> connection_context_;

  // Unowned pointer to the client we're from.
  AsyncMysqlClient* async_client_;

  ConnectionSocketHandler socket_handler_;

  folly::fibers::Baton actionableBaton_;

  ConnectionDyingCallback conn_dying_callback_;

  bool initialized_;

  // Used for signing that the connection is being used in a synchronous call,
  // eg. `query`. MySQL doesn't allow more than one query being made through
  // the same connection at the same time. So same logic goes here.
  // We don't track for async calls, for async calls the unique Connection
  // gets moved to the operation, so the protection is guaranteed.
  bool sync_operation_in_progress_ = false;

  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;
};

template <>
DbQueryResult Connection::query(Query&& query);

template <typename... Args>
DbQueryResult Connection::query(Args&&... args) {
  Query query_obj{std::forward<Args>(args)...};
  return query(std::move(query_obj));
}

template <>
std::shared_ptr<QueryOperation> Connection::beginQuery(
    std::unique_ptr<Connection> conn, Query&& query);

template <typename... Args>
std::shared_ptr<QueryOperation> Connection::beginQuery(
    std::unique_ptr<Connection> conn, Args&&... args) {
  Query query{std::forward<Args>(args)...};
  return beginQuery(std::move(conn), std::move(query));
}

template <>
folly::Future<DbQueryResult> Connection::queryFuture(
  std::unique_ptr<Connection> conn, Query&& query);

template <typename... Args>
folly::Future<DbQueryResult> Connection::queryFuture(
    std::unique_ptr<Connection> conn, Args&&... args) {
  Query query{std::forward<Args>(args)...};
  return queryFuture(std::move(conn), std::move(query));
}

}
}
} // facebook::common::mysql_client

#endif // COMMON_ASYNC_MYSQL_CLIENT_H
