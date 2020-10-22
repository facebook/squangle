/*
 *  Copyright (c) Facebook, Inc. and its affiliates..
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree.
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

#include "squangle/logger/DBEventCounter.h"
#include "squangle/logger/DBEventLogger.h"
#include "squangle/mysql_client/Connection.h"
#include "squangle/mysql_client/DbResult.h"
#include "squangle/mysql_client/Operation.h"
#include "squangle/mysql_client/Query.h"
#include "squangle/mysql_client/Row.h"

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
#include <folly/Portability.h>
#include <folly/Singleton.h>
#include <folly/fibers/Baton.h>
#include <folly/futures/Future.h>
#include <folly/io/async/EventBase.h>
#include <folly/ssl/OpenSSLPtrTypes.h>

namespace facebook {
namespace common {
namespace mysql_client {

using facebook::db::InvalidConnectionException;
using std::string;
using std::unordered_map;

class AsyncMysqlClient;
class SyncMysqlClient;
class Operation;
class ConnectOperation;
class ConnectionKey;
class MysqlConnectionHolder;

typedef std::function<void(std::unique_ptr<MysqlConnectionHolder>)>
    ConnectionDyingCallback;

// MysqlHandler interface that is impletemented by the sync and async
// clients appropriately.
class MysqlHandler {
 public:
  enum Status {
    PENDING,
    DONE,
    ERROR,
  };
  virtual ~MysqlHandler() = default;
  virtual Status tryConnect(
      MYSQL* mysql,
      const ConnectionOptions& opts,
      const ConnectionKey& key,
      int flags) = 0;
  virtual Status runQuery(MYSQL* mysql, folly::StringPiece queryStmt) = 0;
  virtual MYSQL_RES* getResult(MYSQL* mysql) = 0;
  virtual Status nextResult(MYSQL* mysql) = 0;
  virtual Status fetchRow(MYSQL_RES* res, MYSQL_ROW& row) = 0;
};

class MysqlClientBase {
 public:
  virtual ~MysqlClientBase() = default;

  virtual std::unique_ptr<Connection> adoptConnection(
      MYSQL* conn,
      const string& host,
      int port,
      const string& database_name,
      const string& user,
      const string& password);

  // Initiate a connection to a database.  This is the main entrypoint.
  std::shared_ptr<ConnectOperation> beginConnection(
      const string& host,
      int port,
      const string& database_name,
      const string& user,
      const string& password);

  std::shared_ptr<ConnectOperation> beginConnection(ConnectionKey conn_key);

  // Factory method
  virtual std::unique_ptr<Connection> createConnection(
      ConnectionKey conn_key,
      MYSQL* mysql_conn) = 0;

  virtual folly::EventBase* getEventBase() {
    return nullptr;
  }

  void logQuerySuccess(
      const db::QueryLoggingData& logging_data,
      const Connection& conn);

  void logQueryFailure(
      const db::QueryLoggingData& logging_data,
      db::FailureReason reason,
      unsigned int mysqlErrno,
      const std::string& error,
      const Connection& conn);

  void logConnectionSuccess(
      const db::CommonLoggingData& logging_data,
      const ConnectionKey& conn_key,
      const db::ConnectionContextBase* extra_logging_data);

  void logConnectionFailure(
      const db::CommonLoggingData& logging_data,
      db::FailureReason reason,
      const ConnectionKey& conn_key,
      unsigned int mysqlErrno,
      const std::string& error,
      const db::ConnectionContextBase* extra_logging_data);

  db::DBCounterBase* stats() {
    return client_stats_.get();
  }
  db::SquangleLoggerBase* dbLogger() {
    return db_logger_.get();
  }

  void setConnectionCallback(ObserverCallback connection_cb) {
    if (connection_cb_) {
      auto old_cb = connection_cb_;
      connection_cb_ = [old_cb, connection_cb](Operation& op) {
        old_cb(op);
        connection_cb(op);
      };
    } else {
      connection_cb_ = connection_cb;
    }
  }

  explicit MysqlClientBase(
      std::unique_ptr<db::SquangleLoggerBase> db_logger = nullptr,
      std::unique_ptr<db::DBCounterBase> db_stats =
          std::make_unique<db::SimpleDbCounter>());

  virtual bool runInThread(folly::Cob&& fn) = 0;

 protected:
  friend class Connection;
  friend class Operation;
  friend class ConnectOperation;
  friend class ConnectPoolOperation;
  friend class FetchOperation;
  friend class MysqlConnectionHolder;
  friend class AsyncConnectionPool;
  virtual db::SquangleLoggingData makeSquangleLoggingData(
      const ConnectionKey* connKey,
      const db::ConnectionContextBase* connContext) = 0;

  virtual void activeConnectionAdded(const ConnectionKey* key) = 0;
  virtual void activeConnectionRemoved(const ConnectionKey* key) = 0;
  virtual void addOperation(std::shared_ptr<Operation> op) = 0;
  virtual void deferRemoveOperation(Operation* op) = 0;

  virtual MysqlHandler& getMysqlHandler() = 0;

  // Using unique pointer due inheritance virtual calls
  std::unique_ptr<db::SquangleLoggerBase> db_logger_;
  std::unique_ptr<db::DBCounterBase> client_stats_;
  ObserverCallback connection_cb_;
};

// The client itself.  As mentioned above, in general, it isn't
// necessary to create a client; instead, simply call defaultClient()
// and use the client it returns, which is shared process-wide.
class AsyncMysqlClient : public MysqlClientBase {
 public:
  AsyncMysqlClient();
  ~AsyncMysqlClient() override;

  std::unique_ptr<Connection> createConnection(
      ConnectionKey conn_key,
      MYSQL* mysql_conn) override;

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

  FOLLY_NODISCARD folly::SemiFuture<ConnectResult> connectSemiFuture(
      const string& host,
      int port,
      const string& database_name,
      const string& user,
      const string& password,
      const ConnectionOptions& conn_opts = ConnectionOptions());

  [[deprecated(
      "Replaced by the SemiFuture APIs, use SemiFutures and pass an executor")]] FOLLY_NODISCARD
      folly::Future<ConnectResult>
      connectFuture(
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
  FOLLY_NODISCARD std::unique_ptr<Connection> connect(
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

  folly::EventBase* getEventBase() override {
    return &event_base_;
  }

  const std::thread::id threadId() const {
    return thread_.get_id();
  }

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
    ret.callbackDelayMicrosAvg = stats_tracker_->callbackDelayAvg.value();
    ret.ioEventLoopMicrosAvg = getEventBase()->getAvgLoopTime();
    ret.notificationQueueSize = getEventBase()->getNotificationQueueSize();
    ret.ioThreadBusyTime = stats_tracker_->ioThreadBusyTime.value();
    ret.ioThreadIdleTime = stats_tracker_->ioThreadIdleTime.value();
    return ret;
  }

 protected:
  AsyncMysqlClient(
      std::unique_ptr<db::SquangleLoggerBase> db_logger,
      std::unique_ptr<db::DBCounterBase> db_stats);

  bool runInThread(folly::Cob&& fn) override;

  db::SquangleLoggingData makeSquangleLoggingData(
      const ConnectionKey* connKey,
      const db::ConnectionContextBase* connContext) override;

  void activeConnectionAdded(const ConnectionKey* key) override {
    std::unique_lock<std::mutex> l(counters_mutex_);
    ++active_connection_counter_;
    ++connection_references_[*key];
  }

  // Called in MysqlConnectionHolder and ConnectOperation. The ref count should
  // be in incremented when a connection exists or is about to exist.
  // ConnectOperation decrements it when the connection is acquired.
  // MysqlConnectionHolder is counted during its lifetime.
  void activeConnectionRemoved(const ConnectionKey* key) override {
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
  void addOperation(std::shared_ptr<Operation> op) override {
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
  void deferRemoveOperation(Operation* op) override {
    std::unique_lock<std::mutex> l(pending_operations_mutex_);
    // If the queue to remove is empty, schedule a cleanup to occur after
    // this pass through the event loop.
    if (operations_to_remove_.empty()) {
      if (!runInThread([this]() { cleanupCompletedOperations(); })) {
        LOG(DFATAL)
            << "Operation could not be cleaned: error in folly::EventBase";
      }
    }
    operations_to_remove_.push_back(op->getSharedPointer());
  }

 private:
  MysqlHandler& getMysqlHandler() override {
    return mysql_handler_;
  }

  // implementation of MysqlHandler interface
  class AsyncMysqlHandler : public MysqlHandler {
    Status tryConnect(
        MYSQL* mysql,
        const ConnectionOptions& /*opts*/,
        const ConnectionKey& conn_key,
        int flags) override;
    Status runQuery(MYSQL* mysql, folly::StringPiece queryStmt) override;
    Status nextResult(MYSQL* mysql) override;
    Status fetchRow(MYSQL_RES* res, MYSQL_ROW& row) override;
    MYSQL_RES* getResult(MYSQL* mysql) override;
  } mysql_handler_;

  // Private methods, primarily used by Operations and its subclasses.
  friend class AsyncConnectionPool;

  void init();

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

  void cleanupCompletedOperations();

  // event base running the event loop
  folly::EventBase event_base_;

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

  // This only works if you are using AsyncConnectionPool
  std::atomic<uint64_t> pools_conn_limit_;

  class StatsTracker : public folly::EventBaseObserver {
   public:
    void loopSample(int64_t busy_time, int64_t idle_time) override {
      ioThreadBusyTime.addSample(static_cast<double>(busy_time));
      ioThreadIdleTime.addSample(static_cast<double>(idle_time));
    }

    uint32_t getSampleRate() const override {
      // Avoids this being called every loop
      return 16;
    }

    // Average time between a callback being scheduled in the IO Thread and the
    // time it runs
    db::ExponentialMovingAverage callbackDelayAvg{1.0 / 16.0};
    db::ExponentialMovingAverage ioThreadBusyTime{1.0 / 16.0};
    db::ExponentialMovingAverage ioThreadIdleTime{1.0 / 16.0};
  };
  std::shared_ptr<StatsTracker> stats_tracker_;

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
  void timeoutExpired() noexcept override;
  void handlerReady(uint16_t events) noexcept override;
  void setOperation(Operation* op) {
    op_ = op;
  }

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
  Connection(
      MysqlClientBase* mysql_client,
      ConnectionKey conn_key,
      MYSQL* existing_connection);

  virtual ~Connection();

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
      std::unique_ptr<Connection> conn,
      Args&&... args);

  template <typename... Args>
  static std::shared_ptr<MultiQueryOperation> beginMultiQuery(
      std::unique_ptr<Connection> conn,
      Args&&... args);

  static folly::SemiFuture<DbQueryResult> querySemiFuture(
      std::unique_ptr<Connection> conn,
      Query&& query,
      QueryOptions&& options = QueryOptions());

  template <typename... Args>
  [[deprecated(
      "Replaced by the SemiFuture APIs")]] static folly::Future<DbQueryResult>
  queryFuture(std::unique_ptr<Connection> conn, Args&&... args);

  static folly::SemiFuture<DbMultiQueryResult> multiQuerySemiFuture(
      std::unique_ptr<Connection> conn,
      Query&& query,
      QueryOptions&& options = QueryOptions());

  static folly::SemiFuture<DbMultiQueryResult> multiQuerySemiFuture(
      std::unique_ptr<Connection> conn,
      std::vector<Query>&& queries,
      QueryOptions&& options = QueryOptions());

  [[deprecated("Replaced by the SemiFuture APIs")]] static folly::Future<
      DbMultiQueryResult>
  multiQueryFuture(std::unique_ptr<Connection> conn, Query&& query);

  [[deprecated("Replaced by the SemiFuture APIs")]] static folly::Future<
      DbMultiQueryResult>
  multiQueryFuture(
      std::unique_ptr<Connection> conn,
      std::vector<Query>&& queries);

  // An alternate interface that allows for easier re-use of an
  // existing query_op, moving the Connection from the old op and into
  // the new one.  See details above for what args... are.
  template <typename... Args>
  static std::shared_ptr<QueryOperation> beginQuery(
      std::shared_ptr<QueryOperation>& op,
      Args&&... args) {
    CHECK_THROW(op->done(), OperationStateException);
    auto conn = std::move(op->releaseConnection());
    op = beginQuery(std::move(conn), std::forward<Args>(args)...);
    return op;
  }

  // Experimental
  virtual std::shared_ptr<MultiQueryStreamOperation> createOperation(
      Operation::ConnectionProxy&& proxy,
      MultiQuery&& multi_query) {
    return std::make_shared<MultiQueryStreamOperation>(
        std::move(proxy), std::move(multi_query));
  }

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
  static MultiQueryStreamHandler streamMultiQuery(
      std::unique_ptr<Connection> connection,
      std::vector<Query>&& queries,
      const std::unordered_map<std::string, std::string>& attributes =
          std::unordered_map<std::string, std::string>());

  static MultiQueryStreamHandler streamMultiQuery(
      std::unique_ptr<Connection> connection,
      MultiQuery&& multi_query,
      const std::unordered_map<std::string, std::string>& attributes =
          std::unordered_map<std::string, std::string>());

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

  // Called in the libevent thread to create the MYSQL* client.
  void initMysqlOnly();
  void initialize(bool initMysql = true);

  bool hasInitialized() const {
    return initialized_;
  }

  bool ok() const {
    return mysql_connection_ != nullptr;
  }

  void close() {
    if (mysql_connection_) {
      mysql_connection_.reset();
    }
  }

  // Default timeout for queries created by this client.
  void setDefaultQueryTimeout(Duration t) {
    conn_options_.setQueryTimeout(t);
  }
  // TODO #9834064
  void setQueryTimeout(Duration t) {
    conn_options_.setQueryTimeout(t);
  }

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
  string serverInfo() const {
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
  string escapeString(const string& unescaped) {
    CHECK_THROW(mysql_connection_ != nullptr, InvalidConnectionException);
    return Query::escapeString(mysql_connection_->mysql(), unescaped);
  }

  // Returns the number of errors, warnings, and notes generated during
  // execution of the previous SQL statement
  int warningCount() const {
    CHECK_THROW(mysql_connection_ != nullptr, InvalidConnectionException);
    return mysql_warning_count(mysql_connection_->mysql());
  }

  const string& host() const {
    return conn_key_.host;
  }
  int port() const {
    return conn_key_.port;
  }
  const string& user() const {
    return conn_key_.user;
  }
  const string& database() const {
    return conn_key_.db_name;
  }
  const string& password() const {
    return conn_key_.password;
  }

  MysqlClientBase* client() const {
    return mysql_client_;
  }

  long mysqlThreadId() const {
    return mysql_thread_id(mysql_connection_->mysql());
  }

  void disableCloseOnDestroy() {
    if (mysql_connection_) {
      mysql_connection_->disableCloseOnDestroy();
    }
  }

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
    DCHECK(isInEventBaseThread());
    return std::move(mysql_connection_);
  }

  const ConnectionKey* getKey() const {
    return &conn_key_;
  }

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

  bool inTransaction() {
    CHECK_THROW(mysql_connection_ != nullptr, InvalidConnectionException);
    return mysql_connection_->inTransaction();
  }

  const ConnectionOptions& getConnectionOptions() const {
    return conn_options_;
  }

  void setConnectionOptions(const ConnectionOptions& conn_options) {
    conn_options_ = conn_options;
  }

  void setKillOnQueryTimeout(bool killOnQueryTimeout) {
    killOnQueryTimeout_ = killOnQueryTimeout;
  }

  bool getKillOnQueryTimeout() {
    return killOnQueryTimeout_;
  }

  void setConnectionDyingCallback(ConnectionDyingCallback callback) {
    conn_dying_callback_ = callback;
  }

  // Note that the chained callback is invoked in the MySQL client thread
  // and so any callback should execute *very* quickly and not block
  void setPreOperationCallback(ChainedCallback&& callback) {
    pre_operation_callback_ =
        setCallback(std::move(pre_operation_callback_), std::move(callback));
  }

  void setPostOperationCallback(ChainedCallback&& callback) {
    post_operation_callback_ =
        setCallback(std::move(post_operation_callback_), std::move(callback));
  }

  ChainedCallback stealPreOperationCallback() {
    return std::move(pre_operation_callback_);
  }

  ChainedCallback stealPostOperationCallback() {
    return std::move(post_operation_callback_);
  }

  const folly::EventBase* getEventBase() const {
    return client()->getEventBase();
  }

  folly::EventBase* getEventBase() {
    return client()->getEventBase();
  }

  bool isInEventBaseThread() const {
    auto eb = getEventBase();
    return eb == nullptr || eb->isInEventBaseThread();
  }

  virtual bool runInThread(folly::Cob&& fn) {
    return client()->runInThread(std::move(fn));
  }

  template <typename TOp, typename... F, typename... T>
  bool runInThread(TOp* op, void (TOp::*f)(F...), T&&... v) {
    // short circuit
    if (isInEventBaseThread()) {
      (op->*f)(std::forward<T>(v)...);
      return true;
    } else {
      return runInThread(std::bind(f, op, v...));
    }
  }

  // Operations call these methods as the operation becomes unblocked, as
  // callers want to wait for completion, etc.
  virtual void notify() = 0;
  virtual void wait() = 0;
  // Called when a new operation is being started.
  virtual void resetActionable() = 0;

 private:
  // Methods primarily invoked by Operations and AsyncMysqlClient.
  friend class AsyncMysqlClient;
  friend class SyncMysqlClient;
  friend class MysqlClientBase;
  friend class Operation;
  friend class ConnectOperation;
  friend class ConnectPoolOperation;
  friend class FetchOperation;
  friend class QueryOperation;
  friend class MultiQueryOperation;
  friend class MultiQueryStreamOperation;

  ChainedCallback setCallback(
      ChainedCallback orgCallback,
      ChainedCallback newCallback) {
    if (!orgCallback) {
      return newCallback;
    }

    if (!newCallback) {
      return orgCallback;
    }

    return [orgCallback = std::move(orgCallback),
            newCallback = std::move(newCallback)](Operation& op) mutable {
      orgCallback(op);
      newCallback(op);
    };
  }

  ConnectionSocketHandler* socketHandler() {
    return &socket_handler_;
  }

  MYSQL* mysql() const {
    DCHECK(isInEventBaseThread());
    if (mysql_connection_) {
      return mysql_connection_->mysql();
    } else {
      return nullptr;
    }
  }

  MysqlConnectionHolder* mysqlConnection() const {
    DCHECK(isInEventBaseThread());
    return mysql_connection_.get();
  }

  void setMysqlConnectionHolder(
      std::unique_ptr<MysqlConnectionHolder> mysql_connection) {
    CHECK_THROW(mysql_connection_ == nullptr, InvalidConnectionException);
    CHECK_THROW(
        conn_key_ == *mysql_connection->getKey(), InvalidConnectionException);
    mysql_connection_ = std::move(mysql_connection);
  }

  // Helper function that will begin multiqueries or single queries depending
  // on the specified in the templates. Being used to avoid duplicated code
  // that both need to do.
  template <typename QueryType, typename QueryArg>
  static std::shared_ptr<QueryType> beginAnyQuery(
      Operation::ConnectionProxy&& conn_proxy,
      QueryArg&& query);

  void checkOperationInProgress() {
    if (operation_in_progress_) {
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

  bool killOnQueryTimeout_ = false;

  // Context information for logging purposes.
  std::unique_ptr<db::ConnectionContextBase> connection_context_;

  // Unowned pointer to the client we're from.
  MysqlClientBase* mysql_client_;

  ConnectionSocketHandler socket_handler_;

  ConnectionDyingCallback conn_dying_callback_;

  ChainedCallback pre_operation_callback_, post_operation_callback_;

  bool initialized_;

  // Used for signing that the connection is being used in a synchronous call,
  // eg. `query`. MySQL doesn't allow more than one query being made through
  // the same connection at the same time. So same logic goes here.
  // We don't track for async calls, for async calls the unique Connection
  // gets moved to the operation, so the protection is guaranteed.
  bool operation_in_progress_ = false;

  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;
};

// Don't these directly. Used to separate the Connection synchronization
// between AsyncMysqlClient or SyncMysqlClient.
class AsyncConnection : public Connection {
 public:
  AsyncConnection(
      MysqlClientBase* mysql_client,
      ConnectionKey conn_key,
      MYSQL* existing_connection)
      : Connection(mysql_client, conn_key, existing_connection) {}

  // Operations call these methods as the operation becomes unblocked, as
  // callers want to wait for completion, etc.
  void notify() override {
    if (actionableBaton_.try_wait()) {
      LOG(DFATAL) << "asked to notify already-actionable operation";
    }
    actionableBaton_.post();
  }

  void wait() override {
    CHECK_THROW(
        folly::fibers::onFiber() || !isInEventBaseThread(), std::runtime_error);
    actionableBaton_.wait();
  }

  // Called when a new operation is being started.
  void resetActionable() override {
    actionableBaton_.reset();
  }

 private:
  folly::fibers::Baton actionableBaton_;
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
    std::unique_ptr<Connection> conn,
    Query&& query);

template <typename... Args>
std::shared_ptr<QueryOperation> Connection::beginQuery(
    std::unique_ptr<Connection> conn,
    Args&&... args) {
  Query query{std::forward<Args>(args)...};
  return beginQuery(std::move(conn), std::move(query));
}

template <>
[[deprecated("Replaced by the SemiFuture APIs")]] folly::Future<DbQueryResult>
Connection::queryFuture(std::unique_ptr<Connection> conn, Query&& query);

template <typename... Args>
[[deprecated("Replaced by the SemiFuture APIs")]] folly::Future<DbQueryResult>
Connection::queryFuture(std::unique_ptr<Connection> conn, Args&&... args) {
  Query query{std::forward<Args>(args)...};
  return queryFuture(std::move(conn), std::move(query));
}
} // namespace mysql_client
} // namespace common
} // namespace facebook

#endif // COMMON_ASYNC_MYSQL_CLIENT_H
