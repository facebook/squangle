/*
 *  Copyright (c) 2015, Facebook, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */
//
// Operation objects are the handles users of AsyncMysqlClient use to
// interact with connections and queries.  Every action a user
// initiates returns an Operation to track the status of the action,
// report errors, etc.  Operations also offer a way to set callbacks
// for completion.
//
// In general, operations are held in shared_ptr's as ownership is
// unclear.  This allows the construction of Operations, callbacks to
// be set, and the Operation itself be cleaned up by AsyncMysqlClient.
// Conversely, if callbacks aren't being used, the Operation can
// simply be wait()'d upon for completion.
//
// See README for examples.
//
// Implementation detail; Operations straddle the caller's thread and
// the thread managed by the AsyncMysqlClient.  They also are
// responsible for execution of the actual libmysqlclient functions
// and most interactions with libevent.
//
// As mentioned above, an Operation's lifetime is determined by both
// AsyncMysqlClient and the calling point that created an Operation.
// It is permissible to immediately discard an Operation or to hold
// onto it (via a shared_ptr).  However, all calls to methods such as
// result() etc must occur either in the callback or after wait() has
// returned.

#ifndef COMMON_ASYNC_MYSQL_OPERATION_H
#define COMMON_ASYNC_MYSQL_OPERATION_H

#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>
#include <mysql.h>

#include "squangle/mysql_client/Row.h"
#include "squangle/mysql_client/Query.h"
#include "squangle/mysql_client/DbResult.h"
#include "squangle/mysql_client/Connection.h"
#include "squangle/logger/DBEventLogger.h"
#include "folly/Exception.h"
#include "folly/String.h"
#include "folly/dynamic.h"
#include "folly/Memory.h"
#include <folly/io/async/SSLContext.h>
#include <folly/io/async/EventHandler.h>
#include <folly/io/async/AsyncTimeout.h>

namespace facebook {
namespace common {
namespace mysql_client {

using facebook::db::OperationStateException;

class AsyncMysqlClient;
class QueryResult;
class ConnectOperation;
class FetchOperation;
class QueryOperation;
class MultiQueryOperation;
class Operation;
class Connection;
class ConnectionKey;
class ConnectionSocketHandler;
class ConnectionOptions;

enum class QueryCallbackReason;

// Simplify some std::chrono types.
typedef std::chrono::time_point<std::chrono::high_resolution_clock> Timepoint;

// Callbacks for connecting and querying, respectively.  A
// ConnectCallback is invoked when a connection succeeds or fails.  A
// QueryCallback is called for each row block (see Row.h) as well as
// when the query completes (either successfully or with an error).
typedef std::function<void(ConnectOperation&)> ConnectCallback;
// Callback for observer. I will be called for a completed operation,
// after the callback for the specific operation is called, if one is defined.
typedef std::function<void(Operation&)> ObserverCallback;
typedef std::function<void(QueryOperation&, QueryResult*, QueryCallbackReason)>
QueryCallback;
typedef std::function<
    void(MultiQueryOperation&, QueryResult*, QueryCallbackReason)>
MultiQueryCallback;

using std::vector;
using std::string;
// The state of the Operation.  In general, callers will see Unstarted
// (i.e., haven't yet called run()) or Completed (which may mean
// success or error; see OperationResult below).  Pending and
// Cancelling are not visible at times an outside caller might see
// them (since, once run() has been called, wait() must be called
// before inspecting other Operation attributes).
enum class OperationState { Unstarted, Pending, Cancelling, Completed, };

// Once an operation is Completed, it has a result type, indicating
// what ultimately occurred.  These are self-explanatory.
enum class OperationResult { Unknown, Succeeded, Failed, Cancelled, TimedOut, };

// For control flows in callbacks. This indicates the reason a callback was
// fired. When a pack of rows if fetched it is used RowsFetched to
// indicate that new rows are available. QueryBoundary means that the
// fetching for current query has completed successfully, and if any
// query failed (OperationResult is Failed) we use Failure. Success is for
// indicating that all queries have been successfully fetched.
enum class QueryCallbackReason { RowsFetched, QueryBoundary, Failure, Success };

// The abstract base for our available Operations.  Subclasses share
// intimate knowledge with the Operation class (most member variables
// are protected).
class Operation : public std::enable_shared_from_this<Operation> {
 public:
  // No public constructor.
  virtual ~Operation();

  virtual Operation* run();

  // Set a timeout; otherwise FLAGS_async_mysql_timeout_micros is
  // used.
  virtual Operation* setTimeout(Duration timeout) {
    CHECK_THROW(state_ == OperationState::Unstarted, OperationStateException);
    timeout_ = timeout;
    return this;
  }

  Duration getTimeout() { return timeout_; }

  // Did the operation succeed?
  bool ok() const { return done() && result_ == OperationResult::Succeeded; }

  // Is the operation complete (success or failure)?
  bool done() const { return state_ == OperationState::Completed; }

  // host and port we are connected to (or will be connected to).
  const string& host() const;
  int port() const;

  // Try to cancel a pending operation.  This is inherently racey with
  // callbacks; it is possible the callback is being invoked *during*
  // the cancel attempt, so a cancelled operation may still succeed.
  void cancel();

  // Wait for the Operation to complete.
  void wait();

  // Wait for an operation to complete, and CHECK if it fails.  Mainly
  // for testing.
  virtual void mustSucceed() = 0;

  // Information about why this operation failed.
  int mysql_errno() const { return mysql_errno_; }
  const string& mysql_error() const { return mysql_error_; }
  const string& mysql_normalize_error() const { return mysql_normalize_error_; }

  // Get the state and result, as well as readable string versions.
  OperationResult result() const { return result_; }

  folly::StringPiece resultString() const;

  OperationState state() const { return state_; }

  folly::StringPiece stateString() const;

  static folly::StringPiece toString(OperationState state);
  static folly::StringPiece toString(OperationResult result);

  // An Operation can have a folly::dynamic associated with it.  This
  // can represent anything the caller wants to track and is primarily
  // useful in the callback.  Typically this would be a string or
  // integer.  Note, also, such information can be stored inside the
  // callback itself (via a lambda).
  Operation* setUserData(folly::dynamic val) {
    user_data_ = std::move(val);
    return this;
  }

  const folly::dynamic& userData() const { return user_data_; }
  folly::dynamic&& stealUserData() { return std::move(user_data_); }

  // Connections are transferred across operations.  At any one time,
  // there is one unique owner of the connection.
  std::unique_ptr<Connection>&& releaseConnection();
  Connection* connection() { return conn_ptr_->get(); }

  // Various accessors for our Operation's start, end, and total elapsed time.
  Timepoint startTime() const { return start_time_; }
  Timepoint endTime() const {
    CHECK_THROW(state_ == OperationState::Completed, OperationStateException);
    return end_time_;
  }

  Duration elapsed() const {
    CHECK_THROW(state_ == OperationState::Completed, OperationStateException);
    return std::chrono::duration_cast<std::chrono::microseconds>(end_time_ -
                                                                 start_time_);
  }

  void setObserverCallback(ObserverCallback obs_cb);

  // Retrieve the shared pointer that holds this instance.
  std::shared_ptr<Operation> getSharedPointer();

  AsyncMysqlClient* async_client();
 protected:
  class ConnectionProxy;
  explicit Operation(std::unique_ptr<ConnectionProxy>&& conn);


  ConnectionProxy& conn() { return *conn_ptr_.get(); }
  const ConnectionProxy& conn() const { return *conn_ptr_.get(); }

  // Save any mysql errors that occurred (since we may hand off the
  // Connection before the user wants this information).
  void snapshotMysqlErrors();

  // Flag internal async client errors; this always becomes a MySQL
  // error 2000 (CR_UNKNOWN_ERROR) with a suitable descriptive message.
  void setAsyncClientError(StringPiece msg, StringPiece normalizeMsg = "");

  // Same as above, but specify the error code.
  void setAsyncClientError(int mysql_errno,
                           StringPiece msg,
                           StringPiece normalizeMsg = "");

  // Called when an Operation needs to wait for the socket to become
  // readable or writable (aka actionable).
  void waitForSocketActionable();

  // Overridden in child classes and invoked when the socket is
  // actionable.  This function should either completeOperation or
  // waitForSocketActionable.
  virtual void socketActionable() = 0;

  // Called by ConnectionSocketHandler when the operation timed out
  void timeoutTriggered();

  // Our operation has completed.  During completeOperation,
  // specializedCompleteOperation is invoked for subclasses to perform
  // their own finalization (typically annotating errors and handling
  // timeouts).
  void completeOperation(OperationResult result);
  void completeOperationInner(OperationResult result);
  virtual Operation* specializedRun() = 0;
  virtual void specializedTimeoutTriggered() = 0;
  virtual void specializedCompleteOperation() = 0;

  // Base class for a wrapper around the 2 types of connection
  // pointers we accept in the Operation:
  // - OwnedConnection: will hold an unique_ptr to the Connection
  //   for the async calls of the API, so the ownership is clear;
  // - ReferencedConnection: allows synchronous calls without moving unique_ptrs
  //   to the Operation;
  class ConnectionProxy {
   public:
    ConnectionProxy() {}

    virtual ~ConnectionProxy() {}

    virtual Connection* get() = 0;
    virtual const Connection* get() const = 0;

    Connection* operator->() { return get(); }
    const Connection* operator->() const { return get(); }

    virtual std::unique_ptr<Connection>&& releaseConnection() = 0;

    ConnectionProxy(ConnectionProxy&&) = default;
    ConnectionProxy& operator=(ConnectionProxy&&) = default;

    ConnectionProxy(ConnectionProxy const&) = delete;
    ConnectionProxy& operator=(ConnectionProxy const&) = delete;
  };

  class OwnedConnection : public ConnectionProxy {
   public:
    explicit OwnedConnection(std::unique_ptr<Connection>&& conn);

    Connection* get() override { return conn_.get(); }
    const Connection* get() const override { return conn_.get(); }

    std::unique_ptr<Connection>&& releaseConnection() override;

   private:
    std::unique_ptr<Connection> conn_;
  };

  class ReferencedConnection : public ConnectionProxy {
   public:
    explicit ReferencedConnection(Connection* conn)
        : ConnectionProxy(), conn_(conn) {}

    Connection* get() override { return conn_; }

    const Connection* get() const override { return conn_; }

    // Releasing connection in sync mode is not needed since it doesn't hold
    // an unique_ptr, also, the user never holds the Operation in this mode.
    // This will throw exception in case gets called.
    std::unique_ptr<Connection>&& releaseConnection() override;

   private:
    Connection* conn_;
  };

  // Data members; subclasses freely interact with these.
  OperationState state_;
  OperationResult result_;

  // Our client is not owned by us. It must outlive all active Operations.
  Duration timeout_;
  Timepoint start_time_;
  Timepoint end_time_;

  // Our Connection object.  Created by ConnectOperation and moved
  // into QueryOperations.
  std::unique_ptr<ConnectionProxy> conn_ptr_;

  // Errors that may have occurred.
  int mysql_errno_;
  string mysql_error_;
  string mysql_normalize_error_;

  // This mutex protects the operation cancel process when the state
  // is being checked in `run` and the operation is being cancelled in other
  // thread.
  std::mutex run_state_mutex_;

 private:
  folly::dynamic user_data_;
  ObserverCallback observer_callback_;
  std::unique_ptr<db::ConnectionContextBase> connection_context_;

  AsyncMysqlClient* async_client_;

  bool cancel_on_run_ = false;

  Operation() = delete;
  Operation(const Operation&) = delete;
  Operation& operator=(const Operation&) = delete;

  friend class Connection;
  friend class ConnectionSocketHandler;
};

// An operation representing a pending connection.  Constructed via
// AsyncMysqlClient::beginConnection.
class ConnectOperation : public Operation {
 public:
  virtual ~ConnectOperation();

  void setCallback(ConnectCallback cb) { connect_callback_ = cb; }

  const string& database() const { return conn_key_.db_name; }
  const string& user() const { return conn_key_.user; }
  int connectionFlags() const { return flags_; }

  const ConnectionKey* getKey() const { return &conn_key_; }

  // Get and set MySQL 5.6 connection attributes.
  ConnectOperation* setConnectionAttribute(const string& attr,
                                           const string& value) {
    CHECK_THROW(state_ == OperationState::Unstarted, OperationStateException);
    connection_attributes_[attr] = value;
    return this;
  }

  const std::unordered_map<string, string>& connectionAttributes() const {
    return connection_attributes_;
  }

  ConnectOperation* setConnectionAttributes(
      const std::unordered_map<string, string>& attributes) {
    connection_attributes_ = attributes;
    return this;
  }

  ConnectOperation* setSSLContext(
      std::shared_ptr<folly::SSLContext> ssl_context) {
    ssl_context_ = ssl_context;
    return this;
  }

  // Default timeout for queries created by the connection this
  // operation will create.
  ConnectOperation* setDefaultQueryTimeout(Duration t) {
    default_query_timeout_ = t;
    return this;
  }

  // To set connection flags before connecting process has started.
  ConnectOperation* setConnectionFlag(int new_flags) {
    CHECK_THROW(state_ != OperationState::Unstarted, OperationStateException);
    flags_ = new_flags;
    return this;
  }

  // To add connection flags before connecting process has started.
  ConnectOperation* addConnectionFlag(int new_flags) {
    CHECK_THROW(state_ != OperationState::Unstarted, OperationStateException);
    flags_ |= new_flags;
    return this;
  }

  ConnectOperation* setConnectionContext(
      std::unique_ptr<db::ConnectionContextBase>&& e) {
    CHECK_THROW(state_ == OperationState::Unstarted, OperationStateException);
    connection_context_ = std::move(e);
    return this;
  }

  db::ConnectionContextBase* getConnectionContext() {
    CHECK_THROW(state_ == OperationState::Unstarted, OperationStateException);
    return connection_context_.get();
  }

  // Don't call this; it's public strictly for AsyncMysqlClient to be
  // able to call make_shared.
  ConnectOperation(AsyncMysqlClient* async_client, ConnectionKey conn_key);

  virtual void mustSucceed();

  // Overriding to narrow the return type
  // Each connect attempt will take at most this timeout to retry to acquire
  // the connection.
  ConnectOperation* setTimeout(Duration timeout) {
    attempt_timeout_ = timeout;
    Operation::setTimeout(timeout);
    return this;
  }

  ConnectOperation* setUserData(folly::dynamic val) {
    Operation::setUserData(std::move(val));
    return this;
  }

  // Sets the total timeout that the connect operation will use.
  // Each attempt will take at most `setTimeout`. Use this in case
  // you have strong timeout restrictions but still want the connection to
  // retry.
  ConnectOperation* setTotalTimeout(Duration total_timeout) {
    timeout_ = min(timeout_, total_timeout);
    total_timeout_ = total_timeout;
    return this;
  }

  // Sets the number of attempts this operation will try to acquire a mysql
  // connection.
  ConnectOperation* setConnectAttempts(uint32_t max_attempts) {
    max_attempts_ = max_attempts;
    return this;
  }

  uint32_t attemptsMade() const { return attempts_made_; }

  Duration getAttemptTimeout() const { return attempt_timeout_; }

  ConnectOperation* setConnectionOptions(const ConnectionOptions& conn_opts);

  static constexpr Duration kMinimumViableConnectTimeout =
      std::chrono::microseconds(50);

 protected:
  virtual void attemptFailed(OperationResult result);
  virtual void attemptSucceeded(OperationResult result);

  virtual ConnectOperation* specializedRun();
  virtual void socketActionable();
  virtual void specializedTimeoutTriggered();
  virtual void specializedCompleteOperation();

  // Removes the Client ref, it can be called by child classes without needing
  // to add them as friend classes of AsyncMysqlClient
  virtual void removeClientReference();

  bool shouldCompleteOperation(OperationResult result);

  void storeSSLSession();

  uint32_t max_attempts_ = 1;
  uint32_t attempts_made_ = 0;

  Duration total_timeout_;
  Duration attempt_timeout_;

 private:
  void logConnectCompleted(OperationResult result);

  const ConnectionKey conn_key_;
  int flags_;
  Duration default_query_timeout_;

  // Context information for logging purposes.
  std::unique_ptr<db::ConnectionContextBase> connection_context_;

  // MySQL 5.6 connection attributes.  Sent at time of connect.
  std::unordered_map<string, string> connection_attributes_;

  std::shared_ptr<folly::SSLContext> ssl_context_;

  ConnectCallback connect_callback_;
  bool active_in_client_;

  friend class AsyncMysqlClient;
};

// A fetching operation (query or multiple queries) use the same primary
// actions. This is an abstract base for this kind of operation.
class FetchOperation : public Operation {
 public:
  virtual ~FetchOperation();

  virtual void mustSucceed();

  // Return the query as it was sent to MySQL (i.e., for a single
  // query, the query itself, but for multiquery, all queries
  // combined).
  folly::fbstring getExecutedQuery() const {
    CHECK_THROW(state_ != OperationState::Unstarted, OperationStateException);
    return rendered_multi_query_;
  }

  // Number of queries that succeed to execute
  int numQueriesExecuted() {
    CHECK_THROW(state_ != OperationState::Pending, OperationStateException);
    return num_queries_executed_;
  }

 protected:
  virtual FetchOperation* specializedRun();

  explicit FetchOperation(std::unique_ptr<ConnectionProxy>&& conn,
                          db::QueryType query_type);

  enum class FetchAction {
    StartQuery,
    InitFetch,
    Fetch,
    CompleteQuery,
    CompleteOperation
  };

  // In socket actionable it is analyzed the action that is required to continue
  // the operation. For example, if the fetch action is StartQuery, it runs
  // query or requests more results depending if it had already ran or not the
  // query. The same process happens for the other FetchActions.
  // The action member can be changed in other member functions called in
  // socketActionable to keep the fetching flow running.
  virtual void socketActionable();
  virtual void specializedTimeoutTriggered();
  virtual void specializedCompleteOperation();

  // Overridden in child classes and invoked when the Query fetching
  // has done specific actions that might be needed for report (callbacks,
  // store fetched data, initialize data).
  virtual void specializedInitQuery() = 0;
  virtual void specializedRowsFetched(RowBlock&& row_block) = 0;
  virtual void specializedCompleteQuery(bool success, bool more_results) = 0;

  virtual folly::fbstring renderedQuery() = 0;

  // Current query data
  int num_fields_;
  uint64_t num_rows_seen_;
  bool cancel_;
  int num_queries_executed_;

  std::shared_ptr<RowFields> row_fields_info_;
  std::unique_ptr<QueryResult> query_result_;

  FetchAction fetch_action_;

  folly::fbstring rendered_multi_query_;

  // For stats purposes
  db::QueryType query_type_;

 private:
  bool slurpRows();
  bool readMysqlFields();

  bool query_executed_;

  MYSQL_RES* mysql_query_result_;
  Timepoint last_row_time_;
};

// An operation representing a query.  If a callback is set, it
// invokes the callback as rows arrive.  If there is no callback, it
// buffers all results into memory and makes them available as a
// RowBlock.  This is inefficient for large results.
//
// Constructed via Connection::beginQuery.
class QueryOperation : public FetchOperation {
 public:
  virtual ~QueryOperation();

  void setCallback(QueryCallback cb) { query_callback_ = cb; }

  // Steal all rows.  Only valid if there is no callback.  Inefficient
  // for large result sets.
  QueryResult&& stealQueryResult() {
    CHECK_THROW(ok(), OperationStateException);
    return std::move(*query_result_);
  }

  const QueryResult& queryResult() const {
    CHECK_THROW(ok(), OperationStateException);
    return *query_result_;
  }

  // Returns the Query of this operation
  const Query& getQuery() const { return query_; }

  // Steal all rows.  Only valid if there is no callback.  Inefficient
  // for large result sets.
  vector<RowBlock>&& stealRows() { return query_result_->stealRows(); }

  const vector<RowBlock>& rows() const { return query_result_->rows(); }

  // Last insert id (aka mysql_insert_id).
  uint64_t lastInsertId() const { return query_result_->lastInsertId(); }

  // Number of rows affected (aka mysql_affected_rows).
  uint64_t numRowsAffected() const { return query_result_->numRowsAffected(); }

  void setQueryResult(QueryResult query_result) {
    query_result_ = folly::make_unique<QueryResult>(std::move(query_result));
  }

  // Don't call this; it's public strictly for Connection to be able
  // to call make_shared.
  QueryOperation(std::unique_ptr<ConnectionProxy>&& connection,
                 Query&& query);

  // Overriding to narrow the return type
  QueryOperation* setTimeout(Duration timeout) {
    Operation::setTimeout(timeout);
    return this;
  }

  QueryOperation* setUserData(folly::dynamic val) {
    Operation::setUserData(std::move(val));
    return this;
  }

 protected:
  virtual folly::fbstring renderedQuery();

  virtual void specializedInitQuery();
  virtual void specializedRowsFetched(RowBlock&& row_block);
  virtual void specializedCompleteQuery(bool success, bool more_results);

  // Calls the FetchOperation specializedCompleteOperation and then does
  // callbacks if needed
  virtual void specializedCompleteOperation();

 private:
  Query query_;
  QueryCallback query_callback_;

  friend class Connection;
};

// An operation representing a query with multiple statements.
// If a callback is set, it invokes the callback as rows arrive.
// If there is no callback, it buffers all results into memory
// and makes them available as a RowBlock.
// This is inefficient for large results.
//
// Constructed via Connection::beginMultiQuery.
class MultiQueryOperation : public FetchOperation {
 public:
  virtual ~MultiQueryOperation();

  // Set our callback.  This is invoked multiple times -- once for
  // every RowBatch and once, with nullptr for the RowBatch,
  // indicating the query is complete.
  void setCallback(MultiQueryCallback cb) { query_callback_ = cb; }

  // Steal all rows. Only valid if there is no callback. Inefficient
  // for large result sets.
  // Only call after the query has finished, don't use it inside callbacks
  std::vector<QueryResult>&& stealQueryResults() {
    CHECK_THROW(done(), OperationStateException);
    return std::move(query_results_);
  }

  // Only call this after the query has finished and don't use it inside
  // callbacks
  const vector<QueryResult>& queryResults() const {
    CHECK_THROW(done(), OperationStateException);
    return query_results_;
  }

  // Returns the Query for a query index.
  const Query& getQuery(int index) const {
    CHECK_THROW(0 <= index || index < queries_.size(), std::invalid_argument);
    return queries_[index];
  }

  void setQueryResults(std::vector<QueryResult> query_results) {
    query_results_ = std::move(query_results);
  }

  // Don't call this; it's public strictly for Connection to be able
  // to call make_shared.
  MultiQueryOperation(std::unique_ptr<ConnectionProxy>&& connection,
                      std::vector<Query>&& queries);

  // Overriding to narrow the return type
  MultiQueryOperation* setTimeout(Duration timeout) {
    Operation::setTimeout(timeout);
    return this;
  }

  MultiQueryOperation* setUserData(folly::dynamic val) {
    Operation::setUserData(std::move(val));
    return this;
  }

 protected:
  virtual folly::fbstring renderedQuery();

  virtual void specializedInitQuery();
  virtual void specializedRowsFetched(RowBlock&& row_block);
  virtual void specializedCompleteQuery(bool success, bool more_results);

  // Calls the FetchOperation specializedCompleteOperation and then does
  // callbacks if needed
  virtual void specializedCompleteOperation();

 private:
  const std::vector<Query> queries_;
  MultiQueryCallback query_callback_;

  // Storage fields for every statement in the query
  // Only to be used if there is no callback set.
  std::vector<QueryResult> query_results_;

  friend class Connection;
};

// Helper function to build the result for a ConnectOperation in the sync mode.
// It will block the thread and return the acquired connection, in case of
// error, it will throw MysqlException as expected in the sync mode.
std::unique_ptr<Connection> blockingConnectHelper(
    std::shared_ptr<ConnectOperation>& conn_op);
}
}
} // facebook::common::mysql_client

#endif // COMMON_ASYNC_MYSQL_OPERATION_H
