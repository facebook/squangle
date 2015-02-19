/*
 *  Copyright (c) 2015, Facebook, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */

#include "squangle/mysql_client/Operation.h"

#include <errmsg.h> // mysql

#include "folly/Memory.h"
#include "squangle/mysql_client/AsyncMysqlClient.h"

#ifndef NO_LIB_GFLAGS
#include "common/config/Flags.h"
DEFINE_int64(async_mysql_timeout_micros,
             60 * 1000 * 1000,
             "default timeout, in micros, for mysql operations");
#endif

namespace facebook {
namespace common {
namespace mysql_client {

#ifdef NO_LIB_GFLAGS
  int64_t FLAGS_async_mysql_timeout_micros = 60 * 1000 * 1000;
#endif

namespace chrono = std::chrono;

Operation::Operation(std::unique_ptr<ConnectionProxy>&& safe_conn)
    : state_(OperationState::Unstarted),
      result_(OperationResult::Unknown),
      conn_ptr_(std::move(safe_conn)),
      mysql_errno_(0),
      user_data_{},
      observer_callback_(nullptr),
      async_client_(conn()->async_client_) {
  timeout_ = Duration(FLAGS_async_mysql_timeout_micros);
  conn()->resetActionable();
}

Operation::~Operation() {}

void Operation::waitForSocketActionable() {
  CHECK_EQ(std::this_thread::get_id(), async_client()->threadId());

  MYSQL* mysql = conn()->mysql();
  uint16_t event_mask = 0;
  switch (mysql->net.async_blocking_state) {
  case NET_NONBLOCKING_READ:
    event_mask |= ata::TEventHandler::READ;
    break;
  case NET_NONBLOCKING_WRITE:
  case NET_NONBLOCKING_CONNECT:
    event_mask |= ata::TEventHandler::WRITE;
    break;
  default:
    LOG(FATAL) << "Unknown nonblocking status "
               << mysql->net.async_blocking_state;
  }

  auto end = timeout_ + start_time_;
  auto now = chrono::high_resolution_clock::now();
  if (now >= end) {
    timeoutTriggered();
    return;
  }

  conn()->socketHandler()->scheduleTimeout(
      chrono::duration_cast<chrono::milliseconds>(end - now).count());
  conn()->socketHandler()->registerHandler(event_mask);
}

void Operation::cancel() {
  {
    // This code competes with `run()` to see who changes `state_` first,
    // since they both have the combination `check and change` this must
    // be locked
    std::unique_lock<std::mutex> l(run_state_mutex_);

    if (state_ == OperationState::Cancelling ||
        state_ == OperationState::Completed) {
      // If the cancel was already called we dont do the cancelling
      // process again
      return;
    }

    if (state_ == OperationState::Unstarted) {
      cancel_on_run_ = true;
      // wait the user to call "run()" to run the completeOperation
      // otherwise we will throw exception
      return;
    }
  }

  state_ = OperationState::Cancelling;
  if (!async_client()->runInThread([this]() {
        completeOperation(OperationResult::Cancelled);
      })) {
    // if a strange error happen in TEventBase , mark it cancelled now
    completeOperationInner(OperationResult::Cancelled);
  }
}

void Operation::timeoutTriggered() { specializedTimeoutTriggered(); }

Operation* Operation::run() {
  {
    std::unique_lock<std::mutex> l(run_state_mutex_);
    if (cancel_on_run_) {
      state_ = OperationState::Cancelling;
      cancel_on_run_ = false;
      async_client()->runInThread([this]() {
        completeOperation(OperationResult::Cancelled);
      });
      return this;
    }
    CHECK_THROW(state_ == OperationState::Unstarted, OperationStateException);
    state_ = OperationState::Pending;
  }
  start_time_ = chrono::high_resolution_clock::now();
  return specializedRun();
}

void Operation::completeOperation(OperationResult result) {
  CHECK_EQ(std::this_thread::get_id(), async_client()->threadId());
  if (state_ == OperationState::Completed) {
    return;
  }

  CHECK_THROW(state_ == OperationState::Pending ||
                  state_ == OperationState::Cancelling ||
                  state_ == OperationState::Unstarted,
              OperationStateException);
  completeOperationInner(result);
}

void Operation::completeOperationInner(OperationResult result) {
  state_ = OperationState::Completed;
  result_ = result;
  end_time_ = chrono::high_resolution_clock::now();
  if ((result == OperationResult::Cancelled ||
       result == OperationResult::TimedOut) &&
      conn()->hasInitialized()) {
    // Cancelled/timed out ops leave our connection in an undefined
    // state.  Close it to prevent trouble.
    conn()->close();
  }

  conn()->socketHandler()->unregisterHandler();
  conn()->socketHandler()->cancelTimeout();

  // Save async_client() before running specializedCompleteOperation
  // as it may release conn().
  auto client = async_client();
  specializedCompleteOperation();

  // call observer callback
  if (observer_callback_) {
    observer_callback_(*this);
  }

  client->deferRemoveOperation(this);
}

std::unique_ptr<Connection>&& Operation::releaseConnection() {
  CHECK_THROW(state_ == OperationState::Completed ||
                  state_ == OperationState::Unstarted,
              OperationStateException);
  return std::move(conn_ptr_->releaseConnection());
}

void Operation::snapshotMysqlErrors() {
  mysql_errno_ = ::mysql_errno(conn()->mysql());
  mysql_error_ = ::mysql_error(conn()->mysql());
}

void Operation::setAsyncClientError(StringPiece msg) {
  mysql_errno_ = CR_UNKNOWN_ERROR;
  mysql_error_ = msg.toString();
}

void Operation::setAsyncClientError(int mysql_errno, StringPiece msg) {
  mysql_errno_ = mysql_errno;
  mysql_error_ = msg.toString();
}

void Operation::wait() {
  CHECK_THROW(std::this_thread::get_id() != async_client()->threadId(),
              std::runtime_error);
  return conn()->wait();
}

AsyncMysqlClient* Operation::async_client() { return async_client_; }

std::shared_ptr<Operation> Operation::getSharedPointer() {
  CHECK_EQ(std::this_thread::get_id(), async_client()->threadId());
  return shared_from_this();
}

const string& Operation::host() const { return conn()->host(); }
int Operation::port() const { return conn()->port(); }

void Operation::setObserverCallback(ObserverCallback obs_cb) {
  CHECK_THROW(state_ == OperationState::Unstarted, OperationStateException);
  // allow more callbacks to be set
  if (observer_callback_) {
    ObserverCallback old_obs_cb = observer_callback_;
    observer_callback_ = [obs_cb, old_obs_cb](Operation& op) {
      obs_cb(op);
      old_obs_cb(op);
    };
  } else {
    observer_callback_ = obs_cb;
  }
}

ConnectOperation::ConnectOperation(AsyncMysqlClient* async_client,
                                   ConnectionKey conn_key)
    : Operation(folly::make_unique<Operation::OwnedConnection>(
          folly::make_unique<Connection>(async_client, conn_key, nullptr))),
      total_timeout_(Duration(FLAGS_async_mysql_timeout_micros)),
      attempt_timeout_(Duration(FLAGS_async_mysql_timeout_micros)),
      conn_key_(conn_key),
      flags_(CLIENT_MULTI_STATEMENTS),
      default_query_timeout_(0),
      active_in_client_(true) {
  async_client->activeConnectionAdded(&conn_key_);
}

ConnectOperation* ConnectOperation::setConnectionOptions(
    const ConnectionOptions& conn_opts) {
  setTimeout(conn_opts.getTimeout());
  setDefaultQueryTimeout(conn_opts.getQueryTimeout());
  setConnectionAttributes(conn_opts.getConnectionAttributes());
  setConnectAttempts(conn_opts.getConnectAttempts());
  setTotalTimeout(conn_opts.getTotalTimeout());
  return this;
}

bool ConnectOperation::shouldCompleteOperation(OperationResult result) {
  // Cancelled doesn't really get to this point, the Operation is forced to
  // complete by Operation, adding this check here just-in-case.
  if (attempts_made_ >= max_attempts_ || result == OperationResult::Cancelled) {
    return true;
  }

  auto now =
      std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(1);
  if (now > start_time_ + total_timeout_) {
    return true;
  }

  return false;
}

void ConnectOperation::attemptFailed(OperationResult result) {
  ++attempts_made_;
  if (shouldCompleteOperation(result)) {
    completeOperation(result);
    return;
  }

  logConnectFailed(result);

  conn()->socketHandler()->unregisterHandler();
  conn()->socketHandler()->cancelTimeout();
  conn()->close();

  auto now = std::chrono::high_resolution_clock::now();
  // Adjust timeout
  auto timeout_attempt_based =
      attempt_timeout_ +
      std::chrono::duration_cast<std::chrono::milliseconds>(now - start_time_);
  timeout_ = min(timeout_attempt_based, total_timeout_);

  specializedRun();
}

void ConnectOperation::attemptSucceeded(OperationResult result) {
  ++attempts_made_;
  completeOperation(result);
}

ConnectOperation* ConnectOperation::specializedRun() {
  if (!async_client()->runInThread([this]() {
        if (attempts_made_ == 0) {
          conn()->initialize();
        } else {
          conn()->initMysqlOnly();
        }
        removeClientReference();
        if (!conn()->mysql()) {
          setAsyncClientError("connection initialization failed");
          attemptFailed(OperationResult::Failed);
          return;
        }

        mysql_options(conn()->mysql(), MYSQL_OPT_CONNECT_ATTR_RESET, 0);
        for (const auto& kv : connection_attributes_) {
          mysql_options4(conn()->mysql(),
                         MYSQL_OPT_CONNECT_ATTR_ADD,
                         kv.first.c_str(),
                         kv.second.c_str());
        }

        bool res =
            mysql_real_connect_nonblocking_init(conn()->mysql(),
                                                conn_key_.host.c_str(),
                                                conn_key_.user.c_str(),
                                                conn_key_.password.c_str(),
                                                conn_key_.db_name.c_str(),
                                                conn_key_.port,
                                                nullptr,
                                                flags_);
        if (res == 0 || async_client()->delicate_connection_failure_) {
          setAsyncClientError("mysql_real_connect_nonblocking_init failed");
          attemptFailed(OperationResult::Failed);
          return;
        }
        conn()->socketHandler()->setOperation(this);

        // connect is immediately "ready" to do one loop
        socketActionable();
      })) {
    completeOperationInner(OperationResult::Failed);
  }
  return this;
}

ConnectOperation::~ConnectOperation() { removeClientReference(); }

void ConnectOperation::socketActionable() {
  CHECK_EQ(std::this_thread::get_id(), async_client()->threadId());
  int error;
  CHECK_THROW(conn()->mysql()->async_op_status == ASYNC_OP_CONNECT,
              InvalidConnectionException);
  net_async_status status =
      mysql_real_connect_nonblocking_run(conn()->mysql(), &error);
  auto fd = mysql_get_file_descriptor(conn()->mysql());
  if (status == NET_ASYNC_COMPLETE) {
    if (error == 0) {
      if (fd <= 0) {
        LOG(ERROR) << "Unexpected invalid file descriptor on completed, "
                   << "errorless connect.  fd=" << fd;
        setAsyncClientError(
            "mysql_get_file_descriptor returned an invalid "
            "descriptor");
        attemptFailed(OperationResult::Failed);
        return;
      } else {
        conn()->socketHandler()->changeHandlerFD(fd);
        conn()->mysqlConnection()->connectionOpened();
        attemptSucceeded(OperationResult::Succeeded);
        return;
      }
    } else {
      snapshotMysqlErrors();
      attemptFailed(OperationResult::Failed);
      return;
    }
  } else {
    if (fd <= 0) {
      LOG(ERROR) << "Unexpected invalid file descriptor on completed, "
                 << "pending connect.  fd=" << fd;
      setAsyncClientError(
          "mysql_get_file_descriptor returned an invalid "
          "descriptor");
      attemptFailed(OperationResult::Failed);
      return;
    } else {
      conn()->socketHandler()->changeHandlerFD(fd);
      waitForSocketActionable();
      return;
    }
  }
  DCHECK(false) << "should not reach here";
}

void ConnectOperation::specializedTimeoutTriggered() {
  auto delta = chrono::high_resolution_clock::now() - start_time_;
  int64_t delta_micros =
      chrono::duration_cast<chrono::microseconds>(delta).count();
  auto msg =
      folly::stringPrintf("async connect to %s:%d timed out (took %.2fms)",
                          host().c_str(),
                          port(),
                          delta_micros / 1000.0);
  setAsyncClientError(CR_SERVER_LOST, msg);
  attemptFailed(OperationResult::TimedOut);
}

void ConnectOperation::logConnectFailed(OperationResult result) {
  // If the connection wasn't initialized, it's because the operation
  // was cancelled before anything started, so we don't do the logs
  auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
      end_time_ - start_time_);
  if (conn()->hasInitialized()) {
    if (result == OperationResult::Succeeded) {
      async_client()->logConnectionSuccess(elapsed, conn()->getKey());
    } else {
      db::FailureReason reason = db::FailureReason::DATABASE_ERROR;
      if (result == OperationResult::TimedOut) {
        reason = db::FailureReason::TIMEOUT;
      } else {
        reason = db::FailureReason::CANCELLED;
      }
      async_client()->logConnectionFailure(
          reason, elapsed, conn()->getKey(), conn()->mysql());
    }
  }
}

void ConnectOperation::specializedCompleteOperation() {
  logConnectFailed(result_);
  // If connection_initialized_ is false the only way to complete the
  // operation is by cancellation
  DCHECK(conn()->hasInitialized() || result_ == OperationResult::Cancelled);

  conn()->setDefaultQueryTimeout(default_query_timeout_);

  conn()->notify();

  if (connect_callback_) {
    connect_callback_(*this);
    // Release callback since no other callbacks will be made
    connect_callback_ = nullptr;
  }
  // In case this operation didn't even get the chance to run, we still need
  // to remove the reference it added to the async client
  removeClientReference();
}

void ConnectOperation::mustSucceed() {
  run();
  wait();
  if (!ok()) {
    LOG(FATAL) << "Connect failed: " << mysql_error_;
  }
}

void ConnectOperation::removeClientReference() {
  if (active_in_client_) {
    // It's safe to call the client since we still have a ref counting
    // it won't die before it goes to 0
    active_in_client_ = false;
    async_client()->activeConnectionRemoved(&conn_key_);
  }
}

FetchOperation::FetchOperation(std::unique_ptr<ConnectionProxy>&& conn,
                               db::QueryType query_type)
    : Operation(std::move(conn)),
      num_fields_(0),
      num_rows_seen_(0),
      cancel_(false),
      num_queries_executed_(0),
      query_result_(folly::make_unique<QueryResult>(0)),
      fetch_action_(FetchAction::StartQuery),
      query_type_(query_type),
      query_executed_(false),
      mysql_query_result_(nullptr) {}

FetchOperation* FetchOperation::specializedRun() {
  if (!async_client()->runInThread([this]() {
        try {
          rendered_multi_query_ = renderedQuery();
          socketActionable();
        }
        catch (std::invalid_argument& e) {
          setAsyncClientError(string("Unable to parse Query: ") + e.what());
          completeOperation(OperationResult::Failed);
        }
      })) {
    completeOperationInner(OperationResult::Failed);
  }

  return this;
}

void FetchOperation::socketActionable() {
  CHECK_EQ(std::this_thread::get_id(), async_client()->threadId());

  // This loops runs the fetch actions required to successfully execute query,
  // request next results, fetch results, identify errors and complete operation
  // and queries.
  // All callbacks are done in the specialized methods that children must
  // override.
  // Some actions may request an action above it (like CompleteQuery may request
  // StartQuery) this is why we use this loop.
  while (1) {

    // When the fetch action is StartQuery it means either we need to execute
    // the query or ask for new results.
    // Next Actions:
    //  - StartQuery: may continue with StartQuery if socket not actionable, in
    //                this case socketActionable is exited;
    //  - CompleteOperation: if it fails to execute query or request next
    //                       results.
    //  - InitFetch: no errors during results request, so we initiate fetch.
    if (fetch_action_ == FetchAction::StartQuery) {
      int error = 0;
      int status = 0;

      if (query_executed_) {
        status = mysql_next_result_nonblocking(conn()->mysql(), &error);
      } else {
        status = mysql_real_query_nonblocking(conn()->mysql(),
                                              rendered_multi_query_.data(),
                                              rendered_multi_query_.size(),
                                              &error);
      }

      if (status != NET_ASYNC_COMPLETE) {
        waitForSocketActionable();
        return;
      }

      query_executed_ = true;
      query_result_ = folly::make_unique<QueryResult>(num_queries_executed_);
      if (error) {
        fetch_action_ = FetchAction::CompleteOperation;
        snapshotMysqlErrors();
      } else {
        fetch_action_ = FetchAction::InitFetch;
      }
    }

    // Prior fetch start we read the values that may indicate errors, rows to
    // fetch or not. The initialize from children classes is called either way
    // to signal that any other calls from now are regarding a new query.
    // Next Actions:
    //  - CompleteOperation: in case an error occurred
    //  - Fetch: there are rows to fetch in this query
    //  - CompleteQuery: no rows to fetch (complete query will read rowsAffected
    //                   and lastInsertId to add to result
    if (fetch_action_ == FetchAction::InitFetch) {
      mysql_query_result_ = mysql_use_result(conn()->mysql());
      num_fields_ = mysql_field_count(conn()->mysql());

      // Check to see if this an empty query or an error
      if (!mysql_query_result_ && num_fields_ > 0) {
        // Fail Query
        snapshotMysqlErrors();
        fetch_action_ = FetchAction::CompleteOperation;
      } else {
        bool rows_to_fetch = readMysqlFields();
        if (rows_to_fetch) {
          query_result_->setRowFields(row_fields_info_);
          fetch_action_ = FetchAction::Fetch;
        } else {
          fetch_action_ = FetchAction::CompleteQuery;
        }
      }
      specializedInitQuery();
    }

    // This action is going to stick around until all rows are fetched or an
    // error occurs. In case of all rows were fetched or error, slurpRows is
    // going to return true, it returns false when it needs to wait for socket
    // actionable. slurpRows calls specializedRowsFetched in children classes in
    // case we have new rows.
    // Next Actions:
    //  - Fetch: in case it needs to fetch more rows, we break the loop and wait
    //           for socketActionable to be called again
    //  - CompleteQuery: an error occurred or rows finished to fetch
    if (fetch_action_ == FetchAction::Fetch) {
      bool query_finished = slurpRows();
      if (query_finished) {
        snapshotMysqlErrors();
        fetch_action_ = FetchAction::CompleteQuery;
      } else {
        // Wait for socket actionable to continue fetch
        break;
      }
    }

    // In case the query has least started and finished by error or not, here
    // the final checks and data are gathered for the current query.
    // It checks if any errors occurred during query, and call children classes
    // to deal with their specialized query completion.
    // Next Actions:
    //  - StartQuery: There are more results and children is not opposed to it.
    //                QueryOperation child sets to CompleteOperation, since it
    //                is not supposed to receive more than one result.
    //  - CompleteOperation: In case an error occurred during query or there are
    //                       no more results to read.
    if (fetch_action_ == FetchAction::CompleteQuery) {
      if (mysql_errno_ != 0) {
        query_result_->setOperationResult(OperationResult::Failed);

        fetch_action_ = FetchAction::CompleteOperation;
        // finished with error and has no more results
        specializedCompleteQuery(false, false);
      } else {
        // Stash these away now (when we're in the async mysql thread).
        query_result_->setNumRowsAffected(mysql_affected_rows(conn()->mysql()));
        query_result_->setLastInsertId(mysql_insert_id(conn()->mysql()));
        query_result_->setOperationResult(OperationResult::Succeeded);

        auto more_results = mysql_more_results(conn()->mysql());
        fetch_action_ = more_results ? FetchAction::StartQuery
                                     : FetchAction::CompleteOperation;
        // Call it after setting the fetch_action_ so the child class can
        // decide if it wants to change the state
        specializedCompleteQuery(true, more_results);
        ++num_queries_executed_;
      }
      if (mysql_query_result_) {
        mysql_free_result(mysql_query_result_);
        mysql_query_result_ = nullptr;
      }
    }

    // Once this action is set, the operation is going to be completed no matter
    // the reason it was called. It exists the loop.
    if (fetch_action_ == FetchAction::CompleteOperation) {
      if (cancel_) {
        state_ = OperationState::Cancelling;
        completeOperation(OperationResult::Cancelled);
      } else if (mysql_errno_ != 0) {
        completeOperation(OperationResult::Failed);
      } else {
        completeOperation(OperationResult::Succeeded);
      }
      break;
    }
  }
}

namespace {
void appendRow(RowBlock* block,
               MYSQL_ROW mysql_row,
               unsigned long* field_lengths,
               int num_fields) {
  block->startRow();
  for (int i = 0; i < num_fields; ++i) {
    if (mysql_row[i] == nullptr) {
      block->appendNull();
    } else {
      block->appendValue(StringPiece(mysql_row[i], field_lengths[i]));
    }
  }
  block->finishRow();
}
}

bool FetchOperation::slurpRows() {
  bool query_finished = false;
  // Fetch current query
  RowBlock row_block(row_fields_info_);
  while (1) {
    MYSQL_ROW row;
    int status = mysql_fetch_row_nonblocking(mysql_query_result_, &row);
    if (status != NET_ASYNC_COMPLETE) {
      waitForSocketActionable();
      break;
    }
    last_row_time_ = chrono::high_resolution_clock::now();
    if (row == nullptr) {
      query_finished = true;
      break;
    }
    unsigned long* field_lengths = mysql_fetch_lengths(mysql_query_result_);
    appendRow(&row_block, row, field_lengths, row_fields_info_->numFields());
    ++num_rows_seen_;
  }

  // Process fetched rows
  if (!row_block.empty()) {
    specializedRowsFetched(std::move(row_block));
  }

  // Wait for socket actionable
  return query_finished;
}

bool FetchOperation::readMysqlFields() {
  std::vector<string> field_names;
  std::unordered_map<string, int> field_name_map;
  std::vector<uint64_t> mysql_field_flags;
  std::vector<enum_field_types> mysql_field_types;

  field_names.clear();
  field_name_map.clear();
  if (num_fields_ == 0) {
    return false;
  }

  field_names.reserve(num_fields_);
  for (int i = 0; i < num_fields_; ++i) {
    MYSQL_FIELD* mysql_field = mysql_fetch_field(mysql_query_result_);
    if (mysql_field) {
      field_names.emplace_back(mysql_field->name, mysql_field->name_length);
      mysql_field_flags.push_back(mysql_field->flags);
      mysql_field_types.push_back(mysql_field->type);
      field_name_map[mysql_field->name] = i;
    }
  }
  row_fields_info_ = std::make_shared<RowFields>(std::move(field_name_map),
                                                 std::move(field_names),
                                                 std::move(mysql_field_flags),
                                                 std::move(mysql_field_types));

  return true;
}

void FetchOperation::specializedTimeoutTriggered() {
  auto delta = chrono::high_resolution_clock::now() - start_time_;
  auto row_delta = chrono::high_resolution_clock::now() - last_row_time_;
  int64_t delta_micros =
      chrono::duration_cast<chrono::microseconds>(delta).count();
  int64_t row_delta_micros =
      chrono::duration_cast<chrono::microseconds>(row_delta).count();
  std::string msg;
  if (num_rows_seen_ > 0) {
    msg = folly::stringPrintf(
        "async query timed out (%lu rows, took %.2fms, "
        "last row time %.2fms)",
        num_rows_seen_,
        delta_micros / 1000.0,
        row_delta_micros / 1000.0);
  } else {
    msg =
        folly::stringPrintf("async query timed out (no rows seen, took %.2fms)",
                            delta_micros / 1000.0);
  }
  setAsyncClientError(CR_NET_READ_INTERRUPTED, msg);
  completeOperation(OperationResult::TimedOut);
}

void FetchOperation::specializedCompleteOperation() {
  // Stats for query
  if (result_ == OperationResult::Succeeded) {
    async_client()->logQuerySuccess(elapsed(),
                                    query_type_,
                                    num_queries_executed_,
                                    rendered_multi_query_,
                                    conn()->getKey());
  } else {
    db::FailureReason reason = db::FailureReason::DATABASE_ERROR;
    if (result_ == OperationResult::Cancelled) {
      reason = db::FailureReason::CANCELLED;
    } else {
      reason = db::FailureReason::TIMEOUT;
    }
    async_client()->logQueryFailure(reason,
                                    elapsed(),
                                    query_type_,
                                    num_queries_executed_,
                                    rendered_multi_query_,
                                    conn()->getKey(),
                                    conn()->mysql());
  }

  // Probably cancelled and wasn't initialized
  if (query_result_ == nullptr) {
    query_result_ = folly::make_unique<QueryResult>(num_queries_executed_);
  }
  query_result_->setOperationResult(result_);
  conn()->notify();
}

FetchOperation::~FetchOperation() {
  if (mysql_query_result_) {
    mysql_free_result(mysql_query_result_);
  }
}

void FetchOperation::mustSucceed() {
  run();
  wait();
  if (!ok()) {
    LOG(FATAL) << "Query failed: " << mysql_error_;
  }
}

QueryOperation::QueryOperation(std::unique_ptr<ConnectionProxy>&& conn,
                               Query&& query)
    : FetchOperation(std::move(conn), db::QueryType::MultiQuery),
      query_(std::move(query)) {}

folly::fbstring QueryOperation::renderedQuery() {
  CHECK_EQ(async_client()->threadId(), std::this_thread::get_id());
  return query_.render(conn()->mysql());
}

void QueryOperation::specializedInitQuery() {
  // Doesn't have anything to initialize
}

void QueryOperation::specializedRowsFetched(RowBlock&& row_block) {
  if (query_callback_) {
    query_result_->setPartialRows(std::move(row_block));
    query_callback_(
        *this, query_result_.get(), QueryCallbackReason::RowsFetched);
  } else {
    query_result_->appendRowBlock(std::move(row_block));
  }
}

void QueryOperation::specializedCompleteQuery(bool success, bool more_results) {
  if (more_results) {
    // Bad usage of QueryOperation, we are going to cancel the query
    cancel_ = true;
  }
  if (success) {
    query_result_->setPartial(false);
  }
  // We are not going to make callback to user now since this only one query,
  // we make when we finish the operation
  fetch_action_ = FetchAction::CompleteOperation;
}

void QueryOperation::specializedCompleteOperation() {
  // Do parent first
  FetchOperation::specializedCompleteOperation();

  if (query_callback_) {
    query_callback_(*this,
                    query_result_.get(),
                    result_ == OperationResult::Succeeded
                        ? QueryCallbackReason::Success
                        : QueryCallbackReason::Failure);
    // Release callback since no other callbacks will be made
    query_callback_ = nullptr;
  }
}

QueryOperation::~QueryOperation() {}

MultiQueryOperation::MultiQueryOperation(
    std::unique_ptr<ConnectionProxy>&& conn, std::vector<Query>&& queries)
    : FetchOperation(std::move(conn), db::QueryType::MultiQuery),
      queries_(std::move(queries)) {}

folly::fbstring MultiQueryOperation::renderedQuery() {
  CHECK_EQ(async_client()->threadId(), std::this_thread::get_id());
  return Query::renderMultiQuery(conn()->mysql(), queries_);
}

void MultiQueryOperation::specializedInitQuery() {
  // Doesn't have anything to initialize
}

void MultiQueryOperation::specializedRowsFetched(RowBlock&& row_block) {
  if (query_callback_) {
    query_result_->setPartialRows(std::move(row_block));
    query_callback_(
        *this, query_result_.get(), QueryCallbackReason::RowsFetched);
  } else {
    query_result_->appendRowBlock(std::move(row_block));
  }
}

void MultiQueryOperation::specializedCompleteQuery(bool success,
                                                   bool more_results) {
  query_result_->setPartial(false);
  if (success) {
    if (query_callback_) {
      query_callback_(
          *this, query_result_.get(), QueryCallbackReason::QueryBoundary);
    }
    // If it failed we don't make the callback here, we wait to callback in
    // completeOperation
  }
  if (!query_callback_ && more_results) {
    // leave the last one to emplace_back in specializedCompleteOperation
    query_results_.emplace_back(std::move(*query_result_.get()));
    query_result_.reset();
  }
}

void MultiQueryOperation::specializedCompleteOperation() {
  // If there is no query callback, we need to move the last row in
  // before calling specializedCompleteOperation -- otherwise,
  // specializedCompleteOperation will notify the operation as being
  // complete while we then try to mutate query_results_.
  if (!query_callback_) {
    if (result_ == OperationResult::Succeeded) {
      query_results_.emplace_back(std::move(*query_result_.get()));
      query_result_.reset();
    }
  }

  // Now complete the operation.
  FetchOperation::specializedCompleteOperation();

  // If there was a callback, it fires now.
  if (query_callback_) {
    query_callback_(*this,
                    query_result_.get(),
                    result_ == OperationResult::Succeeded
                        ? QueryCallbackReason::Success
                        : QueryCallbackReason::Failure);
    // Release callback since no other callbacks will be made
    query_callback_ = nullptr;
  }
}

MultiQueryOperation::~MultiQueryOperation() {}

folly::StringPiece Operation::resultString() const {
  return Operation::toString(result());
}

folly::StringPiece Operation::stateString() const {
  return Operation::toString(state());
}

folly::StringPiece Operation::toString(OperationState state) {
  switch (state) {
  case OperationState::Unstarted:
    return "Unstarted";
  case OperationState::Pending:
    return "Pending";
  case OperationState::Cancelling:
    return "Cancelling";
  case OperationState::Completed:
    return "Completed";
  }
  DCHECK(false)
      << "unable to convert state to string: " << static_cast<int>(state);
  return "Unknown state";
}

folly::StringPiece Operation::toString(OperationResult result) {
  switch (result) {
  case OperationResult::Succeeded:
    return "Succeeded";
  case OperationResult::Unknown:
    return "Unknown";
  case OperationResult::Failed:
    return "Failed";
  case OperationResult::Cancelled:
    return "Cancelled";
  case OperationResult::TimedOut:
    return "TimedOut";
  }
  DCHECK(false)
      << "unable to convert result to string: " << static_cast<int>(result);
  return "Unknown result";
}

Operation::OwnedConnection::OwnedConnection(
    std::unique_ptr<Connection>&& conn)
    : Operation::ConnectionProxy(), conn_(std::move(conn)) {}

std::unique_ptr<Connection>&&
Operation::OwnedConnection::releaseConnection() {
  return std::move(conn_);
}

std::unique_ptr<Connection>&&
Operation::ReferencedConnection::releaseConnection() {
  throw std::runtime_error("ReleaseConnection can't be called in sync mode");
}

std::unique_ptr<Connection> blockingConnectHelper(
    std::shared_ptr<ConnectOperation>& conn_op) {
  conn_op->run()->wait();
  if (!conn_op->ok()) {
    throw MysqlException(conn_op->result(),
                         conn_op->mysql_errno(),
                         conn_op->mysql_error(),
                         *conn_op->getKey(),
                         conn_op->elapsed());
  }

  return std::move(conn_op->releaseConnection());
}
}
}
} // namespace facebook::common::mysql_client
