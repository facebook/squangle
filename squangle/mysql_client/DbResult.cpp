/*
 *  Copyright (c) 2016, Facebook, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */

#include "squangle/mysql_client/DbResult.h"
#include "squangle/mysql_client/AsyncMysqlClient.h"
#include "squangle/mysql_client/Operation.h"

#include <ostream>

namespace facebook {
namespace common {
namespace mysql_client {

std::ostream& operator<<(
    std::ostream& stream,
    MultiQueryStreamHandler::State state) {
  stream << MultiQueryStreamHandler::toString(state);
  return stream;
}

MysqlException::MysqlException(OperationResult failure_type,
                               int mysql_errno,
                               const std::string& mysql_error,
                               const ConnectionKey conn_key,
                               Duration elapsed_time)
    : Exception(failure_type == OperationResult::Failed
                    ? folly::format(
                          "Mysql error {}. {}", mysql_errno, mysql_error).str()
                    : Operation::toString(failure_type)),
      OperationResultBase(conn_key, elapsed_time),
      failure_type_(failure_type),
      mysql_errno_(mysql_errno),
      mysql_error_(mysql_error) {}

bool DbResult::ok() const {
  return result_ == OperationResult::Succeeded;
}

DbResult::DbResult(
    std::unique_ptr<Connection>&& conn,
    OperationResult result,
    const ConnectionKey conn_key,
    Duration elapsed)
    : OperationResultBase(conn_key, elapsed),
      conn_(std::move(conn)),
      result_(result) {}

std::unique_ptr<Connection> DbResult::releaseConnection() {
  return std::move(conn_);
}

QueryResult::QueryResult(int query_num)
    : query_num_(query_num),
      partial_(true),
      num_rows_(0),
      num_rows_affected_(0),
      last_insert_id_(0),
      operation_result_(OperationResult::Unknown) {}

QueryResult::QueryResult(QueryResult&& other) noexcept
    : row_fields_info_(other.row_fields_info_),
      query_num_(other.query_num_),
      partial_(other.partial_),
      num_rows_(other.num_rows_),
      num_rows_affected_(other.num_rows_affected_),
      last_insert_id_(other.last_insert_id_),
      operation_result_(other.operation_result_),
      row_blocks_(std::move(other.row_blocks_)) {
  other.row_blocks_.clear();
  other.num_rows_ = 0;
}

QueryResult& QueryResult::operator=(QueryResult&& other) {
  if (this != &other) {
    row_fields_info_ = other.row_fields_info_;
    query_num_ = other.query_num_;
    partial_ = other.partial_;
    num_rows_ = other.num_rows_;
    num_rows_affected_ = other.num_rows_affected_;
    last_insert_id_ = other.last_insert_id_;
    operation_result_ = other.operation_result_;

    row_blocks_ = std::move(other.row_blocks_);
    other.row_blocks_.clear();
    other.num_rows_ = 0;
  }
  return *this;
}

bool QueryResult::ok() const {
  return (partial_ && operation_result_ == OperationResult::Unknown) ||
      operation_result_ == OperationResult::Succeeded;
}

bool QueryResult::succeeded() const {
  return operation_result_ == OperationResult::Succeeded;
}

void QueryResult::setOperationResult(OperationResult op_result) {
  operation_result_ = op_result;
}

StreamedQueryResult::StreamedQueryResult(
    MultiQueryStreamHandler* stream_handler,
    size_t query_idx)
    : stream_handler_(stream_handler), query_idx_(query_idx) {}

StreamedQueryResult::~StreamedQueryResult() {
  // In case of premature deletion.
  checkAccessToResult();
}

StreamedQueryResult::Iterator StreamedQueryResult::begin() {
  StreamedQueryResult::Iterator begin_it(this, 0);
  begin_it.increment();
  return begin_it;
}

StreamedQueryResult::Iterator StreamedQueryResult::end() {
  return StreamedQueryResult::Iterator(this, -1);
}

void StreamedQueryResult::Iterator::increment() {
  auto row = query_res_->nextRow();
  if (!row) {
    row_num_ = -1;
    return;
  }
  ++row_num_;
  current_row_ = std::move(row);
}

const EphemeralRow& StreamedQueryResult::Iterator::dereference() const {
  return *current_row_;
}

folly::Optional<EphemeralRow> StreamedQueryResult::nextRow() {
  checkStoredException();
  // Blocks when the stream is over and we need to handle IO
  auto current_row = stream_handler_->fetchOneRow(this);
  if (!current_row) {
    if (exception_wrapper_) {
      exception_wrapper_.throwException();
    }
  } else {
    ++num_rows_;
  }
  return current_row;
}

void StreamedQueryResult::checkStoredException() {
  if (exception_wrapper_) {
    exception_wrapper_.throwException();
  }
}

void StreamedQueryResult::checkAccessToResult() {
  checkStoredException();
  if (stream_handler_) {
    stream_handler_->fetchQueryEnd(this);
  }
}

void StreamedQueryResult::setResult(
    int64_t affected_rows,
    int64_t last_insert_id) {
  num_affected_rows_ = affected_rows;
  last_insert_id_ = last_insert_id;
}

void StreamedQueryResult::setException(folly::exception_wrapper ex) {
  exception_wrapper_ = ex;
}

void StreamedQueryResult::freeHandler() {
  stream_handler_.reset();
}

MultiQueryStreamHandler::MultiQueryStreamHandler(
    std::shared_ptr<MultiQueryStreamOperation> op)
    : operation_(op) {}

folly::Optional<StreamedQueryResult> MultiQueryStreamHandler::nextQuery() {
  if (state_ == State::RunQuery) {
    start();
  }

  // Runs in User thread
  state_baton_.wait();
  DCHECK(operation_->isPaused() || operation_->done());

  folly::Optional<StreamedQueryResult> res;
  // Accepted states: InitResult, OperationSucceeded or OperationFailed
  if (state_ == State::InitResult) {
    res.assign(StreamedQueryResult(this, ++curr_query_));
    resumeOperation();
  } else if (state_ == State::OperationFailed) {
    handleQueryFailed(nullptr);
  } else if (state_ != State::OperationSucceeded) {
    LOG(DFATAL) << "Bad state transition. Perhaps reading next result without"
                << " deleting or consuming current stream? Current state is "
                << toString(state_) << ".";
    handleBadState();
  }
  return res;
}

std::unique_ptr<Connection> MultiQueryStreamHandler::releaseConnection() {
  // Runs in User thread
  state_baton_.wait();
  if (state_ == State::OperationSucceeded || state_ == State::OperationFailed) {
    return operation_->releaseConnection();
  }

  exception_wrapper_ = folly::make_exception_wrapper<OperationStateException>(
      "Trying to release connection without consuming stream");
  LOG(DFATAL) << "Releasing the Connection without reading result. Read stream"
              << " content or delete stream result. Current state "
              << toString(state_) << ".";
  handleBadState();

  // Should throw above.
  return nullptr;
}

void MultiQueryStreamHandler::streamCallback(
    FetchOperation* op,
    StreamState op_state) {
  // Runs in IO Thread
  if (op_state == StreamState::InitQuery) {
    op->pauseForConsumer();
    state_ = State::InitResult;
  } else if (op_state == StreamState::RowsReady) {
    op->pauseForConsumer();
    state_ = State::ReadRows;
  } else if (op_state == StreamState::QueryEnded) {
    op->pauseForConsumer();
    state_ = State::ReadResult;
  } else if (op_state == StreamState::Success) {
    state_ = State::OperationSucceeded;
  } else {
    exception_wrapper_ = folly::make_exception_wrapper<QueryException>(
        op->numCurrentQuery(),
        op->result(),
        op->mysql_errno(),
        op->mysql_error(),
        *op->connection()->getKey(),
        op->elapsed());
    state_ = State::OperationFailed;
  }
  state_baton_.post();
}

folly::Optional<EphemeralRow> MultiQueryStreamHandler::fetchOneRow(
    StreamedQueryResult* result) {
  checkStreamedQueryResult(result);
  state_baton_.wait();
  // Accepted states: ReadRows, ReadResult, OperationFailed
  if (state_ == State::ReadRows) {
    if (!operation_->rowStream()->hasNext()) {
      resumeOperation();
      // Recursion to get `wait` and double check the stream.
      return fetchOneRow(result);
    }
    return folly::Optional<EphemeralRow>(
        operation_->rowStream()->consumeRow());
  }

  if (state_ == State::ReadResult) {
    handleQueryEnded(result);
  } else if (state_ == State::OperationFailed) {
    handleQueryFailed(result);
  } else {
    LOG(DFATAL) << "Bad state transition. Only ReadRows, ReadResult and "
                << "OperationFailed are allowed. Received " << toString(state_)
                << ".";
    handleBadState();
  }
  return folly::Optional<EphemeralRow>();
}

void MultiQueryStreamHandler::fetchQueryEnd(StreamedQueryResult* result) {
  checkStreamedQueryResult(result);
  state_baton_.wait();
  // Accepted states: ReadResult, OperationFailed
  if (state_ == State::ReadResult) {
    handleQueryEnded(result);
  } else if (state_ == State::OperationFailed) {
    handleQueryFailed(result);
  } else {
    LOG(DFATAL) << "Expected end of query, but received " << toString(state_)
                << ".";
    handleBadState();
  }
}

void MultiQueryStreamHandler::resumeOperation() {
  state_baton_.reset();
  operation_->resume();
}

void MultiQueryStreamHandler::handleQueryEnded(StreamedQueryResult* result) {
  result->setResult(
      operation_->currentAffectedRows(), operation_->currentLastInsertId());
  result->freeHandler();
  resumeOperation();
}

void MultiQueryStreamHandler::handleQueryFailed(StreamedQueryResult* result) {
  DCHECK(exception_wrapper_);
  if (result) {
    result->setException(exception_wrapper_);
  } else {
    exception_wrapper_.throwException();
  }
}

void MultiQueryStreamHandler::handleBadState() {
  operation_->cancel();
  resumeOperation();
}

void MultiQueryStreamHandler::start() {
  CHECK_EQ(state_, State::RunQuery);
  CHECK(operation_);
  operation_->setCallback(this);
  state_ = State::WaitForInitResult;
  operation_->run();
}

void MultiQueryStreamHandler::checkStreamedQueryResult(
    StreamedQueryResult* result) {
  CHECK_EQ(result->stream_handler_.get(), this);
  CHECK_EQ(result->query_idx_, curr_query_);
}

std::string MultiQueryStreamHandler::toString(State state) {
  switch (state) {
    case State::InitResult:
      return "InitResult";
    case State::ReadRows:
      return "ReadRows";
    case State::ReadResult:
      return "ReadResult";
    case State::OperationSucceeded:
      return "OperationSucceeded";
    case State::OperationFailed:
      return "OperationFailed";
    default:
      LOG(DFATAL) << "Illegal state";
  }
  return "Unknown";
}

MultiQueryStreamHandler::MultiQueryStreamHandler(
    MultiQueryStreamHandler&& other) noexcept {
  // Its OK to move another object only if we haven't
  // yet invoked nextQuery on it or if the operation
  // is done()
  CHECK(other.state_ == State::RunQuery || other.operation_->done());
  operation_ =  std::move(other.operation_);
}

MultiQueryStreamHandler::~MultiQueryStreamHandler() {
  CHECK(
      state_ == State::OperationSucceeded || state_ == State::OperationFailed);
  CHECK(operation_->done());
}

}
}
}
