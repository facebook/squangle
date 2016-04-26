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
#include "squangle/mysql_client/Operation.h"
#include "squangle/mysql_client/AsyncMysqlClient.h"

namespace facebook {
namespace common {
namespace mysql_client {

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

bool DbResult::ok() const { return result_ == OperationResult::Succeeded; }

DbResult::DbResult(std::unique_ptr<Connection>&& conn,
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
    MultiQueryStreamHandler* stream_handler)
    : stream_handler_(stream_handler) {}

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
  auto current_row = stream_handler_->fetchOneRow();
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
    stream_handler_->fetchQueryEnd();
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
  stream_handler_ = nullptr;
}

MultiQueryStreamHandler::MultiQueryStreamHandler(
    std::shared_ptr<FetchOperation> op)
    : fetch_operation_(op) {}

std::unique_ptr<StreamedQueryResult> MultiQueryStreamHandler::nextQuery() {
  // Runs in User thread
  state_baton_.wait();
  DCHECK(fetch_operation_->isPaused() || fetch_operation_->done());

  std::unique_ptr<StreamedQueryResult> res;
  // Accepted states: InitResult, OperationSucceeded or OperationFailed
  if (state_ == State::InitResult) {
    res = folly::make_unique<StreamedQueryResult>(this);
    current_result_ = res.get();
    resumeOperation();
  } else if (state_ == State::OperationFailed) {
    handleQueryFailed();
  } else if (state_ != State::OperationSucceeded) {
    handleBadState();
  }
  return res;
}

std::unique_ptr<Connection> MultiQueryStreamHandler::releaseConnection() {
  // Runs in User thread
  state_baton_.wait();
  if (state_ == State::OperationSucceeded || state_ == State::OperationFailed) {
    return fetch_operation_->releaseConnection();
  }

  exception_wrapper_ = folly::make_exception_wrapper<OperationStateException>(
      "Trying to release connection without consuming stream");
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
    DCHECK(current_result_);
    op->pauseForConsumer();
    state_ = State::ReadRows;
  } else if (op_state == StreamState::QueryEnded) {
    DCHECK(current_result_);
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

folly::Optional<EphemeralRow> MultiQueryStreamHandler::fetchOneRow() {
  state_baton_.wait();
  // Accepted states: ReadRows, ReadResult, OperationFailed
  if (state_ == State::ReadRows) {
    if (!fetch_operation_->rowStream()->hasNext()) {
      this->resumeOperation();
      // Recursion to get `wait` and double check the stream.
      return fetchOneRow();
    }
    return folly::Optional<EphemeralRow>(
        fetch_operation_->rowStream()->consumeRow());
  }

  if (state_ == State::ReadResult) {
    handleQueryEnded();
  } else if (state_ == State::OperationFailed) {
    handleQueryFailed();
  } else {
    handleBadState();
  }
  return folly::Optional<EphemeralRow>();
}

void MultiQueryStreamHandler::fetchQueryEnd() {
  state_baton_.wait();
  // Accepted states: ReadResult, OperationFailed
  if (state_ == State::ReadResult) {
    handleQueryEnded();
  } else if (state_ == State::OperationFailed) {
    handleQueryFailed();
  } else {
    handleBadState();
  }
}

void MultiQueryStreamHandler::resumeOperation() {
  state_baton_.reset();
  fetch_operation_->resume();
}

void MultiQueryStreamHandler::handleQueryEnded() {
  current_result_->setResult(
      fetch_operation_->currentAffectedRows(),
      fetch_operation_->currentLastInsertId());
  current_result_->freeHandler();
  current_result_ = nullptr;
  resumeOperation();
}

void MultiQueryStreamHandler::handleQueryFailed() {
  DCHECK(exception_wrapper_);
  if (current_result_) {
    current_result_->setException(exception_wrapper_);
  } else {
    exception_wrapper_.throwException();
  }
}
void MultiQueryStreamHandler::handleBadState() {
  // TODO: This is for bad usage. We need to improve the error messages and
  // how to diagnose. Simple for now.
  LOG(DFATAL) << "Something went wrong. Waiting for rows but got "
              << toString(state_);
  fetch_operation_->cancel();
  resumeOperation();
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
}
}
}
