/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "squangle/mysql_client/FetchOperation.h"
#include "squangle/mysql_client/ConnectOperation.h"
#include "squangle/mysql_client/Connection.h"

namespace facebook::common::mysql_client {

FetchOperation::FetchOperation(
    std::unique_ptr<FetchOperationImpl> impl,
    std::vector<Query>&& queries)
    : FetchOperation(std::move(impl), MultiQuery(std::move(queries))) {}

FetchOperation::FetchOperation(
    std::unique_ptr<FetchOperationImpl> impl,
    MultiQuery&& multi_query)
    : queries_(std::move(multi_query)), impl_(std::move(impl)) {
  if (!impl_) {
    throw std::runtime_error("ConnectOperationImpl is null");
  }

  impl_->setOperation(*this);
}

RowStream::RowStream(
    std::unique_ptr<InternalResult> mysql_query_result,
    std::unique_ptr<InternalRowMetadata> metadata)
    : mysql_query_result_(std::move(mysql_query_result)),
      row_fields_(std::make_shared<EphemeralRowFields>(std::move(metadata))) {}

EphemeralRow RowStream::consumeRow() {
  if (!current_row_.has_value()) {
    LOG(DFATAL) << "Illegal operation";
  }
  EphemeralRow eph_row(std::move(*current_row_));
  current_row_.reset();
  return eph_row;
}

bool RowStream::hasNext() {
  // Slurp needs to happen after `consumeRow` has been called.
  // Because it will move the buffer.
  slurp();
  // First iteration
  return current_row_.has_value();
}

bool RowStream::slurp() {
  CHECK_THROW(mysql_query_result_ != nullptr, db::OperationStateException);
  if (current_row_.has_value() || query_finished_) {
    return true;
  }
  auto [result, row] = mysql_query_result_->fetchRow();
  if (result == PENDING) {
    return false;
  }

  if (row == nullptr) {
    query_finished_ = true;
    return true;
  }
  current_row_.assign(EphemeralRow(std::move(row), row_fields_));
  query_result_size_ += current_row_->calculateRowLength();
  ++num_rows_seen_;
  return true;
}

void FetchOperationImpl::setFetchAction(FetchAction action) {
  if (isPaused()) {
    paused_action_ = action;
  } else {
    active_fetch_action_ = action;
  }
}

const MultiQuery& FetchOperationImpl::queries() const {
  return getOp().queries();
}

const InternalConnection& FetchOperationImpl::getInternalConnection() const {
  return conn().getInternalConnection();
}

uint64_t FetchOperationImpl::currentLastInsertId() const {
  CHECK_THROW(isStreamAccessAllowed(), db::OperationStateException);
  return current_last_insert_id_;
}

uint64_t FetchOperationImpl::currentAffectedRows() const {
  CHECK_THROW(isStreamAccessAllowed(), db::OperationStateException);
  return current_affected_rows_;
}

const std::string& FetchOperationImpl::currentRecvGtid() const {
  CHECK_THROW(isStreamAccessAllowed(), db::OperationStateException);
  return current_recv_gtid_;
}

const AttributeMap& FetchOperationImpl::currentRespAttrs() const {
  CHECK_THROW(isStreamAccessAllowed(), db::OperationStateException);
  return current_resp_attrs_;
}

unsigned int FetchOperationImpl::currentWarningsCount() const {
  CHECK_THROW(isStreamAccessAllowed(), db::OperationStateException);
  return current_warnings_count_;
}

RowStream* FetchOperationImpl::rowStream() {
  CHECK_THROW(isStreamAccessAllowed(), db::OperationStateException);
  return current_row_stream_.get_pointer();
}

AttributeMap FetchOperationImpl::readResponseAttributes() {
  return conn().getResponseAttributes();
}

FetchOperation& FetchOperationImpl::getOp() const {
  DCHECK(op_ && dynamic_cast<FetchOperation*>(op_) != nullptr);
  return *(FetchOperation*)op_;
}

void FetchOperation::mustSucceed() {
  run().wait();
  if (!ok()) {
    throw db::RequiredOperationFailedException(
        "Query failed: " + mysql_error());
  }
}

folly::StringPiece FetchOperationImpl::toString(FetchAction action) {
  switch (action) {
    case FetchAction::StartQuery:
      return "StartQuery";
    case FetchAction::InitFetch:
      return "InitFetch";
    case FetchAction::Fetch:
      return "Fetch";
    case FetchAction::WaitForConsumer:
      return "WaitForConsumer";
    case FetchAction::CompleteQuery:
      return "CompleteQuery";
    case FetchAction::CompleteOperation:
      return "CompleteOperation";
  }
  LOG(DFATAL) << "unable to convert result to string: "
              << static_cast<int>(action);
  return "Unknown result";
}

} // namespace facebook::common::mysql_client
