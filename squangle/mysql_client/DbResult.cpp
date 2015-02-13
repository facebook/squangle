/*
 *  Copyright (c) 2015, Facebook, Inc.
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
}
}
}
