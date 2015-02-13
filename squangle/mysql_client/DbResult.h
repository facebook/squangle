/*
 *  Copyright (c) 2015, Facebook, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */
#ifndef COMMON_ASYNC_MYSQL_RESULT_H
#define COMMON_ASYNC_MYSQL_RESULT_H

#include "squangle/mysql_client/Row.h"
#include "squangle/mysql_client/Connection.h"
#include "squangle/base/ExceptionUtil.h"

#include <chrono>
#include <folly/Exception.h>

namespace facebook {
namespace common {
namespace mysql_client {

using facebook::db::Exception;
using facebook::db::OperationStateException;

enum class OperationResult;

// Basic info about the Operation and connection info, everything that is
// common between failure and success should come here.
class OperationResultBase {
 public:
  OperationResultBase(const ConnectionKey conn_key, Duration elapsed_time)
      : conn_key_(conn_key), elapsed_time_(elapsed_time) {}

  const ConnectionKey* getConnectionKey() const { return &conn_key_; }

  Duration elapsed() const { return elapsed_time_; }

 private:
  const ConnectionKey conn_key_;
  const Duration elapsed_time_;
};

// This exception represents a basic mysql error, either during a connection
// opening or querying, when it times out or a  mysql error happened
// (invalid host or query, disconnected during query, etc)
class MysqlException : public Exception,
                       public OperationResultBase {
 public:
  MysqlException(OperationResult failure_type,
                 int mysql_errno,
                 const std::string& mysql_error,
                 const ConnectionKey conn_key,
                 Duration elapsed_time);

  int mysql_errno() const { return mysql_errno_; }
  const std::string& mysql_error() const { return mysql_error_; }

  // Returns the type of error occured:
  // Cancelled: query it was cancelled by the client or user
  // Failed: generally a mysql error happened during the query,
  // more details will be in `mysql_errno`
  // Timeout: query timed out and the client timer was triggered
  OperationResult failureType() const { return failure_type_; }

 private:
  OperationResult failure_type_;

  int mysql_errno_;
  std::string mysql_error_;
};

// This exception represents a Query error, when it times out or a
// mysql error happened (invalid query, disconnected during query, etc)
class QueryException : public MysqlException {
 public:
  QueryException(int num_executed_queries,
                 OperationResult failure_type,
                 int mysql_errno,
                 const std::string& mysql_error,
                 const ConnectionKey conn_key,
                 Duration elapsed_time)
      : MysqlException(
            failure_type, mysql_errno, mysql_error, conn_key, elapsed_time),
        num_executed_queries_(num_executed_queries) {}

  // In case of MultiQuery was ran, some queries might have succeeded
  int numExecutedQueries() const { return num_executed_queries_; }

 private:
  int num_executed_queries_;
};

class Connection;

class DbResult : public OperationResultBase {
 public:
  DbResult(std::unique_ptr<Connection>&& conn,
           OperationResult result,
           const ConnectionKey conn_key,
           Duration elapsed_time);

  bool ok() const;

  // releases the connection that was used for the async operation.
  // Call this only in the future interface.
  std::unique_ptr<Connection> releaseConnection();

  OperationResult operationResult() const { return result_; }

 private:
  std::unique_ptr<Connection> conn_;

  OperationResult result_;
};

typedef DbResult ConnectResult;

template <typename SingleMultiResult>
class FetchResult : public DbResult {
 public:
  FetchResult(SingleMultiResult query_result,
              int num_queries_executed,
              std::unique_ptr<Connection>&& conn,
              OperationResult result,
              const ConnectionKey conn_key,
              Duration elapsed)
      : DbResult(std::move(conn), result, conn_key, elapsed),
        fetch_result_(std::move(query_result)),
        num_queries_executed_(num_queries_executed) {}

  int numQueriesExecuted() const { return num_queries_executed_; }

  const SingleMultiResult& queryResult() { return fetch_result_; }

  SingleMultiResult&& stealQueryResult() { return std::move(fetch_result_); }

 private:
  SingleMultiResult fetch_result_;

  int num_queries_executed_;
};

// A QueryResult encapsulates the data regarding a query, as rows fetched,
// last insert id, etc.
// It is intended to create a layer over a collection of RowBlock of the same
// query.
// It is not not given index access for sake of efficiency that in this
// collection.
//
// Iterator access is provided to loop between Row's. We give the rows
// sequencially in the same order they where added in RowBlocks.
//
// for (const auto& row : query_result) {
//  ...
// }
class QueryResult {
 public:
  class Iterator;

  explicit QueryResult(int queryNum);

  ~QueryResult() {}

  // Move Constructor
  QueryResult(QueryResult&& other) noexcept;

  // Move assignment
  QueryResult& operator=(QueryResult&& other);

  int queryNum() const { return query_num_; }

  bool hasRows() const { return num_rows_ > 0; }

  // Partial is set true when this has part of the results, as in a callback.
  // When the QueryResult has all the data for a query this is false, typically
  // when the query is completely ran before user starts capturing results.
  bool partial() const { return partial_; }

  // A QueryResult is ok when it is a partial result during the operation
  // given in callbacks or the query has succeeded.
  bool ok() const;

  // TODO#4890524: Remove this call
  bool succeeded() const;

  RowFields* getRowFields() const { return row_fields_info_.get(); }

  // Only call this if you are in a callback and really want the Rows.
  // If you want to iterate through rows just use the iterator class here.
  RowBlock& currentRowBlock() {
    CHECK_THROW(partial() && row_blocks_.size() == 1, OperationStateException);
    return row_blocks_[0];
  }

  // Only call this if you are in a callback and really want just the ownership
  // over the Rows. Another way to obtain the ownership is moving the
  // QueryResult to a location of your preference and the Rows are going to be
  // moved to the new location.
  // If you want to iterate through rows just use the iterator class here.
  RowBlock stealCurrentRowBlock() {
    CHECK_THROW(partial() && row_blocks_.size() == 1, OperationStateException);
    RowBlock ret(std::move(row_blocks_[0]));
    row_blocks_.clear();
    return ret;
  }

  // Only call this if the fetch operation has ended. There are two ways to own
  // the RowBlocks, you can either use this method or move QueryResult to a
  // location of your preference and the RowBlocks are going to be moved to
  // the new location as well. If you want to iterate through rows just use the
  // iterator class here.
  vector<RowBlock>&& stealRows() { return std::move(row_blocks_); }

  // Only call this if the fetch operation has ended.
  // If you want to iterate through rows just use the iterator class here.
  const vector<RowBlock>& rows() const { return row_blocks_; }

  void setRowBlocks(vector<RowBlock>&& row_blocks) {
    num_rows_ = 0;
    row_blocks_ = std::move(row_blocks);
    for (const auto& block : row_blocks_) {
      num_rows_ += block.numRows();
    }
  }

  void setOperationResult(OperationResult op_result);

  // Last insert id (aka mysql_insert_id).
  uint64_t lastInsertId() const { return last_insert_id_; }

  void setLastInsertId(uint64_t last_insert_id) {
    last_insert_id_ = last_insert_id;
  }

  // Number of rows affected (aka mysql_affected_rows).
  uint64_t numRowsAffected() const { return num_rows_affected_; }

  void setNumRowsAffected(uint64_t num_rows_affected) {
    num_rows_affected_ = num_rows_affected;
  }

  // This can be called for complete or partial results. It's going to return
  // the total of rows stored in the QueryResult.
  size_t numRows() const { return num_rows_; }

  size_t numBlocks() const { return row_blocks_.size(); }

  class Iterator
      : public boost::iterator_facade<Iterator,
                                      const Row,
                                      boost::single_pass_traversal_tag,
                                      const Row> {
   public:
    Iterator(const vector<RowBlock>* row_block_vector,
             size_t block_number,
             size_t row_number_in_block)
        : row_block_vector_(row_block_vector),
          current_block_number_(block_number),
          current_row_in_block_(row_number_in_block) {}

    void increment() {
      ++current_row_in_block_;
      if (current_row_in_block_ >=
          row_block_vector_->at(current_block_number_).numRows()) {
        ++current_block_number_;
        current_row_in_block_ = 0;
      }
    }
    const Row dereference() const {
      return row_block_vector_->at(current_block_number_)
          .getRow(current_row_in_block_);
    }
    bool equal(const Iterator& other) const {
      return (row_block_vector_ == other.row_block_vector_ &&
              current_block_number_ == other.current_block_number_ &&
              current_row_in_block_ == other.current_row_in_block_);
    }

   private:
    const vector<RowBlock>* row_block_vector_;
    size_t current_block_number_;
    size_t current_row_in_block_;
  };

  Iterator begin() const { return Iterator(&row_blocks_, 0, 0); }

  Iterator end() const { return Iterator(&row_blocks_, row_blocks_.size(), 0); }

  void setRowFields(std::shared_ptr<RowFields>& row_fields_info) {
    row_fields_info_ = row_fields_info;
  }

  void appendRowBlock(RowBlock&& block) {
    num_rows_ += block.numRows();
    row_blocks_.emplace_back(std::move(block));
  }

  void setPartialRows(RowBlock&& partial_row_blocks_) {
    row_blocks_.clear();
    num_rows_ = partial_row_blocks_.numRows();
    row_blocks_.emplace_back(std::move(partial_row_blocks_));
  }

  void setPartial(bool partial) { partial_ = partial; }

 private:
  std::shared_ptr<RowFields> row_fields_info_;
  int query_num_;
  bool partial_;

  uint64_t num_rows_;
  uint64_t num_rows_affected_;
  uint64_t last_insert_id_;

  OperationResult operation_result_;

  vector<RowBlock> row_blocks_;
};

typedef FetchResult<std::vector<QueryResult>> DbMultiQueryResult;
typedef FetchResult<QueryResult> DbQueryResult;
}
}
} // facebook::common::mysql_client

#endif // COMMON_ASYNC_MYSQL_ROW_H
