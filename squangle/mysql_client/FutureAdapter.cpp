/*
 *  Copyright (c) 2016, Facebook, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */

#include "squangle/mysql_client/FutureAdapter.h"
#include "squangle/mysql_client/AsyncHelpers.h"
#include "squangle/mysql_client/AsyncMysqlClient.h"

#include <folly/MoveWrapper.h>
#include <folly/futures/Promise.h>

namespace facebook {
namespace common {
namespace mysql_client {

folly::SemiFuture<ConnectResult> toSemiFuture(ConnectOperation_ptr conn_op) {
  return toSemiFuture(conn_op.get());
}

folly::SemiFuture<ConnectResult> toSemiFuture(ConnectOperation* conn_op) {
  folly::MoveWrapper<folly::Promise<ConnectResult>> promise;
  auto future = promise->getSemiFuture();

  conn_op->setCallback([promise](ConnectOperation& op) mutable {
    // TODO: Improve the creation of the result, it's like this just to test
    if (op.ok()) {
      // succeeded, let's build the result
      ConnectResult conn_res(
          op.releaseConnection(),
          op.result(),
          *op.getKey(),
          op.elapsed(),
          op.attemptsMade());
      promise->setValue(std::move(conn_res));
    } else {
      auto conn = op.releaseConnection();
      MysqlException excep(
          op.result(),
          op.mysql_errno(),
          op.mysql_error(),
          *conn->getKey(),
          op.elapsed());
      promise->setException(excep);
    }
  });
  conn_op->run();

  return future;
}

folly::SemiFuture<DbQueryResult> toSemiFuture(QueryOperation_ptr& query_op) {
  return toSemiFuture(query_op.get());
}

folly::SemiFuture<DbQueryResult> toSemiFuture(QueryOperation* query_op) {
  folly::MoveWrapper<folly::Promise<DbQueryResult>> promise;
  auto future = promise->getSemiFuture();

  QueryAppenderCallback appender_callback = [promise](
      QueryOperation& op,
      QueryResult query_result,
      QueryCallbackReason reason) mutable {
    if (reason == QueryCallbackReason::Success) {
      auto conn_key = *op.connection()->getKey();
      DbQueryResult result(
          std::move(query_result),
          op.numQueriesExecuted(),
          op.resultSize(),
          op.releaseConnection(),
          op.result(),
          conn_key,
          op.elapsed());
      promise->setValue(std::move(result));
    } else {
      auto conn = op.releaseConnection();
      QueryException excep(
          op.numQueriesExecuted(),
          op.result(),
          op.mysql_errno(),
          op.mysql_error(),
          *conn->getKey(),
          op.elapsed());
      promise->setException(excep);
    }
  };

  query_op->setCallback(resultAppender(appender_callback));
  query_op->run();
  return future;
}

folly::SemiFuture<DbMultiQueryResult> toSemiFuture(
    MultiQueryOperation_ptr& mquery_op) {
  return toSemiFuture(mquery_op.get());
}

folly::SemiFuture<DbMultiQueryResult> toSemiFuture(
    MultiQueryOperation* mquery_op) {
  folly::MoveWrapper<folly::Promise<DbMultiQueryResult>> promise;
  auto future = promise->getSemiFuture();

  MultiQueryAppenderCallback appender_callback = [promise](
      MultiQueryOperation& op,
      std::vector<QueryResult> query_results,
      QueryCallbackReason reason) mutable {
    if (reason == QueryCallbackReason::Success) {
      auto conn_key = *op.connection()->getKey();
      DbMultiQueryResult result(
          std::move(query_results),
          op.numQueriesExecuted(),
          op.resultSize(),
          op.releaseConnection(),
          op.result(),
          conn_key,
          op.elapsed());
      promise->setValue(std::move(result));
    } else {
      auto conn = op.releaseConnection();
      QueryException excep(
          op.numQueriesExecuted(),
          op.result(),
          op.mysql_errno(),
          op.mysql_error(),
          *conn->getKey(),
          op.elapsed());
      promise->setException(excep);
    }
  };

  mquery_op->setCallback(resultAppender(appender_callback));
  mquery_op->run();
  return future;
}

folly::Future<ConnectResult> toFuture(ConnectOperation_ptr conn_op) {
  return toFuture(conn_op.get());
}

folly::Future<ConnectResult> toFuture(ConnectOperation* conn_op) {
  return toFuture(toSemiFuture(conn_op));
}

folly::Future<DbQueryResult> toFuture(QueryOperation_ptr& query_op) {
  return toFuture(query_op.get());
}

folly::Future<DbQueryResult> toFuture(QueryOperation* query_op) {
  return toFuture(toSemiFuture(query_op));
}

folly::Future<DbMultiQueryResult> toFuture(MultiQueryOperation_ptr& mquery_op) {
  return toFuture(mquery_op.get());
}

folly::Future<DbMultiQueryResult> toFuture(MultiQueryOperation* mquery_op) {
  return toFuture(toSemiFuture(mquery_op));
}

folly::Future<ConnectResult> toFuture(folly::SemiFuture<ConnectResult>&& fut) {
  return std::move(fut).toUnsafeFuture();
}

folly::Future<DbQueryResult> toFuture(folly::SemiFuture<DbQueryResult>&& fut) {
  return std::move(fut).toUnsafeFuture();
}

folly::Future<DbMultiQueryResult> toFuture(
    folly::SemiFuture<DbMultiQueryResult>&& fut) {
  return std::move(fut).toUnsafeFuture();
}

}
}
} // facebook::common::mysql_client
