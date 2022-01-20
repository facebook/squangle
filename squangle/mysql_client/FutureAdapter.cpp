/*
 *  Copyright (c) Facebook, Inc. and its affiliates..
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree.
 *
 */

#include "squangle/mysql_client/FutureAdapter.h"
#include "folly/futures/Future.h"
#include "squangle/mysql_client/AsyncHelpers.h"
#include "squangle/mysql_client/AsyncMysqlClient.h"
#include "squangle/mysql_client/DbResult.h"

#include <folly/MoveWrapper.h>
#include <folly/futures/Promise.h>

namespace facebook {
namespace common {
namespace mysql_client {

folly::SemiFuture<ConnectResult> toSemiFuture(ConnectOperation_ptr conn_op) {
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

folly::SemiFuture<DbQueryResult> toSemiFuture(QueryOperation_ptr query_op) {
  // Use the pre-query callback if we have it, or else an empty SemiFuture
  auto sfut = query_op->callbacks_.pre_query_callback_
      ? query_op->callbacks_.pre_query_callback_(*query_op)
      : folly::makeSemiFuture(folly::unit);
  return std::move(sfut)
      .deferValue([query_op = std::move(query_op)](auto&& /* unused */) {
        folly::MoveWrapper<
            folly::Promise<std::pair<DbQueryResult, AsyncPostQueryCallback>>>
            promise;
        auto future = promise->getSemiFuture();

        QueryAppenderCallback appender_callback =
            [promise](
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
                promise->setValue(std::make_pair(
                    std::move(result),
                    std::move(op.callbacks_.post_query_callback_)));
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
      })
      .deferValue(
          [](std::pair<DbQueryResult, AsyncPostQueryCallback>&& resultPair) {
            if (resultPair.second) {
              // If we have a callback set, wrap (and then unwrap) the
              // result to/from the callback's std::variant wrapper
              return resultPair
                  .second(AsyncPostQueryResult(std::move(resultPair.first)))
                  .deferValue([](AsyncPostQueryResult&& result) {
                    return std::get<DbQueryResult>(std::move(result));
                  });
            }
            return folly::makeSemiFuture(std::move(resultPair.first));
          });
}

folly::SemiFuture<DbMultiQueryResult> toSemiFuture(
    MultiQueryOperation_ptr mquery_op) {
  // Use the pre-query callback if we have it, or else an empty SemiFuture

  auto sfut = mquery_op->callbacks_.pre_query_callback_
      ? mquery_op->callbacks_.pre_query_callback_(*mquery_op)
      : folly::makeSemiFuture(folly::unit);
  return std::move(sfut)
      .deferValue([mquery_op = std::move(mquery_op)](auto&& /* unused */) {
        folly::MoveWrapper<folly::Promise<
            std::pair<DbMultiQueryResult, AsyncPostQueryCallback>>>
            promise;
        auto future = promise->getSemiFuture();

        MultiQueryAppenderCallback appender_callback =
            [promise](
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
                promise->setValue(std::make_pair(
                    std::move(result),
                    std::move(op.callbacks_.post_query_callback_)));
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
      })
      .deferValue([](std::pair<DbMultiQueryResult, AsyncPostQueryCallback>&&
                         resultPair) {
        if (resultPair.second) {
          // If we have a callback set, wrap (and then unwrap) the
          // result to/from the callback's std::variant wrapper
          return resultPair
              .second(AsyncPostQueryResult(std::move(resultPair.first)))
              .deferValue([](AsyncPostQueryResult&& result) {
                return std::get<DbMultiQueryResult>(std::move(result));
              });
        }
        return folly::makeSemiFuture(std::move(resultPair.first));
      });
}

folly::Future<ConnectResult> toFuture(ConnectOperation_ptr conn_op) {
  return toFuture(toSemiFuture(std::move(conn_op)));
}

folly::Future<DbQueryResult> toFuture(QueryOperation_ptr query_op) {
  return toFuture(toSemiFuture(std::move(query_op)));
}

folly::Future<DbMultiQueryResult> toFuture(MultiQueryOperation_ptr mquery_op) {
  return toFuture(toSemiFuture(std::move(mquery_op)));
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

} // namespace mysql_client
} // namespace common
} // namespace facebook
