/*
 *  Copyright (c) 2015, Facebook, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */

/*
 * toFuture is our interface for database operation using folly::Future.
 * It's completely compatible with the `Operation` interface, so to use
 * futures all you need is to pass the `Operation`.
 *
 */

#ifndef COMMON_ASYNC_MYSQL_FUTURE_ADAPTER_H
#define COMMON_ASYNC_MYSQL_FUTURE_ADAPTER_H

#include "squangle/mysql_client/Operation.h"
#include "squangle/mysql_client/DbResult.h"

#include <folly/futures/Future.h>

namespace facebook {
namespace common {
namespace mysql_client {

typedef std::shared_ptr<ConnectOperation> ConnectOperation_ptr;
typedef std::shared_ptr<QueryOperation> QueryOperation_ptr;
typedef std::shared_ptr<MultiQueryOperation> MultiQueryOperation_ptr;

// Future for ConnectOperation
folly::Future<ConnectResult> toFuture(ConnectOperation* conn_op);
folly::Future<ConnectResult> toFuture(ConnectOperation_ptr conn_op);

// Future for QueryOperation
folly::Future<DbQueryResult> toFuture(QueryOperation* query_op);
folly::Future<DbQueryResult> toFuture(QueryOperation_ptr& query_op);

// Future for MultiQueryOperation
folly::Future<DbMultiQueryResult> toFuture(
    MultiQueryOperation* mquery_op);
folly::Future<DbMultiQueryResult> toFuture(
    MultiQueryOperation_ptr& mquery_op);
}
}
} // facebook::common::mysql_client

#endif // COMMON_ASYNC_MYSQL_FUTURE_ADAPTER_H
