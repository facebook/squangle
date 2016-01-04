/*
 *  Copyright (c) 2016, Facebook, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */

#include "squangle/mysql_client/AsyncHelpers.h"

namespace facebook {
namespace common {
namespace mysql_client {

QueryCallback resultAppender(const QueryAppenderCallback& callback) {
  // rowBlocks is deleted when the appended RowBlocks are passed to
  // QueryAppenderCallback
  auto* rowBlocks = new vector<RowBlock>();
  return [callback, rowBlocks](
      QueryOperation& op, QueryResult* res, QueryCallbackReason reason) {
    if (reason == QueryCallbackReason::RowsFetched) {
      rowBlocks->push_back(res->stealCurrentRowBlock());
    } else {
      // Failure or success, set rowblocks in the result and send it to callback
      // It's important to use this final result because of the other values as
      // lastInsertId, numRowsAffected. It also avoid forgetting to copy
      // something.
      res->setRowBlocks(std::move(*rowBlocks));
      delete rowBlocks;
      callback(op, std::move(*res), reason);
    }
  };
}

MultiQueryCallback resultAppender(const MultiQueryAppenderCallback& callback) {
  auto* rowBlocks = new vector<RowBlock>();
  auto* allResults = new vector<QueryResult>();
  return [callback, rowBlocks, allResults](
      MultiQueryOperation& op, QueryResult* res, QueryCallbackReason reason) {
    if (reason == QueryCallbackReason::RowsFetched) {
      rowBlocks->push_back(res->stealCurrentRowBlock());
    } else if (reason == QueryCallbackReason::QueryBoundary) {
      // wrap up one query
      res->setRowBlocks(std::move(*rowBlocks));
      allResults->push_back(std::move(*res));
      rowBlocks->clear();
    } else {
      callback(op, std::move(*allResults), reason);
      delete allResults;
      delete rowBlocks;
    }
  };
}
}
}
}
