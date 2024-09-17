/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "squangle/mysql_client/FetchOperation.h"
#include "squangle/mysql_client/mysql_protocol/MysqlOperationImpl.h"

namespace facebook::common::mysql_client::mysql_protocol {

class MysqlFetchOperationImpl : public MysqlOperationImpl,
                                public FetchOperationImpl {
 public:
  explicit MysqlFetchOperationImpl(
      std::unique_ptr<MysqlOperationImpl::ConnectionProxy> conn)
      : OperationBase(std::move(conn)) {}

 protected:
  void specializedRunImpl();
  void specializedRun() override;

  // In actionable it is analyzed the action that is required to continue the
  // operation. For example, if the fetch action is StartQuery, it runs query or
  // requests more results depending if it had already ran or not the query. The
  // same process happens for the other FetchActions. The action member can be
  // changed in other member functions called in actionable to keep the fetching
  // flow running.
  void actionable() override;
  void specializedTimeoutTriggered() override;
  void specializedCompleteOperation() override;

  void cancel() {
    cancel_ = true;
    setFetchAction(FetchAction::CompleteQuery);
  }

  void pauseForConsumer() override;
  void resume() override;
  bool isPaused() const override;

 private:
  void resumeImpl();
  // Checks if the current thread has access to stream, or result data.
  bool isStreamAccessAllowed() const override;

  // Asynchronously kill a currently running query, returns
  // before the query is killed
  void killRunningQuery();

  bool cancel_ = false;
};

} // namespace facebook::common::mysql_client::mysql_protocol
