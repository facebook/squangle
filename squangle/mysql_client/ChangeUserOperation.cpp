/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "squangle/mysql_client/ChangeUserOperation.h"
#include "squangle/mysql_client/Connection.h"

namespace facebook::common::mysql_client {

InternalConnection::Status ChangeUserOperation::runSpecialOperation() {
  return conn().getInternalConnection().changeUser(key_);
}

} // namespace facebook::common::mysql_client
