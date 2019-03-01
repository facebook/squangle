/*
 *  Copyright (c) 2015, Facebook, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */
#include "squangle/logger/DBEventLogger.h"

#include <folly/Random.h>

namespace facebook {
namespace db {

void ConnectionContextBase::collectNormalValues(
    AddNormalValueFunction add) const {
  add("is_ssl", folly::to<std::string, bool>(isSslConnection));
  add("is_ssl_session_reused", folly::to<std::string, bool>(sslSessionReused));
}

void DBSimpleLogger::logQuerySuccess(
    const QueryLoggingData& data,
    const SquangleLoggingData&) {
  VLOG(2) << "[" << api_name_ << "]"
          << " query (\"" << data.query << "\") succeeded.";
}

void DBSimpleLogger::logQueryFailure(
    const QueryLoggingData& data,
    FailureReason,
    unsigned int,
    const std::string&,
    const SquangleLoggingData&) {
  VLOG(2) << "[" << api_name_ << "]"
          << " query (\"" << data.query << "\") failed.";
}

void DBSimpleLogger::logConnectionSuccess(
    const CommonLoggingData&,
    const SquangleLoggingData& connInfo) {
  VLOG(2) << "[" << api_name_ << "]"
          << " connection with " << connInfo.connKey->host << " succeeded";
}

void DBSimpleLogger::logConnectionFailure(
    const CommonLoggingData&,
    FailureReason,
    unsigned int,
    const std::string&,
    const SquangleLoggingData& connInfo) {
  VLOG(2) << "[" << api_name_ << "]"
          << " connection with " << connInfo.connKey->host << " failed";
}
}
}
