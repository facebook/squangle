/*
 *  Copyright (c) Facebook, Inc. and its affiliates.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree.
 *
 */
#include "squangle/logger/DBEventLogger.h"

#include <glog/logging.h>

#include <folly/Random.h>

namespace facebook {
namespace db {

void ConnectionContextBase::collectNormalValues(
    AddNormalValueFunction add) const {
  add("is_ssl", folly::to<std::string>(isSslConnection));
  add("is_ssl_session_reused", folly::to<std::string>(sslSessionReused));
  if (sslCertCn.hasValue()) {
    add("ssl_server_cert_cn", sslCertCn.value());
  }
  if (sslCertSan.hasValue() && !sslCertSan.value().empty()) {
    add("ssl_server_cert_san", folly::join(',', sslCertSan.value()));
  }
  if (sslCertIdentities.hasValue() && !sslCertIdentities.value().empty()) {
    add("ssl_server_cert_identities",
        folly::join(',', sslCertIdentities.value()));
  }
  if (!endpointVersion.empty()) {
    add("endpoint_version", endpointVersion);
  }
}

void ConnectionContextBase::collectIntValues(
    AddIntValueFunction add) const {
  add("ssl_server_cert_validated", isServerCertValidated ? 1 : 0);
}

folly::Optional<std::string> ConnectionContextBase::getNormalValue(
    folly::StringPiece key) const {
  if (key == "is_ssl") {
    return folly::to<std::string>(isSslConnection);
  } else if (key == "is_ssl_session_reused") {
    return folly::to<std::string>(sslSessionReused);
  } else if (key == "ssl_server_cert_cn") {
    return sslCertCn;
  } else if (key == "ssl_server_cert_san") {
    if (sslCertSan.hasValue()) {
      return folly::join(',', sslCertSan.value());
    } else {
      return folly::none;
    }
  } else if (key == "ssl_server_cert_identities") {
    if (sslCertIdentities.hasValue()) {
      return folly::join(',', sslCertIdentities.value());
    } else {
      return folly::none;
    }
  } else if (key == "endpoint_version" && !endpointVersion.empty()) {
    return endpointVersion;
  } else {
    return folly::none;
  }
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
} // namespace db
} // namespace facebook
