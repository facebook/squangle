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

#include "folly/Random.h"

namespace facebook {
namespace db {

void DBSimpleLogger::logQuerySuccess(Duration query_time,
                                     QueryType query_type,
                                     int queries_executed,
                                     const std::string& query,
                                     const std::string& db_hostname,
                                     const std::string& user,
                                     const std::string& db_name,
                                     const int db_port) {
  VLOG(2) << "[" << api_name_ << "]"
          << " query (\"" << query << "\") succeeded.";
}

void DBSimpleLogger::logQueryFailure(FailureReason reason,
                                     Duration query_time,
                                     QueryType query_type,
                                     int queries_executed,
                                     const std::string& query,
                                     const std::string& db_hostname,
                                     const std::string& user,
                                     const std::string& db_name,
                                     const int db_port,
                                     MYSQL* mysqlConn) {
  VLOG(2) << "[" << api_name_ << "]"
          << " query (\"" << query << "\") failed.";
}

void DBSimpleLogger::logConnectionSuccess(Duration connect_time,
                                          const std::string& db_hostname,
                                          const std::string& user,
                                          const std::string& db_name,
                                          const int db_port) {
  VLOG(2) << "[" << api_name_ << "]"
          << " connection with " << db_hostname << " succeeded";
}

void DBSimpleLogger::logConnectionFailure(FailureReason reason,
                                          Duration connect_time,
                                          const std::string& db_hostname,
                                          const std::string& user,
                                          const std::string& db_name,
                                          const int db_port,
                                          MYSQL* mysqlConn) {
  VLOG(2) << "[" << api_name_ << "]"
          << " connection with " << db_hostname << " failed";
}
}
}
