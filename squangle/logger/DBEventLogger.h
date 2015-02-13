/*
 *  Copyright (c) 2015, Facebook, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */
#ifndef COMMON_DB_EVENT_LOGGER_H
#define COMMON_DB_EVENT_LOGGER_H

#include <mysql.h>
#include <errmsg.h> // also MySQL

#include <chrono>
#include <string>

namespace facebook {
namespace db {

enum class FailureReason {
  BAD_USAGE,
  TIMEOUT,
  CANCELLED,
  DATABASE_ERROR,
};

enum class QueryType {
  Unknown, // Default value
  SingleQuery,
  MultiQuery,
};

typedef std::chrono::duration<uint64_t, std::micro> Duration;

// Base class for logging events of db client apis. This should be used as an
// abstract and the children have specific ways to log.
class DBLoggerBase {
 public:
  // The api name should be given to differentiate the kind of client being used
  // to contact DB.
  explicit DBLoggerBase(const std::string api_name) : api_name_(api_name) {}

  virtual ~DBLoggerBase() {}

  // Basic logging functions to be overloaded in children
  virtual void logQuerySuccess(Duration query_time,
                               QueryType query_type,
                               int queries_executed,
                               const std::string& query,
                               const std::string& db_hostname,
                               const std::string& user,
                               const std::string& db_name,
                               const int db_port) = 0;

  virtual void logQueryFailure(FailureReason reason,
                               Duration query_time,
                               QueryType query_type,
                               int queries_executed,
                               const std::string& query,
                               const std::string& db_hostname,
                               const std::string& user,
                               const std::string& db_name,
                               const int db_port,
                               MYSQL* mysqlConn) = 0;

  virtual void logConnectionSuccess(Duration connect_time,
                                    const std::string& db_hostname,
                                    const std::string& user,
                                    const std::string& db_name,
                                    const int db_port) = 0;

  virtual void logConnectionFailure(FailureReason reason,
                                    Duration connect_time,
                                    const std::string& db_hostname,
                                    const std::string& user,
                                    const std::string& db_name,
                                    const int db_port,
                                    MYSQL* mysqlConn) = 0;

  const char* QueryTypeString(QueryType type) {
    switch (type) {
    case QueryType::SingleQuery:
      return "SingleQuery";
    case QueryType::MultiQuery:
      return "MultiQuery";
    default:
      return "Unknown";
    }
    return "(should not happen)";
  }

  const char* FailureString(FailureReason reason) {
    switch (reason) {
    case FailureReason::BAD_USAGE:
      return "BadUsage";
    case FailureReason::TIMEOUT:
      return "Timeout";
    case FailureReason::CANCELLED:
      return "Cancelled";
    case FailureReason::DATABASE_ERROR:
      return "DatabaseError";
    }
    return "(should not happen)";
  }

 protected:
  std::string api_name_;
};

// This is a simple version of the base logger as an example for other versions.
class DBSimpleLogger : public DBLoggerBase {
 public:
  explicit DBSimpleLogger(const std::string api_name)
      : DBLoggerBase(api_name) {}

  virtual ~DBSimpleLogger() {}

  virtual void logQuerySuccess(Duration query_time,
                               QueryType query_type,
                               int queries_executed,
                               const std::string& query,
                               const std::string& db_hostname,
                               const std::string& user,
                               const std::string& db_name,
                               const int db_port);

  virtual void logQueryFailure(FailureReason reason,
                               Duration query_time,
                               QueryType query_type,
                               int queries_executed,
                               const std::string& query,
                               const std::string& db_hostname,
                               const std::string& user,
                               const std::string& db_name,
                               const int db_port,
                               MYSQL* mysqlConn);

  virtual void logConnectionSuccess(Duration connect_time,
                                    const std::string& db_hostname,
                                    const std::string& user,
                                    const std::string& db_name,
                                    const int db_port);

  virtual void logConnectionFailure(FailureReason reason,
                                    Duration connect_time,
                                    const std::string& db_hostname,
                                    const std::string& user,
                                    const std::string& db_name,
                                    const int db_port,
                                    MYSQL* mysqlConn);
};
}
}

#endif // COMMON_DB_EVENT_LOGGER_H
