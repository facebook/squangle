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

#include <errmsg.h> // MySQL

#include <chrono>
#include <string>

#include "squangle/base/ConnectionKey.h"
#include "squangle/logger/DBEventCounter.h"

namespace facebook {
namespace db {

enum class FailureReason {
  BAD_USAGE,
  TIMEOUT,
  CANCELLED,
  DATABASE_ERROR,
};

enum class OperationType {
  None,
  Query,
  MultiQuery,
  MultiQueryStream,
  Connect,
  PoolConnect,
  TestDatabase
};

typedef std::function<void(folly::StringPiece key, folly::StringPiece value)>
    AddNormalValueFunction;
/*
 * Base class to allow dynamic logging data efficiently saved in Squangle core
 * classes. Should be used for data about the connection.
 */
class ConnectionContextBase {
 public:
  virtual ~ConnectionContextBase() {}
  virtual void collectNormalValues(AddNormalValueFunction add) const;
  bool isSslConnection = false;
  bool sslSessionReused = false;
};

typedef std::chrono::duration<uint64_t, std::micro> Duration;

struct SquangleLoggingData {
  SquangleLoggingData(const common::mysql_client::ConnectionKey* conn_key,
                      const ConnectionContextBase* conn_context)
      : connKey(conn_key), connContext(conn_context) {}
  const common::mysql_client::ConnectionKey* connKey;
  const ConnectionContextBase* connContext;
  db::ClientPerfStats clientPerfStats;
};

struct CommonLoggingData {
  CommonLoggingData(OperationType op, Duration duration)
      : operation_type(op), operation_duration(duration) {}
  OperationType operation_type;
  Duration operation_duration;
};

struct QueryLoggingData : CommonLoggingData {
  QueryLoggingData(
      OperationType op,
      Duration duration,
      int queries,
      const std::string& queryString,
      int rows)
      : CommonLoggingData(op, duration),
        queries_executed(queries),
        query(queryString),
        rows_received(rows) {}
  int queries_executed;
  std::string query;
  int rows_received;
};

// Base class for logging events of db client apis. This should be used as an
// abstract and the children have specific ways to log.
template <typename TConnectionInfo>
class DBLoggerBase {
 public:
  // The api name should be given to differentiate the kind of client being used
  // to contact DB.
  explicit DBLoggerBase(std::string api_name)
      : api_name_(std::move(api_name)) {}

  virtual ~DBLoggerBase() {}

  // Basic logging functions to be overloaded in children
  virtual void logQuerySuccess(
      const QueryLoggingData& loggingData,
      const TConnectionInfo& connInfo) = 0;

  virtual void logQueryFailure(
      const QueryLoggingData& logging_data,
      FailureReason reason,
      unsigned int mysqlErrno,
      const std::string& error,
      const TConnectionInfo& connInfo) = 0;

  virtual void logConnectionSuccess(
      const CommonLoggingData& logging_data,
      const TConnectionInfo& connInfo) = 0;

  virtual void logConnectionFailure(
      const CommonLoggingData& logging_data,
      FailureReason reason,
      unsigned int mysqlErrno,
      const std::string& error,
      const TConnectionInfo& connInfo) = 0;

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

  folly::StringPiece toString(OperationType operation_type) {
    switch (operation_type) {
      case OperationType::None:
        return "None";
      case OperationType::Query:
        return "Query";
      case OperationType::MultiQuery:
        return "MultiQuery";
      case OperationType::MultiQueryStream:
        return "MultiQueryStream";
      case OperationType::Connect:
        return "Connect";
      case OperationType::PoolConnect:
        return "PoolConnect";
      case OperationType::TestDatabase:
        return "TestDatabase";
    }
    return "(should not happen)";
  }

 protected:
  const std::string api_name_;
};

typedef DBLoggerBase<SquangleLoggingData> SquangleLoggerBase;
// This is a simple version of the base logger as an example for other versions.
class DBSimpleLogger : public SquangleLoggerBase {
 public:
  explicit DBSimpleLogger(std::string api_name)
      : DBLoggerBase(std::move(api_name)) {}

  virtual ~DBSimpleLogger() {}

  void logQuerySuccess(
      const QueryLoggingData& logging_data,
      const SquangleLoggingData& connInfo) override;

  void logQueryFailure(
      const QueryLoggingData& logging_data,
      FailureReason reason,
      unsigned int mysqlErrno,
      const std::string& error,
      const SquangleLoggingData& connInfo) override;

  void logConnectionSuccess(
      const CommonLoggingData& logging_data,
      const SquangleLoggingData& connInfo) override;

  void logConnectionFailure(
      const CommonLoggingData& logging_data,
      FailureReason reason,
      unsigned int mysqlErrno,
      const std::string& error,
      const SquangleLoggingData& connInfo) override;
};
}
}

#endif // COMMON_DB_EVENT_LOGGER_H
