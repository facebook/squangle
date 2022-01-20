// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#include <folly/ssl/Init.h>

#include "squangle/mysql_client/Connection.h"
#include "squangle/mysql_client/MysqlClientBase.h"

namespace {

// Used to initialize the SSL and MySQL libraries once per binary
class InitMysqlLibrary {
 public:
  InitMysqlLibrary() {
    folly::ssl::init();
    mysql_library_init(-1, nullptr, nullptr);
  }
};

} // namespace

namespace facebook::common::mysql_client {

MysqlClientBase::MysqlClientBase(
    std::unique_ptr<db::SquangleLoggerBase> db_logger,
    std::unique_ptr<db::DBCounterBase> db_stats)
    : db_logger_(std::move(db_logger)), client_stats_(std::move(db_stats)) {
  static InitMysqlLibrary unused;
}

void MysqlClientBase::logQuerySuccess(
    const db::QueryLoggingData& logging_data,
    const Connection& conn) {
  auto conn_context = conn.getConnectionContext();
  stats()->incrSucceededQueries(conn_context);

  if (db_logger_) {
    db_logger_->logQuerySuccess(
        logging_data, makeSquangleLoggingData(conn.getKey(), conn_context));
  }
}

void MysqlClientBase::logQueryFailure(
    const db::QueryLoggingData& logging_data,
    db::FailureReason reason,
    unsigned int mysqlErrno,
    const std::string& error,
    const Connection& conn) {
  auto conn_context = conn.getConnectionContext();
  stats()->incrFailedQueries(conn_context, mysqlErrno);

  if (db_logger_) {
    db_logger_->logQueryFailure(
        logging_data,
        reason,
        mysqlErrno,
        error,
        makeSquangleLoggingData(conn.getKey(), conn_context));
  }
}

void MysqlClientBase::logConnectionSuccess(
    const db::CommonLoggingData& logging_data,
    const ConnectionKey& conn_key,
    const db::ConnectionContextBase* connection_context) {
  if (db_logger_) {
    db_logger_->logConnectionSuccess(
        logging_data, makeSquangleLoggingData(&conn_key, connection_context));
  }
}

void MysqlClientBase::logConnectionFailure(
    const db::CommonLoggingData& logging_data,
    db::FailureReason reason,
    const ConnectionKey& conn_key,
    unsigned int mysqlErrno,
    const std::string& error,
    const db::ConnectionContextBase* connection_context) {
  stats()->incrFailedConnections(connection_context, mysqlErrno);

  if (db_logger_) {
    db_logger_->logConnectionFailure(
        logging_data,
        reason,
        mysqlErrno,
        error,
        makeSquangleLoggingData(&conn_key, connection_context));
  }
}

std::shared_ptr<ConnectOperation> MysqlClientBase::beginConnection(
    const std::string& host,
    int port,
    const std::string& database_name,
    const std::string& user,
    const std::string& password) {
  return beginConnection(
      ConnectionKey(host, port, database_name, user, password));
}

std::shared_ptr<ConnectOperation> MysqlClientBase::beginConnection(
    ConnectionKey conn_key) {
  auto ret = std::make_shared<ConnectOperation>(this, std::move(conn_key));
  if (connection_cb_) {
    ret->setObserverCallback(connection_cb_);
  }
  addOperation(ret);
  return ret;
}

std::unique_ptr<Connection> MysqlClientBase::adoptConnection(
    MYSQL* raw_conn,
    const std::string& host,
    int port,
    const std::string& database_name,
    const std::string& user,
    const std::string& password) {
  auto conn = createConnection(
      ConnectionKey(host, port, database_name, user, password), raw_conn);
  conn->socketHandler()->changeHandlerFD(
      folly::NetworkSocket::fromFd(mysql_get_socket_descriptor(raw_conn)));
  return conn;
}

} // namespace facebook::common::mysql_client
