#include "squangle/mysql_client/SyncMysqlClient.h"

namespace facebook {
namespace common {
namespace mysql_client {

std::unique_ptr<Connection> SyncMysqlClient::createConnection(
    ConnectionKey conn_key,
    MYSQL* mysql_conn) {
  return std::make_unique<SyncConnection>(this, conn_key, mysql_conn);
}

std::unique_ptr<Connection> SyncMysqlClient::adoptConnection(
    MYSQL* raw_conn,
    const std::string& host,
    int port,
    const std::string& database_name,
    const std::string& user,
    const std::string& password) {
  auto conn = MysqlClientBase::adoptConnection(
      raw_conn, host, port, database_name, user, password);
  return conn;
}

void SyncMysqlClient::adoptConnection(Connection& conn) {
  auto* syncConn = dynamic_cast<SyncConnection*>(&conn);
  CHECK(syncConn != nullptr);
  syncConn->mysql_client_ = this;
}
} // namespace mysql_client
} // namespace common
} // namespace facebook
