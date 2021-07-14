/*
 *  Copyright (c) Facebook, Inc. and its affiliates..
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree.
 *
 */

#include "squangle/mysql_client/Connection.h"
#include "squangle/mysql_client/AsyncConnectionPool.h"
#include "squangle/mysql_client/AsyncMysqlClient.h"

namespace facebook {
namespace common {
namespace mysql_client {

MysqlConnectionHolder::MysqlConnectionHolder(
    MysqlClientBase* client,
    MYSQL* mysql,
    ConnectionKey conn_key,
    bool already_open)
    : client_(client),
      mysql_(mysql),
      conn_key_(std::move(conn_key)),
      conn_context_(nullptr),
      connection_opened_(already_open),
      can_reuse_(true) {
  client_->activeConnectionAdded(&conn_key_);
  creation_time_ = std::chrono::steady_clock::now();
}

MysqlConnectionHolder::MysqlConnectionHolder(
    std::unique_ptr<MysqlConnectionHolder> from_holder,
    const ConnectionKey* connKey)
    : client_(from_holder->client_),
      conn_key_((connKey) ? *connKey : from_holder->conn_key_),
      conn_context_(nullptr),
      creation_time_(from_holder->creation_time_),
      last_activity_time_(from_holder->last_activity_time_),
      connection_opened_(from_holder->connection_opened_),
      can_reuse_(from_holder->can_reuse_) {
  mysql_ = from_holder->stealMysql();
  client_->activeConnectionAdded(&conn_key_);
  if (from_holder->conn_context_) {
    conn_context_ = from_holder->conn_context_->copy();
  }
}

bool MysqlConnectionHolder::inTransaction() {
  return mysql_->server_status & SERVER_STATUS_IN_TRANS;
}

MysqlConnectionHolder::~MysqlConnectionHolder() {
  if (close_fd_on_destroy_ && mysql_) {
    auto mysql = mysql_;
    auto client = client_;
    // Close our connection in the thread from which it was created.
    if (!client_->runInThread([mysql]() {
        // Unregister server cert validation callback
        const void* callback{nullptr};
        mysql_options(mysql, MYSQL_OPT_TLS_CERT_CALLBACK, &callback);

        mysql_close(mysql);
    })) {
      LOG(DFATAL)
          << "Mysql connection couldn't be closed: error in folly::EventBase";
    }
    if (connection_opened_) {
      if (conn_context_) {
        client_->stats()->incrClosedConnections(
            conn_context_->getNormalValue("shardmap"),
            conn_context_->getNormalValue("endpoint_type"));
      } else {
        client_->stats()->incrClosedConnections(folly::none, folly::none);
      }
    }
  }
  client_->activeConnectionRemoved(&conn_key_);
}

void MysqlConnectionHolder::connectionOpened() {
  connection_opened_ = true;
  last_activity_time_ = std::chrono::steady_clock::now();

  if (conn_context_) {
    client_->stats()->incrOpenedConnections(
        conn_context_->getNormalValue("shardmap"),
        conn_context_->getNormalValue("endpoint_type"));
  } else {
    client_->stats()->incrOpenedConnections(folly::none, folly::none);
  }
}

} // namespace mysql_client
} // namespace common
} // namespace facebook
