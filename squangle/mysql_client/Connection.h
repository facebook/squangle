/*
 *  Copyright (c) 2016, Facebook, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */
#ifndef COMMON_ASYNC_CONNECTION_H
#define COMMON_ASYNC_CONNECTION_H

#include <folly/String.h>
#include <mysql.h>
#include <chrono>

#include "squangle/base/ConnectionKey.h"

namespace facebook {
namespace common {
namespace mysql_client {

using folly::StringPiece;
using std::string;
using std::unordered_map;

class MysqlClientBase;
class AsyncConnectionPool;

typedef std::chrono::duration<uint64_t, std::micro> Duration;
typedef std::chrono::time_point<std::chrono::steady_clock> Timepoint;

// Holds the mysql connection for easier re use
class MysqlConnectionHolder {
 public:
  MysqlConnectionHolder(
      MysqlClientBase* client,
      MYSQL* mysql,
      const ConnectionKey conn_key,
      bool connection_already_open = false);

  // Closes the connection in hold
  virtual ~MysqlConnectionHolder();
  const string& host() const {
    return conn_key_.host;
  }
  int port() const {
    return conn_key_.port;
  }
  const string& user() const {
    return conn_key_.user;
  }
  const string& database() const {
    return conn_key_.db_name;
  }
  const string& password() const {
    return conn_key_.password;
  }
  MYSQL* mysql() const {
    return mysql_;
  }

  void setCreationTime(Timepoint creation_time) {
    creation_time_ = creation_time;
  }

  void setReusable(bool reusable) {
    can_reuse_ = reusable;
  }

  bool isReusable() {
    return can_reuse_;
  }

  // Don't close the mysql fd in the destructor. Useful when connections
  // are managed outside this library.
  void disableCloseOnDestroy() {
    close_fd_on_destroy_ = false;
  }

  // Returns whether or not the connection is in a transaction based on server
  // status
  bool inTransaction();

  Timepoint getCreationTime() {
    return creation_time_;
  }

  const ConnectionKey* getKey() {
    return &conn_key_;
  }

  void connectionOpened();

  bool isConnectionOpened() {
    return connection_opened_;
  }

  Timepoint getLastActivityTime() {
    return last_activity_time_;
  }

  void setLastActivityTime(Timepoint last_activity_time) {
    last_activity_time_ = last_activity_time;
  }

  // Useful for removing the raw mysql connection and leaving this class to be
  // destroyed without closing it
  MYSQL* stealMysql() {
    auto ret = mysql_;
    mysql_ = nullptr;
    return ret;
  }

 protected:
  // This constructor takes ownership of the origin holder and copies the data
  // from it, then steals the ownership of the MYSQL* connection. After that the
  // origin is deleted.
  explicit MysqlConnectionHolder(
      std::unique_ptr<MysqlConnectionHolder> from_holder);

  MysqlClientBase* client_;

 private:
  // Our MYSQL handle as well as a file descriptor used for
  // notification of completed operations.
  MYSQL* mysql_;
  const ConnectionKey conn_key_;
  Timepoint creation_time_;
  Timepoint last_activity_time_;
  bool connection_opened_ = false;
  bool close_fd_on_destroy_ = true;

  bool can_reuse_;

  // copy not allowed
  MysqlConnectionHolder() = delete;
  MysqlConnectionHolder(const MysqlConnectionHolder&) = delete;
};
}
}
} // facebook::common::mysql_client

#endif // COMMON_ASYNC_CONNECTION_H
