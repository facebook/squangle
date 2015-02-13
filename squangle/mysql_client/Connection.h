/*
 *  Copyright (c) 2015, Facebook, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */
#ifndef COMMON_ASYNC_CONNECTION_H
#define COMMON_ASYNC_CONNECTION_H

#include <chrono>
#include <folly/String.h>
#include <folly/Format.h>
#include <mysql.h>

namespace facebook {
namespace common {
namespace mysql_client {

typedef std::chrono::duration<uint64_t, std::micro> Duration;
typedef std::chrono::time_point<std::chrono::high_resolution_clock> Timepoint;

using folly::StringPiece;
using std::string;
using std::unordered_map;

class AsyncMysqlClient;
class AsyncConnectionPool;

// This class encapsulates the data that differentiates 2 connections:
// host, port, db name and user. We also store the password to avoid
// allowing a coonection with wrong password be accepted.
// We also store the key as string (wihtout the password and special tag)
// for debugging purposes and to use as keys in other maps
class ConnectionKey {
 public:
  const string host;
  const int port;
  const string db_name;
  const string user;
  // keeping password to avoid password error
  const string password;
  const string special_tag;
  const size_t hash;

  ConnectionKey(StringPiece sp_host,
                int sp_port,
                StringPiece sp_db_name,
                StringPiece sp_user,
                StringPiece sp_password,
                StringPiece sp_special_tag = "")
      : host(sp_host.toString()),
        port(sp_port),
        db_name(sp_db_name.toString()),
        user(sp_user.toString()),
        password(sp_password.toString()),
        special_tag(sp_special_tag.toString()),
        hash(folly::hash::hash_combine(sp_host.hash(),
                                       sp_port,
                                       sp_db_name.hash(),
                                       sp_user.hash(),
                                       sp_password.hash(),
                                       sp_special_tag.hash())) {}

  bool operator==(const ConnectionKey& rhs) const {
    return hash == rhs.hash && host == rhs.host && port == rhs.port &&
           db_name == rhs.db_name && user == rhs.user &&
           password == rhs.password && special_tag == rhs.special_tag;
  }

  bool operator!=(const ConnectionKey& rhs) const { return !(*this == rhs); }

  string getDisplayString() const {
    return folly::sformat("{} [{}] ({}@{}:{})",
                         db_name, special_tag, user, host, port);
  }
};

// Holds the mysql connection for easier re use
class MysqlConnectionHolder {
 public:
  MysqlConnectionHolder(AsyncMysqlClient* client,
                        MYSQL* mysql,
                        const ConnectionKey conn_key,
                        bool connection_already_open = false);

  // Closes the connection in hold
  virtual ~MysqlConnectionHolder();
  const string& host() const { return conn_key_.host; }
  int port() const { return conn_key_.port; }
  const string& user() const { return conn_key_.user; }
  const string& database() const { return conn_key_.db_name; }
  const string& password() const { return conn_key_.password; }
  MYSQL* mysql() const { return mysql_; }

  void setCreationTime(Timepoint creation_time) {
    creation_time_ = creation_time;
  }

  void setReusable(bool reusable) { can_reuse_ = reusable; }

  bool isReusable() { return can_reuse_; }

  // Returns whether or not the connection is in a transaction based on server
  // status
  bool inTransaction();

  Timepoint getCreationTime() { return creation_time_; }

  const ConnectionKey* getKey() { return &conn_key_; }

  void connectionOpened();

  // Useful for removing the raw mysql connection and leaving this class to be
  // destroyed without closing it
  MYSQL* stealMysql() {
    auto ret = mysql_;
    mysql_ = nullptr;
    return ret;
  }

 protected:
  // This contructor takes ownership of the origin holder and copies the data
  // from it, then steals the owenship of the MYSQL* connection. After that the
  // origin is deleted.
  explicit MysqlConnectionHolder(
      std::unique_ptr<MysqlConnectionHolder> from_holder);

  AsyncMysqlClient* async_client_;

 private:
  // Our MYSQL handle as well as a file descriptor used for
  // notification of completed operations.
  MYSQL* mysql_;
  const ConnectionKey conn_key_;
  Timepoint creation_time_;
  bool connection_opened_;

  // Amount of time that this connection can live
  bool can_reuse_;

  // copy not allowed
  MysqlConnectionHolder() = delete;
  MysqlConnectionHolder(const MysqlConnectionHolder&) = delete;
};
}
}
} // facebook::common::mysql_client

// make default template of unordered_map/unordered_set works for ConnectionKey
namespace std {
using facebook::common::mysql_client::ConnectionKey;
template <>
struct hash<ConnectionKey> {
  size_t operator()(const ConnectionKey& k) const { return k.hash; }
};
} // std

#endif // COMMON_ASYNC_CONNECTION_H
