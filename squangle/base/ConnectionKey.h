/*
 *  Copyright (c) 2015, Facebook, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */

#pragma once

#include <folly/String.h>
#include <folly/Format.h>

namespace facebook {
namespace common {
namespace mysql_client {

// This class encapsulates the data that differentiates 2 connections:
// host, port, db name and user. We also store the password to avoid
// allowing a connection with wrong password be accepted.
// We also store the key as string (without the password and special tag)
// for debugging purposes and to use as keys in other maps
class ConnectionKey {
 public:
  const std::string host;
  const int port;
  const std::string db_name;
  const std::string user;
  // keeping password to avoid password error
  const std::string password;
  const std::string special_tag;
  const size_t hash;

  ConnectionKey(folly::StringPiece sp_host,
                int sp_port,
                folly::StringPiece sp_db_name,
                folly::StringPiece sp_user,
                folly::StringPiece sp_password,
                folly::StringPiece sp_special_tag = "")
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

  std::string getDisplayString() const {
    return folly::sformat("{} [{}] ({}@{}:{})", db_name, special_tag, user,
                          host, port);
  }
};
}
}
} // facebook::common::mysql_client

// make default template of unordered_map/unordered_set works for ConnectionKey
namespace std {
template <>
struct hash<facebook::common::mysql_client::ConnectionKey> {
  size_t operator()(
      const facebook::common::mysql_client::ConnectionKey& k) const {
    return k.hash;
  }
};
} // std
