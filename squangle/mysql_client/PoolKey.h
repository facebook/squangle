/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "squangle/base/ConnectionKey.h"
#include "squangle/mysql_client/ConnectionOptions.h"

namespace facebook::common::mysql_client {

class PoolKey {
 public:
  // Hashes Connections and Operations waiting for connections based on basic
  // Connection info (ConnectionKey) and Connection Attributes.
  PoolKey(ConnectionKey conn_key, ConnectionOptions conn_opts)
      : connKey_(std::move(conn_key)), connOptions_(std::move(conn_opts)) {
    options_hash_ = folly::hash::hash_range(
        connOptions_.getAttributes().begin(),
        connOptions_.getAttributes().end());
    partial_hash_ =
        folly::hash::hash_combine(connKey_.partial_hash(), options_hash_);
    full_hash_ = folly::hash::hash_combine(connKey_.hash(), options_hash_);
  }

  FOLLY_NODISCARD bool operator==(const PoolKey& rhs) const noexcept {
    return full_hash_ == rhs.full_hash_ && options_hash_ == rhs.options_hash_ &&
        connKey_ == rhs.connKey_;
  }

  FOLLY_NODISCARD bool operator!=(const PoolKey& rhs) const noexcept {
    return !(*this == rhs);
  }

  FOLLY_NODISCARD bool partialCompare(const PoolKey& rhs) const noexcept {
    return partial_hash_ == rhs.partial_hash_ &&
        options_hash_ == rhs.options_hash_ &&
        connKey_.partialEqual(rhs.connKey_);
  }

  FOLLY_NODISCARD const ConnectionKey& getConnectionKey() const noexcept {
    return connKey_;
  }

  FOLLY_NODISCARD const ConnectionOptions& getConnectionOptions()
      const noexcept {
    return connOptions_;
  }

  FOLLY_NODISCARD size_t getHash() const noexcept {
    return full_hash_;
  }

  FOLLY_NODISCARD size_t getPartialHash() const noexcept {
    return partial_hash_;
  }

  FOLLY_NODISCARD size_t getOptionsHash() const noexcept {
    return options_hash_;
  }

 private:
  ConnectionKey connKey_;
  ConnectionOptions connOptions_;

  size_t options_hash_;
  size_t full_hash_;
  size_t partial_hash_;
};

struct PoolKeyStats {
  size_t open_connections;
  size_t pending_connections;
  size_t connection_limit;
};

std::ostream& operator<<(std::ostream& os, const PoolKey& key);

class PoolKeyHash {
 public:
  size_t operator()(const PoolKey& k) const {
    return k.getHash();
  }
};

class PoolKeyPartialHash {
 public:
  size_t operator()(const PoolKey& k) const {
    return k.getPartialHash();
  }

  bool operator()(const PoolKey& lhs, const PoolKey& rhs) const {
    return lhs.partialCompare(rhs);
  }
};

} // namespace facebook::common::mysql_client
