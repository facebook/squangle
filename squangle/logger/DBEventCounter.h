/*
 *  Copyright (c) 2015, Facebook, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */
#ifndef COMMON_DB_CLIENT_STATS_H
#define COMMON_DB_CLIENT_STATS_H

#include <atomic>

namespace facebook {
namespace db {

class DBCounterBase {
 public:
  DBCounterBase() {}

  virtual ~DBCounterBase() {}

  // opened connections
  virtual void incrOpenedConnections() = 0;

  // closed connections
  virtual void incrClosedConnections() = 0;

  // failed connections
  virtual void incrFailedConnections() = 0;

  // query failures
  virtual void incrFailedQueries() = 0;

  // query successes
  virtual void incrSucceededQueries() = 0;
};

// Holds the stats for a client, thread safe and allows some good stats in
// tests
class SimpleDbCounter : public DBCounterBase {
 public:
  SimpleDbCounter()
      : DBCounterBase(),
        opened_connections_(0),
        closed_connections_(0),
        failed_connections_(0),
        failed_queries_(0),
        succeeded_queries_(0) {}

  // opened connections
  uint64_t numOpenedConnections() {
    return opened_connections_.load(std::memory_order_relaxed);
  }

  void incrOpenedConnections() {
    opened_connections_.fetch_add(1, std::memory_order_relaxed);
  }

  // closed connections
  uint64_t numClosedConnections() {
    return closed_connections_.load(std::memory_order_relaxed);
  }

  void incrClosedConnections() {
    closed_connections_.fetch_add(1, std::memory_order_relaxed);
  }

  // failed connections
  uint64_t numFailedConnections() {
    return failed_connections_.load(std::memory_order_relaxed);
  }

  void incrFailedConnections() {
    failed_connections_.fetch_add(1, std::memory_order_relaxed);
  }

  // query failures
  uint64_t numFailedQueries() {
    return failed_queries_.load(std::memory_order_relaxed);
  }

  void incrFailedQueries() {
    failed_queries_.fetch_add(1, std::memory_order_relaxed);
  }

  // query successes
  uint64_t numSucceededQueries() {
    return succeeded_queries_.load(std::memory_order_relaxed);
  }

  void incrSucceededQueries() {
    succeeded_queries_.fetch_add(1, std::memory_order_relaxed);
  }

  // For logging porpuses
  void printStats();

 private:
  std::atomic<uint64_t> opened_connections_;
  std::atomic<uint64_t> closed_connections_;
  std::atomic<uint64_t> failed_connections_;
  std::atomic<uint64_t> failed_queries_;
  std::atomic<uint64_t> succeeded_queries_;
};

class PoolStats {
 public:
  PoolStats()
      : created_pool_connections_(0),
        destroyed_pool_connections_(0),
        connections_requested_(0),
        pool_hits_(0),
        pool_misses_(0) {}
  // created connections
  uint64_t numCreatedPoolConnections() {
    return created_pool_connections_.load(std::memory_order_relaxed);
  }

  void incrCreatedPoolConnections() {
    created_pool_connections_.fetch_add(1, std::memory_order_relaxed);
  }
  // destroyed connections
  uint64_t numDestroyedPoolConnections() {
    return destroyed_pool_connections_.load(std::memory_order_relaxed);
  }

  void incrDestroyedPoolConnections() {
    destroyed_pool_connections_.fetch_add(1, std::memory_order_relaxed);
  }

  // Number of connect operation that were requests, this helps us to compare
  // this with the actual number of connections that were open
  uint64_t numConnectionsRequested() {
    return connections_requested_.load(std::memory_order_relaxed);
  }

  void incrConnectionsRequested() {
    connections_requested_.fetch_add(1, std::memory_order_relaxed);
  }

  // How many times the pool had a connection ready in the cache
  uint64_t numPoolHits() { return pool_hits_.load(std::memory_order_relaxed); }

  void incrPoolHits() { pool_hits_.fetch_add(1, std::memory_order_relaxed); }

  // how many times the pool didn't have a connection right in cache
  uint64_t numPoolMisses() {
    return pool_misses_.load(std::memory_order_relaxed);
  }

  void incrPoolMisses() {
    pool_misses_.fetch_add(1, std::memory_order_relaxed);
  }

 private:
  std::atomic<uint64_t> created_pool_connections_;
  std::atomic<uint64_t> destroyed_pool_connections_;
  std::atomic<uint64_t> connections_requested_;
  std::atomic<uint64_t> pool_hits_;
  std::atomic<uint64_t> pool_misses_;
};
}
} // facebook::db

#endif // COMMON_DB_CLIENT_STATS_H
