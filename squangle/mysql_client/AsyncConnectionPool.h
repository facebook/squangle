/*
 *  Copyright (c) 2015, Facebook, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */
//
// Asynchronous Connection Pool based on our async MySQL client.
//
// This pool offers na async way to acquire a connection by creating new ones
// or recycling an existing one. Also provides a way to limit the number of
// open connections per database/user and for the client.
//
// AsyncConnectionPool - This pool holds multiple MySQL connections and
//   manages them to make sure only healthy connections are given back.
//   The interface to request a connection works just like the
//   AsyncMysqlClient, an ConnectPoolOperation is started by `beginConnection`.
//
// ConnectPoolOperation - An abstraction of ConnectOperation that instead of
//   opening a new connection, requests a connection to the pool it was created
//   by. The usage and error treat are the same.

#ifndef COMMON_ASYNC_CONNECTION_POOL_H
#define COMMON_ASYNC_CONNECTION_POOL_H

#include <chrono>
#include <memory>
#include <unordered_map>
#include <list>
#include <folly/String.h>
#include <folly/futures/Future.h>

#include "squangle/mysql_client/Connection.h"
#include "squangle/mysql_client/Operation.h"
#include "squangle/mysql_client/AsyncMysqlClient.h"
#include "squangle/logger/DBEventCounter.h"

namespace facebook {
namespace common {
namespace mysql_client {

using db::PoolStats;
using std::string;
using std::unordered_map;
using folly::StringPiece;

class ConnectPoolOperation;
class AsyncConnectionPool;

// In order to keep always healthy connections avoid and avoid holding one
// connection for way too long, we have the options:
//   Age: Connection will be closed when reaches a limit from the tie it
// was opened. With this option the connections also get killed by idle time.
//   IdleTime: Doesn't close a connection due the total time it has been opened,
// only closes a connection due being idle for a given amount of time.
enum class ExpirationPolicy {
  Age,
  IdleTime,
  // TODO: Add KeepAlive option, this one will keep it alive and only close by
  // age
};

class PoolOptions {
 public:
  // Doing these the long way as we want to avoid depending on C++14 or FB's
  // TimeLiterals.h for Open Source.
  static constexpr Duration kDefaultMaxAge = std::chrono::seconds(60);
  static constexpr Duration kDefaultMaxIdleTime = std::chrono::seconds(4);
  static constexpr std::chrono::milliseconds kCleanUpTimeout =
    std::chrono::milliseconds(300);
  static const int kDefaultMaxOpenConn = 50;

  PoolOptions()
      : per_key_limit_(kDefaultMaxOpenConn),
        pool_limit_(kDefaultMaxOpenConn * 100),
        idle_timeout_(kDefaultMaxIdleTime),
        age_timeout_(kDefaultMaxAge),
        exp_policy_(ExpirationPolicy::Age) {}

  PoolOptions& setPerKeyLimit(int conn_limit) {
    per_key_limit_ = conn_limit;
    return *this;
  }
  PoolOptions& setPoolLimit(int total_limit) {
    pool_limit_ = total_limit;
    return *this;
  }
  PoolOptions& setIdleTimeout(Duration idle_timeout) {
    idle_timeout_ = idle_timeout;
    return *this;
  }
  PoolOptions& setAgeTimeout(Duration age_timeout) {
    age_timeout_ = age_timeout;
    return *this;
  }
  PoolOptions& setExpPolicy(ExpirationPolicy exp_policy) {
    exp_policy_ = exp_policy;
    return *this;
  }

  uint64_t getPerKeyLimit() const { return per_key_limit_; }
  uint64_t getPoolLimit() const { return pool_limit_; }
  Duration getIdleTimeout() const { return idle_timeout_; }
  Duration getAgeTimeout() const { return age_timeout_; }
  ExpirationPolicy getExpPolicy() const { return exp_policy_; }

 private:
  uint64_t per_key_limit_;
  uint64_t pool_limit_;
  Duration idle_timeout_;
  Duration age_timeout_;
  ExpirationPolicy exp_policy_;
};

class MysqlPooledHolder : public MysqlConnectionHolder {
 public:
  // Constructed based on an already existing MysqlConnectionHolder, the values
  // are going to be copied and the old holder will be destroyed.
  MysqlPooledHolder(std::unique_ptr<MysqlConnectionHolder> holder_base,
                    std::weak_ptr<AsyncConnectionPool> weak_pool);

  ~MysqlPooledHolder();

  void setLifeDuration(Duration dur) { good_for_ = dur; }

  Duration getLifeDuration() { return good_for_; }

  void setOwnerPool(std::weak_ptr<AsyncConnectionPool> pool);

 private:
  void removeFromPool();

  Duration good_for_;
  std::weak_ptr<AsyncConnectionPool> weak_pool_;
};

typedef std::list<std::unique_ptr<MysqlPooledHolder>> MysqlConnectionList;
typedef std::list<std::weak_ptr<ConnectPoolOperation>> PoolOpList;

//  This pool manages and creates mysql connections asynchronous using an async
// client and its event thread. Multiple pools can use the same client.
//  The pool MUST always be acquired by using `makePool`.
//
// How a connection is acquired:
//   First `beginConnection` is called as we always do with the client. Using
// the pool will only change the type of the returned Operation,
// `ConnectPoolOperation`. Different from ConnectOperation, instead of opening
// a new connection on `run()`, the pool operation will call
// `registerForConnection`. The pool will then check if there are any spare
// connections available to return or create a new one (based on the limits).
// In the event of not having any available connection later scenario, the
// operation will be queued until a connection is ready for it.
// With the connection in hands, the pool will assign it to the operation
// by calling `connectionCallback`. The last call will work like a
// `socketActionable` call triggering the complete operation procedures.
//   For the user the usage is the same as acquiring a connection using
// `AsyncMysqlClient`, but now using the pool.
class AsyncConnectionPool {
 public:
  // Don't use std::chrono::duration::MAX to avoid overflows
  static std::shared_ptr<AsyncConnectionPool> makePool(
      AsyncMysqlClient* mysql_client,
      const PoolOptions& pool_options = PoolOptions());

  // The destructor will start the shutdown phase
  ~AsyncConnectionPool();

  folly::Future<ConnectResult> connectFuture(
      const string& host,
      int port,
      const string& database_name,
      const string& user,
      const string& password,
      const ConnectionOptions& conn_opts = ConnectionOptions());

  folly::Future<ConnectResult> connectFuture(
      const string& host,
      int port,
      const string& database_name,
      const string& user,
      const string& password,
      const string& special_tag,
      const ConnectionOptions& conn_opts = ConnectionOptions());

  std::unique_ptr<Connection> connect(
      const string& host,
      int port,
      const string& database_name,
      const string& user,
      const string& password,
      const ConnectionOptions& conn_opts = ConnectionOptions());

  // Returns a ConnectPoolOperation that will abstract the wait for the client
  // to find or create a connection for the operation.
  // In shutting down mode, this will return a cancelled operation
  // (same as the client).
  std::shared_ptr<ConnectOperation> beginConnection(
      const string& host,
      int port,
      const string& database_name,
      const string& user,
      const string& password,
      const string& special_tag = "");

  // Returns the client that this pool is using
  AsyncMysqlClient* getMysqlClient() { return mysql_client_; }

  // It will clean the pool and block any new connections or operations
  // Shutting down phase:
  // Once the destructor is called, we lock `shutdown_mutex_`, set
  // shutting_down_ to true and schedule `cleanup_timer_` to cancel (this is the
  // real reason we need to wait for shutdown_condvar_). shutting_down_ will
  // avoid us to accept new requests for connection or try to recycle
  // connections
  // The remaining connections or operations that are linked to this pool
  // will know (using their weak_pointer to this pool) that the pool is dead
  // and proceed without the pool.
  void shutdown();

  PoolStats* stats() { return &pool_stats_; }

  // Don't use the constructor directly, only public to use make_shared
  AsyncConnectionPool(AsyncMysqlClient* mysql_client,
                      const PoolOptions& pool_options);

 protected:
  void setSelfWeakPointer(std::weak_ptr<AsyncConnectionPool> self_pointer) {
    self_pointer_ = self_pointer;
  }

 private:
  friend class Connection;
  friend class MysqlPooledHolder;
  friend class Operation;
  friend class ConnectPoolOperation;
  friend class FetchOperation;

  // Caches the connection in case it's marked as reusable (default). If the
  // connection is in a transaction or the user marked as not reusable, then
  // we close it.
  void recycleMysqlConnection(std::unique_ptr<MysqlConnectionHolder> mysqlConn);

  // Used by ConnectPoolOperation to register that this operation needs a
  // connection.
  // If there is a connection available, it will schedule a callback call in the
  // client thread. If the pool has no connections available, it will try to
  // create more depending on the amount of already open connection.
  // In case there is a chance for poolOp receive a connection, it will be
  // queued in the pool.
  // TODO#4527126: no chance it will be able to get a connection soon, so fail
  // fast.
  void registerForConnection(ConnectPoolOperation* poolOp);

  // Used internally when want a new connection. It checks if we should open
  // more connections, if so it creates ConnectOperation and once the
  // operation is completed the callback will call `addConnection`.
  // If the expiration policy is age, here we set the life limit of the
  // connection.
  // If we fail in creating the connection, `failedToConnect` will be called.
  // TODO#4527126: maybe have a cache for the unreachable hosts to fail faster
  // these connections.
  void tryRequestNewConnection(const ConnectionKey* connKey,
                               Duration timeout = std::chrono::seconds(10));

  // Used for when we fail to open a requested connection. In case of mysql
  // failure (e.g. bad password) we propagate the error to all queued
  // ConnectPoolOperation's.
  void failedToConnect(ConnectOperation& conn_op);

  // Anytime a connection is supposed to be added to the pool, being fresh or
  // recycled, we check if there is an operation in wait list for the
  // ConnectionKey inside MysqlPooledHolder, if there is, the match is made
  // here. Otherwise it is added to the pool.
  // At this point, the connection should already have been cleaned.
  // Should only be used internally
  void addConnection(std::unique_ptr<MysqlPooledHolder> mysqlConn,
                     bool brand_new);

  // Checks if the limits (global, connections open or being open by pool, or
  // limit per key) can fit one more connection. As a final check, checks if
  // it's a waste to create a new connection to avoid start opening a new
  // connection when we already have enough being open for the demand in queue.
  bool canCreateMoreConnections(const ConnectionKey* conn_key);

  ///////////// Counter control functions

  // Note that unlike the AsyncMysqlClient this similar counter is for open
  // connections only, the intent of opening a connect is controled separetely.
  void addOpenConnection(const ConnectionKey* conn_key);
  void addOpeningConn(const ConnectionKey* conn_key);

  void removeOpenConnection(const ConnectionKey* conn_key);
  void removeOpeningConn(const ConnectionKey* conn_key);

  void connectionSpotFreed(const ConnectionKey* conn_key);

  std::pair<uint64_t, uint64_t> getConnKeyStatus(const ConnectionKey* conn_key);

  // Auxiliary class to isolate the queue code. Clean ups also happen in this
  // class, it mainly manages the ConnectPoolOperation and
  // MysqlPooledHolder containers.
  class ConnStorage {
   public:
    explicit ConnStorage(std::thread::id allowed_threadid,
                         size_t conn_limit,
                         Duration max_idle_time)
        : allowed_thread_id_(allowed_threadid),
          conn_limit_(conn_limit),
          max_idle_time_(max_idle_time) {}

    ~ConnStorage() {}

    // Returns an shared pointer of the oldest valid operation in the queue for
    // the given ConnectionKey. The returned operation is removed from the
    // queue. We return shared to avoid the instance dying (for any reason)
    // before the connection is given to it.
    std::shared_ptr<ConnectPoolOperation> popOperation(
        const ConnectionKey* conn_key);

    // Puts the new operation in the end of the list
    void queueOperation(std::shared_ptr<ConnectPoolOperation>& poolOp);

    // Calls failureCallback with the error description and removed all
    // the operations for conn_key from the queue.
    void failOperations(const ConnectionKey* conn_key,
                        OperationResult op_result,
                        int mysql_errno,
                        const string& mysql_error);

    // Returns a connection for the given ConnectionKey. The connection will be
    // removed from the queue. Depending on the policy, it will give the oldest
    // inserted connection (fifo) or the most recent inserted (lifo).
    std::unique_ptr<MysqlPooledHolder> popConnection(
        const ConnectionKey* conn_key);

    // Puts the new connection in the back of the list.
    void queueConnection(std::unique_ptr<MysqlPooledHolder> newConn);

    // Checks and removes the connection that reached their idle time or age
    // limit.
    void cleanupConnections();

    // Checks and removes the weak ptrs that already expired, so we have a
    // better approximation of the number of operations really waiting.
    void cleanupOperations();

    // Cancels all operations that are still in the queue and clears all the
    // storage
    void clearAll();

    size_t numQueuedOperations(const ConnectionKey* conn_key) {
      CHECK_EQ(std::this_thread::get_id(), allowed_thread_id_);
      return waitList_[*conn_key].size();
    }

   private:
    // We keep a copy to check that all manipulation is coming from the
    // right thread.
    std::thread::id allowed_thread_id_;

    // This pool holds weak_ptr to the operation in wait list to avoid holding
    // async client in the draining process in case the operation has already
    // been discarded by the creator before got a connection. This also serves
    // to avoid giving them connections
    unordered_map<ConnectionKey, MysqlConnectionList> stock_;
    unordered_map<ConnectionKey, PoolOpList> waitList_;

    size_t conn_limit_;
    Duration max_idle_time_;

    // Maps the pointer address to the time that it should be expire, the clean
    // up of expired connections is called by `AsyncConnectionPool`
    std::unordered_map<MysqlPooledHolder*, Timepoint> idle_expiration_time_;
  } conn_storage_;

  class CleanUpTimer : public ata::TAsyncTimeout {
   public:
    explicit CleanUpTimer(ata::TEventBase* base, ConnStorage* pool);
    virtual void timeoutExpired() noexcept;

   private:
    ConnStorage* pool_;
  } cleanup_timer_;

  AsyncMysqlClient* mysql_client_;

  // per ConnectionKey
  const size_t conn_per_key_limit_;
  // Limit the total of open connections (or being opened)
  const size_t pool_conn_limit_;
  Duration connection_age_timeout_;
  ExpirationPolicy expiration_policy_;

  // Protects the read and writes of connection counters
  std::mutex counter_mutex_;

  uint32_t num_open_connections_ = 0;
  // Counts the number of open connections for a given connectionKey
  unordered_map<ConnectionKey, uint64_t> open_connections_;
  // Same as above but for connections that we are still opening
  // This one doesn't need locking, only accessed by client thread
  uint32_t num_pending_connections_ = 0;
  unordered_map<ConnectionKey, uint64_t> pending_connections_;

  // Used in the destructor to wait cleanup_timer_ be called. It's required by
  // `shutdown_condvar_`
  std::mutex shutdown_mutex_;

  // These members help with the control of the shutting down stage
  bool shutting_down_ = false;
  std::condition_variable shutdown_condvar_;
  std::atomic<bool> finished_shutdown_;

  // To allow us to pass weak_ptr for the PoolOperation`s
  std::weak_ptr<AsyncConnectionPool> self_pointer_;

  // Counters for connections created, cache hits and misses, etc.
  PoolStats pool_stats_;

  AsyncConnectionPool(const AsyncConnectionPool&) = delete;
  AsyncConnectionPool& operator=(const AsyncConnectionPool&) = delete;
};

class ConnectPoolOperation : public ConnectOperation {
 public:
  virtual ~ConnectPoolOperation() {}

  // Don't call this; it's public strictly for AsyncConnectionPool to be
  // able to call make_shared.
  ConnectPoolOperation(std::weak_ptr<AsyncConnectionPool> pool,
                       AsyncMysqlClient* client,
                       ConnectionKey conn_key)
      : ConnectOperation(client, conn_key), pool_(pool) {}

 protected:
  virtual void attemptFailed(OperationResult result) override;

  virtual ConnectPoolOperation* specializedRun();
  virtual void specializedTimeoutTriggered();
  virtual void socketActionable();

 private:
  // Called when the connection is matched by the pool client
  void connectionCallback(std::unique_ptr<MysqlPooledHolder> mysql_conn);
  // Called when the connection that the pool is trying to acquire failed
  void failureCallback(OperationResult failure,
                       int mysql_errno,
                       const string& mysql_error);

  std::weak_ptr<AsyncConnectionPool> pool_;

  friend class AsyncConnectionPool;
};
}
}
} // facebook::common::mysql_client

#endif // COMMON_ASYNC_CONNECTION_POOL_H
