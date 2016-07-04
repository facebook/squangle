// Copyright 2004-present Facebook.  All rights reserved.
//

#pragma once

#include "squangle/mysql_client/AsyncMysqlClient.h"

#include <folly/Random.h>

namespace facebook {
namespace common {
namespace mysql_client {

class AsyncMysqlClientFactory {
 public:
  std::shared_ptr<AsyncMysqlClient> makeClient() {
    return std::shared_ptr<AsyncMysqlClient>(
        new AsyncMysqlClient(), AsyncMysqlClient::deleter);
  }
};

/*
 * For MySQL heavy applications, we require more AsyncMysqlClient's.
 * Given that each spins a thread.
 * This is a simple round robin of pools.
 */
// TClientFactory is here just to allow the PoolOptions to be passed as well.
template <class TClient, class TClientFactory>
class ClientPool {
 public:
  explicit ClientPool(
      std::unique_ptr<TClientFactory> client_factory,
      size_t num_clients = 10) {
    if (num_clients == 0) {
      throw std::logic_error(
          "Invalid number of clients, it needs to be more than 0");
    }
    client_pool_.reserve(num_clients);
    for (int i = 0; i < num_clients; ++i) {
      client_pool_.emplace_back(client_factory->makeClient());
    }
  }

  // TODO: Allow sharding key to be passed
  std::shared_ptr<TClient> getClient() {
    auto idx = folly::Random::rand32() % client_pool_.size();
    return client_pool_[idx];
  }

  static std::shared_ptr<TClient> getClientFromDefault() {
    auto client_pool =
        folly::Singleton<ClientPool<TClient, TClientFactory>>::try_get();
    if (!client_pool) {
      throw std::logic_error(
          "MultiMysqlClientPool singleton has already been destroyed.");
    }
    return client_pool->getClient();
  }

 private:
  std::vector<std::shared_ptr<TClient>> client_pool_;
};
}
}
}
