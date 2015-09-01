#ifndef COMMON_ASYNC_MYSQL_SSL_SESSION_CACHE_H
#define COMMON_ASYNC_MYSQL_SSL_SESSION_CACHE_H

#include <string>

#include <folly/EvictingCacheMap.h>
#include "wangle/client/ssl/SSLSession.h"

namespace facebook {
namespace common {
namespace mysql_client {

/*
 * A simple LRU cache for SSL sessions.
 * */

class SSLSessionCache {
 public:
  explicit SSLSessionCache(std::size_t maxSize) : sessionMap_(maxSize) {}

  void setSSLSession(const std::string& host,
                     int port,
                     wangle::SSLSessionPtr ssl_session) {
    sessionMap_.set(std::make_pair(host, port), std::move(ssl_session));
  }

  SSL_SESSION* getSSLSession(const std::string& host, int port) {
    auto ssl_session_iter = sessionMap_.find(std::make_pair(host, port));
    return (ssl_session_iter != sessionMap_.end())
               ? ssl_session_iter->second.get()
               : nullptr;
  }

  size_t size() const {
    return sessionMap_.size();
  }

  bool removeSSLSession(const std::string& host, int port) {
    return sessionMap_.erase(make_pair(host, port)) > 0;
  }

 private:
  folly::EvictingCacheMap<std::pair<std::string, int>, wangle::SSLSessionPtr>
      sessionMap_;
};

}
}
} // facebook::common::mysql_client

#endif
