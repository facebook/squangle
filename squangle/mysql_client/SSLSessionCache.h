#ifndef COMMON_ASYNC_MYSQL_SSL_SESSION_CACHE_H
#define COMMON_ASYNC_MYSQL_SSL_SESSION_CACHE_H

#include <folly/EvictingCacheMap.h>

#include <string>

class SessionDestructor {
 public:
  void operator()(SSL_SESSION* session) {
    if (session) {
      SSL_SESSION_free(session);
    }
  }
};

typedef std::unique_ptr<SSL_SESSION, SessionDestructor> SSLSessionPtr;

/*
 * A simple LRU cache for SSL sessions.
 * */

class SSLSessionCache {
 public:
  explicit SSLSessionCache(std::size_t maxSize) : sessionMap_(maxSize) {}

  void setSSLSession(const std::string& host,
                     int port,
                     SSLSessionPtr ssl_session) {
    sessionMap_.set(std::make_pair(host, port), std::move(ssl_session));
  }

  SSL_SESSION* getSSLSession(const std::string& host, int port) {
    auto ssl_session_iter = sessionMap_.find(std::make_pair(host, port));
    return (ssl_session_iter != sessionMap_.end())
               ? ssl_session_iter->second.get()
               : nullptr;
  }

 private:
  folly::EvictingCacheMap<std::pair<std::string, int>, SSLSessionPtr>
      sessionMap_;
};

#endif
