// Copyright 2004-present Facebook.  All rights reserved.

#pragma once

#include <folly/io/async/SSLContext.h>
#include <wangle/client/ssl/SSLSession.h>
#include "squangle/base/ConnectionKey.h"

namespace facebook {
namespace common {
namespace mysql_client {

/* Interface for an SSL connection options Provider */
class SSLOptionsProviderBase {
 public:
  virtual ~SSLOptionsProviderBase() {}

  // The SSL Context and Session options to be set for the connection
  virtual std::shared_ptr<folly::SSLContext> getSSLContext(
      const common::mysql_client::ConnectionKey* conn_key) = 0;

  virtual SSL_SESSION* getSSLSession(
      const common::mysql_client::ConnectionKey* conn_key) = 0;

  // this function is called when an SSL connection is successfully established
  virtual void storeSSLSession(
      wangle::SSLSessionPtr ssl_session,
      const common::mysql_client::ConnectionKey* conn_key) = 0;
};
}
}
}
