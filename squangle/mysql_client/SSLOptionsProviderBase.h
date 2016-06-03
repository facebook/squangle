// Copyright 2004-present Facebook.  All rights reserved.

#pragma once

#include <wangle/client/ssl/SSLSession.h>

namespace folly {
class SSLContext;
}

namespace facebook {
namespace common {
namespace mysql_client {

/* Interface for an SSL connection options Provider */
class SSLOptionsProviderBase {
 public:
  virtual ~SSLOptionsProviderBase() {}

  // The SSL Context and Session options to be set for the connection
  virtual std::shared_ptr<folly::SSLContext> getSSLContext() = 0;

  virtual SSL_SESSION* getSSLSession() = 0;

  // this function is called when an SSL connection is successfully established
  virtual void storeSSLSession(wangle::SSLSessionPtr ssl_session) = 0;
};
}
}
}
