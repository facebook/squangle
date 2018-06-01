// Copyright 2004-present Facebook.  All rights reserved.

#include "squangle/mysql_client/SSLOptionsProviderBase.h"
#include <folly/io/async/SSLContext.h>

namespace facebook {
namespace common {
namespace mysql_client {

bool SSLOptionsProviderBase::setMysqlSSLOptions(MYSQL* mysql) {
  auto sslContext = getSSLContext();
  if (!sslContext) {
    return false;
  }
  // We need to set ssl_mode because we set it to disabled after we call
  // mysql_init.
  enum mysql_ssl_mode ssl_mode = SSL_MODE_PREFERRED;
  mysql_options(mysql, MYSQL_OPT_SSL_MODE, &ssl_mode);
  mysql_options(mysql, MYSQL_OPT_SSL_CONTEXT, sslContext->getSSLCtx());
  auto sslSession = getSSLSession();
  if (sslSession) {
    mysql_options4(mysql, MYSQL_OPT_SSL_SESSION, sslSession, (void*)1);
  }
  return true;
}

bool SSLOptionsProviderBase::storeMysqlSSLSession(MYSQL* mysql) {
  auto reused = mysql_get_ssl_session_reused(mysql);
  if (!reused) {
    wangle::SSLSessionPtr session((SSL_SESSION*)mysql_get_ssl_session(mysql));
    if (session) {
      storeSSLSession(std::move(session));
    }
  }
  return reused;
}
}
}
}
