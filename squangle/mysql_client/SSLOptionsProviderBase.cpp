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
