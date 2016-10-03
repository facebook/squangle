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
}
}
}
