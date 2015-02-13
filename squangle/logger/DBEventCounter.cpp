/*
 *  Copyright (c) 2015, Facebook, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */
#include "squangle/logger/DBEventCounter.h"
#include <glog/logging.h>

namespace facebook {
namespace db {

void SimpleDbCounter::printStats() {
  LOG(INFO) << "Client Stats\n"
            << "Opened Connections " << numOpenedConnections() << "\n"
            << "Closed Connections " << numClosedConnections() << "\n"
            << "Failed Queries " << numFailedQueries() << "\n"
            << "Succeeded Queries " << numSucceededQueries() << "\n";
}
}
} // facebook::db
