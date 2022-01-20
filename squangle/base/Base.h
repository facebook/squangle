// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#pragma once

#include <chrono>

namespace facebook::common::mysql_client {

using Duration = std::chrono::duration<uint64_t, std::micro>;
using Timepoint = std::chrono::time_point<std::chrono::steady_clock>;

} // namespace facebook::common::mysql_client
