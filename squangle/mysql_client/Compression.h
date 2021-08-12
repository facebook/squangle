// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#pragma once

#include <mysql.h> // @manual

namespace facebook::common::mysql_client {

// Compression algorithms supported by MySQL:
// https://fburl.com/diffusion/kq2fbmr2
enum CompressionAlgorithm {
  ZLIB,
  ZSTD,
  ZSTD_STREAM,
  LZ4F_STREAM,
};

bool setCompressionOption(MYSQL* mysql, CompressionAlgorithm algo);

#if MYSQL_VERSION_ID >= 80000
#if MYSQL_VERSION_ID < 80018
mysql_compression_lib getCompressionValue(CompressionAlgorithm algo);
#else
std::optional<CompressionAlgorithm> parseCompressionName(std::string_view name);
const std::string& getCompressionName(CompressionAlgorithm algo);
#endif
#endif

} // namespace facebook::common::mysql_client
