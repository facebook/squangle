/*
 *  Copyright (c) 2015, Facebook, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */
#ifndef COMMON_DB_EXCEPTION_UTIL_H
#define COMMON_DB_EXCEPTION_UTIL_H

#include <folly/Conv.h>

#include <chrono>
#include <exception>
#include <string>

namespace facebook {
namespace db {

typedef std::chrono::duration<uint64_t, std::micro> Duration;

// Stolen from proxygen/Exception.h.
class Exception : public std::exception {
 public:
  explicit Exception(std::string const& msg) : msg_(msg) {}
  Exception(const Exception& other) : msg_(other.msg_) {}
  Exception(Exception&& other) noexcept : msg_(std::move(other.msg_)) {}

  template <typename... Args>
  explicit Exception(Args&&... args)
      : msg_(folly::to<std::string>(std::forward<Args>(args)...)) {}

  ~Exception() throw() override {}

  // std::exception methods
  const char* what() const throw() override {
    return msg_.c_str();
  }

 private:
  const std::string msg_;
};

class OperationStateException : public Exception {
 public:
  explicit OperationStateException(std::string const& msg) : Exception(msg) {}
};

class InvalidConnectionException : public Exception {
 public:
  explicit InvalidConnectionException(std::string const& msg)
      : Exception(msg) {}
};

class RequiredOperationFailedException : public Exception {
 public:
  explicit RequiredOperationFailedException(std::string const& msg)
      : Exception(msg) {}
};
} // namespace db
} // namespace facebook

#endif
