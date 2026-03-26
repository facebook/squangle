/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/coro/AsyncPipe.h>

namespace facebook::common::mysql_client {

// This class manages the combination of the AsyncPipe and the AsyncGenerator
// that is returned when an AsyncPipe is created.  Without this the call sites
// have to manage the two different objects.  When using this class you just
// have one object to manage.
template <typename Payload>
class AsyncPipeUsingGenerator {
 public:
  using Pipe = folly::coro::AsyncPipe<Payload>;
  using AsyncGen = folly::coro::AsyncGenerator<Payload&&>;

  AsyncPipeUsingGenerator() : AsyncPipeUsingGenerator(Pipe::create()) {}

  bool write(Payload val) {
    if (!pipe_) {
      throw std::runtime_error("Writing to closed pipe");
    }
    return pipe_->write(std::move(val));
  }

  folly::coro::Task<typename AsyncGen::NextResult> next() {
    if (!gen_) {
      co_yield folly::coro::co_error(
          std::runtime_error("Reading from closed pipe"));
    }

    auto resTry = co_await co_awaitTry(gen_->next());
    if (resTry.hasException()) {
      // Close pipe on exception
      gen_.reset();
      co_yield folly::coro::co_error(std::move(resTry.exception()));
    }

    auto res = std::move(*resTry);
    if (!res) {
      // Close pipe on no more results
      gen_.reset();
    }

    co_return res;
  }

  void closeWriter() {
    if (pipe_) {
      std::move(*pipe_).close();
      pipe_.reset();
    }
  }

  void closeWriter(folly::exception_wrapper ew) {
    if (pipe_) {
      std::move(*pipe_).close(std::move(ew));
      pipe_.reset();
    }
  }

  bool isReaderClosed() const noexcept {
    return !gen_;
  }

 private:
  explicit AsyncPipeUsingGenerator(std::pair<AsyncGen, Pipe> pair)
      : gen_(std::move(pair.first)), pipe_(std::move(pair.second)) {}

  std::optional<AsyncGen> gen_;
  std::optional<Pipe> pipe_;
};

} // namespace facebook::common::mysql_client
