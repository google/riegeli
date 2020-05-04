// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef RIEGELI_BYTES_STD_IO_H_
#define RIEGELI_BYTES_STD_IO_H_

#include <memory>
#include <utility>

#include "absl/strings/str_format.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Wraps `Writer*`, automatically flushing it in the destructor.
class FlushingWriterPtr {
 public:
  explicit FlushingWriterPtr(Writer* writer) noexcept : writer_(writer) {}

  FlushingWriterPtr(FlushingWriterPtr&& that) noexcept;
  FlushingWriterPtr& operator=(FlushingWriterPtr&& that) noexcept;

  ~FlushingWriterPtr();

  Writer* get() const { return writer_; }
  Writer& operator*() const { return *get(); }
  Writer* operator->() const { return get(); }

  /*implicit*/ operator Writer*() const { return get(); }

  // Support `absl::Format(flushing_writer_ptr, format, args...)`.
  /*implicit*/ operator absl::FormatRawSink() const {
    return absl::FormatRawSink(get());
  }

 private:
  Writer* writer_ = nullptr;
};

// A singleton `Reader` reading from standard input.
//
// The default `StdIn()` (unless `SetStdIn()` was used) does not support random
// access, and the initial position is assumed to be 0.
//
// Calling `StdIn()` automatically flushes `StdOut()` first.
Reader* StdIn();

// A singleton `Writer` writing to standard output.
//
// The default `StdOut()` (unless `SetStdOut()` was used)  does not support
// random access, and the initial position is assumed to be 0.
//
// `StdOut()` is automatically flushed at process exit.
//
// In contrast to `std::cout`, `StdOut()` is fully buffered (not line buffered)
// even if it refers to an interactive device.
Writer* StdOut();

// A singleton `Writer` writing to standard error.
//
// The default `StdErr()` (unless `SetStdErr()` was used)  does not support
// random access, and the initial position is assumed to be 0.
//
// Calling `StdErr()` automatically flushes `StdOut()` first.
//
// `StdErr()` is automatically flushed at the end of the full expression which
// calls `StdErr()`, and at process exit.
FlushingWriterPtr StdErr();

// Like `StdIn()`, but without automatically flushing `StdOut()` first.
Reader* JustStdIn();

// Like `StdErr()`, but without automatically flushing `StdOut()` first, and
// without automatically flushing `JustStdErr()` at the end of the full
// expression which calls `JustStdErr()`.
Writer* JustStdErr();

// Replaces `StdIn()` with a new `Reader`. Returns the previous `Reader`.
std::unique_ptr<Reader> SetStdIn(std::unique_ptr<Reader> value);

// Replaces `StdOut()` with a new `Writer`. Returns the previous `Writer`.
std::unique_ptr<Writer> SetStdOut(std::unique_ptr<Writer> value);

// Replaces `StdErr()` with a new `Writer`. Returns the previous `Writer`.
std::unique_ptr<Writer> SetStdErr(std::unique_ptr<Writer> value);

// Implementation details follow.

inline FlushingWriterPtr::FlushingWriterPtr(FlushingWriterPtr&& that) noexcept
    : writer_(std::exchange(that.writer_, nullptr)) {}

inline FlushingWriterPtr& FlushingWriterPtr::operator=(
    FlushingWriterPtr&& that) noexcept {
  writer_ = std::exchange(that.writer_, nullptr);
  return *this;
}

inline FlushingWriterPtr::~FlushingWriterPtr() {
  if (writer_ != nullptr) writer_->Flush(FlushType::kFromProcess);
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_STD_IO_H_
