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

#include "absl/base/attributes.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/fd_reader.h"
#include "riegeli/bytes/fd_writer.h"

namespace riegeli {

// A new `Reader` reading from standard input (by default from the same source
// as fd 0, `std::cin`, and `stdin`).
//
// A `StdIn` must be explicitly closed or synced in order for its position to be
// synchronized to the actual standard input. Closing a `StdIn` does not close
// its file descriptor.
//
// Warning: synchronizing the position is feasible only if standard input
// supports random access, otherwise standard input will have an unpredictable
// amount of extra data consumed because of buffering. Nevertheless, closing
// a `StdIn` and then creating another in the same process preserves these
// pending data.
//
// At most one `StdIn` should be open at a time, and it should not be combined
// with accessing standard input by other means.
class StdIn : public FdReader<UnownedFd> {
 public:
  // Creates a closed `StdIn`.
  explicit StdIn(Closed) noexcept : FdReader(kClosed) {}

  // Will read from standard input.
  explicit StdIn(Options options = Options());

  StdIn(StdIn&& that) noexcept;
  StdIn& operator=(StdIn&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `StdIn`. This avoids
  // constructing a temporary `StdIn` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Options options = Options());

 protected:
  void Done() override;
};

// A new `Writer` writing to standard output (by default to the same destination
// as fd 1, `std::cout`, and `stdout`).
//
// In contrast to `std::cout` and `stdout`, `StdOut` is fully buffered (not line
// buffered) even if it refers to an interactive device.
//
// A `StdOut` must be explicitly closed or flushed, and `Close()` or `Flush()`
// must succeed, in order for its output to be guaranteed to be available in the
// actual standard output. Closing a `StdOut` does not close its file
// descriptor. Flushing a `StdOut` explicitly might be needed:
//  * Before reading from standard input, so that output written so far appears
//    before waiting for input.
//  * Before writing to standard error, so that output written to different
//    streams ultimately leading to the same destination appears in the correct
//    order.
//
// At most one `StdOut` should be open at a time, and it should not be combined
// with accessing standard output by other means at the same time. Switching
// between means requires closing the old `StdOut` or flushing the object
// becoming inactive (`std::cout.flush()`, `std::fflush(stdout)`) and may
// require repositioning the object becoming active (`std::cout.seekp()`,
// `std::fseek(stdout)`).
//
// As an alternative to `StdOut`, creating and later closing an
// `OStreamWriter(&std::cout)` makes it easier to combine writing to a `Writer`
// with accessing `std::cout`.
class StdOut : public FdWriter<UnownedFd> {
 public:
  // Creates a closed `StdOut`.
  explicit StdOut(Closed) noexcept : FdWriter(kClosed) {}

  // Will write to standard output.
  explicit StdOut(Options options = Options());

  StdOut(StdOut&& that) noexcept;
  StdOut& operator=(StdOut&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `StdOut`. This avoids
  // constructing a temporary `StdOut` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Options options = Options());
};

// A new `Writer` writing to standard error (by default to the same destination
// as fd 2, `std::cerr`, `std::clog`, and `stderr`).
//
// In contrast to `std::cerr` and `stderr`, `StdErr` is fully buffered (not
// unbuffered).
//
// A `StdErr` must be explicitly closed or flushed, and `Close()` or `Flush()`
// must succeed, in order for its output to be guaranteed to be available in the
// actual standard error. Closing a `StdErr` does not close its file descriptor.
// Flushing a `StdErr` explicitly might be needed after writing a complete
// message, so that it appears promptly.
//
// At most one `StdErr` should be open at a time, and it should not be combined
// with accessing standard error by other means at the same time. Switching
// between means requires closing the old `StdErr` or flushing the object
// becoming inactive (`std::clog.flush()`) and may require repositioning the
// object becoming active (`std::cerr.seekp()`, `std::clog.seekp()`,
// `std::fseek(stderr)`).
//
// As an alternative to `StdErr`, creating and later closing an
// `OStreamWriter(&std::cerr)` makes it easier to combine writing to a `Writer`
// with accessing `std::cerr`.
class StdErr : public FdWriter<UnownedFd> {
 public:
  // Creates a closed `StdErr`.
  explicit StdErr(Closed) noexcept : FdWriter(kClosed) {}

  // Will write to standard error.
  explicit StdErr(Options options = Options());

  StdErr(StdErr&& that) noexcept;
  StdErr& operator=(StdErr&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `StdErr`. This avoids
  // constructing a temporary `StdErr` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Options options = Options());
};

// Sets file descriptors used by future instances of `Std{In,Out,Err}` in the
// constructor. Restores the old value in the destructor. This affects global
// state and is not thread safe.
//
// This is intended for testing of code which hardcodes usage of standard
// streams. The preferred way of testing is to avoid redirecting standard
// streams, by letting the code take explicit parameters specifying a `Reader`
// or `Writer`, or their factory.
//
// Standard streams can also be redirected at a lower level, using `dup2()`.

class InjectedStdInFd {
 public:
  explicit InjectedStdInFd(int fd);

  InjectedStdInFd(const InjectedStdInFd&) = delete;
  InjectedStdInFd& operator=(const InjectedStdInFd&) = delete;

  ~InjectedStdInFd();

 private:
  int old_fd_;
  ChainBlock old_pending_;
};

class InjectedStdOutFd {
 public:
  explicit InjectedStdOutFd(int fd);

  InjectedStdOutFd(const InjectedStdOutFd&) = delete;
  InjectedStdOutFd& operator=(const InjectedStdOutFd&) = delete;

  ~InjectedStdOutFd();

 private:
  int old_fd_;
};

class InjectedStdErrFd {
 public:
  explicit InjectedStdErrFd(int fd);

  InjectedStdErrFd(const InjectedStdErrFd&) = delete;
  InjectedStdErrFd& operator=(const InjectedStdErrFd&) = delete;

  ~InjectedStdErrFd();

 private:
  int old_fd_;
};

// Implementation details follow.

inline StdIn::StdIn(StdIn&& that) noexcept
    : FdReader(static_cast<FdReader&&>(that)) {}

inline StdIn& StdIn::operator=(StdIn&& that) noexcept {
  FdReader::operator=(static_cast<FdReader&&>(that));
  return *this;
}

inline void StdIn::Reset(Closed) { FdReader::Reset(kClosed); }

inline StdOut::StdOut(StdOut&& that) noexcept
    : FdWriter(static_cast<FdWriter&&>(that)) {}

inline StdOut& StdOut::operator=(StdOut&& that) noexcept {
  FdWriter::operator=(static_cast<FdWriter&&>(that));
  return *this;
}

inline void StdOut::Reset(Closed) { FdWriter::Reset(kClosed); }

inline StdErr::StdErr(StdErr&& that) noexcept
    : FdWriter(static_cast<FdWriter&&>(that)) {}

inline StdErr& StdErr::operator=(StdErr&& that) noexcept {
  FdWriter::operator=(static_cast<FdWriter&&>(that));
  return *this;
}

inline void StdErr::Reset(Closed) { FdWriter::Reset(kClosed); }

}  // namespace riegeli

#endif  // RIEGELI_BYTES_STD_IO_H_
