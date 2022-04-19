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

#include "absl/base/attributes.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/fd_reader.h"
#include "riegeli/bytes/fd_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// A new `Reader` reading from standard input (by default from the same source
// as fd 0, `std::cin`, and `stdin`).
//
// A `NewStdIn` must be explicitly closed or synced in order for its position to
// be synchronized to the actual standard input. Closing a `NewStdIn` does not
// close its file descriptor.
//
// Warning: synchronizing the position is feasible only if standard input
// supports random access, otherwise standard input will have an unpredictable
// amount of extra data consumed because of buffering. Nevertheless, closing
// a `NewStdIn` and then creating another in the same process preserves these
// pending data.
//
// At most one `NewStdIn` should be open at a time, and it should not be
// combined with accessing standard input by other means.
class NewStdIn : public FdReader<UnownedFd> {
 public:
  // Creates a closed `NewStdIn`.
  explicit NewStdIn(Closed) noexcept : FdReader(kClosed) {}

  // Will read from standard input.
  explicit NewStdIn(Options options = Options());

  NewStdIn(NewStdIn&& that) noexcept;
  NewStdIn& operator=(NewStdIn&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `NewStdIn`. This avoids
  // constructing a temporary `NewStdIn` and moving from it.
  void Reset(Closed);
  void Reset(Options options = Options());

 protected:
  void Done() override;
};

// A new `Writer` writing to standard output (by default to the same destination
// as fd 1, `std::cout`, and `stdout`).
//
// In contrast to `std::cout` and `stdout`, `NewStdOut` is fully buffered (not
// line buffered) even if it refers to an interactive device.
//
// A `NewStdOut` must be explicitly closed or flushed, and `Close()` or
// `Flush()` must succeed, in order for its output to be guaranteed to be
// available in the actual standard output. Closing a `NewStdOut` does not close
// its file descriptor. Flushing a `NewStdOut` explicitly might be needed:
//  * Before reading from standard input, so that output written so far appears
//    before waiting for input.
//  * Before writing to standard error, so that output written to different
//    streams ultimately leading to the same destination appears in the correct
//    order.
//
// At most one `NewStdOut` should be open at a time, and it should not be
// combined with accessing standard output by other means at the same time.
// Switching between means requires closing the old `NewStdOut` or flushing the
// object becoming inactive (`std::cout.flush()`, `std::fflush(stdout)`) and may
// require repositioning the object becoming active (`std::cout.seekp()`,
// `std::fseek(stdout)`).
//
// As an alternative to `NewStdOut`, creating and later closing an
// `OStreamWriter(&std::cout)` makes it easier to combine writing to a `Writer`
// with accessing `std::cout`.
class NewStdOut : public FdWriter<UnownedFd> {
 public:
  // Creates a closed `NewStdOut`.
  explicit NewStdOut(Closed) noexcept : FdWriter(kClosed) {}

  // Will write to standard output.
  explicit NewStdOut(Options options = Options());

  NewStdOut(NewStdOut&& that) noexcept;
  NewStdOut& operator=(NewStdOut&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `NewStdOut`. This avoids
  // constructing a temporary `NewStdOut` and moving from it.
  void Reset(Closed);
  void Reset(Options options = Options());
};

// A new `Writer` writing to standard error (by default to the same destination
// as fd 2, `std::cerr`, `std::clog`, and `stderr`).
//
// In contrast to `std::cerr` and `stderr`, `NewStdErr` is fully buffered (not
// unbuffered).
//
// A `NewStdErr` must be explicitly closed or flushed, and `Close()` or
// `Flush()` must succeed, in order for its output to be guaranteed to be
// available in the actual standard error. Closing a `NewStdErr` does not close
// its file descriptor. Flushing a `NewStdErr` explicitly might be needed after
// writing a complete message, so that it appears promptly.
//
// At most one `NewStdErr` should be open at a time, and it should not be
// combined with accessing standard error by other means at the same time.
// Switching between means requires closing the old `NewStdErr` or flushing the
// object becoming inactive (`std::clog.flush()`) and may require repositioning
// the object becoming active (`std::cerr.seekp()`, `std::clog.seekp()`,
// `std::fseek(stderr)`).
//
// As an alternative to `NewStdErr`, creating and later closing an
// `OStreamWriter(&std::cerr)` makes it easier to combine writing to a `Writer`
// with accessing `std::cerr`.
class NewStdErr : public FdWriter<UnownedFd> {
 public:
  // Creates a closed `NewStdErr`.
  explicit NewStdErr(Closed) noexcept : FdWriter(kClosed) {}

  // Will write to standard error.
  explicit NewStdErr(Options options = Options());

  NewStdErr(NewStdErr&& that) noexcept;
  NewStdErr& operator=(NewStdErr&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `NewStdErr`. This avoids
  // constructing a temporary `NewStdErr` and moving from it.
  void Reset(Closed);
  void Reset(Options options = Options());
};

// Sets file descriptors used by future instances of `NewStd{In,Out,Err}` in the
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

// A singleton `Reader` reading from standard input (by default from the same
// source as `std::cin` and `stdin`).
//
// Warning: if `StdIn()` is used, standard input will have an unpredictable
// amount of extra data consumed because of buffering.
//
// `StdIn()` should not be combined with changing the position of standard input
// accessed by other means, such as file descriptor 0, `std::cin`, or `stdin`.
ABSL_DEPRECATED("Use NewStdIn instead")
Reader& StdIn();

// A singleton `Writer` writing to standard output (by default to the same
// destination as `std::cout` and `stdout`).
//
// In contrast to `std::cout` and `stdout`, `StdOut()` is fully buffered (not
// line buffered) even if it refers to an interactive device.
//
// `StdOut()` is automatically flushed at process exit. Flushing it explicitly
// with `StdOut().Flush()` might be needed:
// * Before reading from standard input, so that output written so far appears
//   before waiting for input.
// * Before writing to standard error, so that output written to different
//   streams ultimately leading to the same destination appears in the correct
//   order.
//
// `StdOut()` should not be combined with changing the position of standard
// output accessed by other means, such as fd 1, `std::cout`, or `stdout`,
// except that if random access is not used, careful interleaving of multiple
// writers is possible: flushing is needed before switching to another writer
// (`StdOut().Flush()`, `std::cout.flush()`, `std::fflush(stdout)`, nothing
// needed for fd 1), and `pos()` does not take other writers into account.
//
// As an alternative to `StdOut()`, creating and later closing an
// `OStreamWriter(&std::cout)` makes it easier to combine writing to a `Writer`
// with accessing `std::cout` afterwards.
ABSL_DEPRECATED("Use NewStdOut instead")
Writer& StdOut();

// A singleton `Writer` writing to standard error (by default to the same
// destination as `std::cerr`, `std::clog`, and `stderr`).
//
// In contrast to `std::cerr` and `stderr`, `StdErr()` is fully buffered (not
// unbuffered).
//
// `StdErr()` is automatically flushed at process exit. Flushing it explicitly
// with `StdErr().Flush()` might be needed after writing a complete message,
// so that it appears promptly.
//
// `StdErr()` should not be combined with changing the position of standard
// error accessed by other means, such as fd 2, `std::cerr`, `std::clog`, or
// `stderr`, except that if random access is not used, careful interleaving of
// multiple writers is possible: flushing is needed before switching to another
// writer (`StdErr().Flush()`, `std::clog.flush()`, nothing needed for fd 2,
// `std::cerr`, or `stderr`), and `pos()` does not take other writers into
// account.
//
// As an alternative to `StdErr()`, creating and later closing an
// `OStreamWriter(&std::cerr)` makes it easier to combine writing to a `Writer`
// with accessing `std::cerr` afterwards.
ABSL_DEPRECATED("Use NewStdErr instead")
Writer& StdErr();

// Replaces `StdIn()` with a new `Reader`. Returns the previous `Reader`.
ABSL_DEPRECATED("Pass a Reader explicitly instead")
std::unique_ptr<Reader> SetStdIn(std::unique_ptr<Reader> value);

// Replaces `StdOut()` with a new `Writer`. Returns the previous `Writer`.
ABSL_DEPRECATED("Pass a Writer explicitly instead")
std::unique_ptr<Writer> SetStdOut(std::unique_ptr<Writer> value);

// Replaces `StdErr()` with a new `Writer`. Returns the previous `Writer`.
ABSL_DEPRECATED("Pass a Writer explicitly instead")
std::unique_ptr<Writer> SetStdErr(std::unique_ptr<Writer> value);

// Implementation details follow.

inline NewStdIn::NewStdIn(NewStdIn&& that) noexcept
    : FdReader(static_cast<FdReader&&>(that)) {}

inline NewStdIn& NewStdIn::operator=(NewStdIn&& that) noexcept {
  FdReader::operator=(static_cast<FdReader&&>(that));
  return *this;
}

inline void NewStdIn::Reset(Closed) { FdReader::Reset(kClosed); }

inline NewStdOut::NewStdOut(NewStdOut&& that) noexcept
    : FdWriter(static_cast<FdWriter&&>(that)) {}

inline NewStdOut& NewStdOut::operator=(NewStdOut&& that) noexcept {
  FdWriter::operator=(static_cast<FdWriter&&>(that));
  return *this;
}

inline void NewStdOut::Reset(Closed) { FdWriter::Reset(kClosed); }

inline NewStdErr::NewStdErr(NewStdErr&& that) noexcept
    : FdWriter(static_cast<FdWriter&&>(that)) {}

inline NewStdErr& NewStdErr::operator=(NewStdErr&& that) noexcept {
  FdWriter::operator=(static_cast<FdWriter&&>(that));
  return *this;
}

inline void NewStdErr::Reset(Closed) { FdWriter::Reset(kClosed); }

}  // namespace riegeli

#endif  // RIEGELI_BYTES_STD_IO_H_
