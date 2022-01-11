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

#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// A singleton `Reader` reading from standard input (by default from the same
// source as `std::cin` and `stdin`).
//
// Warning: if `StdIn()` is used, standard input will have an unpredictable
// amount of extra data consumed because of buffering.
//
// `StdIn()` should not be combined with changing the position of standard input
// accessed by other means, such as file descriptor 0, `std::cin`, or `stdin`.
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
Writer& StdErr();

// Replaces `StdIn()` with a new `Reader`. Returns the previous `Reader`.
std::unique_ptr<Reader> SetStdIn(std::unique_ptr<Reader> value);

// Replaces `StdOut()` with a new `Writer`. Returns the previous `Writer`.
std::unique_ptr<Writer> SetStdOut(std::unique_ptr<Writer> value);

// Replaces `StdErr()` with a new `Writer`. Returns the previous `Writer`.
std::unique_ptr<Writer> SetStdErr(std::unique_ptr<Writer> value);

}  // namespace riegeli

#endif  // RIEGELI_BYTES_STD_IO_H_
