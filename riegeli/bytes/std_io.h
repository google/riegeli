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

#include "riegeli/base/base.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// A singleton `Reader` reading from standard input (`std::cin`).
//
// The default `StdIn()` (unless `SetStdIn()` was used) does not support random
// access, and the initial position is assumed to be 0.
//
// Warning: when `StdIn()` is used, `std::cin` will have an unpredictable amount
// of extra data consumed because of buffering.
Reader& StdIn();

// A singleton `Writer` writing to standard output (`std::cout`).
//
// The default `StdOut()` (unless `SetStdOut()` was used) does not support
// random access, and the initial position is assumed to be 0. In contrast to
// `std::cout`, `StdOut()` is fully buffered (not line buffered) even if it
// refers to an interactive device.
//
// `StdOut()` is automatically flushed at process exit. Flushing it explicitly
// with `StdOut().Flush(FlushType::kFromProcess)` might be needed:
// * Before reading from `std::cin` or `StdIn()`, so that output written so far
//   appears before waiting for input.
// * Before writing to `std::cout`, `std::cerr`, or `StdErr()`, so that output
//   written to different streams ultimately leading to the same destination
//   appears in the correct order.
Writer& StdOut();

// A singleton `Writer` writing to standard error (`std::cerr`).
//
// The default `StdErr()` (unless `SetStdErr()` was used) does not support
// random access, and the initial position is assumed to be 0. In contrast to
// `std::cerr`, `StdErr()` is fully buffered (not unbuffered).
//
// `StdErr()` is automatically flushed at process exit. Flushing it explicitly
// with `StdErr().Flush(FlushType::kFromProcess)` might be needed after writing
// a complete message, so that it appears promptly.
Writer& StdErr();

// Replaces `StdIn()` with a new `Reader`. Returns the previous `Reader`.
std::unique_ptr<Reader> SetStdIn(std::unique_ptr<Reader> value);

// Replaces `StdOut()` with a new `Writer`. Returns the previous `Writer`.
std::unique_ptr<Writer> SetStdOut(std::unique_ptr<Writer> value);

// Replaces `StdErr()` with a new `Writer`. Returns the previous `Writer`.
std::unique_ptr<Writer> SetStdErr(std::unique_ptr<Writer> value);

}  // namespace riegeli

#endif  // RIEGELI_BYTES_STD_IO_H_
