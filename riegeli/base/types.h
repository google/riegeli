// Copyright 2017 Google LLC
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

#ifndef RIEGELI_BASE_TYPES_H_
#define RIEGELI_BASE_TYPES_H_

#include <stddef.h>
#include <stdint.h>

#include <ios>
#include <type_traits>

namespace riegeli {

// Position in a stream of bytes, used also for stream sizes.
//
// This is an unsigned integer type at least as wide as `size_t`,
// `std::streamoff`, and `uint64_t`.
using Position =
    std::common_type_t<size_t, std::make_unsigned_t<std::streamoff>, uint64_t>;

// Specifies the scope of objects to flush and the intended data durability
// (without a guarantee).
enum class FlushType {
  // Makes data written so far visible in other objects, propagating flushing
  // through owned dependencies of the given writer.
  kFromObject = 0,
  // Makes data written so far visible outside the process, propagating flushing
  // through dependencies of the given writer. This is generally the default.
  kFromProcess = 1,
  // Makes data written so far visible outside the process and durable in case
  // of operating system crash, propagating flushing through dependencies of the
  // given writer.
  kFromMachine = 2,
};

// Specifies the scope of objects to synchronize.
enum class SyncType {
  // Propagates synchronization through owned dependencies of the given reader.
  kFromObject = 0,
  // Propagates synchronization through all dependencies of the given reader.
  // This is generally the default.
  kFromProcess = 1,
};

}  // namespace riegeli

#endif  // RIEGELI_BASE_TYPES_H_
