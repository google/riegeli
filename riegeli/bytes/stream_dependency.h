// Copyright 2019 Google LLC
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

#ifndef RIEGELI_BYTES_STREAM_DEPENDENCY_H_
#define RIEGELI_BYTES_STREAM_DEPENDENCY_H_

#include <type_traits>

#include "absl/meta/type_traits.h"

namespace riegeli {
namespace internal {

// There is no `std::istream::close()` nor `std::ostream::close()`, but some
// subclasses have `close()`, e.g. `std::ifstream`, `std::ofstream`,
// `std::fstream`. It is important to call `close()` before their destructor
// to detect errors.
//
// `CloseStream(stream)` calls `stream->close()` if that is defined, otherwise
// does nothing.

template <typename T, typename Enable = void>
struct HasClose : public std::false_type {};

template <typename T>
struct HasClose<T, absl::void_t<decltype(std::declval<T>().close())>>
    : public std::true_type {};

template <typename Stream, std::enable_if_t<!HasClose<Stream>::value, int> = 0>
inline void CloseStream(Stream* src) {}

template <typename Stream, std::enable_if_t<HasClose<Stream>::value, int> = 0>
inline void CloseStream(Stream* src) {
  src->close();
}

}  // namespace internal
}  // namespace riegeli

#endif  // RIEGELI_BYTES_STREAM_DEPENDENCY_H_
