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

#ifndef RIEGELI_BYTES_IOSTREAM_INTERNAL_H_
#define RIEGELI_BYTES_IOSTREAM_INTERNAL_H_

#include <istream>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"

namespace riegeli::iostream_internal {

// There is no `std::istream::close()` nor `std::ostream::close()`, but some
// subclasses have `close()`, e.g. `std::ifstream`, `std::ofstream`,
// `std::fstream`. It is important to call `close()` before their destructor
// to detect errors.
//
// `iostream_internal::Close(stream)` calls `stream->close()` if that is
// defined, otherwise does nothing.

template <typename T, typename Enable = void>
struct HasClose : std::false_type {};

template <typename T>
struct HasClose<T, std::void_t<decltype(std::declval<T>().close())>>
    : std::true_type {};

template <typename Stream, std::enable_if_t<!HasClose<Stream>::value, int> = 0>
inline void Close(ABSL_ATTRIBUTE_UNUSED Stream& stream) {}

template <typename Stream, std::enable_if_t<HasClose<Stream>::value, int> = 0>
inline void Close(Stream& stream) {
  stream.close();
}

template <typename T,
          std::enable_if_t<std::is_base_of_v<std::istream, T>, int> = 0>
inline std::istream* DetectIStream(T* stream) {
  return stream;
}

template <typename T,
          std::enable_if_t<!std::is_base_of_v<std::istream, T>, int> = 0>
inline std::istream* DetectIStream(ABSL_ATTRIBUTE_UNUSED T* stream) {
  return nullptr;
}

}  // namespace riegeli::iostream_internal

#endif  // RIEGELI_BYTES_IOSTREAM_INTERNAL_H_
