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

template <typename Stream>
inline void Close(Stream& stream) {
  if constexpr (HasClose<Stream>::value) {
    stream.close();
  }
}

template <typename T>
inline std::istream* DetectIStream(T* stream) {
  if constexpr (std::is_base_of_v<std::istream, T>) {
    return stream;
  } else {
    return nullptr;
  }
}

}  // namespace riegeli::iostream_internal

#endif  // RIEGELI_BYTES_IOSTREAM_INTERNAL_H_
