// Copyright 2023 Google LLC
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

#ifndef RIEGELI_BYTES_WRITE_INT_INTERNAL_H_
#define RIEGELI_BYTES_WRITE_INT_INTERNAL_H_

#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <type_traits>

#include "absl/numeric/int128.h"
#include "riegeli/base/arithmetic.h"

namespace riegeli {
namespace write_int_internal {

// `WriteDec()` with no width parameter writes no leading zeros, except for 0
// itself.
char* WriteDec(uint32_t src, char* dest);
char* WriteDec(uint64_t src, char* dest);
char* WriteDec(absl::uint128 src, char* dest);

// `WriteDecUnsigned()` with no width parameter writes no leading zeros, except
// for 0 itself.

template <typename T, std::enable_if_t<(std::numeric_limits<T>::max() <=
                                        std::numeric_limits<uint32_t>::max()),
                                       int> = 0>
inline char* WriteDecUnsigned(T src, char* dest) {
  return WriteDec(IntCast<uint32_t>(src), dest);
}

template <
    typename T,
    std::enable_if_t<
        (std::numeric_limits<T>::max() > std::numeric_limits<uint32_t>::max() &&
         std::numeric_limits<T>::max() <= std::numeric_limits<uint64_t>::max()),
        int> = 0>
inline char* WriteDecUnsigned(T src, char* dest) {
  return WriteDec(IntCast<uint64_t>(src), dest);
}

template <typename T,
          std::enable_if_t<(std::numeric_limits<T>::max() >
                                std::numeric_limits<uint64_t>::max() &&
                            std::numeric_limits<T>::max() <=
                                std::numeric_limits<absl::uint128>::max()),
                           int> = 0>
inline char* WriteDecUnsigned(T src, char* dest) {
  return WriteDec(IntCast<absl::uint128>(src), dest);
}

// `WriteDec()` with a width parameter writes at least `width` digits.
char* WriteDec(uint32_t src, char* dest, size_t width);
char* WriteDec(uint64_t src, char* dest, size_t width);
char* WriteDec(absl::uint128 src, char* dest, size_t width);

// `WriteDecUnsigned()` with a width parameter writes at least `width` digits.

template <typename T, std::enable_if_t<(std::numeric_limits<T>::max() <=
                                        std::numeric_limits<uint32_t>::max()),
                                       int> = 0>
inline char* WriteDecUnsigned(T src, char* dest, size_t width) {
  return width <= 1 ? WriteDec(IntCast<uint32_t>(src), dest)
                    : WriteDec(IntCast<uint32_t>(src), dest, width);
}

template <
    typename T,
    std::enable_if_t<
        (std::numeric_limits<T>::max() > std::numeric_limits<uint32_t>::max() &&
         std::numeric_limits<T>::max() <= std::numeric_limits<uint64_t>::max()),
        int> = 0>
inline char* WriteDecUnsigned(T src, char* dest, size_t width) {
  return width <= 1 ? WriteDec(IntCast<uint64_t>(src), dest)
                    : WriteDec(IntCast<uint64_t>(src), dest, width);
}

template <typename T,
          std::enable_if_t<(std::numeric_limits<T>::max() >
                                std::numeric_limits<uint64_t>::max() &&
                            std::numeric_limits<T>::max() <=
                                std::numeric_limits<absl::uint128>::max()),
                           int> = 0>
inline char* WriteDecUnsigned(T src, char* dest, size_t width) {
  return width <= 1 ? WriteDec(IntCast<absl::uint128>(src), dest)
                    : WriteDec(IntCast<absl::uint128>(src), dest, width);
}

// `WriteDecBackward()` writes no leading zeros, except for 0 itself.
char* WriteDecBackward(uint32_t src, char* dest);
char* WriteDecBackward(uint64_t src, char* dest);
char* WriteDecBackward(absl::uint128 src, char* dest);

// `WriteDecUnsignedBackward()` writes no leading zeros, except for 0 itself.

template <typename T, std::enable_if_t<(std::numeric_limits<T>::max() <=
                                        std::numeric_limits<uint32_t>::max()),
                                       int> = 0>
inline char* WriteDecUnsignedBackward(T src, char* dest) {
  return WriteDecBackward(IntCast<uint32_t>(src), dest);
}

template <
    typename T,
    std::enable_if_t<
        (std::numeric_limits<T>::max() > std::numeric_limits<uint32_t>::max() &&
         std::numeric_limits<T>::max() <= std::numeric_limits<uint64_t>::max()),
        int> = 0>
inline char* WriteDecUnsignedBackward(T src, char* dest) {
  return WriteDecBackward(IntCast<uint64_t>(src), dest);
}

template <typename T,
          std::enable_if_t<(std::numeric_limits<T>::max() >
                                std::numeric_limits<uint64_t>::max() &&
                            std::numeric_limits<T>::max() <=
                                std::numeric_limits<absl::uint128>::max()),
                           int> = 0>
inline char* WriteDecUnsignedBackward(T src, char* dest) {
  return WriteDecBackward(IntCast<absl::uint128>(src), dest);
}

}  // namespace write_int_internal
}  // namespace riegeli

#endif  // RIEGELI_BYTES_WRITE_INT_INTERNAL_H_
