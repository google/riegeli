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

namespace riegeli::write_int_internal {

template <typename T, typename Target, typename Enable = void>
struct FitsIn;

template <typename T, typename Target>
struct FitsIn<T, Target,
              std::enable_if_t<std::disjunction_v<
                  std::conjunction<IsUnsignedInt<T>, IsUnsignedInt<Target>>,
                  std::conjunction<IsSignedInt<T>, IsSignedInt<Target>>>>>
    : std::bool_constant<(std::numeric_limits<T>::max() <=
                          std::numeric_limits<Target>::max())> {};

// `WriteDec()` with no width parameter writes no leading zeros, except for 0
// itself.
char* WriteDec(uint32_t src, char* dest);
char* WriteDec(uint64_t src, char* dest);
char* WriteDec(absl::uint128 src, char* dest);
char* WriteDec(int32_t src, char* dest);
char* WriteDec(int64_t src, char* dest);
char* WriteDec(absl::int128 src, char* dest);

// `WriteDecUnsigned()` with no width parameter writes no leading zeros, except
// for 0 itself.

template <typename T, std::enable_if_t<FitsIn<T, uint32_t>::value, int> = 0>
inline char* WriteDecUnsigned(T src, char* dest) {
  return WriteDec(IntCast<uint32_t>(src), dest);
}

template <typename T, std::enable_if_t<
                          std::conjunction_v<std::negation<FitsIn<T, uint32_t>>,
                                             FitsIn<T, uint64_t>>,
                          int> = 0>
inline char* WriteDecUnsigned(T src, char* dest) {
  return WriteDec(IntCast<uint64_t>(src), dest);
}

template <typename T, std::enable_if_t<
                          std::conjunction_v<std::negation<FitsIn<T, uint64_t>>,
                                             FitsIn<T, absl::uint128>>,
                          int> = 0>
inline char* WriteDecUnsigned(T src, char* dest) {
  return WriteDec(IntCast<absl::uint128>(src), dest);
}

// `WriteDecSigned()` with no width parameter writes no leading zeros, except
// for 0 itself.

template <typename T, std::enable_if_t<FitsIn<T, int32_t>::value, int> = 0>
inline char* WriteDecSigned(T src, char* dest) {
  return WriteDec(IntCast<int32_t>(src), dest);
}

template <typename T,
          std::enable_if_t<std::conjunction_v<std::negation<FitsIn<T, int32_t>>,
                                              FitsIn<T, int64_t>>,
                           int> = 0>
inline char* WriteDecSigned(T src, char* dest) {
  return WriteDec(IntCast<int64_t>(src), dest);
}

template <typename T,
          std::enable_if_t<std::conjunction_v<std::negation<FitsIn<T, int64_t>>,
                                              FitsIn<T, absl::int128>>,
                           int> = 0>
inline char* WriteDecSigned(T src, char* dest) {
  return WriteDec(IntCast<absl::int128>(src), dest);
}

// `WriteDec()` with a width parameter writes at least `width` characters.
char* WriteDec(uint32_t src, char* dest, size_t width);
char* WriteDec(uint64_t src, char* dest, size_t width);
char* WriteDec(absl::uint128 src, char* dest, size_t width);
char* WriteDec(int32_t src, char* dest, size_t width);
char* WriteDec(int64_t src, char* dest, size_t width);
char* WriteDec(absl::int128 src, char* dest, size_t width);

// `WriteDecUnsigned()` with a width parameter writes at least `width`
// characters.

template <typename T, std::enable_if_t<FitsIn<T, uint32_t>::value, int> = 0>
inline char* WriteDecUnsigned(T src, char* dest, size_t width) {
  return width <= 1 ? WriteDec(IntCast<uint32_t>(src), dest)
                    : WriteDec(IntCast<uint32_t>(src), dest, width);
}

template <typename T, std::enable_if_t<
                          std::conjunction_v<std::negation<FitsIn<T, uint32_t>>,
                                             FitsIn<T, uint64_t>>,
                          int> = 0>
inline char* WriteDecUnsigned(T src, char* dest, size_t width) {
  return width <= 1 ? WriteDec(IntCast<uint64_t>(src), dest)
                    : WriteDec(IntCast<uint64_t>(src), dest, width);
}

template <typename T, std::enable_if_t<
                          std::conjunction_v<std::negation<FitsIn<T, uint64_t>>,
                                             FitsIn<T, absl::uint128>>,
                          int> = 0>
inline char* WriteDecUnsigned(T src, char* dest, size_t width) {
  return width <= 1 ? WriteDec(IntCast<absl::uint128>(src), dest)
                    : WriteDec(IntCast<absl::uint128>(src), dest, width);
}

// `WriteDecSigned()` with a width parameter writes at least `width` characters.

template <typename T, std::enable_if_t<FitsIn<T, int32_t>::value, int> = 0>
inline char* WriteDecSigned(T src, char* dest, size_t width) {
  return width <= 1 ? WriteDec(IntCast<int32_t>(src), dest)
                    : WriteDec(IntCast<int32_t>(src), dest, width);
}

template <typename T,
          std::enable_if_t<std::conjunction_v<std::negation<FitsIn<T, int32_t>>,
                                              FitsIn<T, int64_t>>,
                           int> = 0>
inline char* WriteDecSigned(T src, char* dest, size_t width) {
  return width <= 1 ? WriteDec(IntCast<int64_t>(src), dest)
                    : WriteDec(IntCast<int64_t>(src), dest, width);
}

template <typename T,
          std::enable_if_t<std::conjunction_v<std::negation<FitsIn<T, int64_t>>,
                                              FitsIn<T, absl::int128>>,
                           int> = 0>
inline char* WriteDecSigned(T src, char* dest, size_t width) {
  return width <= 1 ? WriteDec(IntCast<absl::int128>(src), dest)
                    : WriteDec(IntCast<absl::int128>(src), dest, width);
}

// `WriteDecBackward()` writes no leading zeros, except for 0 itself.
char* WriteDecBackward(uint32_t src, char* dest);
char* WriteDecBackward(uint64_t src, char* dest);
char* WriteDecBackward(absl::uint128 src, char* dest);
char* WriteDecBackward(int32_t src, char* dest);
char* WriteDecBackward(int64_t src, char* dest);
char* WriteDecBackward(absl::int128 src, char* dest);

// `WriteDecUnsignedBackward()` writes no leading zeros, except for 0 itself.

template <typename T, std::enable_if_t<FitsIn<T, uint32_t>::value, int> = 0>
inline char* WriteDecUnsignedBackward(T src, char* dest) {
  return WriteDecBackward(IntCast<uint32_t>(src), dest);
}

template <typename T, std::enable_if_t<
                          std::conjunction_v<std::negation<FitsIn<T, uint32_t>>,
                                             FitsIn<T, uint64_t>>,
                          int> = 0>
inline char* WriteDecUnsignedBackward(T src, char* dest) {
  return WriteDecBackward(IntCast<uint64_t>(src), dest);
}

template <typename T, std::enable_if_t<
                          std::conjunction_v<std::negation<FitsIn<T, uint64_t>>,
                                             FitsIn<T, absl::uint128>>,
                          int> = 0>
inline char* WriteDecUnsignedBackward(T src, char* dest) {
  return WriteDecBackward(IntCast<absl::uint128>(src), dest);
}

// `WriteDecSignedBackward()` writes no leading zeros, except for 0 itself.

template <typename T, std::enable_if_t<FitsIn<T, int32_t>::value, int> = 0>
inline char* WriteDecSignedBackward(T src, char* dest) {
  return WriteDecBackward(IntCast<int32_t>(src), dest);
}

template <typename T,
          std::enable_if_t<std::conjunction_v<std::negation<FitsIn<T, int32_t>>,
                                              FitsIn<T, int64_t>>,
                           int> = 0>
inline char* WriteDecSignedBackward(T src, char* dest) {
  return WriteDecBackward(IntCast<int64_t>(src), dest);
}

template <typename T,
          std::enable_if_t<std::conjunction_v<std::negation<FitsIn<T, int64_t>>,
                                              FitsIn<T, absl::int128>>,
                           int> = 0>
inline char* WriteDecSignedBackward(T src, char* dest) {
  return WriteDecBackward(IntCast<absl::int128>(src), dest);
}

size_t StringifiedSize(uint32_t src);
size_t StringifiedSize(uint64_t src);
size_t StringifiedSize(absl::uint128 src);
size_t StringifiedSize(int32_t src);
size_t StringifiedSize(int64_t src);
size_t StringifiedSize(absl::int128 src);

template <typename T, std::enable_if_t<FitsIn<T, uint32_t>::value, int> = 0>
inline size_t StringifiedSizeUnsigned(T src) {
  return StringifiedSize(IntCast<uint32_t>(src));
}

template <typename T, std::enable_if_t<
                          std::conjunction_v<std::negation<FitsIn<T, uint32_t>>,
                                             FitsIn<T, uint64_t>>,
                          int> = 0>
inline size_t StringifiedSizeUnsigned(T src) {
  return StringifiedSize(IntCast<uint64_t>(src));
}

template <typename T, std::enable_if_t<
                          std::conjunction_v<std::negation<FitsIn<T, uint64_t>>,
                                             FitsIn<T, absl::uint128>>,
                          int> = 0>
inline size_t StringifiedSizeUnsigned(T src) {
  return StringifiedSize(IntCast<absl::uint128>(src));
}

template <typename T, std::enable_if_t<FitsIn<T, int32_t>::value, int> = 0>
inline size_t StringifiedSizeSigned(T src) {
  return StringifiedSize(IntCast<int32_t>(src));
}

template <typename T,
          std::enable_if_t<std::conjunction_v<std::negation<FitsIn<T, int32_t>>,
                                              FitsIn<T, int64_t>>,
                           int> = 0>
inline size_t StringifiedSizeSigned(T src) {
  return StringifiedSize(IntCast<int64_t>(src));
}

template <typename T,
          std::enable_if_t<std::conjunction_v<std::negation<FitsIn<T, int64_t>>,
                                              FitsIn<T, absl::int128>>,
                           int> = 0>
inline size_t StringifiedSizeSigned(T src) {
  return StringifiedSize(IntCast<absl::int128>(src));
}

}  // namespace riegeli::write_int_internal

#endif  // RIEGELI_BYTES_WRITE_INT_INTERNAL_H_
