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

#include "riegeli/varint/varint_reading.h"

#include <stddef.h>
#include <stdint.h>

#include <cstring>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/bytes/reader.h"

namespace riegeli::varint_internal {

namespace {

template <typename T>
size_t kMaxLengthVarint;

template <>
constexpr size_t kMaxLengthVarint<uint32_t> = kMaxLengthVarint32;
template <>
constexpr size_t kMaxLengthVarint<uint64_t> = kMaxLengthVarint64;

template <typename T, size_t initial_index, size_t length>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline T ReadVarintValue(const char* src, T acc) {
  if constexpr (initial_index < length) {
    const T byte = T{static_cast<uint8_t>(src[initial_index])};
    acc += (byte - 1) << (initial_index * 7);
    return ReadVarintValue<T, initial_index + 1, length>(src, acc);
  } else {
    return acc;
  }
}

template <typename T, bool canonical, size_t initial_index, size_t index>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool ReadVarintFromReaderBufferLoop(
    Reader& src, const char* cursor, T acc, T& dest) {
  const T byte = T{static_cast<uint8_t>(cursor[index])};
  if constexpr (index == kMaxLengthVarint<T> - 1) {
    // Last possible byte.
    if (ABSL_PREDICT_FALSE(byte >= T{1} << (sizeof(T) * 8 - index * 7))) {
      // The representation is longer than `kMaxLengthVarint<T>`
      // or the represented value does not fit in `T`.
      return false;
    }
  } else if (byte >= 0x80) {
    return ReadVarintFromReaderBufferLoop<T, canonical, initial_index,
                                          index + 1>(src, cursor, acc, dest);
  }
  if constexpr (canonical) {
    if (ABSL_PREDICT_FALSE(byte == 0)) return false;
  }
  acc = ReadVarintValue<T, initial_index, index + 1>(cursor, acc);
  src.move_cursor(index + 1);
  dest = acc;
  return true;
}

template <typename T, bool canonical, size_t index>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool ReadVarintFromReaderLoop(Reader& src,
                                                                  T acc,
                                                                  T& dest) {
  if (ABSL_PREDICT_FALSE(!src.Pull(index + 1, kMaxLengthVarint<T>))) {
    return false;
  }
  const T byte = T{static_cast<uint8_t>(src.cursor()[index])};
  acc += (byte - 1) << (index * 7);
  if constexpr (index == kMaxLengthVarint<T> - 1) {
    // Last possible byte.
    if (ABSL_PREDICT_FALSE(byte >= T{1} << (sizeof(T) * 8 - index * 7))) {
      // The representation is longer than `kMaxLengthVarint<T>`
      // or the represented value does not fit in `T`.
      return false;
    }
  } else if (byte >= 0x80) {
    return ReadVarintFromReaderLoop<T, canonical, index + 1>(src, acc, dest);
  }
  if constexpr (canonical) {
    if (ABSL_PREDICT_FALSE(byte == 0)) return false;
  }
  src.move_cursor(index + 1);
  dest = acc;
  return true;
}

template <typename T, bool canonical, size_t initial_index, size_t index>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool ReadVarintFromCordBufferLoop(
    absl::Cord::CharIterator& src, const char* cursor, T acc, T& dest) {
  const T byte = T{static_cast<uint8_t>(cursor[index])};
  if constexpr (index == kMaxLengthVarint<T> - 1) {
    // Last possible byte.
    if (ABSL_PREDICT_FALSE(byte >= T{1} << (sizeof(T) * 8 - index * 7))) {
      // The representation is longer than `kMaxLengthVarint<T>`
      // or the represented value does not fit in `T`.
      return false;
    }
  } else if (byte >= 0x80) {
    return ReadVarintFromCordBufferLoop<T, canonical, initial_index, index + 1>(
        src, cursor, acc, dest);
  }
  if constexpr (canonical) {
    if (ABSL_PREDICT_FALSE(byte == 0)) return false;
  }
  acc = ReadVarintValue<T, initial_index, index + 1>(cursor, acc);
  absl::Cord::Advance(&src, index + 1);
  dest = acc;
  return true;
}

template <typename T, bool canonical, size_t index>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool ReadVarintFromCordLoop(
    absl::Cord::CharIterator& src, size_t available, T acc, T& dest) {
  RIEGELI_ASSERT_GT(available, index)
      << "Failed precondition of ReadVarintFromCordLoop(): not enough data";
  const T byte = T{static_cast<uint8_t>(*src)};
  acc += (byte - 1) << (index * 7);
  if constexpr (index == kMaxLengthVarint<T> - 1) {
    // Last possible byte.
    if (ABSL_PREDICT_FALSE(byte >= T{1} << (sizeof(T) * 8 - index * 7))) {
      // The representation is longer than `kMaxLengthVarint<T>`
      // or the represented value does not fit in `T`.
      return false;
    }
  } else if (byte >= 0x80) {
    if (ABSL_PREDICT_FALSE(available == index + 1)) return false;
    ++src;
    return ReadVarintFromCordLoop<T, canonical, index + 1>(src, available, acc,
                                                           dest);
  }
  if constexpr (canonical) {
    if (ABSL_PREDICT_FALSE(byte == 0)) return false;
  }
  ++src;
  dest = acc;
  return true;
}

template <typename T, bool canonical, size_t initial_index, size_t index>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline size_t ReadVarintFromArrayLoop(
    const char* src, size_t available, T acc, T& dest) {
  if (ABSL_PREDICT_FALSE(available == index)) return 0;
  const T byte = T{static_cast<uint8_t>(src[index])};
  if constexpr (index == kMaxLengthVarint<T> - 1) {
    // Last possible byte.
    if (ABSL_PREDICT_FALSE(byte >= T{1} << (sizeof(T) * 8 - index * 7))) {
      // The representation is longer than `kMaxLengthVarint<T>`
      // or the represented value does not fit in `T`.
      return 0;
    }
  } else if (byte >= 0x80) {
    return ReadVarintFromArrayLoop<T, canonical, initial_index, index + 1>(
        src, available, acc, dest);
  }
  if constexpr (canonical) {
    if (ABSL_PREDICT_FALSE(byte == 0)) return false;
  }
  dest = ReadVarintValue<T, initial_index, index + 1>(src, acc);
  return index + 1;
}

template <size_t initial_index, size_t length>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline void CopyVarintValue(const char* src,
                                                         char* dest) {
  std::memcpy(dest + initial_index, src + initial_index,
              length - initial_index);
}

template <typename T, bool canonical, size_t initial_index, size_t index>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline size_t CopyVarintFromReaderBufferLoop(
    Reader& src, const char* cursor, char* dest) {
  const uint8_t byte = static_cast<uint8_t>(cursor[index]);
  if constexpr (index == kMaxLengthVarint<T> - 1) {
    // Last possible byte.
    if (ABSL_PREDICT_FALSE(byte >= T{1} << (sizeof(T) * 8 - index * 7))) {
      // The representation is longer than `kMaxLengthVarint<T>`
      // or the represented value does not fit in `T`.
      return 0;
    }
  } else if (byte >= 0x80) {
    return CopyVarintFromReaderBufferLoop<T, canonical, initial_index,
                                          index + 1>(src, cursor, dest);
  }
  if constexpr (canonical) {
    if (ABSL_PREDICT_FALSE(byte == 0)) return 0;
  }
  CopyVarintValue<initial_index, index + 1>(cursor, dest);
  src.move_cursor(index + 1);
  return index + 1;
}

template <typename T, bool canonical, size_t initial_index, size_t index>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline size_t CopyVarintFromReaderLoop(
    Reader& src, char* dest) {
  if (ABSL_PREDICT_FALSE(!src.Pull(index + 1, kMaxLengthVarint<T>))) return 0;
  const uint8_t byte = static_cast<uint8_t>(src.cursor()[index]);
  if constexpr (index == kMaxLengthVarint<T> - 1) {
    // Last possible byte.
    if (ABSL_PREDICT_FALSE(byte >= 1u << (sizeof(T) * 8 - index * 7))) {
      // The representation is longer than `kMaxLengthVarint<T>`
      // or the represented value does not fit in `T`.
      return 0;
    }
  } else if (byte >= 0x80) {
    return CopyVarintFromReaderLoop<T, canonical, initial_index, index + 1>(
        src, dest);
  }
  if constexpr (canonical) {
    if (ABSL_PREDICT_FALSE(byte == 0)) return 0;
  }
  CopyVarintValue<initial_index, index + 1>(src.cursor(), dest);
  src.move_cursor(index + 1);
  return index + 1;
}

template <typename T, bool canonical, size_t initial_index, size_t index>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline size_t CopyVarintFromCordBufferLoop(
    absl::Cord::CharIterator& src, const char* cursor, char* dest) {
  const uint8_t byte = static_cast<uint8_t>(cursor[index]);
  if constexpr (index == kMaxLengthVarint<T> - 1) {
    // Last possible byte.
    if (ABSL_PREDICT_FALSE(byte >= 1u << (sizeof(T) * 8 - index * 7))) {
      // The representation is longer than `kMaxLengthVarint<T>`
      // or the represented value does not fit in `T`.
      return 0;
    }
  } else if (byte >= 0x80) {
    return CopyVarintFromCordBufferLoop<T, canonical, initial_index, index + 1>(
        src, cursor, dest);
  }
  if constexpr (canonical) {
    if (ABSL_PREDICT_FALSE(byte == 0)) return 0;
  }
  CopyVarintValue<initial_index, index + 1>(cursor, dest);
  absl::Cord::Advance(&src, index + 1);
  return index + 1;
}

template <typename T, bool canonical, size_t index>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline size_t CopyVarintFromCordLoop(
    absl::Cord::CharIterator& src, size_t available, char* dest) {
  RIEGELI_ASSERT_GT(available, index)
      << "Failed precondition of CopyVarintFromCordLoop(): not enough data";
  const uint8_t byte = static_cast<uint8_t>(*src);
  dest[index] = static_cast<char>(byte);
  if constexpr (index == kMaxLengthVarint<T> - 1) {
    // Last possible byte.
    if (ABSL_PREDICT_FALSE(byte >= 1u << (sizeof(T) * 8 - index * 7))) {
      // The representation is longer than `kMaxLengthVarint<T>`
      // or the represented value does not fit in `T`.
      return 0;
    }
  } else if (byte >= 0x80) {
    if (ABSL_PREDICT_FALSE(available == index + 1)) return 0;
    ++src;
    return CopyVarintFromCordLoop<T, canonical, index + 1>(src, available,
                                                           dest);
  }
  if constexpr (canonical) {
    if (ABSL_PREDICT_FALSE(byte == 0)) return 0;
  }
  ++src;
  return index + 1;
}

template <typename T, bool canonical, size_t initial_index, size_t index>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline size_t CopyVarintFromArrayLoop(
    const char* src, size_t available, char* dest) {
  if (ABSL_PREDICT_FALSE(available == index)) return 0;
  const uint8_t byte = static_cast<uint8_t>(src[index]);
  if constexpr (index == kMaxLengthVarint<T> - 1) {
    // Last possible byte.
    if (ABSL_PREDICT_FALSE(byte >= 1u << (sizeof(T) * 8 - index * 7))) {
      // The representation is longer than `kMaxLengthVarint<T>`
      // or the represented value does not fit in `T`.
      return 0;
    }
  } else if (byte >= 0x80) {
    return CopyVarintFromArrayLoop<T, canonical, initial_index, index + 1>(
        src, available, dest);
  }
  if constexpr (canonical) {
    if (ABSL_PREDICT_FALSE(byte == 0)) return 0;
  }
  CopyVarintValue<initial_index, index + 1>(src, dest);
  return index + 1;
}

template <typename T, bool canonical, size_t index>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool SkipVarintFromReaderBufferLoop(
    Reader& src, const char* cursor) {
  const uint8_t byte = static_cast<uint8_t>(cursor[index]);
  if constexpr (index == kMaxLengthVarint<T> - 1) {
    // Last possible byte.
    if (ABSL_PREDICT_FALSE(byte >= T{1} << (sizeof(T) * 8 - index * 7))) {
      // The representation is longer than `kMaxLengthVarint<T>`
      // or the represented value does not fit in `T`.
      return false;
    }
  } else if (byte >= 0x80) {
    return SkipVarintFromReaderBufferLoop<T, canonical, index + 1>(src, cursor);
  }
  if constexpr (canonical) {
    if (ABSL_PREDICT_FALSE(byte == 0)) return false;
  }
  src.move_cursor(index + 1);
  return true;
}

template <typename T, bool canonical, size_t index>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool SkipVarintFromReaderLoop(Reader& src) {
  if (ABSL_PREDICT_FALSE(!src.Pull(index + 1, kMaxLengthVarint<T>))) {
    return false;
  }
  const uint8_t byte = static_cast<uint8_t>(src.cursor()[index]);
  if constexpr (index == kMaxLengthVarint<T> - 1) {
    // Last possible byte.
    if (ABSL_PREDICT_FALSE(byte >= 1u << (sizeof(T) * 8 - index * 7))) {
      // The representation is longer than `kMaxLengthVarint<T>`
      // or the represented value does not fit in `T`.
      return false;
    }
  } else if (byte >= 0x80) {
    return SkipVarintFromReaderLoop<T, canonical, index + 1>(src);
  }
  if constexpr (canonical) {
    if (ABSL_PREDICT_FALSE(byte == 0)) return false;
  }
  src.move_cursor(index + 1);
  return true;
}

template <typename T, bool canonical, size_t initial_index, size_t index>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool SkipVarintFromCordBufferLoop(
    absl::Cord::CharIterator& src, const char* cursor) {
  const T byte = T{static_cast<uint8_t>(cursor[index])};
  if constexpr (index == kMaxLengthVarint<T> - 1) {
    // Last possible byte.
    if (ABSL_PREDICT_FALSE(byte >= T{1} << (sizeof(T) * 8 - index * 7))) {
      // The representation is longer than `kMaxLengthVarint<T>`
      // or the represented value does not fit in `T`.
      return false;
    }
  } else if (byte >= 0x80) {
    return SkipVarintFromCordBufferLoop<T, canonical, initial_index, index + 1>(
        src, cursor);
  }
  if constexpr (canonical) {
    if (ABSL_PREDICT_FALSE(byte == 0)) return false;
  }
  absl::Cord::Advance(&src, index + 1);
  return true;
}

template <typename T, bool canonical, size_t index>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool SkipVarintFromCordLoop(
    absl::Cord::CharIterator& src, size_t available) {
  RIEGELI_ASSERT_GT(available, index)
      << "Failed precondition of SkipVarintFromCordLoop(): not enough data";
  const T byte = T{static_cast<uint8_t>(*src)};
  if constexpr (index == kMaxLengthVarint<T> - 1) {
    // Last possible byte.
    if (ABSL_PREDICT_FALSE(byte >= T{1} << (sizeof(T) * 8 - index * 7))) {
      // The representation is longer than `kMaxLengthVarint<T>`
      // or the represented value does not fit in `T`.
      return false;
    }
  } else if (byte >= 0x80) {
    if (ABSL_PREDICT_FALSE(available == index + 1)) return false;
    ++src;
    return SkipVarintFromCordLoop<T, canonical, index + 1>(src, available);
  }
  if constexpr (canonical) {
    if (ABSL_PREDICT_FALSE(byte == 0)) return false;
  }
  return true;
}

template <typename T, bool canonical, size_t index>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline size_t SkipVarintFromArrayLoop(
    const char* src, size_t available) {
  if (ABSL_PREDICT_FALSE(available == index)) return 0;
  const uint8_t byte = static_cast<uint8_t>(src[index]);
  if constexpr (index == kMaxLengthVarint<T> - 1) {
    // Last possible byte.
    if (ABSL_PREDICT_FALSE(byte >= 1u << (sizeof(T) * 8 - index * 7))) {
      // The representation is longer than `kMaxLengthVarint<T>`
      // or the represented value does not fit in `T`.
      return 0;
    }
  } else if (byte >= 0x80) {
    return SkipVarintFromArrayLoop<T, canonical, index + 1>(src, available);
  }
  if constexpr (canonical) {
    if (ABSL_PREDICT_FALSE(byte == 0)) return 0;
  }
  return index + 1;
}

}  // namespace

template <typename T, bool canonical, size_t initial_index>
bool ReadVarintFromReaderBuffer(Reader& src, const char* cursor, T acc,
                                T& dest) {
  RIEGELI_ASSERT_GE(src.available(), initial_index)
      << "Failed precondition of ReadVarintFromReaderBuffer(): "
         "not enough buffered data";
  if (ABSL_PREDICT_TRUE(src.available() >= kMaxLengthVarint<T>) ||
      static_cast<uint8_t>(src.limit()[-1]) < 0x80) {
    return ReadVarintFromReaderBufferLoop<T, canonical, initial_index,
                                          initial_index>(src, cursor, acc,
                                                         dest);
  }
  // Do not inline this call to avoid a frame pointer.
  return ReadVarintFromReader<T, canonical, initial_index>(src, acc, dest);
}

template bool ReadVarintFromReaderBuffer<uint32_t, false, 2>(Reader& src,
                                                             const char* cursor,
                                                             uint32_t acc,
                                                             uint32_t& dest);
template bool ReadVarintFromReaderBuffer<uint64_t, false, 2>(Reader& src,
                                                             const char* cursor,
                                                             uint64_t acc,
                                                             uint64_t& dest);
template bool ReadVarintFromReaderBuffer<uint32_t, true, 2>(Reader& src,
                                                            const char* cursor,
                                                            uint32_t acc,
                                                            uint32_t& dest);
template bool ReadVarintFromReaderBuffer<uint64_t, true, 2>(Reader& src,
                                                            const char* cursor,
                                                            uint64_t acc,
                                                            uint64_t& dest);

template <typename T, bool canonical, size_t initial_index>
bool ReadVarintFromReader(Reader& src, T acc, T& dest) {
  RIEGELI_ASSERT_GE(src.available(), initial_index)
      << "Failed precondition of ReadVarintFromReader(): "
         "not enough buffered data";
  return ReadVarintFromReaderLoop<T, canonical, initial_index>(src, acc, dest);
}

template bool ReadVarintFromReader<uint32_t, false, 1>(Reader& src,
                                                       uint32_t acc,
                                                       uint32_t& dest);
template bool ReadVarintFromReader<uint64_t, false, 1>(Reader& src,
                                                       uint64_t acc,
                                                       uint64_t& dest);
template bool ReadVarintFromReader<uint32_t, true, 1>(Reader& src, uint32_t acc,
                                                      uint32_t& dest);
template bool ReadVarintFromReader<uint64_t, true, 1>(Reader& src, uint64_t acc,
                                                      uint64_t& dest);

template <typename T, bool canonical, size_t initial_index>
bool ReadVarintFromCordBuffer(absl::Cord::CharIterator& src, size_t available,
                              T acc, T& dest) {
  const absl::string_view chunk = absl::Cord::ChunkRemaining(src);
  RIEGELI_ASSERT_GE(chunk.size(), initial_index)
      << "Failed precondition of ReadVarintFromCordBuffer(): "
         "not enough buffered data";
  const size_t available_in_buffer = UnsignedMin(available, chunk.size());
  if (ABSL_PREDICT_TRUE(available_in_buffer >= kMaxLengthVarint<T>) ||
      static_cast<uint8_t>(chunk[available_in_buffer - 1]) < 0x80) {
    return ReadVarintFromCordBufferLoop<T, canonical, initial_index,
                                        initial_index>(src, chunk.data(), acc,
                                                       dest);
  }
  // Do not inline this call to avoid a frame pointer.
  return ReadVarintFromCord<T, canonical, initial_index>(src, available, acc,
                                                         dest);
}

template bool ReadVarintFromCordBuffer<uint32_t, false, 2>(
    absl::Cord::CharIterator& src, size_t available, uint32_t acc,
    uint32_t& dest);
template bool ReadVarintFromCordBuffer<uint64_t, false, 2>(
    absl::Cord::CharIterator& src, size_t available, uint64_t acc,
    uint64_t& dest);
template bool ReadVarintFromCordBuffer<uint32_t, true, 2>(
    absl::Cord::CharIterator& src, size_t available, uint32_t acc,
    uint32_t& dest);
template bool ReadVarintFromCordBuffer<uint64_t, true, 2>(
    absl::Cord::CharIterator& src, size_t available, uint64_t acc,
    uint64_t& dest);

template <typename T, bool canonical, size_t initial_index>
bool ReadVarintFromCord(absl::Cord::CharIterator& src, size_t available, T acc,
                        T& dest) {
  RIEGELI_ASSERT_GT(available, initial_index)
      << "Failed precondition of ReadVarintFromCord(): not enough data";
  absl::Cord::Advance(&src, initial_index);
  return ReadVarintFromCordLoop<T, canonical, initial_index>(src, available,
                                                             acc, dest);
}

template bool ReadVarintFromCord<uint32_t, false, 1>(
    absl::Cord::CharIterator& src, size_t available, uint32_t acc,
    uint32_t& dest);
template bool ReadVarintFromCord<uint64_t, false, 1>(
    absl::Cord::CharIterator& src, size_t available, uint64_t acc,
    uint64_t& dest);
template bool ReadVarintFromCord<uint32_t, true, 1>(
    absl::Cord::CharIterator& src, size_t available, uint32_t acc,
    uint32_t& dest);
template bool ReadVarintFromCord<uint64_t, true, 1>(
    absl::Cord::CharIterator& src, size_t available, uint64_t acc,
    uint64_t& dest);

template <typename T, bool canonical, size_t initial_index>
size_t ReadVarintFromArray(const char* src, size_t available, T acc, T& dest) {
  RIEGELI_ASSERT_GE(available, initial_index)
      << "Failed precondition of ReadVarintFromArray(): not enough data";
  return ReadVarintFromArrayLoop<T, canonical, initial_index, initial_index>(
      src, available, acc, dest);
}

template size_t ReadVarintFromArray<uint32_t, false, 2>(const char* src,
                                                        size_t available,
                                                        uint32_t acc,
                                                        uint32_t& dest);
template size_t ReadVarintFromArray<uint64_t, false, 2>(const char* src,
                                                        size_t available,
                                                        uint64_t acc,
                                                        uint64_t& dest);
template size_t ReadVarintFromArray<uint32_t, true, 2>(const char* src,
                                                       size_t available,
                                                       uint32_t acc,
                                                       uint32_t& dest);
template size_t ReadVarintFromArray<uint64_t, true, 2>(const char* src,
                                                       size_t available,
                                                       uint64_t acc,
                                                       uint64_t& dest);

template <typename T, bool canonical, size_t initial_index>
size_t CopyVarintFromReaderBuffer(Reader& src, const char* cursor, char* dest) {
  RIEGELI_ASSERT_GE(src.available(), initial_index)
      << "Failed precondition of CopyVarintFromReaderBuffer(): "
         "not enough buffered data";
  if (ABSL_PREDICT_TRUE(src.available() >= kMaxLengthVarint<T>) ||
      static_cast<uint8_t>(src.limit()[-1]) < 0x80) {
    return CopyVarintFromReaderBufferLoop<T, canonical, initial_index,
                                          initial_index>(src, cursor, dest);
  }
  // Do not inline this call to avoid a frame pointer.
  return CopyVarintFromReader<T, canonical, initial_index>(src, dest);
}

template size_t CopyVarintFromReaderBuffer<uint32_t, false, 2>(
    Reader& src, const char* cursor, char* dest);
template size_t CopyVarintFromReaderBuffer<uint64_t, false, 2>(
    Reader& src, const char* cursor, char* dest);
template size_t CopyVarintFromReaderBuffer<uint32_t, true, 2>(
    Reader& src, const char* cursor, char* dest);
template size_t CopyVarintFromReaderBuffer<uint64_t, true, 2>(
    Reader& src, const char* cursor, char* dest);

template <typename T, bool canonical, size_t initial_index>
size_t CopyVarintFromReader(Reader& src, char* dest) {
  RIEGELI_ASSERT_GE(src.available(), initial_index)
      << "Failed precondition of CopyVarintFromReader(): "
         "not enough buffered data";
  return CopyVarintFromReaderLoop<T, canonical, initial_index, initial_index>(
      src, dest);
}

template size_t CopyVarintFromReader<uint32_t, false, 1>(Reader& src,
                                                         char* dest);
template size_t CopyVarintFromReader<uint64_t, false, 1>(Reader& src,
                                                         char* dest);
template size_t CopyVarintFromReader<uint32_t, true, 1>(Reader& src,
                                                        char* dest);
template size_t CopyVarintFromReader<uint64_t, true, 1>(Reader& src,
                                                        char* dest);

template <typename T, bool canonical, size_t initial_index>
size_t CopyVarintFromCordBuffer(absl::Cord::CharIterator& src, size_t available,
                                char* dest) {
  const absl::string_view chunk = absl::Cord::ChunkRemaining(src);
  RIEGELI_ASSERT_GE(chunk.size(), initial_index)
      << "Failed precondition of CopyVarintFromCordBuffer(): "
         "not enough buffered data";
  const size_t available_in_buffer = UnsignedMin(available, chunk.size());
  if (ABSL_PREDICT_TRUE(available_in_buffer >= kMaxLengthVarint<T>) ||
      static_cast<uint8_t>(chunk[available_in_buffer - 1]) < 0x80) {
    return CopyVarintFromCordBufferLoop<T, canonical, initial_index,
                                        initial_index>(src, chunk.data(), dest);
  }
  // Do not inline this call to avoid a frame pointer.
  return CopyVarintFromCord<T, canonical, initial_index>(src, available, dest);
}

template size_t CopyVarintFromCordBuffer<uint32_t, false, 2>(
    absl::Cord::CharIterator& src, size_t available, char* dest);
template size_t CopyVarintFromCordBuffer<uint64_t, false, 2>(
    absl::Cord::CharIterator& src, size_t available, char* dest);
template size_t CopyVarintFromCordBuffer<uint32_t, true, 2>(
    absl::Cord::CharIterator& src, size_t available, char* dest);
template size_t CopyVarintFromCordBuffer<uint64_t, true, 2>(
    absl::Cord::CharIterator& src, size_t available, char* dest);

template <typename T, bool canonical, size_t initial_index>
size_t CopyVarintFromCord(absl::Cord::CharIterator& src, size_t available,
                          char* dest) {
  RIEGELI_ASSERT_GT(available, initial_index)
      << "Failed precondition of CopyVarintFromCord(): not enough data";
  absl::Cord::Advance(&src, initial_index);
  return CopyVarintFromCordLoop<T, canonical, initial_index>(src, available,
                                                             dest);
}

template size_t CopyVarintFromCord<uint32_t, false, 1>(
    absl::Cord::CharIterator& src, size_t available, char* dest);
template size_t CopyVarintFromCord<uint64_t, false, 1>(
    absl::Cord::CharIterator& src, size_t available, char* dest);
template size_t CopyVarintFromCord<uint32_t, true, 1>(
    absl::Cord::CharIterator& src, size_t available, char* dest);
template size_t CopyVarintFromCord<uint64_t, true, 1>(
    absl::Cord::CharIterator& src, size_t available, char* dest);

template <typename T, bool canonical, size_t initial_index>
size_t CopyVarintFromArray(const char* src, size_t available, char* dest) {
  RIEGELI_ASSERT_GE(available, initial_index)
      << "Failed precondition of CopyVarintFromArray(): not enough data";
  return CopyVarintFromArrayLoop<T, canonical, initial_index, initial_index>(
      src, available, dest);
}

template size_t CopyVarintFromArray<uint32_t, false, 2>(const char* src,
                                                        size_t available,
                                                        char* dest);
template size_t CopyVarintFromArray<uint64_t, false, 2>(const char* src,
                                                        size_t available,
                                                        char* dest);
template size_t CopyVarintFromArray<uint32_t, true, 2>(const char* src,
                                                       size_t available,
                                                       char* dest);
template size_t CopyVarintFromArray<uint64_t, true, 2>(const char* src,
                                                       size_t available,
                                                       char* dest);

template <typename T, bool canonical, size_t initial_index>
bool SkipVarintFromReaderBuffer(Reader& src, const char* cursor) {
  RIEGELI_ASSERT_GE(src.available(), initial_index)
      << "Failed precondition of SkipVarintFromReaderBuffer(): "
         "not enough buffered data";
  if (ABSL_PREDICT_TRUE(src.available() >= kMaxLengthVarint<T>) ||
      static_cast<uint8_t>(src.limit()[-1]) < 0x80) {
    return SkipVarintFromReaderBufferLoop<T, canonical, initial_index>(src,
                                                                       cursor);
  }
  // Do not inline this call to avoid a frame pointer.
  return SkipVarintFromReader<T, canonical, initial_index>(src);
}

template bool SkipVarintFromReaderBuffer<uint32_t, false, 2>(
    Reader& src, const char* cursor);
template bool SkipVarintFromReaderBuffer<uint64_t, false, 2>(
    Reader& src, const char* cursor);
template bool SkipVarintFromReaderBuffer<uint32_t, true, 2>(Reader& src,
                                                            const char* cursor);
template bool SkipVarintFromReaderBuffer<uint64_t, true, 2>(Reader& src,
                                                            const char* cursor);

template <typename T, bool canonical, size_t initial_index>
bool SkipVarintFromReader(Reader& src) {
  RIEGELI_ASSERT_GE(src.available(), initial_index)
      << "Failed precondition of SkipVarintFromReader(): "
         "not enough buffered data";
  return SkipVarintFromReaderLoop<T, canonical, initial_index>(src);
}

template bool SkipVarintFromReader<uint32_t, false, 1>(Reader& src);
template bool SkipVarintFromReader<uint64_t, false, 1>(Reader& src);
template bool SkipVarintFromReader<uint32_t, true, 1>(Reader& src);
template bool SkipVarintFromReader<uint64_t, true, 1>(Reader& src);

template <typename T, bool canonical, size_t initial_index>
bool SkipVarintFromCordBuffer(absl::Cord::CharIterator& src, size_t available) {
  const absl::string_view chunk = absl::Cord::ChunkRemaining(src);
  RIEGELI_ASSERT_GE(chunk.size(), initial_index)
      << "Failed precondition of SkipVarintFromCordBuffer(): "
         "not enough buffered data";
  const size_t available_in_buffer = UnsignedMin(available, chunk.size());
  if (ABSL_PREDICT_TRUE(available_in_buffer >= kMaxLengthVarint<T>) ||
      static_cast<uint8_t>(chunk[available_in_buffer - 1]) < 0x80) {
    return SkipVarintFromCordBufferLoop<T, canonical, initial_index,
                                        initial_index>(src, chunk.data());
  }
  // Do not inline this call to avoid a frame pointer.
  return SkipVarintFromCord<T, canonical, initial_index>(src, available);
}

template bool SkipVarintFromCordBuffer<uint32_t, false, 2>(
    absl::Cord::CharIterator& src, size_t available);
template bool SkipVarintFromCordBuffer<uint64_t, false, 2>(
    absl::Cord::CharIterator& src, size_t available);
template bool SkipVarintFromCordBuffer<uint32_t, true, 2>(
    absl::Cord::CharIterator& src, size_t available);
template bool SkipVarintFromCordBuffer<uint64_t, true, 2>(
    absl::Cord::CharIterator& src, size_t available);

template <typename T, bool canonical, size_t initial_index>
bool SkipVarintFromCord(absl::Cord::CharIterator& src, size_t available) {
  RIEGELI_ASSERT_GT(available, initial_index)
      << "Failed precondition of SkipVarintFromCord(): not enough data";
  absl::Cord::Advance(&src, initial_index);
  return SkipVarintFromCordLoop<T, canonical, initial_index>(src, available);
}

template bool SkipVarintFromCord<uint32_t, false, 1>(
    absl::Cord::CharIterator& src, size_t available);
template bool SkipVarintFromCord<uint64_t, false, 1>(
    absl::Cord::CharIterator& src, size_t available);
template bool SkipVarintFromCord<uint32_t, true, 1>(
    absl::Cord::CharIterator& src, size_t available);
template bool SkipVarintFromCord<uint64_t, true, 1>(
    absl::Cord::CharIterator& src, size_t available);

template <typename T, bool canonical, size_t initial_index>
size_t SkipVarintFromArray(const char* src, size_t available) {
  RIEGELI_ASSERT_GE(available, initial_index)
      << "Failed precondition of SkipVarintFromArray(): not enough data";
  return SkipVarintFromArrayLoop<T, canonical, initial_index>(src, available);
}

template size_t SkipVarintFromArray<uint32_t, false, 2>(const char* src,
                                                        size_t available);
template size_t SkipVarintFromArray<uint64_t, false, 2>(const char* src,
                                                        size_t available);
template size_t SkipVarintFromArray<uint32_t, true, 2>(const char* src,
                                                       size_t available);
template size_t SkipVarintFromArray<uint64_t, true, 2>(const char* src,
                                                       size_t available);

}  // namespace riegeli::varint_internal
