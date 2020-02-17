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

#include "riegeli/bytes/reader_utils.h"

#include <stddef.h>

#include <cstring>
#include <limits>
#include <string>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/canonical_errors.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

bool ReadAll(Reader* src, absl::string_view* dest, size_t max_size) {
  max_size = UnsignedMin(max_size, dest->max_size());
  if (src->SupportsSize()) {
    Position size;
    if (ABSL_PREDICT_FALSE(!src->Size(&size))) {
      *dest = absl::string_view();
      return false;
    }
    const Position remaining = SaturatingSub(size, src->pos());
    if (ABSL_PREDICT_FALSE(remaining > max_size)) {
      if (ABSL_PREDICT_FALSE(!src->Read(dest, max_size))) {
        if (ABSL_PREDICT_FALSE(!src->healthy())) return false;
      }
      return src->Fail(ResourceExhaustedError("Size limit exceeded"));
    }
    if (ABSL_PREDICT_FALSE(!src->Read(dest, IntCast<size_t>(remaining)))) {
      return src->healthy();
    }
    return true;
  } else {
    do {
      if (ABSL_PREDICT_FALSE(src->available() > max_size)) {
        *dest = absl::string_view(src->cursor(), max_size);
        src->move_cursor(max_size);
        return src->Fail(ResourceExhaustedError("Size limit exceeded"));
      }
    } while (src->Pull(src->available() + 1,
                       SaturatingAdd(src->available(), src->available())));
    *dest = absl::string_view(src->cursor(), src->available());
    src->move_cursor(src->available());
    return src->healthy();
  }
}

bool ReadAll(Reader* src, std::string* dest, size_t max_size) {
  max_size = UnsignedMin(max_size, dest->max_size() - dest->size());
  if (src->SupportsSize()) {
    Position size;
    if (ABSL_PREDICT_FALSE(!src->Size(&size))) return false;
    const Position remaining = SaturatingSub(size, src->pos());
    if (ABSL_PREDICT_FALSE(remaining > max_size)) {
      if (ABSL_PREDICT_FALSE(!src->Read(dest, max_size))) {
        if (ABSL_PREDICT_FALSE(!src->healthy())) return false;
      }
      return src->Fail(ResourceExhaustedError("Size limit exceeded"));
    }
    if (ABSL_PREDICT_FALSE(!src->Read(dest, IntCast<size_t>(remaining)))) {
      return src->healthy();
    }
    return true;
  } else {
    do {
      if (ABSL_PREDICT_FALSE(src->available() > max_size)) {
        dest->append(src->cursor(), max_size);
        src->move_cursor(max_size);
        return src->Fail(ResourceExhaustedError("Size limit exceeded"));
      }
      max_size -= src->available();
      dest->append(src->cursor(), src->available());
      src->move_cursor(src->available());
    } while (src->Pull());
    return src->healthy();
  }
}

bool ReadAll(Reader* src, Chain* dest, size_t max_size) {
  max_size =
      UnsignedMin(max_size, std::numeric_limits<size_t>::max() - dest->size());
  if (src->SupportsSize()) {
    Position size;
    if (ABSL_PREDICT_FALSE(!src->Size(&size))) return false;
    const Position remaining = SaturatingSub(size, src->pos());
    if (ABSL_PREDICT_FALSE(remaining > max_size)) {
      if (ABSL_PREDICT_FALSE(!src->Read(dest, max_size))) {
        if (ABSL_PREDICT_FALSE(!src->healthy())) return false;
      }
      return src->Fail(ResourceExhaustedError("Size limit exceeded"));
    }
    if (ABSL_PREDICT_FALSE(!src->Read(dest, IntCast<size_t>(remaining)))) {
      return src->healthy();
    }
    return true;
  } else {
    do {
      if (ABSL_PREDICT_FALSE(src->available() > max_size)) {
        src->Read(dest, max_size);
        return src->Fail(ResourceExhaustedError("Size limit exceeded"));
      }
      max_size -= src->available();
      src->Read(dest, src->available());
    } while (src->Pull());
    return src->healthy();
  }
}

bool ReadAll(Reader* src, absl::Cord* dest, size_t max_size) {
  max_size =
      UnsignedMin(max_size, std::numeric_limits<size_t>::max() - dest->size());
  if (src->SupportsSize()) {
    Position size;
    if (ABSL_PREDICT_FALSE(!src->Size(&size))) return false;
    const Position remaining = SaturatingSub(size, src->pos());
    if (ABSL_PREDICT_FALSE(remaining > max_size)) {
      if (ABSL_PREDICT_FALSE(!src->Read(dest, max_size))) {
        if (ABSL_PREDICT_FALSE(!src->healthy())) return false;
      }
      return src->Fail(ResourceExhaustedError("Size limit exceeded"));
    }
    if (ABSL_PREDICT_FALSE(!src->Read(dest, IntCast<size_t>(remaining)))) {
      return src->healthy();
    }
    return true;
  } else {
    do {
      if (ABSL_PREDICT_FALSE(src->available() > max_size)) {
        src->Read(dest, max_size);
        return src->Fail(ResourceExhaustedError("Size limit exceeded"));
      }
      max_size -= src->available();
      src->Read(dest, src->available());
    } while (src->Pull());
    return src->healthy();
  }
}

bool CopyAll(Reader* src, Writer* dest, Position max_size) {
  if (src->SupportsSize()) {
    Position size;
    if (ABSL_PREDICT_FALSE(!src->Size(&size))) return false;
    const Position remaining = SaturatingSub(size, src->pos());
    if (ABSL_PREDICT_FALSE(remaining > max_size)) {
      if (ABSL_PREDICT_FALSE(!src->CopyTo(dest, max_size))) {
        return dest->healthy() && src->healthy();
      }
      return src->Fail(ResourceExhaustedError("Size limit exceeded"));
    }
    if (ABSL_PREDICT_FALSE(!src->CopyTo(dest, remaining))) {
      return dest->healthy() && src->healthy();
    }
    return true;
  } else {
    do {
      if (ABSL_PREDICT_FALSE(src->available() > max_size)) {
        if (ABSL_PREDICT_FALSE(!src->CopyTo(dest, max_size))) {
          if (ABSL_PREDICT_FALSE(!dest->healthy())) return false;
        }
        return src->Fail(ResourceExhaustedError("Size limit exceeded"));
      }
      max_size -= src->available();
      if (ABSL_PREDICT_FALSE(!src->CopyTo(dest, src->available()))) {
        if (ABSL_PREDICT_FALSE(!dest->healthy())) return false;
      }
    } while (src->Pull());
    return src->healthy();
  }
}

bool CopyAll(Reader* src, BackwardWriter* dest, size_t max_size) {
  if (src->SupportsSize()) {
    Position size;
    if (ABSL_PREDICT_FALSE(!src->Size(&size))) return false;
    const Position remaining = SaturatingSub(size, src->pos());
    if (ABSL_PREDICT_FALSE(remaining > max_size)) {
      if (ABSL_PREDICT_FALSE(!src->Skip(max_size))) {
        if (ABSL_PREDICT_FALSE(!src->healthy())) return false;
      }
      return src->Fail(ResourceExhaustedError("Size limit exceeded"));
    }
    if (ABSL_PREDICT_FALSE(!src->CopyTo(dest, IntCast<size_t>(remaining)))) {
      return dest->healthy() && src->healthy();
    }
    return true;
  } else {
    Chain data;
    do {
      if (ABSL_PREDICT_FALSE(src->available() > max_size)) {
        src->move_cursor(max_size);
        return src->Fail(ResourceExhaustedError("Size limit exceeded"));
      }
      max_size -= src->available();
      src->Read(&data, src->available());
    } while (src->Pull());
    if (ABSL_PREDICT_FALSE(!src->healthy())) return false;
    return dest->Write(std::move(data));
  }
}

namespace {

ABSL_ATTRIBUTE_COLD bool MaxLengthExceeded(Reader* src, absl::string_view* dest,
                                           size_t max_length) {
  *dest = absl::string_view(src->cursor(), max_length);
  src->move_cursor(max_length);
  src->Fail(ResourceExhaustedError("Line length limit exceeded"));
  return true;
}

ABSL_ATTRIBUTE_COLD bool MaxLengthExceeded(Reader* src, std::string* dest,
                                           size_t max_length) {
  dest->append(src->cursor(), max_length);
  src->move_cursor(max_length);
  src->Fail(ResourceExhaustedError("Line length limit exceeded"));
  return true;
}

ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool FoundNewline(Reader* src,
                                                      absl::string_view* dest,
                                                      ReadLineOptions options,
                                                      size_t length,
                                                      size_t newline_length) {
  const size_t length_with_newline = length + newline_length;
  if (options.keep_newline()) length = length_with_newline;
  if (ABSL_PREDICT_FALSE(length > options.max_length())) {
    return MaxLengthExceeded(src, dest, options.max_length());
  }
  *dest = absl::string_view(src->cursor(), length);
  src->move_cursor(length_with_newline);
  return true;
}

ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool FoundNewline(Reader* src,
                                                      std::string* dest,
                                                      ReadLineOptions options,
                                                      size_t length,
                                                      size_t newline_length) {
  const size_t length_with_newline = length + newline_length;
  if (options.keep_newline()) length = length_with_newline;
  if (ABSL_PREDICT_FALSE(length > options.max_length())) {
    return MaxLengthExceeded(src, dest, options.max_length());
  }
  dest->append(src->cursor(), length);
  src->move_cursor(length_with_newline);
  return true;
}

}  // namespace

bool ReadLine(Reader* src, absl::string_view* dest, ReadLineOptions options) {
  options.set_max_length(UnsignedMin(options.max_length(), dest->max_size()));
  size_t length = 0;
  if (ABSL_PREDICT_FALSE(!src->Pull())) return false;
  do {
    if (options.recognize_cr()) {
      for (const char* newline = src->cursor() + length; newline < src->limit();
           ++newline) {
        if (ABSL_PREDICT_FALSE(*newline == '\n')) {
          return FoundNewline(src, dest, options,
                              PtrDistance(src->cursor(), newline), 1);
        }
        if (ABSL_PREDICT_FALSE(*newline == '\r')) {
          length = PtrDistance(src->cursor(), newline);
          return FoundNewline(src, dest, options, length,
                              ABSL_PREDICT_TRUE(src->Pull(length + 2)) &&
                                      src->cursor()[length + 1] == '\n'
                                  ? size_t{2}
                                  : size_t{1});
        }
      }
    } else {
      const char* const newline = static_cast<const char*>(
          std::memchr(src->cursor() + length, '\n', src->available() - length));
      if (ABSL_PREDICT_TRUE(newline != nullptr)) {
        return FoundNewline(src, dest, options,
                            PtrDistance(src->cursor(), newline), 1);
      }
    }
    length = src->available();
    if (ABSL_PREDICT_FALSE(length > options.max_length())) {
      return MaxLengthExceeded(src, dest, options.max_length());
    }
  } while (src->Pull(length + 1, SaturatingAdd(length, length)));
  *dest = absl::string_view(src->cursor(), src->available());
  src->move_cursor(src->available());
  return true;
}

bool ReadLine(Reader* src, std::string* dest, ReadLineOptions options) {
  dest->clear();
  options.set_max_length(UnsignedMin(options.max_length(), dest->max_size()));
  if (ABSL_PREDICT_FALSE(!src->Pull())) return false;
  do {
    if (options.recognize_cr()) {
      for (const char* newline = src->cursor(); newline < src->limit();
           ++newline) {
        if (ABSL_PREDICT_FALSE(*newline == '\n')) {
          return FoundNewline(src, dest, options,
                              PtrDistance(src->cursor(), newline), 1);
        }
        if (ABSL_PREDICT_FALSE(*newline == '\r')) {
          const size_t length = PtrDistance(src->cursor(), newline);
          return FoundNewline(src, dest, options, length,
                              ABSL_PREDICT_TRUE(src->Pull(length + 2)) &&
                                      src->cursor()[length + 1] == '\n'
                                  ? size_t{2}
                                  : size_t{1});
        }
      }
    } else {
      const char* const newline = static_cast<const char*>(
          std::memchr(src->cursor(), '\n', src->available()));
      if (ABSL_PREDICT_TRUE(newline != nullptr)) {
        return FoundNewline(src, dest, options,
                            PtrDistance(src->cursor(), newline), 1);
      }
    }
    if (ABSL_PREDICT_FALSE(src->available() > options.max_length())) {
      return MaxLengthExceeded(src, dest, options.max_length());
    }
    options.set_max_length(options.max_length() - src->available());
    dest->append(src->cursor(), src->available());
    src->move_cursor(src->available());
  } while (src->Pull());
  return true;
}

namespace internal {

bool StreamingReadVarint32Slow(Reader* src, uint32_t* data) {
  uint32_t acc = 0;
  size_t length = 0;
  for (;;) {
    const uint8_t byte = src->cursor()[length];
    acc |= (uint32_t{byte} & 0x7f) << (length * 7);
    ++length;
    if (ABSL_PREDICT_FALSE(length == kMaxLengthVarint32)) {
      // Last possible byte.
      if (ABSL_PREDICT_FALSE(
              byte >= uint8_t{1} << (32 - (kMaxLengthVarint32 - 1) * 7))) {
        // The representation is longer than `kMaxLengthVarint32`
        // or the represented value does not fit in `uint32_t`.
        return false;
      }
      break;
    }
    if ((byte & 0x80) == 0) break;
    if (ABSL_PREDICT_FALSE(!src->Pull(length + 1, kMaxLengthVarint32))) {
      return false;
    }
  }
  src->move_cursor(length);
  *data = acc;
  return true;
}

bool StreamingReadVarint64Slow(Reader* src, uint64_t* data) {
  uint64_t acc = 0;
  size_t length = 0;
  for (;;) {
    const uint8_t byte = src->cursor()[length];
    acc |= (uint64_t{byte} & 0x7f) << (length * 7);
    ++length;
    if (ABSL_PREDICT_FALSE(length == kMaxLengthVarint64)) {
      // Last possible byte.
      if (ABSL_PREDICT_FALSE(
              byte >= uint8_t{1} << (64 - (kMaxLengthVarint64 - 1) * 7))) {
        // The representation is longer than `kMaxLengthVarint64`
        // or the represented value does not fit in `uint64_t`.
        return false;
      }
      break;
    }
    if ((byte & 0x80) == 0) break;
    if (ABSL_PREDICT_FALSE(!src->Pull(length + 1, kMaxLengthVarint64))) {
      return false;
    }
  }
  src->move_cursor(length);
  *data = acc;
  return true;
}

}  // namespace internal

}  // namespace riegeli
