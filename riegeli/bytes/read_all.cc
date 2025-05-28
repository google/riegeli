// Copyright 2018 Google LLC
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

#include "riegeli/bytes/read_all.h"

#include <stddef.h>

#include <limits>
#include <optional>
#include <string>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/reader.h"

namespace riegeli::read_all_internal {

namespace {

ABSL_ATTRIBUTE_COLD absl::Status MaxLengthExceeded(Reader& src,
                                                   Position max_length) {
  return src.AnnotateStatus(absl::ResourceExhaustedError(
      absl::StrCat("Maximum length exceeded: ", max_length)));
}

absl::Status ReadAllImpl(Reader& src, absl::string_view& dest,
                         size_t max_length) {
  if (src.SupportsSize()) {
    const std::optional<Position> size = src.Size();
    if (ABSL_PREDICT_FALSE(size == std::nullopt)) {
      dest = absl::string_view();
      return src.status();
    }
    const Position remaining = SaturatingSub(*size, src.pos());
    if (ABSL_PREDICT_FALSE(remaining > max_length)) {
      if (ABSL_PREDICT_FALSE(!src.Read(max_length, dest))) {
        if (ABSL_PREDICT_FALSE(!src.ok())) return src.status();
        return absl::OkStatus();
      }
      return MaxLengthExceeded(src, max_length);
    }
    if (ABSL_PREDICT_FALSE(!src.Read(IntCast<size_t>(remaining), dest))) {
      if (ABSL_PREDICT_FALSE(!src.ok())) return src.status();
    }
  } else {
    do {
      if (ABSL_PREDICT_FALSE(src.available() > max_length)) {
        dest = absl::string_view(src.cursor(), max_length);
        src.move_cursor(max_length);
        return MaxLengthExceeded(src, max_length);
      }
    } while (src.Pull(src.available() + 1));
    dest = absl::string_view(src.cursor(), src.available());
    src.move_cursor(src.available());
    if (ABSL_PREDICT_FALSE(!src.ok())) return src.status();
  }
  return absl::OkStatus();
}

absl::Status ReadAndAppendAllImpl(Reader& src, std::string& dest,
                                  size_t max_length) {
  if (src.SupportsSize()) {
    const std::optional<Position> size = src.Size();
    if (ABSL_PREDICT_FALSE(size == std::nullopt)) return src.status();
    const Position remaining = SaturatingSub(*size, src.pos());
    if (ABSL_PREDICT_FALSE(remaining > max_length)) {
      if (ABSL_PREDICT_FALSE(!src.ReadAndAppend(max_length, dest))) {
        if (ABSL_PREDICT_FALSE(!src.ok())) return src.status();
        return absl::OkStatus();
      }
      return MaxLengthExceeded(src, max_length);
    }
    if (ABSL_PREDICT_FALSE(
            !src.ReadAndAppend(IntCast<size_t>(remaining), dest))) {
      if (ABSL_PREDICT_FALSE(!src.ok())) return src.status();
    }
  } else {
    if (ABSL_PREDICT_FALSE(!src.Pull())) {
      if (ABSL_PREDICT_FALSE(!src.ok())) return src.status();
      return absl::OkStatus();
    }
    size_t remaining_max_length = max_length;
    const size_t dest_pos = dest.size();
    if (src.available() < dest.capacity() - dest_pos) {
      // Try to fill all remaining space in `dest`, to avoid copying through the
      // `Chain` in case the remaining length is smaller.
      const size_t length =
          UnsignedMin(dest.capacity() - dest_pos, remaining_max_length);
      dest.resize(dest_pos + length);
      size_t length_read;
      if (!src.Read(length, &dest[dest_pos], &length_read)) {
        dest.erase(dest_pos + length_read);
        if (ABSL_PREDICT_FALSE(!src.ok())) return src.status();
        return absl::OkStatus();
      }
      remaining_max_length -= length_read;
    }
    Chain buffer;
    do {
      if (ABSL_PREDICT_FALSE(src.available() > remaining_max_length)) {
        src.ReadAndAppend(remaining_max_length, buffer);
        std::move(buffer).AppendTo(dest);
        return MaxLengthExceeded(src, max_length);
      }
      remaining_max_length -= src.available();
      src.ReadAndAppend(src.available(), buffer);
    } while (src.Pull());
    std::move(buffer).AppendTo(dest);
    if (ABSL_PREDICT_FALSE(!src.ok())) return src.status();
  }
  return absl::OkStatus();
}

absl::Status ReadAndAppendAllImpl(Reader& src, Chain& dest, size_t max_length) {
  max_length =
      UnsignedMin(max_length, std::numeric_limits<size_t>::max() - dest.size());
  if (src.SupportsSize()) {
    const std::optional<Position> size = src.Size();
    if (ABSL_PREDICT_FALSE(size == std::nullopt)) return src.status();
    const Position remaining = SaturatingSub(*size, src.pos());
    if (ABSL_PREDICT_FALSE(remaining > max_length)) {
      if (ABSL_PREDICT_FALSE(!src.ReadAndAppend(max_length, dest))) {
        if (ABSL_PREDICT_FALSE(!src.ok())) return src.status();
        return absl::OkStatus();
      }
      return MaxLengthExceeded(src, max_length);
    }
    if (ABSL_PREDICT_FALSE(
            !src.ReadAndAppend(IntCast<size_t>(remaining), dest))) {
      if (ABSL_PREDICT_FALSE(!src.ok())) return src.status();
    }
  } else {
    size_t remaining_max_length = max_length;
    do {
      if (ABSL_PREDICT_FALSE(src.available() > remaining_max_length)) {
        src.ReadAndAppend(remaining_max_length, dest);
        return MaxLengthExceeded(src, max_length);
      }
      remaining_max_length -= src.available();
      src.ReadAndAppend(src.available(), dest);
    } while (src.Pull());
    if (ABSL_PREDICT_FALSE(!src.ok())) return src.status();
  }
  return absl::OkStatus();
}

absl::Status ReadAndAppendAllImpl(Reader& src, absl::Cord& dest,
                                  size_t max_length) {
  max_length =
      UnsignedMin(max_length, std::numeric_limits<size_t>::max() - dest.size());
  if (src.SupportsSize()) {
    const std::optional<Position> size = src.Size();
    if (ABSL_PREDICT_FALSE(size == std::nullopt)) return src.status();
    const Position remaining = SaturatingSub(*size, src.pos());
    if (ABSL_PREDICT_FALSE(remaining > max_length)) {
      if (ABSL_PREDICT_FALSE(!src.ReadAndAppend(max_length, dest))) {
        if (ABSL_PREDICT_FALSE(!src.ok())) return src.status();
        return absl::OkStatus();
      }
      return MaxLengthExceeded(src, max_length);
    }
    if (ABSL_PREDICT_FALSE(
            !src.ReadAndAppend(IntCast<size_t>(remaining), dest))) {
      if (ABSL_PREDICT_FALSE(!src.ok())) return src.status();
    }
  } else {
    size_t remaining_max_length = max_length;
    do {
      if (ABSL_PREDICT_FALSE(src.available() > remaining_max_length)) {
        src.ReadAndAppend(remaining_max_length, dest);
        return MaxLengthExceeded(src, max_length);
      }
      remaining_max_length -= src.available();
      src.ReadAndAppend(src.available(), dest);
    } while (src.Pull());
    if (ABSL_PREDICT_FALSE(!src.ok())) return src.status();
  }
  return absl::OkStatus();
}

}  // namespace

absl::Status ReadAllImpl(Reader& src, absl::string_view& dest,
                         size_t max_length, size_t* length_read) {
  const absl::Status status = ReadAllImpl(src, dest, max_length);
  if (length_read != nullptr) *length_read = dest.size();
  return status;
}

absl::Status ReadAllImpl(Reader& src, char* dest, size_t max_length,
                         size_t* length_read) {
  if (!src.Read(max_length, dest, length_read)) {
    if (ABSL_PREDICT_FALSE(!src.ok())) return src.status();
    return absl::OkStatus();
  }
  if (ABSL_PREDICT_FALSE(src.Pull())) return MaxLengthExceeded(src, max_length);
  return absl::OkStatus();
}

absl::Status ReadAllImpl(Reader& src, std::string& dest, size_t max_length,
                         size_t* length_read) {
  dest.clear();
  return ReadAndAppendAllImpl(src, dest, max_length, length_read);
}

absl::Status ReadAllImpl(Reader& src, Chain& dest, size_t max_length,
                         size_t* length_read) {
  dest.Clear();
  return ReadAndAppendAllImpl(src, dest, max_length, length_read);
}

absl::Status ReadAllImpl(Reader& src, absl::Cord& dest, size_t max_length,
                         size_t* length_read) {
  dest.Clear();
  return ReadAndAppendAllImpl(src, dest, max_length, length_read);
}

absl::Status ReadAndAppendAllImpl(Reader& src, std::string& dest,
                                  size_t max_length, size_t* length_read) {
  if (length_read == nullptr) {
    return ReadAndAppendAllImpl(src, dest, max_length);
  }
  const Position pos_before = src.pos();
  const absl::Status status = ReadAndAppendAllImpl(src, dest, max_length);
  RIEGELI_ASSERT_GE(src.pos(), pos_before)
      << "ReadAndAppendAllImpl(std::string&) decreased src.pos()";
  RIEGELI_ASSERT_LE(src.pos() - pos_before, max_length)
      << "ReadAndAppendAllImpl(std::string&) read more than requested";
  *length_read = IntCast<size_t>(src.pos() - pos_before);
  return status;
}

absl::Status ReadAndAppendAllImpl(Reader& src, Chain& dest, size_t max_length,
                                  size_t* length_read) {
  if (length_read == nullptr) {
    return ReadAndAppendAllImpl(src, dest, max_length);
  }
  const Position pos_before = src.pos();
  const absl::Status status = ReadAndAppendAllImpl(src, dest, max_length);
  RIEGELI_ASSERT_GE(src.pos(), pos_before)
      << "ReadAndAppendAllImpl(Chain&) decreased src.pos()";
  RIEGELI_ASSERT_LE(src.pos() - pos_before, max_length)
      << "ReadAndAppendAllImpl(Chain&) read more than requested";
  *length_read = IntCast<size_t>(src.pos() - pos_before);
  return status;
}

absl::Status ReadAndAppendAllImpl(Reader& src, absl::Cord& dest,
                                  size_t max_length, size_t* length_read) {
  if (length_read == nullptr) {
    return ReadAndAppendAllImpl(src, dest, max_length);
  }
  const Position pos_before = src.pos();
  const absl::Status status = ReadAndAppendAllImpl(src, dest, max_length);
  RIEGELI_ASSERT_GE(src.pos(), pos_before)
      << "ReadAndAppendAllImpl(absl::Cord&) decreased src.pos()";
  RIEGELI_ASSERT_LE(src.pos() - pos_before, max_length)
      << "ReadAndAppendAllImpl(absl::Cord&) read more than requested";
  *length_read = IntCast<size_t>(src.pos() - pos_before);
  return status;
}

}  // namespace riegeli::read_all_internal
