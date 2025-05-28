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

#include "riegeli/bytes/copy_all.h"

#include <stddef.h>

#include <optional>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli::copy_all_internal {

namespace {

ABSL_ATTRIBUTE_COLD absl::Status MaxLengthExceeded(Reader& src,
                                                   Position max_length) {
  return src.AnnotateStatus(absl::ResourceExhaustedError(
      absl::StrCat("Maximum length exceeded: ", max_length)));
}

absl::Status CopyAllImpl(Reader& src, Writer& dest, Position max_length,
                         bool set_write_size_hint) {
  if (src.SupportsSize()) {
    const std::optional<Position> size = src.Size();
    if (ABSL_PREDICT_FALSE(size == std::nullopt)) return src.status();
    const Position remaining = SaturatingSub(*size, src.pos());
    if (ABSL_PREDICT_FALSE(remaining > max_length)) {
      if (set_write_size_hint) dest.SetWriteSizeHint(max_length);
      if (ABSL_PREDICT_FALSE(!src.Copy(max_length, dest))) {
        if (ABSL_PREDICT_FALSE(!dest.ok())) return dest.status();
        if (ABSL_PREDICT_FALSE(!src.ok())) return src.status();
        return absl::OkStatus();
      }
      return MaxLengthExceeded(src, max_length);
    }
    if (set_write_size_hint) dest.SetWriteSizeHint(remaining);
    if (ABSL_PREDICT_FALSE(!src.Copy(remaining, dest))) {
      if (ABSL_PREDICT_FALSE(!dest.ok())) return dest.status();
      if (ABSL_PREDICT_FALSE(!src.ok())) return src.status();
    }
  } else {
    Position remaining_max_length = max_length;
    do {
      if (ABSL_PREDICT_FALSE(src.available() > remaining_max_length)) {
        if (ABSL_PREDICT_FALSE(!src.Copy(remaining_max_length, dest))) {
          if (ABSL_PREDICT_FALSE(!dest.ok())) return dest.status();
        }
        return MaxLengthExceeded(src, max_length);
      }
      remaining_max_length -= src.available();
      if (ABSL_PREDICT_FALSE(!src.Copy(src.available(), dest))) {
        if (ABSL_PREDICT_FALSE(!dest.ok())) return dest.status();
      }
    } while (src.Pull());
    if (ABSL_PREDICT_FALSE(!src.ok())) return src.status();
  }
  return absl::OkStatus();
}

}  // namespace

absl::Status CopyAllImpl(Reader& src, Writer& dest, Position max_length,
                         Position* length_read, bool set_write_size_hint) {
  if (length_read == nullptr) {
    return CopyAllImpl(src, dest, max_length, set_write_size_hint);
  }
  const Position pos_before = src.pos();
  const absl::Status status =
      CopyAllImpl(src, dest, max_length, set_write_size_hint);
  RIEGELI_ASSERT_GE(src.pos(), pos_before)
      << "CopyAllImpl(Writer&) decreased src.pos()";
  RIEGELI_ASSERT_LE(src.pos() - pos_before, max_length)
      << "CopyAllImpl(Writer&) read more than requested";
  *length_read = src.pos() - pos_before;
  return status;
}

absl::Status CopyAllImpl(Reader& src, BackwardWriter& dest, size_t max_length,
                         bool set_write_size_hint) {
  if (src.SupportsSize()) {
    const std::optional<Position> size = src.Size();
    if (ABSL_PREDICT_FALSE(size == std::nullopt)) return src.status();
    const Position remaining = SaturatingSub(*size, src.pos());
    if (ABSL_PREDICT_FALSE(remaining > max_length)) {
      if (ABSL_PREDICT_FALSE(!src.Skip(max_length))) {
        if (ABSL_PREDICT_FALSE(!src.ok())) return src.status();
      }
      return MaxLengthExceeded(src, max_length);
    }
    if (set_write_size_hint) dest.SetWriteSizeHint(remaining);
    if (ABSL_PREDICT_FALSE(!src.Copy(IntCast<size_t>(remaining), dest))) {
      if (ABSL_PREDICT_FALSE(!dest.ok())) return dest.status();
      if (ABSL_PREDICT_FALSE(!src.ok())) return src.status();
    }
  } else {
    size_t remaining_max_length = max_length;
    Chain data;
    do {
      if (ABSL_PREDICT_FALSE(src.available() > remaining_max_length)) {
        src.move_cursor(remaining_max_length);
        return MaxLengthExceeded(src, max_length);
      }
      remaining_max_length -= src.available();
      src.ReadAndAppend(src.available(), data);
    } while (src.Pull());
    if (ABSL_PREDICT_FALSE(!src.ok())) return src.status();
    if (ABSL_PREDICT_FALSE(!dest.Write(std::move(data)))) return dest.status();
  }
  return absl::OkStatus();
}

}  // namespace riegeli::copy_all_internal
