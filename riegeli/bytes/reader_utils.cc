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

#include <limits>
#include <string>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

namespace {

ABSL_ATTRIBUTE_COLD bool MaxSizeExceeded(Reader& src, size_t max_size) {
  return src.Fail(absl::ResourceExhaustedError(
      absl::StrCat("Maximum size exceeded: ", max_size)));
}

}  // namespace

bool ReadAll(Reader& src, absl::string_view& dest, size_t max_size) {
  max_size = UnsignedMin(max_size, dest.max_size());
  if (src.SupportsSize()) {
    const absl::optional<Position> size = src.Size();
    if (ABSL_PREDICT_FALSE(size == absl::nullopt)) {
      dest = absl::string_view();
      return false;
    }
    const Position remaining = SaturatingSub(*size, src.pos());
    if (ABSL_PREDICT_FALSE(remaining > max_size)) {
      if (ABSL_PREDICT_FALSE(!src.Read(max_size, dest))) {
        if (ABSL_PREDICT_FALSE(!src.healthy())) return false;
      }
      return MaxSizeExceeded(src, max_size);
    }
    if (ABSL_PREDICT_FALSE(!src.Read(IntCast<size_t>(remaining), dest))) {
      return src.healthy();
    }
    return true;
  } else {
    do {
      if (ABSL_PREDICT_FALSE(src.available() > max_size)) {
        dest = absl::string_view(src.cursor(), max_size);
        src.move_cursor(max_size);
        return MaxSizeExceeded(src, max_size);
      }
    } while (src.Pull(src.available() + 1,
                      SaturatingAdd(src.available(), src.available())));
    dest = absl::string_view(src.cursor(), src.available());
    src.move_cursor(src.available());
    return src.healthy();
  }
}

bool ReadAll(Reader& src, std::string& dest, size_t max_size) {
  dest.clear();
  return ReadAndAppendAll(src, dest, max_size);
}

bool ReadAll(Reader& src, Chain& dest, size_t max_size) {
  dest.Clear();
  return ReadAndAppendAll(src, dest, max_size);
}

bool ReadAll(Reader& src, absl::Cord& dest, size_t max_size) {
  dest.Clear();
  return ReadAndAppendAll(src, dest, max_size);
}

bool ReadAndAppendAll(Reader& src, std::string& dest, size_t max_size) {
  max_size = UnsignedMin(max_size, dest.max_size() - dest.size());
  if (src.SupportsSize()) {
    const absl::optional<Position> size = src.Size();
    if (ABSL_PREDICT_FALSE(size == absl::nullopt)) return false;
    const Position remaining = SaturatingSub(*size, src.pos());
    if (ABSL_PREDICT_FALSE(remaining > max_size)) {
      if (ABSL_PREDICT_FALSE(!src.ReadAndAppend(max_size, dest))) {
        if (ABSL_PREDICT_FALSE(!src.healthy())) return false;
      }
      return MaxSizeExceeded(src, max_size);
    }
    if (ABSL_PREDICT_FALSE(
            !src.ReadAndAppend(IntCast<size_t>(remaining), dest))) {
      return src.healthy();
    }
    return true;
  } else {
    do {
      if (ABSL_PREDICT_FALSE(src.available() > max_size)) {
        dest.append(src.cursor(), max_size);
        src.move_cursor(max_size);
        return MaxSizeExceeded(src, max_size);
      }
      max_size -= src.available();
      dest.append(src.cursor(), src.available());
      src.move_cursor(src.available());
    } while (src.Pull());
    return src.healthy();
  }
}

bool ReadAndAppendAll(Reader& src, Chain& dest, size_t max_size) {
  max_size =
      UnsignedMin(max_size, std::numeric_limits<size_t>::max() - dest.size());
  if (src.SupportsSize()) {
    const absl::optional<Position> size = src.Size();
    if (ABSL_PREDICT_FALSE(size == absl::nullopt)) return false;
    const Position remaining = SaturatingSub(*size, src.pos());
    if (ABSL_PREDICT_FALSE(remaining > max_size)) {
      if (ABSL_PREDICT_FALSE(!src.ReadAndAppend(max_size, dest))) {
        if (ABSL_PREDICT_FALSE(!src.healthy())) return false;
      }
      return MaxSizeExceeded(src, max_size);
    }
    if (ABSL_PREDICT_FALSE(
            !src.ReadAndAppend(IntCast<size_t>(remaining), dest))) {
      return src.healthy();
    }
    return true;
  } else {
    do {
      if (ABSL_PREDICT_FALSE(src.available() > max_size)) {
        src.ReadAndAppend(max_size, dest);
        return MaxSizeExceeded(src, max_size);
      }
      max_size -= src.available();
      src.ReadAndAppend(src.available(), dest);
    } while (src.Pull());
    return src.healthy();
  }
}

bool ReadAndAppendAll(Reader& src, absl::Cord& dest, size_t max_size) {
  max_size =
      UnsignedMin(max_size, std::numeric_limits<size_t>::max() - dest.size());
  if (src.SupportsSize()) {
    const absl::optional<Position> size = src.Size();
    if (ABSL_PREDICT_FALSE(size == absl::nullopt)) return false;
    const Position remaining = SaturatingSub(*size, src.pos());
    if (ABSL_PREDICT_FALSE(remaining > max_size)) {
      if (ABSL_PREDICT_FALSE(!src.ReadAndAppend(max_size, dest))) {
        if (ABSL_PREDICT_FALSE(!src.healthy())) return false;
      }
      return MaxSizeExceeded(src, max_size);
    }
    if (ABSL_PREDICT_FALSE(
            !src.ReadAndAppend(IntCast<size_t>(remaining), dest))) {
      return src.healthy();
    }
    return true;
  } else {
    do {
      if (ABSL_PREDICT_FALSE(src.available() > max_size)) {
        src.ReadAndAppend(max_size, dest);
        return MaxSizeExceeded(src, max_size);
      }
      max_size -= src.available();
      src.ReadAndAppend(src.available(), dest);
    } while (src.Pull());
    return src.healthy();
  }
}

bool CopyAll(Reader& src, Writer& dest, Position max_size) {
  if (src.SupportsSize()) {
    const absl::optional<Position> size = src.Size();
    if (ABSL_PREDICT_FALSE(size == absl::nullopt)) return false;
    const Position remaining = SaturatingSub(*size, src.pos());
    if (ABSL_PREDICT_FALSE(remaining > max_size)) {
      if (ABSL_PREDICT_FALSE(!src.CopyTo(max_size, dest))) {
        return dest.healthy() && src.healthy();
      }
      return MaxSizeExceeded(src, max_size);
    }
    if (ABSL_PREDICT_FALSE(!src.CopyTo(remaining, dest))) {
      return dest.healthy() && src.healthy();
    }
    return true;
  } else {
    do {
      if (ABSL_PREDICT_FALSE(src.available() > max_size)) {
        if (ABSL_PREDICT_FALSE(!src.CopyTo(max_size, dest))) {
          if (ABSL_PREDICT_FALSE(!dest.healthy())) return false;
        }
        return MaxSizeExceeded(src, max_size);
      }
      max_size -= src.available();
      if (ABSL_PREDICT_FALSE(!src.CopyTo(src.available(), dest))) {
        if (ABSL_PREDICT_FALSE(!dest.healthy())) return false;
      }
    } while (src.Pull());
    return src.healthy();
  }
}

bool CopyAll(Reader& src, BackwardWriter& dest, size_t max_size) {
  if (src.SupportsSize()) {
    const absl::optional<Position> size = src.Size();
    if (ABSL_PREDICT_FALSE(size == absl::nullopt)) return false;
    const Position remaining = SaturatingSub(*size, src.pos());
    if (ABSL_PREDICT_FALSE(remaining > max_size)) {
      if (ABSL_PREDICT_FALSE(!src.Skip(max_size))) {
        if (ABSL_PREDICT_FALSE(!src.healthy())) return false;
      }
      return MaxSizeExceeded(src, max_size);
    }
    if (ABSL_PREDICT_FALSE(!src.CopyTo(IntCast<size_t>(remaining), dest))) {
      return dest.healthy() && src.healthy();
    }
    return true;
  } else {
    Chain data;
    do {
      if (ABSL_PREDICT_FALSE(src.available() > max_size)) {
        src.move_cursor(max_size);
        return MaxSizeExceeded(src, max_size);
      }
      max_size -= src.available();
      src.ReadAndAppend(src.available(), data);
    } while (src.Pull());
    if (ABSL_PREDICT_FALSE(!src.healthy())) return false;
    return dest.Write(std::move(data));
  }
}

}  // namespace riegeli
