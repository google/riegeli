// Copyright 2017 Google LLC
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

#ifndef RIEGELI_BYTES_READER_UTILS_H_
#define RIEGELI_BYTES_READER_UTILS_H_

#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <string>

#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Reads all remaining bytes from `src` to `*dest`, clearing any existing data
// in `dest`.
//
// Fails `src` with `absl::ResourceExhaustedError()` if `max_size` would be
// exceeded.
//
// Return values:
//  * `true` (`src.healthy()`)   - success
//  * `false` (`!src.healthy()`) - failure
bool ReadAll(Reader& src, absl::string_view& dest,
             size_t max_size = std::numeric_limits<size_t>::max());
bool ReadAll(Reader& src, std::string& dest,
             size_t max_size = std::numeric_limits<size_t>::max());
bool ReadAll(Reader& src, Chain& dest,
             size_t max_size = std::numeric_limits<size_t>::max());
bool ReadAll(Reader& src, absl::Cord& dest,
             size_t max_size = std::numeric_limits<size_t>::max());

// Reads all remaining bytes from `src` to `*dest`, appending to any existing
// data in `dest`.
//
// Fails `src` with `absl::ResourceExhaustedError()` if `max_size` would be
// exceeded.
//
// Return values:
//  * `true` (`src.healthy()`)   - success
//  * `false` (`!src.healthy()`) - failure
bool ReadAndAppendAll(Reader& src, std::string& dest,
                      size_t max_size = std::numeric_limits<size_t>::max());
bool ReadAndAppendAll(Reader& src, Chain& dest,
                      size_t max_size = std::numeric_limits<size_t>::max());
bool ReadAndAppendAll(Reader& src, absl::Cord& dest,
                      size_t max_size = std::numeric_limits<size_t>::max());

// Reads all remaining bytes from `src` to `dest`.
//
// `CopyAll(Writer&)` writes as much as could be read if reading failed, and
// reads an unspecified length (between what could be written and the
// requested length) if writing failed.
//
// `CopyAll(BackwardWriter&)` writes nothing if reading failed, and reads
// the full requested length even if writing failed.
//
// Fails `src` with `absl::ResourceExhaustedError()` if `max_size` would be
// exceeded.
//
// Return values:
//  * `true` (`dest.healthy() && src.healthy()`)    - success
//  * `false` (`!dest.healthy() || !src.healthy()`) - failure
bool CopyAll(Reader& src, Writer& dest,
             Position max_size = std::numeric_limits<Position>::max());
bool CopyAll(Reader& src, BackwardWriter& dest,
             size_t max_size = std::numeric_limits<size_t>::max());

// Reads a single byte.
//
// Returns `absl::nullopt` on failure, with the current position unchanged.
absl::optional<uint8_t> ReadByte(Reader& src);

// Implementation details follow.

inline absl::optional<uint8_t> ReadByte(Reader& src) {
  if (ABSL_PREDICT_FALSE(!src.Pull())) return absl::nullopt;
  const uint8_t data = static_cast<uint8_t>(*src.cursor());
  src.move_cursor(1);
  return data;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_READER_UTILS_H_
