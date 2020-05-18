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

#ifndef RIEGELI_BYTES_WRITER_UTILS_H_
#define RIEGELI_BYTES_WRITER_UTILS_H_

#include <stddef.h>
#include <stdint.h>

#include <cstring>

#include "absl/base/optimization.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Writes a single byte.
//
// Returns `false` on failure.
bool WriteByte(uint8_t data, Writer& dest);

// Writes the given number of zero bytes.
//
// Returns `false` on failure.
bool WriteZeros(Position length, Writer& dest);

// Implementation details follow.

inline bool WriteByte(uint8_t data, Writer& dest) {
  if (ABSL_PREDICT_FALSE(!dest.Push())) return false;
  *dest.cursor() = static_cast<char>(data);
  dest.move_cursor(1);
  return true;
}

namespace internal {

bool WriteZerosSlow(Position length, Writer& dest);

}  // namespace internal

inline bool WriteZeros(Position length, Writer& dest) {
  if (ABSL_PREDICT_TRUE(length <= dest.available())) {
    if (ABSL_PREDICT_TRUE(
            // `std::memset(nullptr, _, 0)` is undefined.
            length > 0)) {
      std::memset(dest.cursor(), 0, IntCast<size_t>(length));
      dest.move_cursor(IntCast<size_t>(length));
    }
    return true;
  }
  return internal::WriteZerosSlow(length, dest);
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_WRITER_UTILS_H_
