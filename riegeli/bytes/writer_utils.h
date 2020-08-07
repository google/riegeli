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

#include <stdint.h>

#include "absl/base/optimization.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Writes a single byte.
//
// Returns `false` on failure.
bool WriteByte(uint8_t data, Writer& dest);

// Implementation details follow.

inline bool WriteByte(uint8_t data, Writer& dest) {
  if (ABSL_PREDICT_FALSE(!dest.Push())) return false;
  *dest.cursor() = static_cast<char>(data);
  dest.move_cursor(1);
  return true;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_WRITER_UTILS_H_
