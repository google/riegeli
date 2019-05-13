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

#ifndef RIEGELI_BYTES_BACKWARD_WRITER_UTILS_H_
#define RIEGELI_BYTES_BACKWARD_WRITER_UTILS_H_

#include <stdint.h>

#include "absl/base/optimization.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/varint.h"
#include "riegeli/bytes/writer_utils.h"

namespace riegeli {

bool WriteVarint32(BackwardWriter* dest, uint32_t data);
bool WriteVarint64(BackwardWriter* dest, uint64_t data);

// Implementation details follow.

inline bool WriteVarint32(BackwardWriter* dest, uint32_t data) {
  const size_t length = LengthVarint32(data);
  if (ABSL_PREDICT_FALSE(!dest->Push(length))) return false;
  char* start = dest->cursor() - length;
  dest->set_cursor(start);
  WriteVarint32(start, data);
  return true;
}

inline bool WriteVarint64(BackwardWriter* dest, uint64_t data) {
  const size_t length = LengthVarint64(data);
  if (ABSL_PREDICT_FALSE(!dest->Push(length))) return false;
  char* start = dest->cursor() - length;
  dest->set_cursor(start);
  WriteVarint64(start, data);
  return true;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_BACKWARD_WRITER_UTILS_H_
