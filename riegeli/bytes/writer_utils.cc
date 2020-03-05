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

#include "riegeli/bytes/writer_utils.h"

#include <stddef.h>

#include <cstring>

#include "absl/base/optimization.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {
namespace internal {

bool WriteZerosSlow(Position length, Writer* dest) {
  RIEGELI_ASSERT_GT(length, dest->available())
      << "Failed precondition of WriteZerosSlow(): "
         "length too small, use WriteZeros() instead";
  do {
    const size_t available_length = dest->available();
    if (
        // `std::memset(nullptr, _, 0)` is undefined.
        available_length > 0) {
      std::memset(dest->cursor(), 0, available_length);
      dest->move_cursor(available_length);
      length -= available_length;
    }
    if (ABSL_PREDICT_FALSE(!dest->Push(1, length))) return false;
  } while (length > dest->available());
  std::memset(dest->cursor(), 0, IntCast<size_t>(length));
  dest->move_cursor(IntCast<size_t>(length));
  return true;
}

}  // namespace internal
}  // namespace riegeli
