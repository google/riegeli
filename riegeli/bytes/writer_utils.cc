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
#include <stdint.h>
#include <cstring>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {
namespace internal {

bool WriteVarint32Slow(Writer* dest, uint32_t data) {
  char buffer[kMaxLengthVarint32()];
  char* const end = WriteVarint32(buffer, data);
  return dest->Write(absl::string_view(buffer, PtrDistance(buffer, end)));
}

bool WriteVarint64Slow(Writer* dest, uint64_t data) {
  char buffer[kMaxLengthVarint64()];
  char* const end = WriteVarint64(buffer, data);
  return dest->Write(absl::string_view(buffer, PtrDistance(buffer, end)));
}

bool WriteZerosSlow(Writer* dest, Position length) {
  RIEGELI_ASSERT_GT(length, dest->available())
      << "Failed precondition of WriteZerosSlow(): "
         "length too small, use WriteZeros() instead";
  if (dest->available() == 0) {  // memset(nullptr, _, 0) is undefined.
    goto skip_copy;
  }
  do {
    {
      const size_t available_length = dest->available();
      std::memset(dest->cursor(), 0, available_length);
      dest->set_cursor(dest->limit());
      length -= available_length;
    }
  skip_copy:
    if (ABSL_PREDICT_FALSE(!dest->Push())) return false;
  } while (length > dest->available());
  std::memset(dest->cursor(), 0, length);
  dest->set_cursor(dest->cursor() + length);
  return true;
}

}  // namespace internal
}  // namespace riegeli
