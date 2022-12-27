// Copyright 2020 Google LLC
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

#include "riegeli/base/shared_buffer.h"

#include <stddef.h>

#include <functional>

#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/cord_utils.h"

namespace riegeli {

absl::Cord SharedBuffer::ToCord(const char* data, size_t length) const {
  if (length > 0) {
    RIEGELI_ASSERT(std::greater_equal<>()(data, const_data()))
        << "Failed precondition of SharedBuffer::ToCord(): "
           "substring not contained in the buffer";
    RIEGELI_ASSERT(
        std::less_equal<>()(data + length, const_data() + capacity()))
        << "Failed precondition of SharedBuffer::ToCord(): "
           "substring not contained in the buffer";
  }
  // `absl::cord_internal::kMaxInline`.
  static constexpr size_t kMaxInline = 15;
  if (length <= kMaxInline || Wasteful(capacity(), length)) {
    return MakeBlockyCord(absl::string_view(data, length));
  }
  void* ptr = Share();
  return absl::MakeCordFromExternal(absl::string_view(data, length),
                                    [ptr] { DeleteShared(ptr); });
}

}  // namespace riegeli
