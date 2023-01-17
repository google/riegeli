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

#include "riegeli/base/buffer.h"

#include <stddef.h>

#include <functional>
#include <utility>

#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/cord_utils.h"

namespace riegeli {

namespace {

// A releasing callback for embedding a `Buffer` in an `absl::Cord`.
struct Releaser {
  void operator()() const {
    // Nothing to do: the destructor does the work.
  }
  Buffer buffer;
};

}  // namespace

absl::Cord Buffer::ToCord(const char* data, size_t length) && {
  if (data != nullptr || length > 0) {
    RIEGELI_ASSERT(std::greater_equal<>()(data, data_))
        << "Failed precondition of Buffer::ToCord(): "
           "substring not contained in the buffer";
    RIEGELI_ASSERT(std::less_equal<>()(data + length, data_ + capacity_))
        << "Failed precondition of Buffer::ToCord(): "
           "substring not contained in the buffer";
  }
  // `absl::cord_internal::kMaxInline`.
  static constexpr size_t kMaxInline = 15;
  if (length <= kMaxInline || Wasteful(capacity_, length)) {
    return MakeBlockyCord(absl::string_view(data, length));
  }
  return absl::MakeCordFromExternal(absl::string_view(data, length),
                                    Releaser{std::move(*this)});
}

void Buffer::AppendSubstrTo(const char* data, size_t length,
                            absl::Cord& dest) && {
  if (data != nullptr || length > 0) {
    RIEGELI_ASSERT(std::greater_equal<>()(data, data_))
        << "Failed precondition of Buffer::AppendSubstrTo(): "
           "substring not contained in the buffer";
    RIEGELI_ASSERT(std::less_equal<>()(data + length, data_ + capacity_))
        << "Failed precondition of Buffer::AppendSubstrTo(): "
           "substring not contained in the buffer";
  }
  if (length <= MaxBytesToCopyToCord(dest) || Wasteful(capacity_, length)) {
    AppendToBlockyCord(absl::string_view(data, length), dest);
    return;
  }
  dest.Append(absl::MakeCordFromExternal(absl::string_view(data, length),
                                         Releaser{std::move(*this)}));
}

void Buffer::PrependSubstrTo(const char* data, size_t length,
                             absl::Cord& dest) && {
  if (data != nullptr || length > 0) {
    RIEGELI_ASSERT(std::greater_equal<>()(data, data_))
        << "Failed precondition of Buffer::PrependSubstrTo(): "
           "substring not contained in the buffer";
    RIEGELI_ASSERT(std::less_equal<>()(data + length, data_ + capacity_))
        << "Failed precondition of Buffer::PrependSubstrTo(): "
           "substring not contained in the buffer";
  }
  if (length <= MaxBytesToCopyToCord(dest) || Wasteful(capacity_, length)) {
    PrependToBlockyCord(absl::string_view(data, length), dest);
    return;
  }
  dest.Prepend(absl::MakeCordFromExternal(absl::string_view(data, length),
                                          Releaser{std::move(*this)}));
}

}  // namespace riegeli
