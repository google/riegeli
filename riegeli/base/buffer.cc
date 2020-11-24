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

#include <cstring>
#include <functional>
#include <utility>

#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"

namespace riegeli {

absl::Cord BufferToCord(absl::string_view substr, Buffer& buffer) {
  RIEGELI_ASSERT(std::greater_equal<>()(substr.data(), buffer.GetData()))
      << "Failed precondition of BufferToCord(): "
         "substring not contained in the buffer";
  RIEGELI_ASSERT(std::less_equal<>()(substr.data() + substr.size(),
                                     buffer.GetData() + buffer.size()))
      << "Failed precondition of BufferToCord(): "
         "substring not contained in the buffer";

  struct Releaser {
    // TODO: Remove the `absl::string_view` parameter when the Abseil
    // dependency is upgraded.
    void operator()(absl::string_view) const {
      // Nothing to do: the destructor does the work.
    }
    Buffer buffer;
  };

  if (substr.size() <= 15 /* `absl::Cord::InlineRep::kMaxInline` */ ||
      Wasteful(buffer.size(), substr.size())) {
    if (substr.size() <= 4096 - 13 /* `kMaxFlatSize` from cord.cc */) {
      // `absl::Cord(absl::string_view)` allocates a single node of that length.
      return absl::Cord(substr);
    }
    // `absl::Cord(absl::string_view)` would split that length, so rewrite the
    // buffer and use `absl::MakeCordFromExternal()`.
    Buffer new_buffer(substr.size());
    char* const new_data = new_buffer.GetData();
    std::memcpy(new_data, substr.data(), substr.size());
    return absl::MakeCordFromExternal(
        absl::string_view(new_data, substr.size()),
        Releaser{std::move(new_buffer)});
  }

  return absl::MakeCordFromExternal(substr, Releaser{std::move(buffer)});
}

}  // namespace riegeli
