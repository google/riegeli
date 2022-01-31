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

#include <functional>
#include <utility>

#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"

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

absl::Cord Buffer::ToCord(absl::string_view substr) && {
  RIEGELI_ASSERT(std::greater_equal<>()(substr.data(), data()))
      << "Failed precondition of Buffer::ToCord(): "
         "substring not contained in the buffer";
  RIEGELI_ASSERT(
      std::less_equal<>()(substr.data() + substr.size(), data() + capacity()))
      << "Failed precondition of Buffer::ToCord(): "
         "substring not contained in the buffer";
  // `absl::cord_internal::kMaxInline`.
  static constexpr size_t kMaxInline = 15;
  if (substr.size() <= kMaxInline || Wasteful(capacity(), substr.size())) {
    return MakeBlockyCord(substr);
  }
  return absl::MakeCordFromExternal(substr, Releaser{std::move(*this)});
}

void Buffer::AppendSubstrTo(absl::string_view substr, absl::Cord& dest) && {
  RIEGELI_ASSERT(std::greater_equal<>()(substr.data(), data()))
      << "Failed precondition of Buffer::AppendSubstrTo(): "
         "substring not contained in the buffer";
  RIEGELI_ASSERT(
      std::less_equal<>()(substr.data() + substr.size(), data() + capacity()))
      << "Failed precondition of Buffer::AppendSubstrTo(): "
         "substring not contained in the buffer";
  if (substr.size() <= MaxBytesToCopyToCord(dest) ||
      Wasteful(capacity(), substr.size())) {
    AppendToBlockyCord(substr, dest);
    return;
  }
  dest.Append(absl::MakeCordFromExternal(substr, Releaser{std::move(*this)}));
}

void Buffer::PrependSubstrTo(absl::string_view substr, absl::Cord& dest) && {
  RIEGELI_ASSERT(std::greater_equal<>()(substr.data(), data()))
      << "Failed precondition of Buffer::PrependSubstrTo(): "
         "substring not contained in the buffer";
  RIEGELI_ASSERT(
      std::less_equal<>()(substr.data() + substr.size(), data() + capacity()))
      << "Failed precondition of Buffer::PrependSubstrTo(): "
         "substring not contained in the buffer";
  if (substr.size() <= MaxBytesToCopyToCord(dest) ||
      Wasteful(capacity(), substr.size())) {
    PrependToBlockyCord(substr, dest);
    return;
  }
  dest.Prepend(absl::MakeCordFromExternal(substr, Releaser{std::move(*this)}));
}

}  // namespace riegeli
