// Copyright 2022 Google LLC
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

#include "riegeli/base/cord_utils.h"

#include <stddef.h>

#include <cstring>
#include <utility>

#include "absl/base/nullability.h"
#include "absl/strings/cord.h"
#include "absl/strings/cord_buffer.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/string_utils.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli::cord_internal {

void CopyCordToArray(const absl::Cord& src, char* absl_nullable dest) {
  for (const absl::string_view fragment : src.Chunks()) {
    std::memcpy(dest, fragment.data(), fragment.size());
    dest += fragment.size();
  }
}

absl::Cord MakeBlockyCord(absl::string_view src) {
  absl::Cord dest;
  AppendToBlockyCord(src, dest);
  return dest;
}

void AssignToBlockyCord(absl::string_view src, absl::Cord& dest) {
  if (src.size() <= absl::CordBuffer::kDefaultLimit) {
    dest = src;
    return;
  }
  dest.Clear();
  AppendToBlockyCord(src, dest);
}

void AppendToBlockyCord(absl::string_view src, absl::Cord& dest) {
  if (src.empty()) return;
  {
    absl::CordBuffer buffer = dest.GetAppendBuffer(0, 1);
    const size_t existing_length = buffer.length();
    if (existing_length > 0) {
      buffer.SetLength(
          UnsignedMin(existing_length + src.size(), buffer.capacity()));
      std::memcpy(buffer.data() + existing_length, src.data(),
                  buffer.length() - existing_length);
      src.remove_prefix(buffer.length() - existing_length);
      dest.Append(std::move(buffer));
      if (src.empty()) return;
    }
  }
  do {
    absl::CordBuffer buffer = absl::CordBuffer::CreateWithCustomLimit(
        kCordBufferBlockSize, src.size());
    buffer.SetLength(UnsignedMin(src.size(), buffer.capacity()));
    std::memcpy(buffer.data(), src.data(), buffer.length());
    src.remove_prefix(buffer.length());
    dest.Append(std::move(buffer));
  } while (!src.empty());
}

void PrependToBlockyCord(absl::string_view src, absl::Cord& dest) {
  while (!src.empty()) {
    absl::CordBuffer buffer = absl::CordBuffer::CreateWithCustomLimit(
        kCordBufferBlockSize, src.size());
    buffer.SetLength(UnsignedMin(src.size(), buffer.capacity()));
    std::memcpy(buffer.data(), src.data() + src.size() - buffer.length(),
                buffer.length());
    src.remove_suffix(buffer.length());
    dest.Prepend(std::move(buffer));
  }
}

}  // namespace riegeli::cord_internal
