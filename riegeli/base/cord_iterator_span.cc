// Copyright 2025 Google LLC
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

#include "riegeli/base/cord_iterator_span.h"

#include <stddef.h>

#include <cstring>
#include <string>

#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/string_utils.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

void CordIteratorSpan::ReadSlow(absl::Cord::CharIterator& src, size_t length,
                                char* dest) {
  absl::string_view chunk = absl::Cord::ChunkRemaining(src);
  RIEGELI_ASSERT_LT(chunk.size(), length)
      << "Failed precondition of CordIteratorSpan::ReadSlow(): "
         "enough data available, use Read() instead";
  do {
    std::memcpy(dest, chunk.data(), chunk.size());
    absl::Cord::Advance(&src, chunk.size());
    dest += chunk.size();
    length -= chunk.size();
    chunk = absl::Cord::ChunkRemaining(src);
  } while (chunk.size() < length);
  std::memcpy(dest, chunk.data(), length);
  absl::Cord::Advance(&src, length);
}

absl::string_view CordIteratorSpan::ToStringView(std::string& scratch) && {
  absl::Cord::CharIterator& iter = *iterator_;
  size_t length = length_;
  if (length == 0) return absl::string_view();
  absl::string_view chunk = absl::Cord::ChunkRemaining(iter);
  if (ABSL_PREDICT_TRUE(chunk.size() >= length)) {
    absl::Cord::Advance(&iter, length);
    return chunk.substr(0, length);
  }
  scratch.clear();
  ResizeStringAmortized(scratch, length);
  ReadSlow(iter, length, scratch.data());
  return scratch;
}

void CordIteratorSpan::ToString(std::string& dest) && {
  absl::Cord::CharIterator& iter = *iterator_;
  size_t length = length_;
  dest.clear();
  dest.resize(length);
  Read(iter, length, dest.data());
}

}  // namespace riegeli
