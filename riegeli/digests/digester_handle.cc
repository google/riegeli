// Copyright 2023 Google LLC
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

#include "riegeli/digests/digester_handle.h"

#include <stddef.h>

#include <cstring>

#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/types.h"
#include "riegeli/base/zeros.h"

namespace riegeli {

void DigesterBaseHandle::WriteChainFallback(
    void* target, const Chain& src,
    void (*write)(void* target, absl::string_view src)) {
  for (const absl::string_view fragment : src.blocks()) {
    write(target, fragment);
  }
}

void DigesterBaseHandle::WriteCordFallback(
    void* target, const absl::Cord& src,
    void (*write)(void* target, absl::string_view src)) {
  {
    const absl::optional<absl::string_view> flat = src.TryFlat();
    if (flat != absl::nullopt) {
      write(target, *flat);
      return;
    }
  }
  for (const absl::string_view fragment : src.Chunks()) {
    write(target, fragment);
  }
}

void DigesterBaseHandle::WriteZerosFallback(
    void* target, Position length,
    void (*write)(void* target, absl::string_view src)) {
  const absl::string_view kArrayOfZeros = ArrayOfZeros();
  while (length > kArrayOfZeros.size()) {
    write(target, kArrayOfZeros);
    length -= kArrayOfZeros.size();
  }
  write(target,
        absl::string_view(kArrayOfZeros.data(), IntCast<size_t>(length)));
}

void DigesterBaseHandle::DigesterAbslStringifySink::Append(size_t length,
                                                           char src) {
  if (length == 0) return;
  if (src == '\0') {
    digester_.WriteZeros(length);
    return;
  }

  static constexpr size_t kBufferSize = 256;
  char buffer[kBufferSize];
  std::memset(buffer, src, kBufferSize);
  while (length > kBufferSize) {
    digester_.Write(absl::string_view(buffer, kBufferSize));
    length -= kBufferSize;
  }
  digester_.Write(absl::string_view(buffer, length));
}

}  // namespace riegeli
