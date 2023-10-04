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

#include "riegeli/digests/digester.h"

#include <stddef.h>

#include <cstring>

#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/types.h"
#include "riegeli/base/zeros.h"

namespace riegeli {

void DigesterBase::Write(const Chain& src) {
  RIEGELI_ASSERT(is_open())
      << "Failed precondition of DigesterBase::Write(): object closed";
  for (const absl::string_view fragment : src.blocks()) {
    WriteImpl(fragment);
  }
}

void DigesterBase::Write(const absl::Cord& src) {
  RIEGELI_ASSERT(is_open())
      << "Failed precondition of DigesterBase::Write(): object closed";
  {
    const absl::optional<absl::string_view> flat = src.TryFlat();
    if (flat != absl::nullopt) {
      WriteImpl(*flat);
      return;
    }
  }
  for (const absl::string_view fragment : src.Chunks()) {
    WriteImpl(fragment);
  }
}

void DigesterBase::WriteZerosImpl(riegeli::Position length) {
  const absl::string_view kArrayOfZeros = ArrayOfZeros();
  while (length > kArrayOfZeros.size()) {
    WriteImpl(kArrayOfZeros);
    length -= kArrayOfZeros.size();
  }
  WriteImpl(absl::string_view(kArrayOfZeros.data(), IntCast<size_t>(length)));
}

void DigesterBase::DigesterAbslStringifySink::Append(size_t length, char src) {
  if (length == 0) return;
  if (src == '\0') {
    digester_->WriteZeros(length);
    return;
  }

  static constexpr size_t kBufferSize = 256;
  char buffer[kBufferSize];
  std::memset(buffer, src, kBufferSize);
  while (length > kBufferSize) {
    digester_->Write(absl::string_view(buffer, kBufferSize));
    length -= kBufferSize;
  }
  digester_->Write(absl::string_view(buffer, length));
}

}  // namespace riegeli
