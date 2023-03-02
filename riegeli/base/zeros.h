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

#ifndef RIEGELI_BASE_ZEROS_H_
#define RIEGELI_BASE_ZEROS_H_

#include <stddef.h>

#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"

namespace riegeli {

// Returns some fixed number of lazily allocated zero bytes (64 KB).
absl::string_view ArrayOfZeros();

// Returns the given number of zero bytes.
absl::Cord CordOfZeros(size_t length);

// Implementation details follow.

// Inlined so that the compiler can see the fixed size of the result.
inline absl::string_view ArrayOfZeros() {
  static constexpr size_t kArrayOfZerosSize = size_t{64} << 10;
  static const char* const kArrayOfZeros = new char[kArrayOfZerosSize]();
  return absl::string_view(kArrayOfZeros, kArrayOfZerosSize);
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_ZEROS_H_
