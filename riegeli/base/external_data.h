// Copyright 2024 Google LLC
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

#ifndef RIEGELI_BASE_EXTERNAL_DATA_H_
#define RIEGELI_BASE_EXTERNAL_DATA_H_

#include <memory>

#include "absl/strings/string_view.h"

namespace riegeli {

// Type-erased external object with its deleter.
//
// `ExternalStorage` can be decomposed with `void* ExternalStorage::release()`
// and `ExternalStorage::get_deleter() -> void (*)(void*)`.
using ExternalStorage = std::unique_ptr<void, void (*)(void*)>;

// Type-erased external object with its deleter and a substring of a byte array
// it ows.
struct ExternalData {
  // Support `ExternalRef`.
  friend absl::string_view RiegeliToStringView(const ExternalData* self) {
    return self->substr;
  }

  ExternalStorage storage;  // Must outlive usages of `substr`.
  absl::string_view substr;
};

// Creates `ExternalData` holding a copy of `data`.
ExternalData ExternalDataCopy(absl::string_view data);

}  // namespace riegeli

#endif  // RIEGELI_BASE_EXTERNAL_DATA_H_
