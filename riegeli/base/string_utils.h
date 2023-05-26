// Copyright 2021 Google LLC
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

#ifndef RIEGELI_BASE_STRING_UTILS_H_
#define RIEGELI_BASE_STRING_UTILS_H_

#include <stddef.h>

#include <string>

namespace riegeli {

// Resizes `dest` to `new_size`, ensuring that repeated growth has the cost
// proportional to the final size. New contents are unspecified.
void ResizeStringAmortized(std::string& dest, size_t new_size);

}  // namespace riegeli

#endif  // RIEGELI_BASE_STRING_UTILS_H_
