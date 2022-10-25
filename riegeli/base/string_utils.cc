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

#include "riegeli/base/string_utils.h"

#include <stddef.h>

#include <string>

#include "riegeli/base/arithmetic.h"

namespace riegeli {

void ResizeStringAmortized(std::string& dest, size_t new_size) {
  if (new_size > dest.capacity()) {
    dest.reserve(UnsignedMax(
        new_size,
        UnsignedMin(dest.capacity() + dest.capacity() / 2, dest.max_size())));
  }
  dest.resize(new_size);
}

}  // namespace riegeli
