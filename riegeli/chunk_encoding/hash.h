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

#ifndef RIEGELI_CHUNK_ENCODING_HASH_H_
#define RIEGELI_CHUNK_ENCODING_HASH_H_

#include <stdint.h>

#include "absl/strings/string_view.h"
#include "riegeli/base/chain.h"

namespace riegeli::chunk_encoding_internal {

uint64_t Hash(absl::string_view data);
uint64_t Hash(const Chain& data);

}  // namespace riegeli::chunk_encoding_internal

#endif  // RIEGELI_CHUNK_ENCODING_HASH_H_
