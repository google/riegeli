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

#include "riegeli/chunk_encoding/hash.h"

#include <stdint.h>
#include <vector>

#include "absl/strings/string_view.h"
#include "highwayhash/hh_types.h"
#include "highwayhash/highwayhash_target.h"
#include "highwayhash/instruction_sets.h"
#include "riegeli/base/chain.h"

namespace riegeli {
namespace internal {

namespace {

const highwayhash::HHKey kHashKey = {
    0x2f696c6567656952,  // 'Riegeli/'
    0x0a7364726f636572,  // 'records\n'
    0x2f696c6567656952,  // 'Riegeli/'
    0x0a7364726f636572,  // 'records\n'
};

}  // namespace

uint64_t Hash(absl::string_view data) {
  highwayhash::HHResult64 result;
  highwayhash::InstructionSets::Run<highwayhash::HighwayHash>(
      kHashKey, data.data(), data.size(), &result);
  return result;
}

uint64_t Hash(const Chain& data) {
  if (data.empty()) return Hash("");
  if (data.blocks().size() == 1) return Hash(data.blocks().front());
  std::vector<highwayhash::StringView> fragments;
  fragments.reserve(data.blocks().size());
  for (const absl::string_view fragment : data.blocks()) {
    fragments.push_back(
        highwayhash::StringView{fragment.data(), fragment.size()});
  }
  highwayhash::HHResult64 result;
  highwayhash::InstructionSets::Run<highwayhash::HighwayHashCat>(
      kHashKey, fragments.data(), fragments.size(), &result);
  return result;
}

}  // namespace internal
}  // namespace riegeli
