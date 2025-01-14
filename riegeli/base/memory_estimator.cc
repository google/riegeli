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

#include "riegeli/base/memory_estimator.h"

#ifdef __GXX_RTTI
#include <cxxabi.h>  // IWYU pragma: keep
#endif
#include <stddef.h>

#include <algorithm>
#include <cstdlib>
#include <string>
#include <typeindex>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"

namespace riegeli {

bool MemoryEstimatorDefault::RegisterNodeImpl(const void* ptr) {
  return ptr != nullptr && objects_seen_.insert(ptr).second;
}

void MemoryEstimatorReportingUnknownTypes::RegisterUnknownTypeImpl() {
  unknown_types_no_rtti_ = true;
}

void MemoryEstimatorReportingUnknownTypes::RegisterUnknownTypeImpl(
    std::type_index index) {
  unknown_types_.insert(index);
}

std::vector<std::string> MemoryEstimatorReportingUnknownTypes::UnknownTypes()
    const {
  std::vector<std::string> result;
  result.reserve((unknown_types_no_rtti_ ? 1 : 0) + unknown_types_.size());
  if (unknown_types_no_rtti_) result.emplace_back("<no rtti>");
  for (const std::type_index index : unknown_types_) {
#ifdef __GXX_RTTI
    int status = 0;
    char* const demangled =
        abi::__cxa_demangle(index.name(), nullptr, nullptr, &status);
    if (status == 0 && demangled != nullptr) {
      result.emplace_back(demangled);
      std::free(demangled);
      continue;
    }
#endif
    result.emplace_back(index.name());
  }
  std::sort(result.begin(), result.end());
  return result;
}

}  // namespace riegeli
