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

#include "absl/base/optimization.h"
#include "riegeli/base/estimated_allocated_size.h"

#ifdef __GXX_RTTI
#include <cxxabi.h>
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

MemoryEstimator::~MemoryEstimator() = default;

MemoryEstimator::MemoryEstimator(const MemoryEstimator& that)
    : deterministic_for_testing_(that.deterministic_for_testing_),
      unknown_types_no_rtti_(that.unknown_types_no_rtti_),
      total_memory_(that.total_memory_),
      objects_seen_(that.objects_seen_),
      unknown_types_(that.unknown_types_) {}

MemoryEstimator& MemoryEstimator::operator=(const MemoryEstimator& that) {
  deterministic_for_testing_ = that.deterministic_for_testing_;
  unknown_types_no_rtti_ = that.unknown_types_no_rtti_;
  total_memory_ = that.total_memory_;
  objects_seen_ = that.objects_seen_;
  unknown_types_ = that.unknown_types_;
  return *this;
}

MemoryEstimator::MemoryEstimator(MemoryEstimator&& that) noexcept
    : deterministic_for_testing_(
          std::exchange(that.deterministic_for_testing_, false)),
      unknown_types_no_rtti_(std::exchange(that.unknown_types_no_rtti_, false)),
      total_memory_(std::exchange(that.total_memory_, 0)),
      objects_seen_(std::exchange(that.objects_seen_,
                                  absl::flat_hash_set<const void*>())),
      unknown_types_(std::exchange(that.unknown_types_,
                                   absl::flat_hash_set<std::type_index>())) {}

MemoryEstimator& MemoryEstimator::operator=(MemoryEstimator&& that) noexcept {
  deterministic_for_testing_ =
      std::exchange(that.deterministic_for_testing_, false);
  unknown_types_no_rtti_ = std::exchange(that.unknown_types_no_rtti_, false);
  total_memory_ = std::exchange(that.total_memory_, 0);
  objects_seen_ =
      std::exchange(that.objects_seen_, absl::flat_hash_set<const void*>());
  unknown_types_ = std::exchange(that.unknown_types_,
                                 absl::flat_hash_set<std::type_index>());
  return *this;
}

void MemoryEstimator::RegisterDynamicMemory(const void* ptr, size_t memory) {
  RegisterMemory(ABSL_PREDICT_FALSE(deterministic_for_testing())
                     ? EstimatedAllocatedSizeForTesting(memory)
                     : EstimatedAllocatedSize(ptr, memory));
}

void MemoryEstimator::RegisterDynamicMemory(size_t memory) {
  RegisterMemory(ABSL_PREDICT_FALSE(deterministic_for_testing())
                     ? EstimatedAllocatedSizeForTesting(memory)
                     : EstimatedAllocatedSize(memory));
}

bool MemoryEstimator::RegisterNode(const void* ptr) {
  return ptr != nullptr && objects_seen_.insert(ptr).second;
}

std::vector<std::string> MemoryEstimator::UnknownTypes() const {
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
