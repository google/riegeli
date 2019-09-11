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

#include <stddef.h>

#include <utility>

#include "absl/container/flat_hash_set.h"

namespace riegeli {

MemoryEstimator::MemoryEstimator(const MemoryEstimator& that)
    : total_memory_(that.total_memory_), objects_seen_(that.objects_seen_) {}

MemoryEstimator& MemoryEstimator::operator=(const MemoryEstimator& that) {
  total_memory_ = that.total_memory_;
  objects_seen_ = that.objects_seen_;
  return *this;
}

MemoryEstimator::MemoryEstimator(MemoryEstimator&& that) noexcept
    : total_memory_(std::exchange(that.total_memory_, 0)),
      objects_seen_(std::exchange(that.objects_seen_,
                                  absl::flat_hash_set<const void*>())) {}

MemoryEstimator& MemoryEstimator::operator=(MemoryEstimator&& that) noexcept {
  total_memory_ = std::exchange(that.total_memory_, 0);
  objects_seen_ =
      std::exchange(that.objects_seen_, absl::flat_hash_set<const void*>());
  return *this;
}

}  // namespace riegeli
