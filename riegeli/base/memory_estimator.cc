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

#include <utility>

#include "riegeli/base/base.h"

namespace riegeli {

MemoryEstimator::MemoryEstimator(const MemoryEstimator&) = default;

MemoryEstimator& MemoryEstimator::operator=(const MemoryEstimator&) = default;

MemoryEstimator::MemoryEstimator(MemoryEstimator&& src) noexcept
    : total_memory_(riegeli::exchange(src.total_memory_, 0)),
      objects_seen_(std::move(src.objects_seen_)) {
  src.objects_seen_.clear();
}

MemoryEstimator& MemoryEstimator::operator=(MemoryEstimator&& src) noexcept {
  if (&src != this) {
    total_memory_ = riegeli::exchange(src.total_memory_, 0);
    objects_seen_ = std::move(src.objects_seen_);
    src.objects_seen_.clear();
  }
  return *this;
}

}  // namespace riegeli
