// Copyright 2026 Google LLC
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

#ifndef RIEGELI_INTERNED_INTERNED_OBJECT_INTERNAL_H_
#define RIEGELI_INTERNED_INTERNED_OBJECT_INTERNAL_H_

#include <stddef.h>

#include <atomic>
#include <memory>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "riegeli/base/intrusive_shared_ptr.h"
#include "riegeli/interned/interned_internal.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli::interned_internal {

// Stores an interned object with its reference count and interner pointer
// (for local interners).
//
// An `InternedRepr` is referenced from one `Shard` and from some number of
// `Interned::Optional` instances. The reference from `Shard` is conceptually
// a weak reference and is not included in the reference count.
template <typename T, typename Hash, typename Eq, typename Interner>
class InternedRepr {
 public:
  using UniqueRepr = std::unique_ptr<const InternedRepr>;

  // Supports `Shard::Intern()`.
  template <typename Arg>
  static UniqueRepr New(Arg&& arg, Interner interner) {
    return std::make_unique<InternedRepr>(std::forward<Arg>(arg),
                                          std::move(interner));
  }

  // Public for `std::make_unique()`.
  template <typename Arg>
  explicit InternedRepr(Arg&& arg, Interner interner)
      : value_(std::forward<Arg>(arg)), interner_(std::move(interner)) {}

  InternedRepr(const InternedRepr&) = delete;
  InternedRepr& operator=(const InternedRepr&) = delete;

  // Supports `IntrusiveSharedPtr`.
  void Ref() const { Shard::Ref(*this); }
  void Unref() const { Shard::Unref(*this); }
  size_t GetCount() const { return Shard::GetCount(*this); }

  void Immortalize() const {
    ref_count_.store(Shard::kImmortal, std::memory_order_relaxed);
  }

  const T& value() const ABSL_ATTRIBUTE_LIFETIME_BOUND { return value_; }
  std::atomic<size_t>& ref_count() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return ref_count_;
  }
  const Interner& interner() const { return interner_; }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const InternedRepr* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&self->value_);
  }

 private:
  using Shard = Shard<InternedRepr, IntrusiveSharedPtr<const InternedRepr>,
                      Hash, Eq, Interner>;

  T value_;
  mutable std::atomic<size_t> ref_count_ = 1;
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Interner interner_;
};

}  // namespace riegeli::interned_internal

#endif  // RIEGELI_INTERNED_INTERNED_OBJECT_INTERNAL_H_
