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

#ifndef RIEGELI_BASE_INTRUSIVE_REF_COUNT_H_
#define RIEGELI_BASE_INTRUSIVE_REF_COUNT_H_

#include <type_traits>

#include "absl/base/attributes.h"
#include "riegeli/base/intrusive_shared_ptr.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/ref_count.h"

namespace riegeli {

// Deriving `T` from `RefCountedBase<T>` makes it easier to provide functions
// needed by `RefCountedPtr<T>`.
//
// The destructor of `RefCountedBase<T>` is not virtual. The object will be
// deleted by `delete static_cast<T*>(ptr)`. This means that `T` must be the
// actual object type or `T` must define a virtual destructor.
template <typename T>
class ABSL_DEPRECATED("Use SharedPtr instead of RefCountedPtr if possible")
    RefCountedBase {
 public:
  RefCountedBase() noexcept {
    static_assert(std::is_base_of<RefCountedBase<T>, T>::value,
                  "The template argument T in RefCountedBase<T> "
                  "must be the class derived from RefCountedBase<T>");
  }

  // Increments the reference count of `*this`.
  void Ref() const { ref_count_.Ref(); }

  // Decrements the reference count of `*this`. Deletes `this` when the
  // reference count reaches 0.
  void Unref() const {
    if (ref_count_.Unref()) delete static_cast<const T*>(this);
  }

  // Returns `true` if there is only one owner of the object.
  //
  // This can be used to check if the object may be modified.
  bool HasUniqueOwner() const { return ref_count_.HasUniqueOwner(); }
  // Compatibility name.
  bool has_unique_owner() const { return HasUniqueOwner(); }

 protected:
  ~RefCountedBase() = default;

 private:
  RefCount ref_count_;
};

template <typename T>
ABSL_DEPRECATED("Use SharedPtr instead of RefCountedPtr if possible")
using RefCountedPtr = IntrusiveSharedPtr<T>;

// Creates an object wrapped in `IntrusiveSharedPtr`.
//
// `MakeRefCounted()` is to `IntrusiveSharedPtr` like `std::make_unique()` is to
// `std::unique_ptr`, or `std::make_shared()` to `std::shared_ptr`.
template <typename T, typename... Args>
ABSL_DEPRECATED("Use SharedPtr<T>(riegeli::Maker(args...)) instead")
inline IntrusiveSharedPtr<T> MakeRefCounted(Args&&... args) {
  return IntrusiveSharedPtr<T>(riegeli::Maker<T>(std::forward<Args>(args)...));
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_INTRUSIVE_REF_COUNT_H_
