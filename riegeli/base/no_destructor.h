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

#ifndef RIEGELI_BASE_NO_DESTRUCTOR_H_
#define RIEGELI_BASE_NO_DESTRUCTOR_H_

#include <new>
#include <utility>

namespace riegeli {

// `NoDestructor<T>` constructs and stores an object of type `T` but does not
// call its destructor.
//
// It can be used as a static variable in a function to lazily initialize an
// object.
template <typename T>
class NoDestructor {
 public:
  // Forwards constructor arguments to `T` constructor.
  template <typename... Args>
  explicit NoDestructor(Args&&... args) {
    new (storage_) T(std::forward<Args>(args)...);
  }

  // Forwards copy and move construction, e.g. a brace initializer.
  explicit NoDestructor(const T& src) { new (storage_) T(src); }
  explicit NoDestructor(T&& src) { new (storage_) T(std::move(src)); }

  NoDestructor(const NoDestructor&) = delete;
  NoDestructor& operator=(const NoDestructor&) = delete;

  // Smart pointer interface with deep constness.
  T* get() {
    return
#if __cpp_lib_launder >= 201606
        std::launder
#endif
        (reinterpret_cast<T*>(storage_));
  }
  const T* get() const {
    return
#if __cpp_lib_launder >= 201606
        std::launder
#endif
        (reinterpret_cast<const T*>(storage_));
  }
  T& operator*() { return *get(); }
  const T& operator*() const { return *get(); }
  T* operator->() { return get(); }
  const T* operator->() const { return get(); }

 private:
  alignas(T) char storage_[sizeof(T)];
};

}  // namespace riegeli

#endif  // RIEGELI_BASE_NO_DESTRUCTOR_H_
