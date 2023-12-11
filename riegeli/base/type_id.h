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

#ifndef RIEGELI_BASE_TYPE_ID_H_
#define RIEGELI_BASE_TYPE_ID_H_

#include "riegeli/base/compare.h"

namespace riegeli {

// `TypeId::For<A>()` is a token which is equal to `TypeId::For<B>()` whenever
// `A` and `B` are the same type.
//
// `TypeId()` is another value not equal to any other.
class TypeId : public WithEqual<TypeId> {
 public:
  constexpr TypeId() = default;

  TypeId(const TypeId& that) = default;
  TypeId& operator=(const TypeId& that) = default;

  template <typename T>
  static constexpr TypeId For();

  friend constexpr bool operator==(TypeId a, TypeId b) {
    return a.ptr_ == b.ptr_;
  }

 private:
  template <typename T>
  struct TypeIdToken;

  explicit constexpr TypeId(const void* ptr) : ptr_(ptr) {}

  const void* ptr_ = nullptr;
};

// Implementation details follow.

template <typename T>
struct TypeId::TypeIdToken {
  static const char token;
};

template <typename T>
const char TypeId::TypeIdToken<T>::token = '\0';

template <typename T>
constexpr TypeId TypeId::For() {
  return TypeId(&TypeIdToken<T>::token);
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_TYPE_ID_H_
