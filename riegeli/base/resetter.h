// Copyright 2019 Google LLC
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

#ifndef RIEGELI_BASE_RESETTER_H_
#define RIEGELI_BASE_RESETTER_H_

#include <stddef.h>

#include <string>
#include <utility>

#include "absl/strings/string_view.h"

namespace riegeli {

// Resetter<T>::Reset(object, args...) makes *object equivalent to a newly
// constructed object with the given arguments, like *object = T(args...).
//
// It uses *object = arg for a single argument of the same type.
//
// Resetter can be specialized to avoid constructing a temporary object and
// moving from it.
template <typename T>
struct Resetter {
  static void Reset(T* object, const T& src) { *object = src; }

  static void Reset(T* object, T&& src) { *object = std::move(src); }

  template <typename... Args>
  static void Reset(T* object, Args&&... args) {
    *object = T(std::forward<Args>(args)...);
  }
};

// ResetterByReset<T> can be used to provide a specialization of Resetter<T>
// if object->Reset(args...) is a good replacement of *object = T(args...).
//
// It uses *object = arg for a single argument of the same type.
//
// template <>
// struct Resetter<T> : ResetterByReset<T> {};

template <typename T>
struct ResetterByReset {
  static void Reset(T* object, const T& src) { *object = src; }

  static void Reset(T* object, T&& src) { *object = std::move(src); }

  template <typename... Args>
  static void Reset(T* object, Args&&... args) {
    object->Reset(std::forward<Args>(args)...);
  }
};

template <>
struct Resetter<std::string> {
  static void Reset(std::string* object) { object->clear(); }

  static void Reset(std::string* object, size_t length, char ch) {
    object->assign(length, ch);
  }

  static void Reset(std::string* object, absl::string_view src) {
    // TODO: When absl::string_view becomes C++17 std::string_view:
    // object->assign(src);
    object->assign(src.data(), src.size());
  }

  static void Reset(std::string* object, const char* src) {
    object->assign(src);
  }

  static void Reset(std::string* object, const char* src, size_t length) {
    object->assign(src, length);
  }

  static void Reset(std::string* object, const std::string& src) {
    *object = src;
  }

  static void Reset(std::string* object, std::string&& src) {
    *object = std::move(src);
  }

  template <typename... Args>
  static void Reset(std::string* object, Args&&... args) {
    *object = T(std::forward<Args>(args)...);
  }
};

}  // namespace riegeli

#endif  // RIEGELI_BASE_RESETTER_H_
