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

#ifndef RIEGELI_BASE_STATUS_H_
#define RIEGELI_BASE_STATUS_H_

#include <algorithm>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace riegeli {

// Returns an `absl::Status` that is identical to `status` except that the
// `message()` has been replaced with `message`.
//
// OK status values have no message and therefore if `status` is OK, the result
// is unchanged.
absl::Status SetMessage(const absl::Status& status, absl::string_view message);

// Returns an `absl::Status` that is identical to `status` except that the
// `message()` has been augmented by adding `detail` to the end of the original
// message.
//
// `Annotate()` adds the appropriate separators, so callers should not include a
// separator in `detail`. The exact formatting is subject to change, so you
// should not depend on it in your tests.
//
// OK status values have no message and therefore if `status` is OK, the result
// is unchanged.
absl::Status Annotate(const absl::Status& status, absl::string_view detail);

// Generalizes `absl::StatusOr<T>` for types where that is not applicable:
//  * `absl::StatusOr<const T>`           -> `absl::StatusOr<T>`
//  * `absl::StatusOr<T&>`                -> `absl::StatusOr<T*>`
//  * `absl::StatusOr<T&&>`               -> `absl::StatusOr<T*>`
//  * `absl::StatusOr<void>`              -> `absl::Status`
//  * `absl::StatusOr<absl::Status>`      -> `absl::Status`
//  * `absl::StatusOr<absl::StatusOr<T>>` -> `absl::StatusOr<T>`
//
// The primary template provides the default implementation. Specializations
// follow.

template <typename T>
struct StatusOrMaker {
  // The combined type.
  using type = absl::StatusOr<T>;

  // Returns `status` or `work()`. Calls `work()` only if `status.ok()`.
  template <typename Work>
  static type FromStatusOrWork(absl::Status&& status, Work&& work) {
    if (ABSL_PREDICT_FALSE(!status.ok())) return std::move(status);
    return std::forward<Work>(work)();
  }

  // Replaces `result` with `status` if `result` is OK.
  static void Update(type& result, const absl::Status& status) {
    if (result.ok()) result = status;
  }
};

template <typename T>
struct StatusOrMaker<const T> : StatusOrMaker<T> {};

template <typename T>
struct StatusOrMaker<T&> {
  using type = absl::StatusOr<T*>;

  template <typename Work>
  static type FromStatusOrWork(absl::Status&& status, Work&& work) {
    if (ABSL_PREDICT_FALSE(!status.ok())) return std::move(status);
    return &std::forward<Work>(work)();
  }

  static void Update(type& result, const absl::Status& status) {
    if (result.ok()) result = status;
  }
};

template <typename T>
struct StatusOrMaker<T&&> {
  using type = absl::StatusOr<T*>;

  template <typename Work>
  static type FromStatusOrWork(absl::Status&& status, Work&& work) {
    if (ABSL_PREDICT_FALSE(!status.ok())) return std::move(status);
    T&& result = std::forward<Work>(work)();
    return &result;
  }

  static void Update(type& result, const absl::Status& status) {
    if (result.ok()) result = status;
  }
};

template <>
struct StatusOrMaker<void> {
  using type = absl::Status;

  template <typename Work>
  static type FromStatusOrWork(absl::Status&& status, Work&& work) {
    if (ABSL_PREDICT_FALSE(!status.ok())) return std::move(status);
    std::forward<Work>(work)();
    return absl::OkStatus();
  }

  static void Update(type& result, const absl::Status& status) {
    result.Update(status);
  }
};

template <>
struct StatusOrMaker<absl::Status> {
  using type = absl::Status;

  template <typename Work>
  static type FromStatusOrWork(absl::Status&& status, Work&& work) {
    if (ABSL_PREDICT_FALSE(!status.ok())) return std::move(status);
    return std::forward<Work>(work)();
  }

  static void Update(type& result, const absl::Status& status) {
    result.Update(status);
  }
};

template <typename T>
struct StatusOrMaker<absl::StatusOr<T>> {
  using type = absl::StatusOr<T>;

  template <typename Work>
  static type FromStatusOrWork(absl::Status&& status, Work&& work) {
    if (ABSL_PREDICT_FALSE(!status.ok())) return std::move(status);
    return std::forward<Work>(work)();
  }

  static void Update(type& result, const absl::Status& status) {
    if (result.ok()) result = status;
  }
};

// Combines the result of a function and the status of preparing data for the
// function. Even if the status is not OK, the function is called with partial
// data, hence its result is returned.
template <typename T>
struct ABSL_MUST_USE_RESULT StatusAnd {
  T value;
  absl::Status status;
};

// Generalizes `StatusAnd<T>` for types where that is not applicable:
//  * `StatusAnd<void>` -> `absl::Status`
//
// The primary template provides the default implementation. Specializations
// follow.

template <typename T>
struct StatusAndMaker {
  // The combined type.
  using type = StatusAnd<T>;

  // Returns `status` and `work()`.
  //
  // `status` may be modified, but only after calling `work()`.
  template <typename Work>
  static type FromStatusAndWork(absl::Status& status, Work&& work) {
    return {std::forward<Work>(work)(), std::move(status)};
  }

  // Updates the status of `result` with `status` if `result` is OK.
  static void Update(type& result, const absl::Status& status) {
    result.status.Update(status);
  }
};

template <>
struct StatusAndMaker<void> {
  using type = absl::Status;

  template <typename Work>
  static type FromStatusAndWork(absl::Status& status, Work&& work) {
    std::forward<Work>(work)();
    return std::move(status);
  }

  static void Update(type& result, const absl::Status& status) {
    result.Update(status);
  }
};

}  // namespace riegeli

#endif  // RIEGELI_BASE_STATUS_H_
