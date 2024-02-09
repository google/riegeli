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

#include <utility>

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

// `StatusOrMaker<T>::type` and `StatusOrMakerT<T>` generalize
// `absl::StatusOr<T>` for types where that is not applicable:
//  * `absl::StatusOr<const T>`           -> `absl::StatusOr<T>`
//  * `absl::StatusOr<T&>`                -> `absl::StatusOr<T>`
//  * `absl::StatusOr<T&&>`               -> `absl::StatusOr<T>`
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

  // Returns `status`.
  static type FromStatus(const absl::Status& status) { return status; }
  static type FromStatus(absl::Status&& status) { return std::move(status); }

  // Returns `work()`.
  template <typename Work>
  static type FromWork(Work&& work) {
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
struct StatusOrMaker<T&> : StatusOrMaker<T> {};

template <typename T>
struct StatusOrMaker<T&&> : StatusOrMaker<T> {};

template <>
struct StatusOrMaker<void> {
  using type = absl::Status;

  static type FromStatus(const absl::Status& status) { return status; }
  static type FromStatus(absl::Status&& status) { return std::move(status); }

  template <typename Work>
  static type FromWork(Work&& work) {
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

  static type FromStatus(const absl::Status& status) { return status; }
  static type FromStatus(absl::Status&& status) { return std::move(status); }

  template <typename Work>
  static type FromWork(Work&& work) {
    return std::forward<Work>(work)();
  }

  static void Update(type& result, const absl::Status& status) {
    result.Update(status);
  }
};

template <typename T>
struct StatusOrMaker<absl::StatusOr<T>> {
  using type = absl::StatusOr<T>;

  static type FromStatus(const absl::Status& status) { return status; }
  static type FromStatus(absl::Status&& status) { return std::move(status); }

  template <typename Work>
  static type FromWork(Work&& work) {
    return std::forward<Work>(work)();
  }

  static void Update(type& result, const absl::Status& status) {
    if (result.ok()) result = status;
  }
};

template <typename T>
using StatusOrMakerT = typename StatusOrMaker<T>::type;

}  // namespace riegeli

#endif  // RIEGELI_BASE_STATUS_H_
