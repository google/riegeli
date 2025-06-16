// Copyright 2023 Google LLC
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

#ifndef RIEGELI_GCS_GCS_INTERNAL_H_
#define RIEGELI_GCS_GCS_INTERNAL_H_

#include "absl/base/attributes.h"
#include "absl/status/status.h"
#include "google/cloud/status.h"

namespace riegeli::gcs_internal {

inline absl::Status FromCloudStatus(const google::cloud::Status& status) {
  return absl::Status(static_cast<absl::StatusCode>(status.code()),
                      status.message());
}

template <typename T>
T GetOption() {
  return T();
}
template <typename T>
const T& GetOption(const T& option) {
  return option;
}
template <typename T, typename... Options>
const T& GetOption(ABSL_ATTRIBUTE_UNUSED const T& previous_option,
                   const T& option, const Options&... options) {
  return GetOption<T>(option, options...);
}
template <typename T, typename Other, typename... Options>
const T& GetOption(const T& option,
                   ABSL_ATTRIBUTE_UNUSED const Other& other_option,
                   const Options&... options) {
  return GetOption<T>(option, options...);
}
template <typename T, typename Other, typename... Options>
auto GetOption(ABSL_ATTRIBUTE_UNUSED const Other& other_option,
               const Options&... options) {
  return GetOption<T>(options...);
}

}  // namespace riegeli::gcs_internal

#endif  // RIEGELI_GCS_GCS_INTERNAL_H_
