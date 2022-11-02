// Copyright 2022 Google LLC
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

#ifndef RIEGELI_BYTES_WRITE_H_
#define RIEGELI_BYTES_WRITE_H_

#include <stddef.h>

#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Combines creating a `Writer` / `BackwardWriter` (optionally), calling
// `Write()`, and `Close()` (if the `Writer` / `BackwardWriter` is owned).
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the `Writer` / `BackwardWriter`. `Dest` must support
// `Dependency<Writer*, Dest&&>`, e.g. `Writer&` (not owned),
// `ChainWriter<>` (owned). `std::unique_ptr<Writer>` (owned).
// Analogously for `BackwardWriter`.

template <typename Dest,
          std::enable_if_t<IsValidDependency<Writer*, Dest&&>::value, int> = 0>
absl::Status Write(absl::string_view src, Dest&& dest);
template <typename Src, typename Dest,
          std::enable_if_t<std::is_same<Src, std::string>::value &&
                               IsValidDependency<Writer*, Dest&&>::value,
                           int> = 0>
absl::Status Write(Src&& src, Dest&& dest);
template <typename Dest,
          std::enable_if_t<IsValidDependency<Writer*, Dest&&>::value, int> = 0>
ABSL_DEPRECATED("Use Write(absl::string_view) instead.")
absl::Status Write(const char* src, size_t length, Dest&& dest);
template <typename Dest,
          std::enable_if_t<IsValidDependency<Writer*, Dest&&>::value, int> = 0>
absl::Status Write(const Chain& src, Dest&& dest);
template <typename Dest,
          std::enable_if_t<IsValidDependency<Writer*, Dest&&>::value, int> = 0>
absl::Status Write(Chain&& src, Dest&& dest);
template <typename Dest,
          std::enable_if_t<IsValidDependency<Writer*, Dest&&>::value, int> = 0>
absl::Status Write(const absl::Cord& src, Dest&& dest);
template <typename Dest,
          std::enable_if_t<IsValidDependency<Writer*, Dest&&>::value, int> = 0>
absl::Status Write(absl::Cord&& src, Dest&& dest);

template <typename Dest,
          std::enable_if_t<IsValidDependency<BackwardWriter*, Dest&&>::value,
                           int> = 0>
absl::Status Write(absl::string_view src, Dest&& dest);
template <
    typename Src, typename Dest,
    std::enable_if_t<std::is_same<Src, std::string>::value &&
                         IsValidDependency<BackwardWriter*, Dest&&>::value,
                     int> = 0>
absl::Status Write(Src&& src, Dest&& dest);
template <typename Dest,
          std::enable_if_t<IsValidDependency<BackwardWriter*, Dest&&>::value,
                           int> = 0>
absl::Status Write(const Chain& src, Dest&& dest);
template <typename Dest,
          std::enable_if_t<IsValidDependency<BackwardWriter*, Dest&&>::value,
                           int> = 0>
absl::Status Write(Chain&& src, Dest&& dest);
template <typename Dest,
          std::enable_if_t<IsValidDependency<BackwardWriter*, Dest&&>::value,
                           int> = 0>
absl::Status Write(const absl::Cord& src, Dest&& dest);
template <typename Dest,
          std::enable_if_t<IsValidDependency<BackwardWriter*, Dest&&>::value,
                           int> = 0>
absl::Status Write(absl::Cord&& src, Dest&& dest);

// Implementation details follow.

namespace write_internal {

template <typename Src, typename Dest>
inline absl::Status WriteInternal(Src&& src, Dest&& dest) {
  Dependency<Writer*, Dest&&> dest_dep(std::forward<Dest>(dest));
  if (dest_dep.is_owning()) dest_dep->SetWriteSizeHint(src.size());
  absl::Status status;
  if (ABSL_PREDICT_FALSE(!dest_dep->Write(std::forward<Src>(src)))) {
    status = dest_dep->status();
  }
  if (dest_dep.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_dep->Close())) {
      status.Update(dest_dep->status());
    }
  }
  return status;
}

template <typename Src, typename Dest>
inline absl::Status BackwardWriteInternal(Src&& src, Dest&& dest) {
  Dependency<BackwardWriter*, Dest&&> dest_dep(std::forward<Dest>(dest));
  if (dest_dep.is_owning()) dest_dep->SetWriteSizeHint(src.size());
  absl::Status status;
  if (ABSL_PREDICT_FALSE(!dest_dep->Write(std::forward<Src>(src)))) {
    status = dest_dep->status();
  }
  if (dest_dep.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_dep->Close())) {
      status.Update(dest_dep->status());
    }
  }
  return status;
}

}  // namespace write_internal

template <typename Dest,
          std::enable_if_t<IsValidDependency<Writer*, Dest&&>::value, int>>
inline absl::Status Write(absl::string_view src, Dest&& dest) {
  return write_internal::WriteInternal(src, std::forward<Dest>(dest));
}

template <typename Src, typename Dest,
          std::enable_if_t<std::is_same<Src, std::string>::value &&
                               IsValidDependency<Writer*, Dest&&>::value,
                           int>>
inline absl::Status Write(Src&& src, Dest&& dest) {
  // `std::move(src)` is correct and `std::forward<Src>(src)` is not necessary:
  // `Src` is always `std::string`, never an lvalue reference.
  return write_internal::WriteInternal(std::move(src),
                                       std::forward<Dest>(dest));
}

template <typename Dest,
          std::enable_if_t<IsValidDependency<Writer*, Dest&&>::value, int>>
inline absl::Status Write(const char* src, size_t length, Dest&& dest) {
  return write_internal::WriteInternal(absl::string_view(src, length),
                                       std::forward<Dest>(dest));
}

template <typename Dest,
          std::enable_if_t<IsValidDependency<Writer*, Dest&&>::value, int>>
inline absl::Status Write(const Chain& src, Dest&& dest) {
  return write_internal::WriteInternal(src, std::forward<Dest>(dest));
}

template <typename Dest,
          std::enable_if_t<IsValidDependency<Writer*, Dest&&>::value, int>>
inline absl::Status Write(Chain&& src, Dest&& dest) {
  return write_internal::WriteInternal(std::move(src),
                                       std::forward<Dest>(dest));
}

template <typename Dest,
          std::enable_if_t<IsValidDependency<Writer*, Dest&&>::value, int>>
inline absl::Status Write(const absl::Cord& src, Dest&& dest) {
  return write_internal::WriteInternal(src, std::forward<Dest>(dest));
}

template <typename Dest,
          std::enable_if_t<IsValidDependency<Writer*, Dest&&>::value, int>>
inline absl::Status Write(absl::Cord&& src, Dest&& dest) {
  return write_internal::WriteInternal(std::move(src),
                                       std::forward<Dest>(dest));
}

template <
    typename Dest,
    std::enable_if_t<IsValidDependency<BackwardWriter*, Dest&&>::value, int>>
inline absl::Status Write(absl::string_view src, Dest&& dest) {
  return write_internal::BackwardWriteInternal(src, std::forward<Dest>(dest));
}

template <
    typename Src, typename Dest,
    std::enable_if_t<std::is_same<Src, std::string>::value &&
                         IsValidDependency<BackwardWriter*, Dest&&>::value,
                     int>>
inline absl::Status Write(Src&& src, Dest&& dest) {
  // `std::move(src)` is correct and `std::forward<Src>(src)` is not necessary:
  // `Src` is always `std::string`, never an lvalue reference.
  return write_internal::BackwardWriteInternal(std::move(src),
                                               std::forward<Dest>(dest));
}

template <
    typename Dest,
    std::enable_if_t<IsValidDependency<BackwardWriter*, Dest&&>::value, int>>
inline absl::Status Write(const Chain& src, Dest&& dest) {
  return write_internal::BackwardWriteInternal(src, std::forward<Dest>(dest));
}

template <
    typename Dest,
    std::enable_if_t<IsValidDependency<BackwardWriter*, Dest&&>::value, int>>
inline absl::Status Write(Chain&& src, Dest&& dest) {
  return write_internal::BackwardWriteInternal(std::move(src),
                                               std::forward<Dest>(dest));
}

template <
    typename Dest,
    std::enable_if_t<IsValidDependency<BackwardWriter*, Dest&&>::value, int>>
inline absl::Status Write(const absl::Cord& src, Dest&& dest) {
  return write_internal::BackwardWriteInternal(src, std::forward<Dest>(dest));
}

template <
    typename Dest,
    std::enable_if_t<IsValidDependency<BackwardWriter*, Dest&&>::value, int>>
inline absl::Status Write(absl::Cord&& src, Dest&& dest) {
  return write_internal::BackwardWriteInternal(std::move(src),
                                               std::forward<Dest>(dest));
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_WRITE_H_
