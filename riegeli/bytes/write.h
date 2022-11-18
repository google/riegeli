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

#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/meta/type_traits.h"
#include "absl/status/status.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/type_traits.h"
#include "riegeli/base/types.h"
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

template <typename... Args,
          std::enable_if_t<
              absl::conjunction<
                  IsValidDependency<Writer*, GetTypeFromEndT<1, Args&&...>>,
                  TupleElementsSatisfy<RemoveTypesFromEndT<1, Args&&...>,
                                       IsStringifiable>>::value,
              int> = 0>
absl::Status Write(Args&&... args);
template <
    typename... Args,
    std::enable_if_t<
        absl::conjunction<
            IsValidDependency<BackwardWriter*, GetTypeFromEndT<1, Args&&...>>,
            TupleElementsSatisfy<RemoveTypesFromEndT<1, Args&&...>,
                                 IsStringLike>>::value,
        int> = 0>
absl::Status Write(Args&&... args);

// Implementation details follow.

namespace write_internal {

template <typename... Srcs,
          std::enable_if_t<
              absl::conjunction<std::is_same<decltype(riegeli::StringifiedSize(
                                                 std::declval<const Srcs&>())),
                                             size_t>...>::value,
              int> = 0>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline void SetWriteSizeHint(Writer& dest,
                                                          const Srcs&... srcs) {
  dest.SetWriteSizeHint(
      SaturatingAdd<Position>(Position{riegeli::StringifiedSize(srcs)}...));
}

template <typename... Srcs,
          std::enable_if_t<
              !absl::conjunction<std::is_same<decltype(riegeli::StringifiedSize(
                                                  std::declval<const Srcs&>())),
                                              size_t>...>::value,
              int> = 0>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline void SetWriteSizeHint(Writer& dest,
                                                          const Srcs&... srcs) {
}

template <typename... Srcs, typename Dest, size_t... indices>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline absl::Status WriteInternal(
    ABSL_ATTRIBUTE_UNUSED std::tuple<Srcs...> srcs, Dest&& dest,
    std::index_sequence<indices...>) {
  Dependency<Writer*, Dest&&> dest_dep(std::forward<Dest>(dest));
  if (dest_dep.is_owning()) {
    SetWriteSizeHint(*dest_dep, std::get<indices>(srcs)...);
  }
  absl::Status status;
  if (ABSL_PREDICT_FALSE(
          !dest_dep->Write(std::forward<Srcs>(std::get<indices>(srcs))...))) {
    status = dest_dep->status();
  }
  if (dest_dep.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_dep->Close())) {
      status.Update(dest_dep->status());
    }
  }
  return status;
}

template <typename... Srcs, typename Dest, size_t... indices>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline absl::Status BackwardWriteInternal(
    ABSL_ATTRIBUTE_UNUSED std::tuple<Srcs...> srcs, Dest&& dest,
    std::index_sequence<indices...>) {
  Dependency<BackwardWriter*, Dest&&> dest_dep(std::forward<Dest>(dest));
  if (dest_dep.is_owning()) {
    dest_dep->SetWriteSizeHint(SaturatingAdd<Position>(
        Position{riegeli::StringifiedSize(std::get<indices>(srcs))}...));
  }
  absl::Status status;
  if (ABSL_PREDICT_FALSE(
          !dest_dep->Write(std::forward<Srcs>(std::get<indices>(srcs))...))) {
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

template <typename... Args,
          std::enable_if_t<
              absl::conjunction<
                  IsValidDependency<Writer*, GetTypeFromEndT<1, Args&&...>>,
                  TupleElementsSatisfy<RemoveTypesFromEndT<1, Args&&...>,
                                       IsStringifiable>>::value,
              int>>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline absl::Status Write(Args&&... args) {
  return write_internal::WriteInternal(
      RemoveFromEnd<1>(std::forward<Args>(args)...),
      GetFromEnd<1>(std::forward<Args>(args)...),
      std::make_index_sequence<sizeof...(Args) - 1>());
}

template <
    typename... Args,
    std::enable_if_t<
        absl::conjunction<
            IsValidDependency<BackwardWriter*, GetTypeFromEndT<1, Args&&...>>,
            TupleElementsSatisfy<RemoveTypesFromEndT<1, Args&&...>,
                                 IsStringLike>>::value,
        int>>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline absl::Status Write(Args&&... args) {
  return write_internal::BackwardWriteInternal(
      RemoveFromEnd<1>(std::forward<Args>(args)...),
      GetFromEnd<1>(std::forward<Args>(args)...),
      std::make_index_sequence<sizeof...(Args) - 1>());
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_WRITE_H_
