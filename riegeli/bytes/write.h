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
#include "absl/status/status.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/type_traits.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/stringify.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Combines creating a `Writer` / `BackwardWriter` (optionally), calling
// `Write()`, and `Close()` (if the `Writer` / `BackwardWriter` is owned).
//
// The last argument is the destination of some type `Dest`. The remaining
// arguments are the values.
//
// `Dest` specifies the type of the object providing and possibly owning the
// `Writer` / `BackwardWriter`. `Dest` must support
// `DependencyRef<Writer*, Dest>`, e.g. `Writer&` (not owned),
// `ChainWriter<>` (owned), `std::unique_ptr<Writer>` (owned),
// `AnyRef<Writer*>` (maybe owned). Analogously for `BackwardWriter`.

template <
    typename... Args,
    std::enable_if_t<
        std::conjunction_v<
            TargetRefSupportsDependency<Writer*, GetTypeFromEndT<1, Args...>>,
            TupleElementsSatisfy<RemoveTypesFromEndT<1, Args&&...>,
                                 IsStringifiable>>,
        int> = 0>
absl::Status Write(Args&&... args);
template <
    typename... Args,
    std::enable_if_t<std::conjunction_v<
                         TargetRefSupportsDependency<
                             BackwardWriter*, GetTypeFromEndT<1, Args...>>,
                         TupleElementsSatisfy<RemoveTypesFromEndT<1, Args&&...>,
                                              IsStringifiable>>,
                     int> = 0>
absl::Status Write(Args&&... args);

// Implementation details follow.

namespace write_internal {

template <typename... Srcs, typename Dest>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline absl::Status WriteInternal(
    ABSL_ATTRIBUTE_UNUSED std::tuple<Srcs...> srcs, Dest&& dest) {
  DependencyRef<Writer*, Dest> dest_dep(std::forward<Dest>(dest));
  if constexpr (HasStringifiedSize<Srcs...>::value) {
    if (dest_dep.IsOwning()) {
      dest_dep->SetWriteSizeHint(std::apply(
          [](const Srcs&... srcs) { return riegeli::StringifiedSize(srcs...); },
          srcs));
    }
  }
  absl::Status status;
  if (ABSL_PREDICT_FALSE(!std::apply(
          [&](Srcs&&... srcs) {
            return dest_dep->Write(std::forward<Srcs>(srcs)...);
          },
          std::move(srcs)))) {
    status = dest_dep->status();
  }
  if (dest_dep.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_dep->Close())) {
      status.Update(dest_dep->status());
    }
  }
  return status;
}

template <typename... Srcs, typename Dest>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline absl::Status BackwardWriteInternal(
    ABSL_ATTRIBUTE_UNUSED std::tuple<Srcs...> srcs, Dest&& dest) {
  DependencyRef<BackwardWriter*, Dest> dest_dep(std::forward<Dest>(dest));
  if constexpr (HasStringifiedSize<Srcs...>::value) {
    if (dest_dep.IsOwning()) {
      dest_dep->SetWriteSizeHint(std::apply(
          [](const Srcs&... srcs) { return riegeli::StringifiedSize(srcs...); },
          srcs));
    }
  }
  absl::Status status;
  if (ABSL_PREDICT_FALSE(!std::apply(
          [&](Srcs&&... srcs) {
            return dest_dep->Write(std::forward<Srcs>(srcs)...);
          },
          std::move(srcs)))) {
    status = dest_dep->status();
  }
  if (dest_dep.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_dep->Close())) {
      status.Update(dest_dep->status());
    }
  }
  return status;
}

}  // namespace write_internal

template <
    typename... Args,
    std::enable_if_t<
        std::conjunction_v<
            TargetRefSupportsDependency<Writer*, GetTypeFromEndT<1, Args...>>,
            TupleElementsSatisfy<RemoveTypesFromEndT<1, Args&&...>,
                                 IsStringifiable>>,
        int>>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline absl::Status Write(Args&&... args) {
  return write_internal::WriteInternal(
      RemoveFromEnd<1>(std::forward<Args>(args)...),
      GetFromEnd<1>(std::forward<Args>(args)...));
}

template <
    typename... Args,
    std::enable_if_t<std::conjunction_v<
                         TargetRefSupportsDependency<
                             BackwardWriter*, GetTypeFromEndT<1, Args...>>,
                         TupleElementsSatisfy<RemoveTypesFromEndT<1, Args&&...>,
                                              IsStringifiable>>,
                     int>>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline absl::Status Write(Args&&... args) {
  return write_internal::BackwardWriteInternal(
      RemoveFromEnd<1>(std::forward<Args>(args)...),
      GetFromEnd<1>(std::forward<Args>(args)...));
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_WRITE_H_
