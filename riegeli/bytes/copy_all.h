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

#ifndef RIEGELI_BYTES_COPY_ALL_H_
#define RIEGELI_BYTES_COPY_ALL_H_

#include <stddef.h>

#include <limits>
#include <type_traits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Combines creating a `Reader` and/or `Writer` / `BackwardWriter` (optionally),
// copying all remaining data, and `VerifyEndAndClose()` and/or `Close()`
// (if the `Reader` and/or `Writer` / `BackwardWriter` is owned).
//
// `CopyAll(Writer&)` writes as much as could be read if reading failed, reads
// an unspecified length (between what could be written and `max_length`) if
// writing failed, and reads and writes `max_length` if `max_length` was
// exceeded.
//
// `CopyAll(BackwardWriter&)` writes nothing if reading failed, reads an
// unspecified length (between what could be written and `max_length`) if
// writing failed, and reads `max_length` and writes nothing if `max_length` was
// exceeded.
//
// If `length_read != nullptr` then sets `*length_read` to the length read.
// This is equal to the difference between `src.pos()` after and before the
// call.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the `Reader`. `Src` must support
// `DependencyRef<Reader*, Src>`, e.g. `Reader&` (not owned),
// `ChainReader<>` (owned), `std::unique_ptr<Reader>` (owned),
// `AnyRef<Reader*>` (maybe owned).
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the `Writer` / `BackwardWriter`. `Dest` must support
// `DependencyRef<Writer*, Dest>`, e.g. `Writer&` (not owned),
// `ChainWriter<>` (owned), `std::unique_ptr<Writer>` (owned),
// `AnyRef<Writer*>` (maybe owned). Analogously for `BackwardWriter`.
template <typename Src, typename Dest,
          std::enable_if_t<
              std::conjunction_v<TargetRefSupportsDependency<Reader*, Src>,
                                 TargetRefSupportsDependency<Writer*, Dest>>,
              int> = 0>
absl::Status CopyAll(Src&& src, Dest&& dest,
                     Position max_length = std::numeric_limits<Position>::max(),
                     Position* length_read = nullptr);
template <typename Src, typename Dest,
          std::enable_if_t<
              std::conjunction_v<TargetRefSupportsDependency<Reader*, Src>,
                                 TargetRefSupportsDependency<Writer*, Dest>>,
              int> = 0>
absl::Status CopyAll(Src&& src, Dest&& dest, Position* length_read);
template <
    typename Src, typename Dest,
    std::enable_if_t<
        std::conjunction_v<TargetRefSupportsDependency<Reader*, Src>,
                           TargetRefSupportsDependency<BackwardWriter*, Dest>>,
        int> = 0>
absl::Status CopyAll(Src&& src, Dest&& dest,
                     size_t max_length = std::numeric_limits<size_t>::max());

// Implementation details follow.

namespace copy_all_internal {

absl::Status CopyAllImpl(Reader& src, Writer& dest, Position max_length,
                         Position* length_read, bool set_write_size_hint);
absl::Status CopyAllImpl(Reader& src, BackwardWriter& dest, size_t max_length,
                         bool set_write_size_hint);

}  // namespace copy_all_internal

template <typename Src, typename Dest,
          std::enable_if_t<
              std::conjunction_v<TargetRefSupportsDependency<Reader*, Src>,
                                 TargetRefSupportsDependency<Writer*, Dest>>,
              int>>
inline absl::Status CopyAll(Src&& src, Dest&& dest, Position max_length,
                            Position* length_read) {
  DependencyRef<Reader*, Src> src_dep(std::forward<Src>(src));
  DependencyRef<Writer*, Dest> dest_dep(std::forward<Dest>(dest));
  if (src_dep.IsOwning()) src_dep->SetReadAllHint(true);
  absl::Status status = copy_all_internal::CopyAllImpl(
      *src_dep, *dest_dep, max_length, length_read, dest_dep.IsOwning());
  if (dest_dep.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_dep->Close())) {
      status.Update(dest_dep->status());
    }
  }
  if (src_dep.IsOwning()) {
    if (ABSL_PREDICT_TRUE(status.ok())) src_dep->VerifyEnd();
    if (ABSL_PREDICT_FALSE(!src_dep->Close())) status.Update(src_dep->status());
  }
  return status;
}

template <typename Src, typename Dest,
          std::enable_if_t<
              std::conjunction_v<TargetRefSupportsDependency<Reader*, Src>,
                                 TargetRefSupportsDependency<Writer*, Dest>>,
              int>>
inline absl::Status CopyAll(Src&& src, Dest&& dest, Position* length_read) {
  return CopyAll(std::forward<Src>(src), std::forward<Dest>(dest),
                 std::numeric_limits<Position>::max(), length_read);
}

template <
    typename Src, typename Dest,
    std::enable_if_t<
        std::conjunction_v<TargetRefSupportsDependency<Reader*, Src>,
                           TargetRefSupportsDependency<BackwardWriter*, Dest>>,
        int>>
inline absl::Status CopyAll(Src&& src, Dest&& dest, size_t max_length) {
  DependencyRef<Reader*, Src> src_dep(std::forward<Src>(src));
  DependencyRef<BackwardWriter*, Dest> dest_dep(std::forward<Dest>(dest));
  if (src_dep.IsOwning()) src_dep->SetReadAllHint(true);
  absl::Status status = copy_all_internal::CopyAllImpl(
      *src_dep, *dest_dep, max_length, dest_dep.IsOwning());
  if (dest_dep.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_dep->Close())) {
      status.Update(dest_dep->status());
    }
  }
  if (src_dep.IsOwning()) {
    if (ABSL_PREDICT_TRUE(status.ok())) src_dep->VerifyEnd();
    if (ABSL_PREDICT_FALSE(!src_dep->Close())) status.Update(src_dep->status());
  }
  return status;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_COPY_ALL_H_
