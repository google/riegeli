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
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the `Reader`. `Src` must support
// `Dependency<Reader*, Src&&>`, e.g. `Reader&` (not owned),
// `ChainReader<>` (owned), `std::unique_ptr<Reader>` (owned),
// `AnyDependency<Reader*>` (maybe owned).
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the `Writer` / `BackwardWriter`. `Dest` must support
// `Dependency<Writer*, Dest&&>`, e.g. `Writer&` (not owned),
// `ChainWriter<>` (owned). `std::unique_ptr<Writer>` (owned),
// `AnyDependency<Writer*>` (maybe owned). Analogously for `BackwardWriter`.
template <typename Src, typename Dest,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value &&
                               IsValidDependency<Writer*, Dest&&>::value,
                           int> = 0>
absl::Status CopyAll(
    Src&& src, Dest&& dest,
    Position max_length = std::numeric_limits<Position>::max());
template <
    typename Src, typename Dest,
    std::enable_if_t<IsValidDependency<Reader*, Src&&>::value &&
                         IsValidDependency<BackwardWriter*, Dest&&>::value,
                     int> = 0>
absl::Status CopyAll(Src&& src, Dest&& dest,
                     size_t max_length = std::numeric_limits<size_t>::max());

// Implementation details follow.

namespace copy_all_internal {

absl::Status CopyAllImpl(Reader& src, Writer& dest, Position max_length,
                         bool set_write_size_hint);
absl::Status CopyAllImpl(Reader& src, BackwardWriter& dest, size_t max_length,
                         bool set_write_size_hint);

template <typename WriterType, typename LengthType, typename Src, typename Dest>
inline absl::Status CopyAllInternal(Src&& src, Dest&& dest,
                                    LengthType max_length) {
  Dependency<Reader*, Src&&> src_dep(std::forward<Src>(src));
  Dependency<WriterType*, Dest&&> dest_dep(std::forward<Dest>(dest));
  if (src_dep.is_owning()) src_dep->SetReadAllHint(true);
  absl::Status status =
      CopyAllImpl(*src_dep, *dest_dep, max_length, dest_dep.is_owning());
  if (dest_dep.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_dep->Close())) {
      status.Update(dest_dep->status());
    }
  }
  if (src_dep.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_dep->VerifyEndAndClose())) {
      status.Update(src_dep->status());
    }
  }
  return status;
}

}  // namespace copy_all_internal

template <typename Src, typename Dest,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value &&
                               IsValidDependency<Writer*, Dest&&>::value,
                           int>>
absl::Status CopyAll(Src&& src, Dest&& dest, Position max_length) {
  return copy_all_internal::CopyAllInternal<Writer>(
      std::forward<Src>(src), std::forward<Dest>(dest), max_length);
}

template <
    typename Src, typename Dest,
    std::enable_if_t<IsValidDependency<Reader*, Src&&>::value &&
                         IsValidDependency<BackwardWriter*, Dest&&>::value,
                     int>>
absl::Status CopyAll(Src&& src, Dest&& dest, size_t max_length) {
  return copy_all_internal::CopyAllInternal<BackwardWriter>(
      std::forward<Src>(src), std::forward<Dest>(dest), max_length);
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_COPY_ALL_H_
