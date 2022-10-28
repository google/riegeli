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

#ifndef RIEGELI_BYTES_READ_ALL_H_
#define RIEGELI_BYTES_READ_ALL_H_

#include <stddef.h>

#include <limits>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

// Combines creating a `Reader` (optionally), reading all remaining data to
// `dest` (clearing any existing data in `dest`), and `VerifyEndAndClose()`
// (if the `Reader` is owned).
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the `Reader`. `Src` must support
// `Dependency<Reader*, Src&&>`, e.g. `Reader&` (not owned),
// `ChainReader<>` (owned), `std::unique_ptr<Reader>` (owned).
template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int> = 0>
absl::Status ReadAll(Src&& src, std::string& dest,
                     size_t max_length = std::numeric_limits<size_t>::max());
template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int> = 0>
absl::Status ReadAll(Src&& src, Chain& dest,
                     size_t max_length = std::numeric_limits<size_t>::max());
template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int> = 0>
absl::Status ReadAll(Src&& src, absl::Cord& dest,
                     size_t max_length = std::numeric_limits<size_t>::max());

// Combines creating a `Reader` (optionally), reading all remaining data to
// `dest` (appending to any existing data in `dest`), and `VerifyEndAndClose()`
// (if the `Reader` is owned).
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the `Reader`. `Src` must support
// `Dependency<Reader*, Src&&>`, e.g. `Reader&` (not owned),
// `ChainReader<>` (owned), `std::unique_ptr<Reader>` (owned).
template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int> = 0>
absl::Status ReadAndAppendAll(
    Src&& src, std::string& dest,
    size_t max_length = std::numeric_limits<size_t>::max());
template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int> = 0>
absl::Status ReadAndAppendAll(
    Src&& src, Chain& dest,
    size_t max_length = std::numeric_limits<size_t>::max());
template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int> = 0>
absl::Status ReadAndAppendAll(
    Src&& src, absl::Cord& dest,
    size_t max_length = std::numeric_limits<size_t>::max());

// Implementation details follow.

namespace read_all_internal {

absl::Status ReadAllImpl(Reader& src, std::string& dest, size_t max_length);
absl::Status ReadAllImpl(Reader& src, Chain& dest, size_t max_length);
absl::Status ReadAllImpl(Reader& src, absl::Cord& dest, size_t max_length);
absl::Status ReadAndAppendAllImpl(Reader& src, std::string& dest,
                                  size_t max_length);
absl::Status ReadAndAppendAllImpl(Reader& src, Chain& dest, size_t max_length);
absl::Status ReadAndAppendAllImpl(Reader& src, absl::Cord& dest,
                                  size_t max_length);

template <typename Src, typename Dest>
inline absl::Status ReadAllInternal(Src&& src, Dest& dest, size_t max_length) {
  Dependency<Reader*, Src&&> src_dep(std::forward<Src>(src));
  if (src_dep.is_owning()) src_dep->SetReadAllHint(true);
  absl::Status status = ReadAllImpl(*src_dep, dest, max_length);
  if (src_dep.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_dep->VerifyEndAndClose())) {
      status.Update(src_dep->status());
    }
  }
  return status;
}

template <typename Src, typename Dest>
inline absl::Status ReadAndAppendAllInternal(Src&& src, Dest& dest,
                                             size_t max_length) {
  Dependency<Reader*, Src&&> src_dep(std::forward<Src>(src));
  if (src_dep.is_owning()) src_dep->SetReadAllHint(true);
  absl::Status status = ReadAndAppendAllImpl(*src_dep, dest, max_length);
  if (src_dep.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_dep->VerifyEndAndClose())) {
      status.Update(src_dep->status());
    }
  }
  return status;
}

}  // namespace read_all_internal

template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int>>
inline absl::Status ReadAll(Src&& src, std::string& dest, size_t max_length) {
  return read_all_internal::ReadAllInternal(std::forward<Src>(src), dest,
                                            max_length);
}

template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int>>
inline absl::Status ReadAll(Src&& src, Chain& dest, size_t max_length) {
  return read_all_internal::ReadAllInternal(std::forward<Src>(src), dest,
                                            max_length);
}

template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int>>
inline absl::Status ReadAll(Src&& src, absl::Cord& dest, size_t max_length) {
  return read_all_internal::ReadAllInternal(std::forward<Src>(src), dest,
                                            max_length);
}

template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int>>
inline absl::Status ReadAndAppendAll(Src&& src, std::string& dest,
                                     size_t max_length) {
  return read_all_internal::ReadAndAppendAllInternal(std::forward<Src>(src),
                                                     dest, max_length);
}

template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int>>
inline absl::Status ReadAndAppendAll(Src&& src, Chain& dest,
                                     size_t max_length) {
  return read_all_internal::ReadAndAppendAllInternal(std::forward<Src>(src),
                                                     dest, max_length);
}

template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int>>
inline absl::Status ReadAndAppendAll(Src&& src, absl::Cord& dest,
                                     size_t max_length) {
  return read_all_internal::ReadAndAppendAllInternal(std::forward<Src>(src),
                                                     dest, max_length);
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_READ_ALL_H_
