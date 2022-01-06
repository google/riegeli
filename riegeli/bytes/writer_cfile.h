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

#ifndef RIEGELI_BYTES_WRITER_CFILE_H_
#define RIEGELI_BYTES_WRITER_CFILE_H_

#include <errno.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/errno_mapping.h"
#include "riegeli/bytes/cfile_dependency.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// A `FILE` which writes data to a `Writer`.
//
// This requires `fopencookie()` to be supported by the C library.
//
// The `FILE` does not support triggering `Writer::Flush()`; `fflush()` only
// writes the `FILE` buffer to the `Writer`.
//
// The `FILE` supports reading and writing if `Writer::SupportsReadMode()`.
// Otherwise the `FILE` is write-only.
//
// The `FILE` supports `fseek()` if `Writer::SupportsRandomAccess()` or
// `Writer::SupportsReadMode()`, which implies `Reader::SupportsRewind()`.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the `Writer`. `Dest` must support
// `Dependency<Writer*, Dest>`, e.g. `Writer*` (not owned),
// `std::unique_ptr<Writer>` (owned), `ChainWriter<>` (owned).
//
// With a `dest_args` parameter, reads from a `Dest` constructed from elements
// of `dest_args`. This avoids constructing a temporary `Dest` and moving from
// it.
//
// The `Writer` must not be accessed until the `FILE` is closed or no longer
// used.
template <typename Dest>
FILE* WriterCFile(const Dest& dest);
template <typename Dest>
FILE* WriterCFile(Dest&& dest);
template <typename Dest, typename... DestArgs>
FILE* WriterCFile(std::tuple<DestArgs...> dest_args);

// Implementation details follow.

namespace internal {

class WriterCFileCookieBase {
 public:
  WriterCFileCookieBase() noexcept {}

  WriterCFileCookieBase(const WriterCFileCookieBase&) = delete;
  WriterCFileCookieBase& operator=(const WriterCFileCookieBase&) = delete;

  virtual ~WriterCFileCookieBase();

  const char* OpenMode();

  ssize_t Read(char* dest, size_t length);

  ssize_t Write(const char* src, size_t length);

  // Use `int64_t` instead of `off64_t` to avoid a dependency on
  // `#define _GNU_SOURCE` in a header.
  absl::optional<int64_t> Seek(int64_t offset, int whence);

  // Returns 0 on success, or `errno` value on failure.
  virtual int Close() = 0;

 protected:
  virtual Writer* dest_writer() = 0;

  void Initialize(Writer* writer);

  // If `nullptr`, `*dest_writer()` was used last time. If not `nullptr`,
  // `*reader_` was used last time.
  Reader* reader_ = nullptr;
};

inline void WriterCFileCookieBase::Initialize(Writer* writer) {
  RIEGELI_ASSERT(writer != nullptr)
      << "Failed precondition of WriterCFile: null Writer pointer";
}

template <typename Dest>
class WriterCFileCookie : public WriterCFileCookieBase {
 public:
  explicit WriterCFileCookie(const Dest& dest) : dest_(dest) {
    Initialize(dest_.get());
  }
  explicit WriterCFileCookie(Dest&& dest) : dest_(std::move(dest)) {
    Initialize(dest_.get());
  }
  template <typename... DestArgs>
  explicit WriterCFileCookie(std::tuple<DestArgs...> dest_args)
      : dest_(std::move(dest_args)) {
    Initialize(dest_.get());
  }

  int Close() override;

 protected:
  Writer* dest_writer() override { return dest_.get(); }

 private:
  Dependency<Writer*, Dest> dest_;
};

template <typename Dest>
int WriterCFileCookie<Dest>::Close() {
  if (dest_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) {
      return StatusCodeToErrno(dest_->status().code());
    }
  }
  return 0;
}

FILE* WriterCFileImpl(WriterCFileCookieBase* cookie);

}  // namespace internal

template <typename Dest>
FILE* WriterCFile(const Dest& dest) {
  return internal::WriterCFileImpl(
      new internal::WriterCFileCookie<std::decay_t<Dest>>(dest));
}

template <typename Dest>
FILE* WriterCFile(Dest&& dest) {
  return internal::WriterCFileImpl(
      new internal::WriterCFileCookie<std::decay_t<Dest>>(
          std::forward<Dest>(dest)));
}

template <typename Dest, typename... DestArgs>
FILE* WriterCFile(std::tuple<DestArgs...> dest_args) {
  return internal::WriterCFileImpl(
      new internal::WriterCFileCookie<Dest>(std::move(dest_args)));
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_WRITER_CFILE_H_
