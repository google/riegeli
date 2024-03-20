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

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/types.h>

#include <type_traits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/types/optional.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/errno_mapping.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

class Reader;

class WriterCFileOptions {
 public:
  WriterCFileOptions() noexcept {}

  // If `absl::nullopt`, `fflush()` pushes data buffered in the `FILE` to the
  // `Writer`, but does not call `Writer::Flush()`. There is no way to trigger
  // `Writer::Flush()` from the `FILE`.
  //
  // If a `FlushType`, pushing data buffered in the `FILE` to the `Writer` calls
  // `Writer::Flush()`. This is done by `fflush()`, but also when the `FILE`
  // buffer is full. This degrades performance, but `fflush()` works as
  // expected.
  //
  // There is no way for `WriterCFile()` to distinguish `fflush()` from the
  // `FILE` buffer being fill, hence this option.
  //
  // Default: `absl::nullopt`.
  WriterCFileOptions& set_flush_type(absl::optional<FlushType> flush_type) & {
    flush_type_ = flush_type;
    return *this;
  }
  WriterCFileOptions&& set_flush_type(absl::optional<FlushType> flush_type) && {
    return std::move(set_flush_type(flush_type));
  }
  absl::optional<FlushType> flush_type() const { return flush_type_; }

 private:
  absl::optional<FlushType> flush_type_;
};

// A `FILE` which writes data to a `Writer`.
//
// This requires `fopencookie()` to be supported by the C library.
//
// The `FILE` supports reading and writing if `Writer::SupportsReadMode()`.
// Otherwise the `FILE` is write-only.
//
// The `FILE` supports `fseek()` if `Writer::SupportsRandomAccess()` or
// `Writer::SupportsReadMode()`, which implies `Reader::SupportsRewind()`.
// Seeking to the current position is always supported.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the `Writer`. `Dest` must support
// `Dependency<Writer*, Dest>`, e.g. `Writer*` (not owned),
// `ChainWriter<>` (owned), `std::unique_ptr<Writer>` (owned),
// `AnyDependency<Writer*>` (maybe owned).
//
// The `Writer` must not be accessed until the `FILE` is closed. Warning: this
// includes implicit closing of all `FILE` objects which are still open at
// program exit, hence if the `FILE` persists until program exit, then the
// `Writer` must do so as well.
template <typename Dest>
FILE* WriterCFile(Dest&& dest,
                  WriterCFileOptions options = WriterCFileOptions());
template <typename Dest>
FILE* WriterCFile(Initializer<Dest> dest,
                  WriterCFileOptions options = WriterCFileOptions());

// Implementation details follow.

namespace cfile_internal {

class WriterCFileCookieBase {
 public:
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
  explicit WriterCFileCookieBase(absl::optional<FlushType> flush_type) noexcept
      : flush_type_(flush_type) {}

  WriterCFileCookieBase(const WriterCFileCookieBase&) = delete;
  WriterCFileCookieBase& operator=(const WriterCFileCookieBase&) = delete;

  virtual Writer* DestWriter() = 0;

  void Initialize(Writer* writer);

  absl::optional<FlushType> flush_type_;
  // If `nullptr`, `*DestWriter()` was used last time. If not `nullptr`,
  // `*reader_` was used last time.
  Reader* reader_ = nullptr;
};

template <typename Dest>
class WriterCFileCookie : public WriterCFileCookieBase {
 public:
  explicit WriterCFileCookie(Initializer<Dest> dest,
                             absl::optional<FlushType> flush_type)
      : WriterCFileCookieBase(flush_type), dest_(std::move(dest)) {
    Initialize(dest_.get());
  }

  int Close() override;

 protected:
  Writer* DestWriter() override { return dest_.get(); }

 private:
  Dependency<Writer*, Dest> dest_;
};

template <typename Dest>
int WriterCFileCookie<Dest>::Close() {
  if (dest_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) {
      return StatusCodeToErrno(dest_->status().code());
    }
  }
  return 0;
}

FILE* WriterCFileImpl(WriterCFileCookieBase* cookie);

}  // namespace cfile_internal

template <typename Dest>
FILE* WriterCFile(Dest&& dest, WriterCFileOptions options) {
  return WriterCFile(Initializer<std::decay_t<Dest>>(std::forward<Dest>(dest)),
                     std::move(options));
}

template <typename Dest>
FILE* WriterCFile(Initializer<Dest> dest, WriterCFileOptions options) {
  return cfile_internal::WriterCFileImpl(
      new cfile_internal::WriterCFileCookie<Dest>(std::move(dest),
                                                  options.flush_type()));
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_WRITER_CFILE_H_
