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

#ifndef RIEGELI_BYTES_READER_CFILE_H_
#define RIEGELI_BYTES_READER_CFILE_H_

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/types.h>

#include <optional>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/errno_mapping.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/reset.h"
#include "riegeli/bytes/cfile_handle.h"
#include "riegeli/bytes/path_ref.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

class ReaderCFileOptions {
 public:
  ReaderCFileOptions() noexcept {}

  // The filename assumed by the returned `OwnedCFile`.
  //
  // Default: "<unspecified>".
  ReaderCFileOptions& set_filename(PathInitializer filename) &
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    riegeli::Reset(filename_, std::move(filename));
    return *this;
  }
  ReaderCFileOptions&& set_filename(PathInitializer filename) &&
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return std::move(set_filename(std::move(filename)));
  }
  std::string& filename() ABSL_ATTRIBUTE_LIFETIME_BOUND { return filename_; }
  const std::string& filename() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return filename_;
  }

 private:
  std::string filename_ = "<unspecified>";
};

// A read-only `FILE` which reads data from a `Reader`.
//
// This requires `fopencookie()` to be supported by the C library.
//
// The `FILE` supports `fseek()` forwards in any case, and backwards if
// `Reader::SupportsRewind()`.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the `Reader`. `Src` must support `Dependency<Reader*, Src>`,
// e.g. `Reader*` (not owned), `ChainReader<>` (owned),
// `std::unique_ptr<Reader>` (owned), `Any<Reader*>` (maybe owned).
//
// `src` supports `riegeli::Maker<Src>(args...)` to construct `Src` in-place.
//
// The `Reader` must not be accessed until the `FILE` is closed. Warning: this
// includes implicit closing of all `FILE` objects which are still open at
// program exit, hence if the `FILE` persists until program exit, then the
// `Reader` must do so as well.
template <
    typename Src,
    std::enable_if_t<TargetSupportsDependency<Reader*, Src>::value, int> = 0>
OwnedCFile ReaderCFile(Src&& src,
                       ReaderCFileOptions options = ReaderCFileOptions());

// Implementation details follow.

namespace cfile_internal {

class ReaderCFileCookieBase {
 public:
  virtual ~ReaderCFileCookieBase();

  ssize_t Read(char* dest, size_t length);

  // Use `int64_t` instead of `off64_t` to avoid a dependency on
  // `#define _GNU_SOURCE` in a header.
  std::optional<int64_t> Seek(int64_t offset, int whence);

  // Returns 0 on success, or `errno` value on failure.
  virtual int Close() = 0;

 protected:
  ReaderCFileCookieBase() noexcept {}

  ReaderCFileCookieBase(const ReaderCFileCookieBase&) = delete;
  ReaderCFileCookieBase& operator=(const ReaderCFileCookieBase&) = delete;

  virtual Reader* SrcReader() = 0;

  void Initialize(Reader* reader);
};

inline void ReaderCFileCookieBase::Initialize(Reader* reader) {
  RIEGELI_ASSERT_NE(reader, nullptr)
      << "Failed precondition of ReaderCFile(): null Reader pointer";
}

template <typename Src>
class ReaderCFileCookie : public ReaderCFileCookieBase {
 public:
  explicit ReaderCFileCookie(Initializer<Src> src) : src_(std::move(src)) {
    Initialize(src_.get());
  }

  int Close() override;

 protected:
  Reader* SrcReader() override { return src_.get(); }

 private:
  Dependency<Reader*, Src> src_;
};

template <typename Src>
int ReaderCFileCookie<Src>::Close() {
  if (src_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) {
      return StatusCodeToErrno(src_->status().code());
    }
  }
  return 0;
}

OwnedCFile ReaderCFileImpl(ReaderCFileCookieBase* cookie,
                           std::string&& filename);

}  // namespace cfile_internal

template <typename Src,
          std::enable_if_t<TargetSupportsDependency<Reader*, Src>::value, int>>
OwnedCFile ReaderCFile(Src&& src, ReaderCFileOptions options) {
  return cfile_internal::ReaderCFileImpl(
      new cfile_internal::ReaderCFileCookie<TargetT<Src>>(
          std::forward<Src>(src)),
      std::move(options.filename()));
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_READER_CFILE_H_
