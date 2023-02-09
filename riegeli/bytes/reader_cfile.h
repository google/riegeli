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

#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/types/optional.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/errno_mapping.h"
#include "riegeli/bytes/cfile_dependency.h"  // IWYU pragma: export
#include "riegeli/bytes/reader.h"

namespace riegeli {

class ReaderCFileOptions {
 public:
  ReaderCFileOptions() noexcept {}
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
// `std::unique_ptr<Reader>` (owned), `AnyDependency<Reader*>` (maybe owned).
//
// With a `src_args` parameter, reads from a `Src` constructed from elements of
// `src_args`. This avoids constructing a temporary `Src` and moving from it.
//
// The `Reader` must not be accessed until the `FILE` is closed. Warning: this
// includes implicit closing of all `FILE` objects which are still open at
// program exit, hence if the `FILE` persists until program exit, then the
// `Reader` must do so as well.
template <typename Src>
FILE* ReaderCFile(const Src& src,
                  ReaderCFileOptions options = ReaderCFileOptions());
template <typename Src>
FILE* ReaderCFile(Src&& src, ReaderCFileOptions options = ReaderCFileOptions());
template <typename Src, typename... SrcArgs>
FILE* ReaderCFile(std::tuple<SrcArgs...> src_args,
                  ReaderCFileOptions options = ReaderCFileOptions());

// Implementation details follow.

namespace cfile_internal {

class ReaderCFileCookieBase {
 public:
  virtual ~ReaderCFileCookieBase();

  ssize_t Read(char* dest, size_t length);

  // Use `int64_t` instead of `off64_t` to avoid a dependency on
  // `#define _GNU_SOURCE` in a header.
  absl::optional<int64_t> Seek(int64_t offset, int whence);

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
  RIEGELI_ASSERT(reader != nullptr)
      << "Failed precondition of ReaderCFile(): null Reader pointer";
}

template <typename Src>
class ReaderCFileCookie : public ReaderCFileCookieBase {
 public:
  explicit ReaderCFileCookie(const Src& src) : src_(src) {
    Initialize(src_.get());
  }
  explicit ReaderCFileCookie(Src&& src) : src_(std::move(src)) {
    Initialize(src_.get());
  }
  template <typename... SrcArgs>
  explicit ReaderCFileCookie(std::tuple<SrcArgs...> src_args)
      : src_(std::move(src_args)) {
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
  if (src_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) {
      return StatusCodeToErrno(src_->status().code());
    }
  }
  return 0;
}

FILE* ReaderCFileImpl(ReaderCFileCookieBase* cookie);

}  // namespace cfile_internal

template <typename Src>
FILE* ReaderCFile(const Src& src, ReaderCFileOptions options) {
  return cfile_internal::ReaderCFileImpl(
      new cfile_internal::ReaderCFileCookie<std::decay_t<Src>>(src));
}

template <typename Src>
FILE* ReaderCFile(Src&& src, ReaderCFileOptions options) {
  return cfile_internal::ReaderCFileImpl(
      new cfile_internal::ReaderCFileCookie<std::decay_t<Src>>(
          std::forward<Src>(src)));
}

template <typename Src, typename... SrcArgs>
FILE* ReaderCFile(std::tuple<SrcArgs...> src_args, ReaderCFileOptions options) {
  return cfile_internal::ReaderCFileImpl(
      new cfile_internal::ReaderCFileCookie<Src>(std::move(src_args)));
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_READER_CFILE_H_
