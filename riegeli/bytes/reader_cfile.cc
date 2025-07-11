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

// Make `fopencookie()` and `off64_t` available.
#if !defined(_GNU_SOURCE)
#define _GNU_SOURCE
#endif

#include "riegeli/bytes/reader_cfile.h"

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/types.h>

#include <cerrno>
#include <limits>
#include <optional>
#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/errno_mapping.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/cfile_handle.h"
#include "riegeli/bytes/path_ref.h"
#include "riegeli/bytes/reader.h"

namespace riegeli::cfile_internal {

ReaderCFileCookieBase::~ReaderCFileCookieBase() {}

inline ssize_t ReaderCFileCookieBase::Read(char* dest, size_t length) {
  Reader& reader = *SrcReader();
  size_t length_read;
  if (ABSL_PREDICT_FALSE(!reader.ReadSome(length, dest, &length_read) &&
                         !reader.ok())) {
    errno = StatusCodeToErrno(reader.status().code());
    return -1;
  }
  return IntCast<ssize_t>(length_read);
}

inline std::optional<int64_t> ReaderCFileCookieBase::Seek(int64_t offset,
                                                          int whence) {
  Reader& reader = *SrcReader();
  Position new_pos;
  switch (whence) {
    case SEEK_SET:
      if (ABSL_PREDICT_FALSE(offset < 0)) {
        errno = EINVAL;
        return std::nullopt;
      }
      new_pos = IntCast<Position>(offset);
      break;
    case SEEK_CUR:
      new_pos = reader.pos();
      if (offset < 0) {
        if (ABSL_PREDICT_FALSE(NegatingUnsignedCast(offset) > new_pos)) {
          errno = EINVAL;
          return std::nullopt;
        }
        new_pos -= NegatingUnsignedCast(offset);
        if (ABSL_PREDICT_FALSE(new_pos >
                               Position{std::numeric_limits<int64_t>::max()})) {
          errno = EINVAL;
          return std::nullopt;
        }
      } else {
        if (ABSL_PREDICT_FALSE(
                new_pos > Position{std::numeric_limits<int64_t>::max()} ||
                IntCast<Position>(offset) >
                    Position{std::numeric_limits<int64_t>::max()} - new_pos)) {
          errno = EINVAL;
          return std::nullopt;
        }
        new_pos += IntCast<Position>(offset);
      }
      break;
    case SEEK_END: {
      if (ABSL_PREDICT_FALSE(!reader.SupportsSize())) {
        // Indicate that `fseek(SEEK_END)` is not supported.
        errno = ESPIPE;
        return std::nullopt;
      }
      const std::optional<Position> size = reader.Size();
      if (ABSL_PREDICT_FALSE(size == std::nullopt)) {
        errno = StatusCodeToErrno(reader.status().code());
        return std::nullopt;
      }
      if (ABSL_PREDICT_FALSE(offset > 0 ||
                             NegatingUnsignedCast(offset) > *size)) {
        errno = EINVAL;
        return std::nullopt;
      }
      new_pos = *size - NegatingUnsignedCast(offset);
      if (ABSL_PREDICT_FALSE(new_pos >
                             Position{std::numeric_limits<int64_t>::max()})) {
        errno = EINVAL;
        return std::nullopt;
      }
    } break;
    default:
      RIEGELI_ASSUME_UNREACHABLE() << "Unknown seek origin: " << whence;
  }
  if (new_pos >= reader.pos()) {
    // Seeking forwards is supported even if random access is not.
  } else if (ABSL_PREDICT_FALSE(!reader.SupportsRewind())) {
    // Indicate that `fseek()` is not supported.
    errno = ESPIPE;
    return std::nullopt;
  }
  if (ABSL_PREDICT_FALSE(!reader.Seek(new_pos))) {
    if (ABSL_PREDICT_FALSE(!reader.ok())) {
      errno = StatusCodeToErrno(reader.status().code());
    } else {
      errno = EINVAL;
    }
    return std::nullopt;
  }
  return IntCast<int64_t>(new_pos);
}

// `extern "C"` sets the C calling convention for compatibility with
// `fopencookie()`. `static` avoids making symbols public, as `extern "C"`
// trumps anonymous namespace.
extern "C" {

static ssize_t ReaderCFileRead(void* cookie, char* buf, size_t size) {
  return static_cast<ReaderCFileCookieBase*>(cookie)->Read(buf, size);
}

static int ReaderCFileSeek(void* cookie, off64_t* offset, int whence) {
  const std::optional<int64_t> new_pos =
      static_cast<ReaderCFileCookieBase*>(cookie)->Seek(
          IntCast<int64_t>(*offset), whence);
  if (ABSL_PREDICT_FALSE(new_pos == std::nullopt)) {
    *offset = -1;
    return -1;
  }
  *offset = IntCast<off64_t>(*new_pos);
  return 0;
}

static int ReaderCFileClose(void* cookie) {
  const int result = static_cast<ReaderCFileCookieBase*>(cookie)->Close();
  delete static_cast<ReaderCFileCookieBase*>(cookie);
  if (ABSL_PREDICT_FALSE(result != 0)) {
    errno = result;
    return -1;
  }
  return 0;
}

}  // extern "C"

OwnedCFile ReaderCFileImpl(ReaderCFileCookieBase* cookie,
                           std::string&& filename) {
  return OwnedCFile(fopencookie(cookie, "r",
                                {ReaderCFileRead, nullptr, ReaderCFileSeek,
                                 ReaderCFileClose}),
                    std::move(filename));
}

}  // namespace riegeli::cfile_internal
