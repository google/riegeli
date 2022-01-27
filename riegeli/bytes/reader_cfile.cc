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

#include <cerrno>
#include <limits>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/errno_mapping.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {
namespace cfile_internal {

ReaderCFileCookieBase::~ReaderCFileCookieBase() {}

inline ssize_t ReaderCFileCookieBase::Read(char* dest, size_t length) {
  Reader& reader = *src_reader();
  if (ABSL_PREDICT_FALSE(!reader.Pull(1, length))) return 0;
  length = UnsignedMin(length, reader.available());
  const Position pos_before = reader.pos();
  if (ABSL_PREDICT_FALSE(!reader.Read(length, dest))) {
    RIEGELI_ASSERT_GE(reader.pos(), pos_before)
        << "Reader::Read(char*) decreased pos()";
    const Position length_read = reader.pos() - pos_before;
    RIEGELI_ASSERT_LE(length_read, length)
        << "Reader::Read(char*) read more than requested";
    if (length_read > 0) return IntCast<ssize_t>(length_read);
    if (ABSL_PREDICT_FALSE(!reader.healthy())) {
      errno = StatusCodeToErrno(reader.status().code());
      return -1;
    }
    return 0;
  }
  return IntCast<ssize_t>(length);
}

inline absl::optional<int64_t> ReaderCFileCookieBase::Seek(int64_t offset,
                                                           int whence) {
  Reader& reader = *src_reader();
  Position new_pos;
  switch (whence) {
    case SEEK_SET:
      if (ABSL_PREDICT_FALSE(offset < 0)) {
        errno = EINVAL;
        return absl::nullopt;
      }
      new_pos = IntCast<Position>(offset);
      break;
    case SEEK_CUR:
      new_pos = reader.pos();
      if (offset < 0) {
        if (ABSL_PREDICT_FALSE(IntCast<Position>(-offset) > new_pos)) {
          errno = EINVAL;
          return absl::nullopt;
        }
        new_pos -= IntCast<Position>(-offset);
        if (ABSL_PREDICT_FALSE(new_pos >
                               Position{std::numeric_limits<int64_t>::max()})) {
          errno = EINVAL;
          return absl::nullopt;
        }
      } else {
        if (ABSL_PREDICT_FALSE(
                new_pos > Position{std::numeric_limits<int64_t>::max()} ||
                IntCast<Position>(offset) >
                    Position{std::numeric_limits<int64_t>::max()} - new_pos)) {
          errno = EINVAL;
          return absl::nullopt;
        }
        new_pos += IntCast<Position>(offset);
      }
      break;
    case SEEK_END: {
      if (ABSL_PREDICT_FALSE(!reader.SupportsSize())) {
        // Indicate that `fseek(SEEK_END)` is not supported.
        errno = ESPIPE;
        return absl::nullopt;
      }
      const absl::optional<Position> size = reader.Size();
      if (ABSL_PREDICT_FALSE(size == absl::nullopt)) {
        errno = StatusCodeToErrno(reader.status().code());
        return -1;
      }
      if (ABSL_PREDICT_FALSE(offset > 0 ||
                             IntCast<Position>(-offset) > *size)) {
        errno = EINVAL;
        return absl::nullopt;
      }
      new_pos = *size - IntCast<Position>(-offset);
      if (ABSL_PREDICT_FALSE(new_pos >
                             Position{std::numeric_limits<int64_t>::max()})) {
        errno = EINVAL;
        return absl::nullopt;
      }
    } break;
    default:
      RIEGELI_ASSERT_UNREACHABLE() << "Unknown seek origin: " << whence;
  }
  if (new_pos == reader.pos()) {
    // Seeking to the current position is supported even if random access is
    // not.
    return IntCast<int64_t>(new_pos);
  }
  if (ABSL_PREDICT_FALSE(!reader.SupportsRewind())) {
    // Indicate that `fseek()` is not supported.
    errno = ESPIPE;
    return absl::nullopt;
  }
  if (ABSL_PREDICT_FALSE(!reader.Seek(new_pos))) {
    if (ABSL_PREDICT_FALSE(!reader.healthy())) {
      errno = StatusCodeToErrno(reader.status().code());
    } else {
      errno = EINVAL;
    }
    return -1;
  }
  return IntCast<int64_t>(new_pos);
}

extern "C" {

static ssize_t ReaderCFileRead(void* cookie, char* buf, size_t size) {
  return static_cast<ReaderCFileCookieBase*>(cookie)->Read(buf, size);
}

static int ReaderCFileSeek(void* cookie, off64_t* offset, int whence) {
  const absl::optional<int64_t> new_pos =
      static_cast<ReaderCFileCookieBase*>(cookie)->Seek(
          IntCast<int64_t>(*offset), whence);
  if (ABSL_PREDICT_FALSE(new_pos == absl::nullopt)) {
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

FILE* ReaderCFileImpl(ReaderCFileCookieBase* cookie) {
  return fopencookie(
      cookie, "r",
      {ReaderCFileRead, nullptr, ReaderCFileSeek, ReaderCFileClose});
}

}  // namespace cfile_internal
}  // namespace riegeli
