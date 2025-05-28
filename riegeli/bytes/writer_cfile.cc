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

#include "riegeli/bytes/writer_cfile.h"

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/types.h>

#include <cerrno>
#include <limits>
#include <optional>

#include "absl/base/dynamic_annotations.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/errno_mapping.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli::cfile_internal {

WriterCFileCookieBase::~WriterCFileCookieBase() {}

void WriterCFileCookieBase::Initialize(Writer* writer) {
  RIEGELI_ASSERT_NE(writer, nullptr)
      << "Failed precondition of WriterCFile(): null Writer pointer";
  if (flush_type_ != std::nullopt) writer->Flush(*flush_type_);
}

inline const char* WriterCFileCookieBase::OpenMode() {
  Writer& writer = *DestWriter();
  return writer.SupportsReadMode() && writer.SupportsRandomAccess() ? "w+"
                                                                    : "w";
}

inline ssize_t WriterCFileCookieBase::Read(char* dest, size_t length) {
  if (ABSL_PREDICT_FALSE(reader_ == nullptr)) {
    Writer& writer = *DestWriter();
    const Position pos = writer.pos();
    reader_ = writer.ReadMode(pos);
    if (ABSL_PREDICT_FALSE(reader_ == nullptr)) {
      errno = StatusCodeToErrno(writer.status().code());
      return -1;
    }
    if (ABSL_PREDICT_FALSE(reader_->pos() != pos)) {
      if (ABSL_PREDICT_FALSE(!reader_->ok())) {
        errno = StatusCodeToErrno(reader_->status().code());
      } else {
        errno = EINVAL;
      }
      return -1;
    }
  }
  size_t length_read;
  if (ABSL_PREDICT_FALSE(!reader_->ReadSome(length, dest, &length_read) &&
                         !reader_->ok())) {
    errno = StatusCodeToErrno(reader_->status().code());
    return -1;
  }
  return IntCast<ssize_t>(length_read);
}

inline ssize_t WriterCFileCookieBase::Write(const char* src, size_t length) {
  // Msan does not properly track initialization performed by precompiled
  // libraries. The data to write might have been composed by e.g. `fprintf()`.
  ABSL_ANNOTATE_MEMORY_IS_INITIALIZED(src, length);
  Writer& writer = *DestWriter();
  if (ABSL_PREDICT_FALSE(reader_ != nullptr)) {
    const Position pos = reader_->pos();
    reader_ = nullptr;
    if (ABSL_PREDICT_FALSE(!writer.Seek(pos))) {
      if (ABSL_PREDICT_FALSE(!writer.ok())) {
        errno = StatusCodeToErrno(writer.status().code());
      } else {
        errno = EINVAL;
      }
      return 0;
    }
  }
  if (ABSL_PREDICT_FALSE(!writer.Write(absl::string_view(src, length)))) {
    errno = StatusCodeToErrno(writer.status().code());
    return 0;
  }
  if (flush_type_ != std::nullopt) {
    if (ABSL_PREDICT_FALSE(!writer.Flush(*flush_type_))) {
      errno = StatusCodeToErrno(writer.status().code());
      return 0;
    }
  }
  return IntCast<ssize_t>(length);
}

inline std::optional<int64_t> WriterCFileCookieBase::Seek(int64_t offset,
                                                          int whence) {
  Writer& writer = *DestWriter();
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
      new_pos = reader_ != nullptr ? reader_->pos() : writer.pos();
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
      if (ABSL_PREDICT_FALSE(!writer.SupportsRandomAccess())) {
        // Indicate that `fseek(SEEK_END)` is not supported.
        errno = ESPIPE;
        return std::nullopt;
      }
      std::optional<Position> size;
      if (reader_ != nullptr) {
        RIEGELI_ASSERT(reader_->SupportsSize())
            << "Failed postcondition of Writer::ReadMode(): "
               "!Reader::SupportsSize() even though "
               "Writer::SupportsRandomAccess()";
        size = reader_->Size();
        if (ABSL_PREDICT_FALSE(size == std::nullopt)) {
          errno = StatusCodeToErrno(reader_->status().code());
          return std::nullopt;
        }
      } else {
        size = writer.Size();
        if (ABSL_PREDICT_FALSE(size == std::nullopt)) {
          errno = StatusCodeToErrno(writer.status().code());
          return std::nullopt;
        }
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
  if (new_pos == (reader_ != nullptr ? reader_->pos() : writer.pos())) {
    // Seeking to the current position is supported even if random access is
    // not.
    return IntCast<int64_t>(new_pos);
  }
  if (reader_ != nullptr) {
    RIEGELI_ASSERT(reader_->SupportsRewind())
        << "Failed postcondition of Writer::ReadMode(): "
           "!Reader::SupportsRewind()";
    if (ABSL_PREDICT_FALSE(!reader_->Seek(IntCast<Position>(new_pos)))) {
      if (ABSL_PREDICT_FALSE(!reader_->ok())) {
        errno = StatusCodeToErrno(reader_->status().code());
      } else {
        errno = EINVAL;
      }
      return std::nullopt;
    }
  } else {
    if (ABSL_PREDICT_FALSE(!writer.SupportsRandomAccess())) {
      // Indicate that `fseek()` is not supported.
      errno = ESPIPE;
      return std::nullopt;
    }
    if (ABSL_PREDICT_FALSE(!writer.Seek(IntCast<Position>(new_pos)))) {
      if (ABSL_PREDICT_FALSE(!writer.ok())) {
        errno = StatusCodeToErrno(writer.status().code());
      } else {
        errno = EINVAL;
      }
      return std::nullopt;
    }
  }
  return IntCast<int64_t>(new_pos);
}

// `extern "C"` sets the C calling convention for compatibility with
// `fopencookie()`. `static` avoids making symbols public, as `extern "C"`
// trumps anonymous namespace.
extern "C" {

static ssize_t WriterCFileRead(void* cookie, char* buf, size_t size) {
  return static_cast<WriterCFileCookieBase*>(cookie)->Read(buf, size);
}

static ssize_t WriterCFileWrite(void* cookie, const char* buf, size_t size) {
  return static_cast<WriterCFileCookieBase*>(cookie)->Write(buf, size);
}

static int WriterCFileSeek(void* cookie, off64_t* offset, int whence) {
  const std::optional<int64_t> new_pos =
      static_cast<WriterCFileCookieBase*>(cookie)->Seek(
          IntCast<int64_t>(*offset), whence);
  if (ABSL_PREDICT_FALSE(new_pos == std::nullopt)) {
    *offset = -1;
    return -1;
  }
  *offset = IntCast<off64_t>(*new_pos);
  return 0;
}

static int WriterCFileClose(void* cookie) {
  const int result = static_cast<WriterCFileCookieBase*>(cookie)->Close();
  delete static_cast<WriterCFileCookieBase*>(cookie);
  if (ABSL_PREDICT_FALSE(result != 0)) {
    errno = result;
    return -1;
  }
  return 0;
}

}  // extern "C"

FILE* WriterCFileImpl(WriterCFileCookieBase* cookie) {
  return fopencookie(
      cookie, cookie->OpenMode(),
      {WriterCFileRead, WriterCFileWrite, WriterCFileSeek, WriterCFileClose});
}

}  // namespace riegeli::cfile_internal
