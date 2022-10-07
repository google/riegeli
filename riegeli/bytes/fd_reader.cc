// Copyright 2017 Google LLC
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

// Make `pread()` available.
#if !defined(_XOPEN_SOURCE) || _XOPEN_SOURCE < 500
#undef _XOPEN_SOURCE
#define _XOPEN_SOURCE 500
#endif

// Make `off_t` 64-bit even on 32-bit systems.
#undef _FILE_OFFSET_BITS
#define _FILE_OFFSET_BITS 64

#include "riegeli/bytes/fd_reader.h"

#include <fcntl.h>
#include <stddef.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/errno_mapping.h"
#include "riegeli/base/object.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/fd_internal.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

void FdReaderBase::Initialize(int src,
                              absl::optional<std::string>&& assumed_filename,
                              absl::optional<Position> assumed_pos,
                              absl::optional<Position> independent_pos) {
  RIEGELI_ASSERT_GE(src, 0)
      << "Failed precondition of FdReader: negative file descriptor";
  filename_ = fd_internal::ResolveFilename(src, std::move(assumed_filename));
  InitializePos(src, assumed_pos, independent_pos);
}

int FdReaderBase::OpenFd(absl::string_view filename, int mode) {
  RIEGELI_ASSERT((mode & O_ACCMODE) == O_RDONLY || (mode & O_ACCMODE) == O_RDWR)
      << "Failed precondition of FdReader: "
         "mode must include either O_RDONLY or O_RDWR";
  // TODO: When `absl::string_view` becomes C++17 `std::string_view`:
  // `filename_ = filename`
  filename_.assign(filename.data(), filename.size());
again:
  const int src = open(filename_.c_str(), mode, 0666);
  if (ABSL_PREDICT_FALSE(src < 0)) {
    if (errno == EINTR) goto again;
    BufferedReader::Reset(kClosed);
    FailOperation("open()");
    return -1;
  }
  return src;
}

void FdReaderBase::InitializePos(int src, absl::optional<Position> assumed_pos,
                                 absl::optional<Position> independent_pos) {
  RIEGELI_ASSERT(!has_independent_pos_)
      << "Failed precondition of FdReaderBase::InitializePos(): "
         "has_independent_pos_ not reset";
  RIEGELI_ASSERT(!supports_random_access_)
      << "Failed precondition of FdReaderBase::InitializePos(): "
         "supports_random_access_ not reset";
  if (assumed_pos != absl::nullopt) {
    if (ABSL_PREDICT_FALSE(independent_pos != absl::nullopt)) {
      Fail(absl::InvalidArgumentError(
          "FdReaderBase::Options::assumed_pos() and independent_pos() "
          "must not be both set"));
      return;
    }
    if (ABSL_PREDICT_FALSE(*assumed_pos >
                           Position{std::numeric_limits<off_t>::max()})) {
      FailOverflow();
      return;
    }
    set_limit_pos(*assumed_pos);
  } else if (independent_pos != absl::nullopt) {
    has_independent_pos_ = true;
    supports_random_access_ = true;
    if (ABSL_PREDICT_FALSE(*independent_pos >
                           Position{std::numeric_limits<off_t>::max()})) {
      FailOverflow();
      return;
    }
    set_limit_pos(*independent_pos);
  } else {
    const off_t file_pos = lseek(src, 0, SEEK_CUR);
    if (file_pos < 0) {
      // Random access is not supported. Assume 0 as the initial position.
      return;
    }
    set_limit_pos(IntCast<Position>(file_pos));

    // Check if random access is supported.
    if (ABSL_PREDICT_FALSE(absl::StartsWith(filename(), "/sys/"))) {
      // "/sys" files do not support random access. It is hard to reliably
      // recognize them, so `FdReader` checks the filename.
      //
      // Some "/proc" files also do not support random access, but they are
      // recognized by a failing `lseek(SEEK_END)`.
    } else {
      const off_t file_size = lseek(src, 0, SEEK_END);
      if (file_size < 0) {
        // Not supported.
      } else {
        if (ABSL_PREDICT_FALSE(
                lseek(src, IntCast<off_t>(limit_pos()), SEEK_SET) < 0)) {
          FailOperation("lseek()");
          return;
        }
        if (!growing_source_) set_exact_size(IntCast<Position>(file_size));
        supports_random_access_ = true;
      }
    }
  }
  BeginRun();
}

bool FdReaderBase::FailOperation(absl::string_view operation) {
  const int error_number = errno;
  RIEGELI_ASSERT_NE(error_number, 0)
      << "Failed precondition of FdReaderBase::FailOperation(): "
         "zero errno";
  return Fail(
      ErrnoToCanonicalStatus(error_number, absl::StrCat(operation, " failed")));
}

absl::Status FdReaderBase::AnnotateStatusImpl(absl::Status status) {
  if (!filename_.empty()) {
    status = Annotate(status, absl::StrCat("reading ", filename_));
  }
  return BufferedReader::AnnotateStatusImpl(std::move(status));
}

bool FdReaderBase::ReadInternal(size_t min_length, size_t max_length,
                                char* dest) {
  RIEGELI_ASSERT_GT(min_length, 0u)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "nothing to read";
  RIEGELI_ASSERT_GE(max_length, min_length)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "max_length < min_length";
  RIEGELI_ASSERT(ok())
      << "Failed precondition of BufferedReader::ReadInternal(): " << status();
  const int src = SrcFd();
  for (;;) {
    Position max_pos;
    if (exact_size() != absl::nullopt) {
      max_pos = *exact_size();
      if (ABSL_PREDICT_FALSE(limit_pos() >= max_pos)) return false;
    } else {
      max_pos = Position{std::numeric_limits<off_t>::max()};
      if (ABSL_PREDICT_FALSE(limit_pos() >= max_pos)) return FailOverflow();
    }
    const size_t length_to_read =
        UnsignedMin(max_length, max_pos - limit_pos(),
                    size_t{std::numeric_limits<ssize_t>::max()});
  again:
    const ssize_t length_read =
        has_independent_pos_
            ? pread(src, dest, length_to_read, IntCast<off_t>(limit_pos()))
            : read(src, dest, length_to_read);
    if (ABSL_PREDICT_FALSE(length_read < 0)) {
      if (errno == EINTR) goto again;
      return FailOperation(has_independent_pos_ ? "pread()" : "read()");
    }
    if (ABSL_PREDICT_FALSE(length_read == 0)) {
      if (!growing_source_) set_exact_size(limit_pos());
      return false;
    }
    RIEGELI_ASSERT_LE(IntCast<size_t>(length_read), max_length)
        << (has_independent_pos_ ? "pread()" : "read()")
        << " read more than requested";
    move_limit_pos(IntCast<size_t>(length_read));
    if (IntCast<size_t>(length_read) >= min_length) return true;
    dest += length_read;
    min_length -= IntCast<size_t>(length_read);
    max_length -= IntCast<size_t>(length_read);
  }
}

inline bool FdReaderBase::SeekInternal(int src, Position new_pos) {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of FdReaderBase::SeekInternal(): "
         "buffer not empty";
  RIEGELI_ASSERT(supports_random_access())
      << "Failed precondition of FdReaderBase::SeekInternal(): "
         "random access not supported";
  if (!has_independent_pos_) {
    if (ABSL_PREDICT_FALSE(lseek(src, IntCast<off_t>(new_pos), SEEK_SET) < 0)) {
      return FailOperation("lseek()");
    }
  }
  set_limit_pos(new_pos);
  return true;
}

bool FdReaderBase::SeekBehindBuffer(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of BufferedReader::SeekBehindBuffer(): "
         "position in the buffer, use Seek() instead";
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedReader::SeekBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!supports_random_access())) {
    return BufferedReader::SeekBehindBuffer(new_pos);
  }
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  const int src = SrcFd();
  if (new_pos > limit_pos()) {
    // Seeking forwards.
    Position file_size;
    if (exact_size() != absl::nullopt) {
      file_size = *exact_size();
    } else {
      struct stat stat_info;
      if (ABSL_PREDICT_FALSE(fstat(src, &stat_info) < 0)) {
        return FailOperation("fstat()");
      }
      file_size = IntCast<Position>(stat_info.st_size);
      if (!growing_source_) set_exact_size(file_size);
    }
    if (ABSL_PREDICT_FALSE(new_pos > file_size)) {
      // File ends.
      SeekInternal(src, file_size);
      return false;
    }
  }
  return SeekInternal(src, new_pos);
}

absl::optional<Position> FdReaderBase::SizeImpl() {
  if (ABSL_PREDICT_FALSE(!supports_random_access())) {
    // Delegate to base class version which fails, to avoid duplicating the
    // failure message here.
    return BufferedReader::SizeImpl();
  }
  if (ABSL_PREDICT_FALSE(!ok())) return absl::nullopt;
  if (exact_size() != absl::nullopt) return *exact_size();
  const int src = SrcFd();
  struct stat stat_info;
  if (ABSL_PREDICT_FALSE(fstat(src, &stat_info) < 0)) {
    FailOperation("fstat()");
    return absl::nullopt;
  }
  if (!growing_source_) set_exact_size(IntCast<Position>(stat_info.st_size));
  return IntCast<Position>(stat_info.st_size);
}

std::unique_ptr<Reader> FdReaderBase::NewReaderImpl(Position initial_pos) {
  if (ABSL_PREDICT_FALSE(!supports_random_access())) {
    // Delegate to base class version which fails, to avoid duplicating the
    // failure message here.
    return BufferedReader::NewReaderImpl(initial_pos);
  }
  if (ABSL_PREDICT_FALSE(!ok())) return nullptr;
  // `NewReaderImpl()` is thread-safe from this point.
  const int src = SrcFd();
  std::unique_ptr<FdReader<UnownedFd>> reader =
      std::make_unique<FdReader<UnownedFd>>(
          src, FdReaderBase::Options()
                   .set_assumed_filename(filename())
                   .set_independent_pos(initial_pos)
                   .set_growing_source(growing_source_)
                   .set_buffer_options(buffer_options()));
  reader->set_exact_size(exact_size());
  ShareBufferTo(*reader);
  return reader;
}

}  // namespace riegeli
