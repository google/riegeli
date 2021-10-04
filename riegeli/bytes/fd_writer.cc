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

// Make `pwrite()` and `ftruncate()` available.
#if !defined(_XOPEN_SOURCE) || _XOPEN_SOURCE < 500
#undef _XOPEN_SOURCE
#define _XOPEN_SOURCE 500
#endif

// Make `off_t` 64-bit even on 32-bit systems.
#undef _FILE_OFFSET_BITS
#define _FILE_OFFSET_BITS 64

#include "riegeli/bytes/fd_writer.h"

#include <fcntl.h>
#include <stddef.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <limits>
#include <string>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/errno_mapping.h"
#include "riegeli/bytes/buffered_writer.h"

namespace riegeli {

void FdWriterBase::Initialize(int dest, absl::optional<Position> assumed_pos,
                              absl::optional<Position> independent_pos) {
  RIEGELI_ASSERT_GE(dest, 0)
      << "Failed precondition of FdWriter: negative file descriptor";
  SetFilename(dest);
  InitializePos(dest, assumed_pos, independent_pos);
}

inline void FdWriterBase::SetFilename(int dest) {
  if (dest == 1) {
    filename_ = "/dev/stdout";
  } else if (dest == 2) {
    filename_ = "/dev/stderr";
  } else {
    filename_ = absl::StrCat("/proc/self/fd/", dest);
  }
}

int FdWriterBase::OpenFd(absl::string_view filename, int flags,
                         mode_t permissions) {
  RIEGELI_ASSERT((flags & O_ACCMODE) == O_WRONLY ||
                 (flags & O_ACCMODE) == O_RDWR)
      << "Failed precondition of FdWriter: "
         "flags must include either O_WRONLY or O_RDWR";
  // TODO: When `absl::string_view` becomes C++17 `std::string_view`:
  // `filename_ = filename`
  filename_.assign(filename.data(), filename.size());
again:
  const int dest = open(filename_.c_str(), flags, permissions);
  if (ABSL_PREDICT_FALSE(dest < 0)) {
    if (errno == EINTR) goto again;
    FailOperation("open()");
    return -1;
  }
  return dest;
}

inline void FdWriterBase::InitializePos(
    int dest, absl::optional<Position> assumed_pos,
    absl::optional<Position> independent_pos) {
  int flags = 0;
  if (assumed_pos == absl::nullopt && independent_pos == absl::nullopt) {
    // Flags are needed only if `assumed_pos == absl::nullopt` and
    // `independent_pos == absl::nullopt`. Avoid `fcntl()` otherwise.
    flags = fcntl(dest, F_GETFL);
    if (ABSL_PREDICT_FALSE(flags < 0)) {
      FailOperation("fcntl()");
      return;
    }
  }
  return InitializePos(dest, flags, assumed_pos, independent_pos);
}

void FdWriterBase::InitializePos(int dest, int flags,
                                 absl::optional<Position> assumed_pos,
                                 absl::optional<Position> independent_pos) {
  RIEGELI_ASSERT(assumed_pos == absl::nullopt ||
                 independent_pos == absl::nullopt)
      << "Failed precondition of FdWriterBase: "
         "Options::assumed_pos() and Options::independent_pos() are both set";
  RIEGELI_ASSERT(!supports_random_access_)
      << "Failed precondition of FdWriterBase::InitializePos(): "
         "supports_random_access_ not reset";
  RIEGELI_ASSERT(!has_independent_pos_)
      << "Failed precondition of FdWriterBase::InitializePos(): "
         "has_independent_pos_ not reset";
  if (assumed_pos != absl::nullopt) {
    if (ABSL_PREDICT_FALSE(*assumed_pos >
                           Position{std::numeric_limits<off_t>::max()})) {
      FailOverflow();
      return;
    }
    set_start_pos(*assumed_pos);
  } else if (independent_pos != absl::nullopt) {
    supports_random_access_ = true;
    has_independent_pos_ = true;
    if (ABSL_PREDICT_FALSE(*independent_pos >
                           Position{std::numeric_limits<off_t>::max()})) {
      FailOverflow();
      return;
    }
    set_start_pos(*independent_pos);
  } else {
    const off_t file_pos =
        lseek(dest, 0, (flags & O_APPEND) != 0 ? SEEK_END : SEEK_CUR);
    if (file_pos < 0) {
      if (errno == ESPIPE) {
        // Random access is not supported. Assume the current position as 0.
      } else {
        FailOperation("lseek()");
      }
      return;
    }
    set_start_pos(IntCast<Position>(file_pos));
    supports_random_access_ = true;
  }
}

bool FdWriterBase::FailOperation(absl::string_view operation) {
  const int error_number = errno;
  RIEGELI_ASSERT_NE(error_number, 0)
      << "Failed precondition of FdWriterBase::FailOperation(): "
         "zero errno";
  return Fail(
      ErrnoToCanonicalStatus(error_number, absl::StrCat(operation, " failed")));
}

void FdWriterBase::DefaultAnnotateStatus() {
  RIEGELI_ASSERT(!not_failed())
      << "Failed precondition of Object::DefaultAnnotateStatus(): "
         "Object not failed";
  AnnotateStatus(absl::StrCat("writing ", filename_));
  BufferedWriter::DefaultAnnotateStatus();
}

bool FdWriterBase::WriteInternal(absl::string_view src) {
  RIEGELI_ASSERT(!src.empty())
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "nothing to write";
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of BufferedWriter::WriteInternal(): " << status();
  const int dest = dest_fd();
  if (ABSL_PREDICT_FALSE(src.size() >
                         Position{std::numeric_limits<off_t>::max()} -
                             start_pos())) {
    return FailOverflow();
  }
  do {
  again:
    const ssize_t length_written =
        has_independent_pos_
            ? pwrite(dest, src.data(),
                     UnsignedMin(src.size(),
                                 size_t{std::numeric_limits<ssize_t>::max()}),
                     IntCast<off_t>(start_pos()))
            : write(dest, src.data(),
                    UnsignedMin(src.size(),
                                size_t{std::numeric_limits<ssize_t>::max()}));
    if (ABSL_PREDICT_FALSE(length_written < 0)) {
      if (errno == EINTR) goto again;
      return FailOperation(has_independent_pos_ ? "pwrite()" : "write()");
    }
    RIEGELI_ASSERT_GT(length_written, 0)
        << (has_independent_pos_ ? "pwrite()" : "write()") << " returned 0";
    RIEGELI_ASSERT_LE(IntCast<size_t>(length_written), src.size())
        << (has_independent_pos_ ? "pwrite()" : "write()")
        << " wrote more than requested";
    move_start_pos(IntCast<size_t>(length_written));
    src.remove_prefix(IntCast<size_t>(length_written));
  } while (!src.empty());
  return true;
}

bool FdWriterBase::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!BufferedWriter::FlushImpl(flush_type))) return false;
  switch (flush_type) {
    case FlushType::kFromObject:
    case FlushType::kFromProcess:
      return true;
    case FlushType::kFromMachine: {
      const int dest = dest_fd();
      if (ABSL_PREDICT_FALSE(fsync(dest) < 0)) {
        return FailOperation("fsync()");
      }
      return true;
    }
  }
  RIEGELI_ASSERT_UNREACHABLE()
      << "Unknown flush type: " << static_cast<int>(flush_type);
}

inline bool FdWriterBase::SeekInternal(int dest, Position new_pos) {
  RIEGELI_ASSERT_EQ(buffer_size(), 0u)
      << "Failed precondition of FdWriterBase::SeekInternal(): "
         "buffer not empty";
  if (!has_independent_pos_) {
    if (ABSL_PREDICT_FALSE(lseek(dest, IntCast<off_t>(new_pos), SEEK_SET) <
                           0)) {
      return FailOperation("lseek()");
    }
  }
  set_start_pos(new_pos);
  return true;
}

bool FdWriterBase::SeekBehindBuffer(Position new_pos) {
  RIEGELI_ASSERT_EQ(buffer_size(), 0u)
      << "Failed precondition of BufferedWriter::SeekBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  const int dest = dest_fd();
  if (new_pos >= start_pos()) {
    // Seeking forwards.
    struct stat stat_info;
    if (ABSL_PREDICT_FALSE(fstat(dest, &stat_info) < 0)) {
      return FailOperation("fstat()");
    }
    if (ABSL_PREDICT_FALSE(new_pos > IntCast<Position>(stat_info.st_size))) {
      // File ends.
      SeekInternal(dest, IntCast<Position>(stat_info.st_size));
      return false;
    }
  }
  return SeekInternal(dest, new_pos);
}

absl::optional<Position> FdWriterBase::SizeBehindBuffer() {
  RIEGELI_ASSERT_EQ(buffer_size(), 0u)
      << "Failed precondition of BufferedWriter::SizeBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!healthy())) return absl::nullopt;
  const int dest = dest_fd();
  struct stat stat_info;
  if (ABSL_PREDICT_FALSE(fstat(dest, &stat_info) < 0)) {
    FailOperation("fstat()");
    return absl::nullopt;
  }
  return IntCast<Position>(stat_info.st_size);
}

bool FdWriterBase::TruncateBehindBuffer(Position new_size) {
  RIEGELI_ASSERT_EQ(buffer_size(), 0u)
      << "Failed precondition of BufferedWriter::TruncateBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  const int dest = dest_fd();
  if (new_size >= start_pos()) {
    // Seeking forwards.
    struct stat stat_info;
    if (ABSL_PREDICT_FALSE(fstat(dest, &stat_info) < 0)) {
      return FailOperation("fstat()");
    }
    if (ABSL_PREDICT_FALSE(new_size > IntCast<Position>(stat_info.st_size))) {
      // File ends.
      SeekInternal(dest, IntCast<Position>(stat_info.st_size));
      return false;
    }
  }
again:
  if (ABSL_PREDICT_FALSE(ftruncate(dest, IntCast<off_t>(new_size)) < 0)) {
    if (errno == EINTR) goto again;
    return FailOperation("ftruncate()");
  }
  return SeekInternal(dest, new_size);
}

}  // namespace riegeli
