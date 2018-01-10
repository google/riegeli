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

// Make strerror_r(), pwrite(), and ftruncate() available.
#if !defined(_XOPEN_SOURCE) || _XOPEN_SOURCE < 600
#undef _XOPEN_SOURCE
#define _XOPEN_SOURCE 600
#endif

// Make file offsets 64-bit even on 32-bit systems.
#undef _FILE_OFFSET_BITS
#define _FILE_OFFSET_BITS 64

#include "riegeli/bytes/fd_writer.h"

#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <string>
#include <utility>

#include "riegeli/base/assert.h"
#include "riegeli/base/base.h"
#include "riegeli/base/string_view.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

namespace internal {

FdWriterBase::FdWriterBase() : fd_(-1), owns_fd_(false) { MarkCancelled(); }

FdWriterBase::FdWriterBase(int fd, bool owns_fd, size_t buffer_size)
    : BufferedWriter(buffer_size),
      fd_(fd),
      owns_fd_(owns_fd),
      filename_(fd == 1 ? "/dev/stdout"
                        : fd == 2 ? "/dev/stderr"
                                  : "/proc/self/fd/" + std::to_string(fd)) {
  RIEGELI_ASSERT_GE(fd, 0);
}

FdWriterBase::FdWriterBase(std::string filename, int flags, mode_t permissions,
                           size_t buffer_size)
    : BufferedWriter(buffer_size),
      owns_fd_(true),
      filename_(std::move(filename)) {
  RIEGELI_ASSERT((flags & O_ACCMODE) == O_WRONLY ||
                 (flags & O_ACCMODE) == O_RDWR);
again:
  fd_ = open(filename_.c_str(), flags, permissions);
  if (RIEGELI_UNLIKELY(fd_ < 0)) {
    const int error_code = errno;
    if (error_code == EINTR) goto again;
    FailOperation("open()", error_code);
    return;
  }
}

FdWriterBase::FdWriterBase(FdWriterBase&& src) noexcept
    : BufferedWriter(std::move(src)),
      fd_(riegeli::exchange(src.fd_, -1)),
      owns_fd_(riegeli::exchange(src.owns_fd_, false)),
      filename_(std::move(src.filename_)),
      error_code_(riegeli::exchange(src.error_code_, 0)) {
  src.filename_.clear();
}

void FdWriterBase::operator=(FdWriterBase&& src) noexcept {
  RIEGELI_ASSERT(&src != this);
  BufferedWriter::operator=(std::move(src));
  fd_ = riegeli::exchange(src.fd_, -1);
  owns_fd_ = riegeli::exchange(src.owns_fd_, false);
  filename_ = std::move(src.filename_);
  error_code_ = riegeli::exchange(src.error_code_, 0);
  src.filename_.clear();
}

void FdWriterBase::Done() {
  if (RIEGELI_LIKELY(PushInternal())) MaybeSyncPos();
  if (RIEGELI_UNLIKELY(!healthy()) && fd_ >= 0) {
  again:
    if (RIEGELI_UNLIKELY(ftruncate(fd_, 0) < 0) && errno == EINTR) goto again;
  }
  if (owns_fd_) {
// http://austingroupbugs.net/view.php?id=529 explains this mess.
#ifdef POSIX_CLOSE_RESTART
    // Avoid EINTR by using posix_close(_, 0) if available.
    if (RIEGELI_UNLIKELY(posix_close(fd_, 0) < 0)) {
      const int error_code = errno;
      if (error_code != EINPROGRESS && healthy()) {
        FailOperation("posix_close()", error_code);
      }
    }
#else
    if (RIEGELI_UNLIKELY(close(fd_) < 0)) {
      const int error_code = errno;
      // After EINTR it is unspecified whether fd has been closed or not.
      // Assume that it is closed, which is the case e.g. on Linux.
      if (error_code != EINPROGRESS && error_code != EINTR && healthy()) {
        FailOperation("close()", error_code);
      }
    }
#endif
  }
  fd_ = -1;
  owns_fd_ = false;
  // filename_ and error_code_ are not cleared.
  BufferedWriter::Done();
}

bool FdWriterBase::FailOperation(const char* operation, int error_code) {
  RIEGELI_ASSERT(healthy());
  error_code_ = error_code;
  char message[256];
  strerror_r(error_code, message, sizeof(message));
  message[sizeof(message) - 1] = '\0';
  return Fail(std::string(operation) + " failed: " + message + ", writing " +
              filename_);
}

bool FdWriterBase::Flush(FlushType flush_type) {
  if (RIEGELI_UNLIKELY(!PushInternal())) return false;
  if (RIEGELI_UNLIKELY(!MaybeSyncPos())) return false;
  switch (flush_type) {
    case FlushType::kFromObject:
    case FlushType::kFromProcess:
      return true;
    case FlushType::kFromMachine: {
      const int result = fsync(fd_);
      return result == 0;
    }
  }
  RIEGELI_ASSERT(false) << "Unknown flush type: "
                        << static_cast<int>(flush_type);
}

}  // namespace internal

FdWriter::FdWriter() : sync_pos_(false) {}

FdWriter::FdWriter(int fd, Options options)
    : FdWriterBase(fd, options.owns_fd_, options.buffer_size_),
      sync_pos_(options.sync_pos_) {
  InitializePos(O_WRONLY | O_APPEND);
}

FdWriter::FdWriter(std::string filename, int flags, Options options)
    : FdWriterBase(std::move(filename), flags, options.permissions_,
                   options.buffer_size_),
      sync_pos_(options.sync_pos_) {
  RIEGELI_ASSERT(options.owns_fd_);
  if (RIEGELI_LIKELY(healthy())) InitializePos(flags);
}

FdWriter::FdWriter(FdWriter&& src) noexcept
    : internal::FdWriterBase(std::move(src)),
      sync_pos_(riegeli::exchange(src.sync_pos_, false)) {}

FdWriter& FdWriter::operator=(FdWriter&& src) noexcept {
  if (&src != this) {
    internal::FdWriterBase::operator=(std::move(src));
    sync_pos_ = riegeli::exchange(src.sync_pos_, false);
  }
  return *this;
}

FdWriter::~FdWriter() { Cancel(); }

void FdWriter::Done() {
  internal::FdWriterBase::Done();
  sync_pos_ = false;
}

inline void FdWriter::InitializePos(int flags) {
  RIEGELI_ASSERT(healthy());
  RIEGELI_ASSERT_EQ(start_pos_, 0u);
  if (sync_pos_) {
    const off_t result = lseek(fd_, 0, SEEK_CUR);
    if (RIEGELI_UNLIKELY(result < 0)) {
      FailOperation("lseek()", errno);
      return;
    }
    start_pos_ = static_cast<Position>(result);
  } else if ((flags & O_APPEND) != 0) {
    struct stat stat_info;
    if (RIEGELI_UNLIKELY(fstat(fd_, &stat_info) < 0)) {
      const int error_code = errno;
      FailOperation("fstat()", error_code);
      return;
    }
    start_pos_ = static_cast<Position>(stat_info.st_size);
  }
}

bool FdWriter::MaybeSyncPos() {
  RIEGELI_ASSERT(healthy());
  if (sync_pos_) {
    if (RIEGELI_UNLIKELY(lseek(fd_, pos(), SEEK_SET) < 0)) {
      return FailOperation("lseek()", errno);
    }
  }
  return true;
}

bool FdWriter::WriteInternal(string_view src) {
  RIEGELI_ASSERT(!src.empty());
  RIEGELI_ASSERT(healthy());
  do {
  again:
    const ssize_t result = pwrite(fd_, src.data(), src.size(), start_pos_);
    if (RIEGELI_UNLIKELY(result < 0)) {
      const int error_code = errno;
      if (error_code == EINTR) goto again;
      return FailOperation("pwrite()", error_code);
    }
    RIEGELI_ASSERT_GT(result, 0);
    start_pos_ += result;
    src.remove_prefix(result);
  } while (!src.empty());
  return true;
}

bool FdWriter::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos_ || new_pos > pos());
  if (RIEGELI_UNLIKELY(!PushInternal())) return false;
  if (new_pos >= start_pos_) {
    struct stat stat_info;
    if (RIEGELI_UNLIKELY(fstat(fd_, &stat_info) < 0)) {
      const int error_code = errno;
      return FailOperation("fstat()", error_code);
    }
    RIEGELI_ASSERT_GE(stat_info.st_size, 0);
    if (RIEGELI_UNLIKELY(new_pos > static_cast<Position>(stat_info.st_size))) {
      // File ends.
      start_pos_ = static_cast<Position>(stat_info.st_size);
      return false;
    }
  }
  start_pos_ = new_pos;
  return true;
}

bool FdWriter::Size(Position* size) const {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  struct stat stat_info;
  const int result = fstat(fd_, &stat_info);
  if (RIEGELI_UNLIKELY(result < 0)) return false;
  *size = UnsignedMax(static_cast<Position>(stat_info.st_size), pos());
  return true;
}

bool FdWriter::Truncate() {
  // ftruncate() extends the file if needed so there is no need to
  // PushInternal().
  if (RIEGELI_UNLIKELY(!healthy())) return false;
again:
  if (RIEGELI_UNLIKELY(ftruncate(fd_, pos()) < 0)) {
    const int error_code = errno;
    if (error_code == EINTR) goto again;
    return FailOperation("ftruncate()", error_code);
  }
  return true;
}

FdStreamWriter::FdStreamWriter() = default;

FdStreamWriter::FdStreamWriter(int fd, Options options)
    : FdWriterBase(fd, options.owns_fd_, options.buffer_size_) {
  RIEGELI_ASSERT(options.has_assumed_pos_);
  start_pos_ = options.assumed_pos_;
}

FdStreamWriter::FdStreamWriter(std::string filename, int flags, Options options)
    : FdWriterBase(std::move(filename), flags, options.permissions_,
                   options.buffer_size_) {
  RIEGELI_ASSERT(options.owns_fd_);
  if (RIEGELI_UNLIKELY(!healthy())) return;
  if (options.has_assumed_pos_) {
    start_pos_ = options.assumed_pos_;
  } else if ((flags & O_APPEND) != 0) {
    struct stat stat_info;
    if (RIEGELI_UNLIKELY(fstat(fd_, &stat_info) < 0)) {
      const int error_code = errno;
      FailOperation("fstat()", error_code);
      return;
    }
    start_pos_ = static_cast<Position>(stat_info.st_size);
  }
}

FdStreamWriter::FdStreamWriter(FdStreamWriter&& src) noexcept
    : internal::FdWriterBase(std::move(src)) {}

FdStreamWriter& FdStreamWriter::operator=(FdStreamWriter&& src) noexcept {
  if (&src != this) internal::FdWriterBase::operator=(std::move(src));
  return *this;
}

FdStreamWriter::~FdStreamWriter() { Cancel(); }

bool FdStreamWriter::WriteInternal(string_view src) {
  RIEGELI_ASSERT(!src.empty());
  RIEGELI_ASSERT(healthy());
  do {
  again:
    const ssize_t result = write(fd_, src.data(), src.size());
    if (RIEGELI_UNLIKELY(result < 0)) {
      const int error_code = errno;
      if (error_code == EINTR) goto again;
      return FailOperation("write()", error_code);
    }
    RIEGELI_ASSERT_GT(result, 0);
    start_pos_ += result;
    src.remove_prefix(result);
  } while (!src.empty());
  return true;
}

}  // namespace riegeli
