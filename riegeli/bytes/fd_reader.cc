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

// Make strerror_r() and pread() available.
#if !defined(_XOPEN_SOURCE) || _XOPEN_SOURCE < 600
#undef _XOPEN_SOURCE
#define _XOPEN_SOURCE 600
#endif

// Make file offsets 64-bit even on 32-bit systems.
#undef _FILE_OFFSET_BITS
#define _FILE_OFFSET_BITS 64

#include "riegeli/bytes/fd_reader.h"

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
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

namespace internal {

FdReaderBase::FdReaderBase() : fd_(-1), owns_fd_(false) { MarkCancelled(); }

FdReaderBase::FdReaderBase(int fd, bool owns_fd, size_t buffer_size)
    : BufferedReader(buffer_size),
      fd_(fd),
      owns_fd_(owns_fd),
      filename_(fd == 0 ? "/dev/stdin"
                        : "/proc/self/fd/" + std::to_string(fd)) {
  RIEGELI_ASSERT_GE(fd, 0);
}

FdReaderBase::FdReaderBase(std::string filename, int flags, size_t buffer_size)
    : BufferedReader(buffer_size),
      owns_fd_(true),
      filename_(std::move(filename)) {
  RIEGELI_ASSERT((flags & O_ACCMODE) == O_RDONLY ||
                 (flags & O_ACCMODE) == O_RDWR);
again:
  fd_ = open(filename_.c_str(), flags, 0666);
  if (RIEGELI_UNLIKELY(fd_ < 0)) {
    const int error_code = errno;
    if (error_code == EINTR) goto again;
    owns_fd_ = false;
    FailOperation("open()", error_code);
    return;
  }
}

FdReaderBase::FdReaderBase(FdReaderBase&& src) noexcept
    : BufferedReader(std::move(src)),
      fd_(riegeli::exchange(src.fd_, -1)),
      owns_fd_(riegeli::exchange(src.owns_fd_, false)),
      filename_(std::move(src.filename_)),
      error_code_(riegeli::exchange(src.error_code_, 0)) {
  src.filename_.clear();
}

void FdReaderBase::operator=(FdReaderBase&& src) noexcept {
  RIEGELI_ASSERT(&src != this);
  BufferedReader::operator=(std::move(src));
  fd_ = riegeli::exchange(src.fd_, -1);
  owns_fd_ = riegeli::exchange(src.owns_fd_, false);
  filename_ = std::move(src.filename_);
  error_code_ = riegeli::exchange(src.error_code_, 0);
  src.filename_.clear();
}

void FdReaderBase::Done() {
  if (RIEGELI_LIKELY(healthy())) MaybeSyncPos();
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
  BufferedReader::Done();
}

bool FdReaderBase::FailOperation(const char* operation, int error_code) {
  RIEGELI_ASSERT(healthy());
  error_code_ = error_code;
  char message[256];
  strerror_r(error_code, message, sizeof(message));
  message[sizeof(message) - 1] = '\0';
  return Fail(std::string(operation) + " failed: " + message + ", reading " +
              filename_);
}

}  // namespace internal

FdReader::FdReader() : sync_pos_(false) {}

FdReader::FdReader(int fd, Options options)
    : FdReaderBase(fd, options.owns_fd_, options.buffer_size_),
      sync_pos_(options.sync_pos_) {
  InitializePos();
}

FdReader::FdReader(std::string filename, int flags, Options options)
    : FdReaderBase(std::move(filename), flags, options.buffer_size_),
      sync_pos_(options.sync_pos_) {
  RIEGELI_ASSERT(options.owns_fd_);
  if (RIEGELI_LIKELY(healthy())) InitializePos();
}

FdReader::FdReader(FdReader&& src) noexcept
    : internal::FdReaderBase(std::move(src)),
      sync_pos_(riegeli::exchange(src.sync_pos_, false)) {}

FdReader& FdReader::operator=(FdReader&& src) noexcept {
  if (&src != this) {
    internal::FdReaderBase::operator=(std::move(src));
    sync_pos_ = riegeli::exchange(src.sync_pos_, false);
  }
  return *this;
}

FdReader::~FdReader() { Cancel(); }

void FdReader::Done() {
  internal::FdReaderBase::Done();
  sync_pos_ = false;
}

inline void FdReader::InitializePos() {
  RIEGELI_ASSERT(healthy());
  RIEGELI_ASSERT_EQ(limit_pos_, 0u);
  if (sync_pos_) {
    const off_t result = lseek(fd_, 0, SEEK_CUR);
    if (RIEGELI_UNLIKELY(result < 0)) {
      FailOperation("lseek()", errno);
      return;
    }
    limit_pos_ = static_cast<Position>(result);
  }
}

bool FdReader::MaybeSyncPos() {
  RIEGELI_ASSERT(healthy());
  if (sync_pos_) {
    if (RIEGELI_UNLIKELY(lseek(fd_, pos(), SEEK_SET) < 0)) {
      return FailOperation("lseek()", errno);
    }
  }
  return true;
}

bool FdReader::ReadInternal(char* dest, size_t min_length, size_t max_length) {
  RIEGELI_ASSERT_GT(min_length, 0u);
  RIEGELI_ASSERT_GE(max_length, min_length);
  RIEGELI_ASSERT(healthy());
  for (;;) {
  again:
    const ssize_t result = pread(fd_, dest, max_length, limit_pos_);
    if (RIEGELI_UNLIKELY(result < 0)) {
      const int error_code = errno;
      if (error_code == EINTR) goto again;
      return FailOperation("pread()", error_code);
    }
    if (RIEGELI_UNLIKELY(result == 0)) return false;
    limit_pos_ += result;
    if (static_cast<size_t>(result) >= min_length) return true;
    dest += result;
    min_length -= result;
    max_length -= result;
  }
}

bool FdReader::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos_);
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  if (new_pos > limit_pos_) {
    struct stat stat_info;
    if (RIEGELI_UNLIKELY(fstat(fd_, &stat_info) < 0)) {
      const int error_code = errno;
      return FailOperation("fstat()", error_code);
    }
    RIEGELI_ASSERT_GE(stat_info.st_size, 0);
    if (RIEGELI_UNLIKELY(new_pos > static_cast<Position>(stat_info.st_size))) {
      // File ends.
      ClearBuffer();
      limit_pos_ = static_cast<Position>(stat_info.st_size);
      return false;
    }
  }
  ClearBuffer();
  limit_pos_ = new_pos;
  PullSlow();
  return true;
}

bool FdReader::Size(Position* size) const {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  struct stat stat_info;
  const int result = fstat(fd_, &stat_info);
  if (RIEGELI_UNLIKELY(result < 0)) return false;
  *size = static_cast<Position>(stat_info.st_size);
  return true;
}

FdStreamReader::FdStreamReader() = default;

FdStreamReader::FdStreamReader(int fd, Options options)
    : FdReaderBase(fd, true, options.buffer_size_) {
  RIEGELI_ASSERT(options.has_assumed_pos_);
  limit_pos_ = options.assumed_pos_;
}

FdStreamReader::FdStreamReader(std::string filename, int flags, Options options)
    : FdReaderBase(std::move(filename), flags, options.buffer_size_) {
  if (RIEGELI_UNLIKELY(!healthy())) return;
  limit_pos_ = options.assumed_pos_;
}

FdStreamReader::FdStreamReader(FdStreamReader&& src) noexcept
    : internal::FdReaderBase(std::move(src)) {}

FdStreamReader& FdStreamReader::operator=(FdStreamReader&& src) noexcept {
  if (&src != this) internal::FdReaderBase::operator=(std::move(src));
  return *this;
}

FdStreamReader::~FdStreamReader() { Cancel(); }

bool FdStreamReader::ReadInternal(char* dest, size_t min_length,
                                  size_t max_length) {
  RIEGELI_ASSERT_GT(min_length, 0u);
  RIEGELI_ASSERT_GE(max_length, min_length);
  RIEGELI_ASSERT(healthy());
  for (;;) {
  again:
    const ssize_t result = read(fd_, dest, max_length);
    if (RIEGELI_UNLIKELY(result < 0)) {
      const int error_code = errno;
      if (error_code == EINTR) goto again;
      return FailOperation("read()", error_code);
    }
    if (RIEGELI_UNLIKELY(result == 0)) return false;
    limit_pos_ += result;
    if (static_cast<size_t>(result) >= min_length) return true;
    dest += result;
    min_length -= result;
    max_length -= result;
  }
}

}  // namespace riegeli
