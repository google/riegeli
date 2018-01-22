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
#include "riegeli/bytes/fd_holder.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

namespace internal {

FdWriterBase::FdWriterBase() : fd_(-1) { MarkClosed(); }

FdWriterBase::FdWriterBase(int fd, bool owns_fd, size_t buffer_size)
    : BufferedWriter(buffer_size),
      owned_fd_(owns_fd ? fd : -1),
      fd_(fd),
      filename_(fd == 1 ? "/dev/stdout"
                        : fd == 2 ? "/dev/stderr"
                                  : "/proc/self/fd/" + std::to_string(fd)) {
  RIEGELI_ASSERT_GE(fd, 0);
}

FdWriterBase::FdWriterBase(std::string filename, int flags, mode_t permissions,
                           size_t buffer_size)
    : BufferedWriter(buffer_size), filename_(std::move(filename)) {
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
  owned_fd_ = FdHolder(fd_);
}

FdWriterBase::FdWriterBase(FdWriterBase&& src) noexcept
    : BufferedWriter(std::move(src)),
      owned_fd_(std::move(src.owned_fd_)),
      fd_(riegeli::exchange(src.fd_, -1)),
      filename_(riegeli::exchange(src.filename_, std::string())),
      error_code_(riegeli::exchange(src.error_code_, 0)) {}

FdWriterBase& FdWriterBase::operator=(FdWriterBase&& src) noexcept {
  BufferedWriter::operator=(std::move(src));
  owned_fd_ = std::move(src.owned_fd_);
  fd_ = riegeli::exchange(src.fd_, -1);
  filename_ = riegeli::exchange(src.filename_, std::string());
  error_code_ = riegeli::exchange(src.error_code_, 0);
  return *this;
}

FdWriterBase::~FdWriterBase() = default;

void FdWriterBase::Done() {
  if (RIEGELI_LIKELY(PushInternal())) MaybeSyncPos();
  const int error_code = owned_fd_.Close();
  if (RIEGELI_UNLIKELY(error_code != 0) && RIEGELI_LIKELY(healthy())) {
    FailOperation(FdHolder::CloseFunctionName(), error_code);
  }
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
  internal::FdWriterBase::operator=(std::move(src));
  sync_pos_ = riegeli::exchange(src.sync_pos_, false);
  return *this;
}

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
  RIEGELI_ASSERT(cursor_ == start_);
  if (sync_pos_) {
    if (RIEGELI_UNLIKELY(lseek(fd_, pos(), SEEK_SET) < 0)) {
      limit_ = start_;
      return FailOperation("lseek()", errno);
    }
  }
  return true;
}

bool FdWriter::WriteInternal(string_view src) {
  RIEGELI_ASSERT(!src.empty());
  RIEGELI_ASSERT(healthy());
  RIEGELI_ASSERT(cursor_ == start_);
  do {
  again:
    const ssize_t result = pwrite(fd_, src.data(), src.size(), start_pos_);
    if (RIEGELI_UNLIKELY(result < 0)) {
      const int error_code = errno;
      if (error_code == EINTR) goto again;
      limit_ = start_;
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
  RIEGELI_ASSERT(cursor_ == start_);
  if (new_pos >= start_pos_) {
    struct stat stat_info;
    if (RIEGELI_UNLIKELY(fstat(fd_, &stat_info) < 0)) {
      const int error_code = errno;
      limit_ = start_;
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
  if (RIEGELI_UNLIKELY(!PushInternal())) return false;
  RIEGELI_ASSERT(cursor_ == start_);
again:
  if (RIEGELI_UNLIKELY(ftruncate(fd_, start_pos_) < 0)) {
    const int error_code = errno;
    if (error_code == EINTR) goto again;
    limit_ = start_;
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
  internal::FdWriterBase::operator=(std::move(src));
  return *this;
}

bool FdStreamWriter::WriteInternal(string_view src) {
  RIEGELI_ASSERT(!src.empty());
  RIEGELI_ASSERT(healthy());
  RIEGELI_ASSERT(cursor_ == start_);
  do {
  again:
    const ssize_t result = write(fd_, src.data(), src.size());
    if (RIEGELI_UNLIKELY(result < 0)) {
      const int error_code = errno;
      if (error_code == EINTR) goto again;
      limit_ = start_;
      return FailOperation("write()", error_code);
    }
    RIEGELI_ASSERT_GT(result, 0);
    start_pos_ += result;
    src.remove_prefix(result);
  } while (!src.empty());
  return true;
}

}  // namespace riegeli
