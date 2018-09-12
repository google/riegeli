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

// Make pread() available.
#if !defined(_XOPEN_SOURCE) || _XOPEN_SOURCE < 500
#undef _XOPEN_SOURCE
#define _XOPEN_SOURCE 500
#endif

// Make file offsets 64-bit even on 32-bit systems.
#undef _FILE_OFFSET_BITS
#define _FILE_OFFSET_BITS 64

#include "riegeli/bytes/fd_reader.h"

#include <fcntl.h>
#include <stddef.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <cerrno>
#include <limits>
#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/memory_estimator.h"
#include "riegeli/base/object.h"
#include "riegeli/base/str_error.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/fd_dependency.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

namespace {

class MMapRef {
 public:
  MMapRef(void* data, size_t size) : data_(data), size_(size) {}

  MMapRef(MMapRef&& src) noexcept;
  MMapRef& operator=(MMapRef&& src) noexcept;

  ~MMapRef();

  absl::string_view data() const {
    return absl::string_view(static_cast<const char*>(data_), size_);
  }
  void RegisterSubobjects(absl::string_view data,
                          MemoryEstimator* memory_estimator) const;
  void DumpStructure(absl::string_view data, std::ostream& out) const;

 private:
  void* data_;
  size_t size_;
};

MMapRef::MMapRef(MMapRef&& src) noexcept
    : data_(riegeli::exchange(src.data_, nullptr)),
      size_(riegeli::exchange(src.size_, 0)) {}

MMapRef& MMapRef::operator=(MMapRef&& src) noexcept {
  // Exchange data_ early to support self-assignment.
  void* const data = riegeli::exchange(src.data_, nullptr);
  if (data_ != nullptr) {
    const int result = munmap(data_, size_);
    RIEGELI_CHECK_EQ(result, 0) << "munmap() failed: " << StrError(errno);
  }
  data_ = data;
  size_ = riegeli::exchange(src.size_, 0);
  return *this;
}

MMapRef::~MMapRef() {
  if (data_ != nullptr) {
    const int result = munmap(data_, size_);
    RIEGELI_CHECK_EQ(result, 0) << "munmap() failed: " << StrError(errno);
  }
}

void MMapRef::RegisterSubobjects(absl::string_view data,
                                 MemoryEstimator* memory_estimator) const {}

void MMapRef::DumpStructure(absl::string_view data, std::ostream& out) const {
  out << "mmap";
}

}  // namespace

namespace internal {

FdReaderCommon::FdReaderCommon(size_t buffer_size)
    : BufferedReader(UnsignedMin(
          buffer_size, Position{std::numeric_limits<off_t>::max()})) {}

void FdReaderCommon::SetFilename(int src) {
  if (src == 0) {
    filename_ = "/dev/stdin";
  } else {
    filename_ = absl::StrCat("/proc/self/fd/", src);
  }
}

int FdReaderCommon::OpenFd(absl::string_view filename, int flags) {
  filename_.assign(filename.data(), filename.size());
again:
  const int src = open(filename_.c_str(), flags, 0666);
  if (ABSL_PREDICT_FALSE(src < 0)) {
    const int error_code = errno;
    if (error_code == EINTR) goto again;
    FailOperation("open()", error_code);
    return -1;
  }
  return src;
}

bool FdReaderCommon::FailOperation(absl::string_view operation,
                                   int error_code) {
  error_code_ = error_code;
  return Fail(absl::StrCat(operation, " failed: ", StrError(error_code),
                           ", reading ", filename_));
}

}  // namespace internal

void FdReaderBase::Initialize(int src) {
  if (sync_pos_) {
    const off_t result = lseek(src, 0, SEEK_CUR);
    if (ABSL_PREDICT_FALSE(result < 0)) {
      FailOperation("lseek()", errno);
      return;
    }
    limit_pos_ = IntCast<Position>(result);
  }
}

void FdReaderBase::SyncPos(int src) {
  if (sync_pos_) {
    if (ABSL_PREDICT_FALSE(lseek(src, IntCast<off_t>(pos()), SEEK_SET) < 0)) {
      FailOperation("lseek()", errno);
    }
  }
}

bool FdReaderBase::ReadInternal(char* dest, size_t min_length,
                                size_t max_length) {
  RIEGELI_ASSERT_GT(min_length, 0u)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "nothing to read";
  RIEGELI_ASSERT_GE(max_length, min_length)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "max_length < min_length";
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of BufferedReader::ReadInternal(): " << message();
  const int src = src_fd();
  if (ABSL_PREDICT_FALSE(max_length >
                         Position{std::numeric_limits<off_t>::max()} -
                             limit_pos_)) {
    return FailOverflow();
  }
  for (;;) {
  again:
    const ssize_t result = pread(
        src, dest,
        UnsignedMin(max_length, size_t{std::numeric_limits<ssize_t>::max()}),
        IntCast<off_t>(limit_pos_));
    if (ABSL_PREDICT_FALSE(result < 0)) {
      const int error_code = errno;
      if (error_code == EINTR) goto again;
      return FailOperation("pread()", error_code);
    }
    if (ABSL_PREDICT_FALSE(result == 0)) return false;
    RIEGELI_ASSERT_LE(IntCast<size_t>(result), max_length)
        << "pread() read more than requested";
    limit_pos_ += IntCast<size_t>(result);
    if (IntCast<size_t>(result) >= min_length) return true;
    dest += result;
    min_length -= IntCast<size_t>(result);
    max_length -= IntCast<size_t>(result);
  }
}

bool FdReaderBase::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos_)
      << "Failed precondition of Reader::SeekSlow(): "
         "position in the buffer, use Seek() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (new_pos > limit_pos_) {
    // Seeking forwards.
    const int src = src_fd();
    struct stat stat_info;
    if (ABSL_PREDICT_FALSE(fstat(src, &stat_info) < 0)) {
      const int error_code = errno;
      return FailOperation("fstat()", error_code);
    }
    if (ABSL_PREDICT_FALSE(new_pos > IntCast<Position>(stat_info.st_size))) {
      // File ends.
      ClearBuffer();
      limit_pos_ = IntCast<Position>(stat_info.st_size);
      return false;
    }
  }
  ClearBuffer();
  limit_pos_ = new_pos;
  PullSlow();
  return true;
}

bool FdReaderBase::Size(Position* size) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  const int src = src_fd();
  struct stat stat_info;
  if (ABSL_PREDICT_FALSE(fstat(src, &stat_info) < 0)) {
    const int error_code = errno;
    return FailOperation("fstat()", error_code);
  }
  *size = IntCast<Position>(stat_info.st_size);
  return true;
}

bool FdStreamReaderBase::ReadInternal(char* dest, size_t min_length,
                                      size_t max_length) {
  RIEGELI_ASSERT_GT(min_length, 0u)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "nothing to read";
  RIEGELI_ASSERT_GE(max_length, min_length)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "max_length < min_length";
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of BufferedReader::ReadInternal(): " << message();
  const int src = src_fd();
  if (ABSL_PREDICT_FALSE(max_length >
                         Position{std::numeric_limits<off_t>::max()} -
                             limit_pos_)) {
    return FailOverflow();
  }
  for (;;) {
  again:
    const ssize_t result = read(
        src, dest,
        UnsignedMin(max_length, size_t{std::numeric_limits<ssize_t>::max()}));
    if (ABSL_PREDICT_FALSE(result < 0)) {
      const int error_code = errno;
      if (error_code == EINTR) goto again;
      return FailOperation("read()", error_code);
    }
    if (ABSL_PREDICT_FALSE(result == 0)) return false;
    RIEGELI_ASSERT_LE(IntCast<size_t>(result), max_length)
        << "read() read more than requested";
    limit_pos_ += IntCast<size_t>(result);
    if (IntCast<size_t>(result) >= min_length) return true;
    dest += result;
    min_length -= IntCast<size_t>(result);
    max_length -= IntCast<size_t>(result);
  }
}

void FdMMapReaderBase::SetFilename(int src) {
  if (src == 0) {
    filename_ = "/dev/stdin";
  } else {
    filename_ = absl::StrCat("/proc/self/fd/", src);
  }
}

int FdMMapReaderBase::OpenFd(absl::string_view filename, int flags) {
  filename_.assign(filename.data(), filename.size());
again:
  const int src = open(filename_.c_str(), flags, 0666);
  if (ABSL_PREDICT_FALSE(src < 0)) {
    const int error_code = errno;
    if (error_code == EINTR) goto again;
    FailOperation("open()", error_code);
    return -1;
  }
  return src;
}

bool FdMMapReaderBase::FailOperation(absl::string_view operation,
                                     int error_code) {
  error_code_ = error_code;
  return Fail(absl::StrCat(operation, " failed: ", StrError(error_code),
                           ", reading ", filename_));
}

void FdMMapReaderBase::Initialize(int src) {
  struct stat stat_info;
  if (ABSL_PREDICT_FALSE(fstat(src, &stat_info) < 0)) {
    const int error_code = errno;
    FailOperation("fstat()", error_code);
    return;
  }
  if (ABSL_PREDICT_FALSE(IntCast<Position>(stat_info.st_size) >
                         std::numeric_limits<size_t>::max())) {
    Fail("File is too large for mmap()");
    return;
  }
  if (stat_info.st_size != 0) {
    void* const data = mmap(nullptr, IntCast<size_t>(stat_info.st_size),
                            PROT_READ, MAP_SHARED, src, 0);
    if (ABSL_PREDICT_FALSE(data == MAP_FAILED)) {
      const int error_code = errno;
      FailOperation("mmap()", error_code);
      return;
    }
    Chain contents;
    contents.AppendExternal(MMapRef(data, IntCast<size_t>(stat_info.st_size)));
    ChainReader::operator=(ChainReader(std::move(contents)));
    if (sync_pos_) {
      const off_t result = lseek(src, 0, SEEK_CUR);
      if (ABSL_PREDICT_FALSE(result < 0)) {
        FailOperation("lseek()", errno);
        return;
      }
      cursor_ += UnsignedMin(IntCast<Position>(result), available());
    }
  }
}

void FdMMapReaderBase::SyncPos(int src) {
  if (sync_pos_) {
    if (ABSL_PREDICT_FALSE(lseek(src, IntCast<off_t>(pos()), SEEK_SET) < 0)) {
      FailOperation("lseek()", errno);
    }
  }
}

template class FdReader<OwnedFd>;
template class FdReader<int>;
template class FdStreamReader<OwnedFd>;
template class FdMMapReader<OwnedFd>;
template class FdMMapReader<int>;

}  // namespace riegeli
