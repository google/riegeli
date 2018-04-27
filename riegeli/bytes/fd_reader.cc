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
#include "riegeli/bytes/fd_holder.h"
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
  void AddUniqueTo(absl::string_view data,
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

void MMapRef::AddUniqueTo(absl::string_view data,
                          MemoryEstimator* memory_estimator) const {
  memory_estimator->AddMemory(sizeof(*this));
}

void MMapRef::DumpStructure(absl::string_view data, std::ostream& out) const {
  out << "mmap";
}

}  // namespace

namespace internal {

inline FdReaderBase::FdReaderBase(int fd, bool owns_fd, size_t buffer_size)
    : BufferedReader(UnsignedMin(buffer_size,
                                 Position{std::numeric_limits<off_t>::max()})),
      owned_fd_(owns_fd ? fd : -1),
      fd_(fd),
      filename_(fd == 0 ? "/dev/stdin" : absl::StrCat("/proc/self/fd/", fd)) {
  RIEGELI_ASSERT_GE(fd, 0)
      << "Failed precondition of FdReaderBase::FdReaderBase(int): "
         "negative file descriptor";
}

inline FdReaderBase::FdReaderBase(std::string filename, int flags,
                                  size_t buffer_size)
    : BufferedReader(UnsignedMin(buffer_size,
                                 Position{std::numeric_limits<off_t>::max()})),
      filename_(std::move(filename)) {
  RIEGELI_ASSERT((flags & O_ACCMODE) == O_RDONLY ||
                 (flags & O_ACCMODE) == O_RDWR)
      << "Failed precondition of FdReaderBase::FdReaderBase(string): "
         "flags must include O_RDONLY or O_RDWR";
again:
  fd_ = open(filename_.c_str(), flags, 0666);
  if (ABSL_PREDICT_FALSE(fd_ < 0)) {
    const int error_code = errno;
    if (error_code == EINTR) goto again;
    FailOperation("open()", error_code);
    return;
  }
  owned_fd_ = FdHolder(fd_);
}

void FdReaderBase::Done() {
  if (ABSL_PREDICT_TRUE(healthy())) MaybeSyncPos();
  const int error_code = owned_fd_.Close();
  if (ABSL_PREDICT_FALSE(error_code != 0) && ABSL_PREDICT_TRUE(healthy())) {
    FailOperation(FdHolder::CloseFunctionName(), error_code);
  }
  // filename_ and error_code_ are not cleared.
  BufferedReader::Done();
}

inline bool FdReaderBase::FailOperation(absl::string_view operation,
                                        int error_code) {
  error_code_ = error_code;
  return Fail(absl::StrCat(operation, " failed: ", StrError(error_code),
                           ", reading ", filename_));
}

}  // namespace internal

FdReader::FdReader(int fd, Options options)
    : FdReaderBase(fd, options.owns_fd_, options.buffer_size_),
      sync_pos_(options.sync_pos_) {
  InitializePos();
}

FdReader::FdReader(std::string filename, int flags, Options options)
    : FdReaderBase(std::move(filename), flags, options.buffer_size_),
      sync_pos_(options.sync_pos_) {
  RIEGELI_ASSERT(options.owns_fd_)
      << "Failed precondition of FdReader::FdReader(string): "
         "file must be owned if FdReader opens it";
  if (ABSL_PREDICT_TRUE(healthy())) InitializePos();
}

void FdReader::Done() {
  internal::FdReaderBase::Done();
  sync_pos_ = false;
}

inline void FdReader::InitializePos() {
  if (sync_pos_) {
    const off_t result = lseek(fd_, 0, SEEK_CUR);
    if (ABSL_PREDICT_FALSE(result < 0)) {
      FailOperation("lseek()", errno);
      return;
    }
    limit_pos_ = IntCast<Position>(result);
  }
}

bool FdReader::MaybeSyncPos() {
  if (sync_pos_) {
    if (ABSL_PREDICT_FALSE(lseek(fd_, IntCast<off_t>(pos()), SEEK_SET) < 0)) {
      return FailOperation("lseek()", errno);
    }
  }
  return true;
}

bool FdReader::ReadInternal(char* dest, size_t min_length, size_t max_length) {
  RIEGELI_ASSERT_GT(min_length, 0u)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "nothing to read";
  RIEGELI_ASSERT_GE(max_length, min_length)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "max_length < min_length";
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of BufferedReader::ReadInternal(): " << message();
  if (ABSL_PREDICT_FALSE(max_length >
                         Position{std::numeric_limits<off_t>::max()} -
                             limit_pos_)) {
    return FailOverflow();
  }
  for (;;) {
  again:
    const ssize_t result = pread(
        fd_, dest,
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

bool FdReader::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos_)
      << "Failed precondition of Reader::SeekSlow(): "
         "position in the buffer, use Seek() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (new_pos > limit_pos_) {
    // Seeking forwards.
    struct stat stat_info;
    if (ABSL_PREDICT_FALSE(fstat(fd_, &stat_info) < 0)) {
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

bool FdReader::Size(Position* size) const {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  struct stat stat_info;
  const int result = fstat(fd_, &stat_info);
  if (ABSL_PREDICT_FALSE(result < 0)) return false;
  *size = IntCast<Position>(stat_info.st_size);
  return true;
}

FdStreamReader::FdStreamReader(int fd, Options options)
    : FdReaderBase(fd, true, options.buffer_size_) {
  RIEGELI_ASSERT(options.has_assumed_pos_)
      << "Failed precondition of FdStreamReader::FdStreamReader(int): "
         "assumed file position must be specified "
         "if FdStreamReader does not open the file";
  limit_pos_ = options.assumed_pos_;
}

FdStreamReader::FdStreamReader(std::string filename, int flags, Options options)
    : FdReaderBase(std::move(filename), flags, options.buffer_size_) {
  if (ABSL_PREDICT_FALSE(!healthy())) return;
  limit_pos_ = options.assumed_pos_;
}

bool FdStreamReader::ReadInternal(char* dest, size_t min_length,
                                  size_t max_length) {
  RIEGELI_ASSERT_GT(min_length, 0u)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "nothing to read";
  RIEGELI_ASSERT_GE(max_length, min_length)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "max_length < min_length";
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of BufferedReader::ReadInternal(): " << message();
  if (ABSL_PREDICT_FALSE(max_length >
                         Position{std::numeric_limits<off_t>::max()} -
                             limit_pos_)) {
    return FailOverflow();
  }
  for (;;) {
  again:
    const ssize_t result = read(
        fd_, dest,
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

FdMMapReader::FdMMapReader(int fd, Options options)
    : Reader(State::kOpen),
      filename_(fd == 0 ? "/dev/stdin" : absl::StrCat("/proc/self/fd/", fd)) {
  RIEGELI_ASSERT_GE(fd, 0)
      << "Failed precondition of FdMMapReader::FdMMapReader(int): "
         "negative file descriptor";
  Initialize(fd, options);
}

FdMMapReader::FdMMapReader(std::string filename, int flags, Options options)
    : Reader(State::kOpen), filename_(std::move(filename)) {
  RIEGELI_ASSERT((flags & O_ACCMODE) == O_RDONLY ||
                 (flags & O_ACCMODE) == O_RDWR)
      << "Failed precondition of FdMMapReader::FdMMapReader(string): "
         "flags must include O_RDONLY or O_RDWR";
  RIEGELI_ASSERT(options.owns_fd_)
      << "Failed precondition of FdMMapReader::FdMMapReader(string): "
         "file must be owned if FdMMapReader opens it";
again:
  const int fd = open(filename_.c_str(), flags, 0666);
  if (ABSL_PREDICT_FALSE(fd < 0)) {
    const int error_code = errno;
    if (error_code == EINTR) goto again;
    FailOperation("open()", error_code);
    return;
  }
  Initialize(fd, options);
}

void FdMMapReader::Done() {
  contents_ = Chain();
  // filename_ and error_code_ are not cleared.
  Reader::Done();
}

inline void FdMMapReader::Initialize(int fd, Options options) {
  internal::FdHolder owned_fd(options.owns_fd_ ? fd : -1);
  struct stat stat_info;
  if (ABSL_PREDICT_FALSE(fstat(fd, &stat_info) < 0)) {
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
                            PROT_READ, MAP_SHARED, fd, 0);
    if (ABSL_PREDICT_FALSE(data == MAP_FAILED)) {
      const int error_code = errno;
      FailOperation("mmap()", error_code);
      return;
    }
    contents_.AppendExternal(MMapRef(data, IntCast<size_t>(stat_info.st_size)));
    start_ = iter()->data();
    cursor_ = start_;
    limit_ = start_ + iter()->size();
    limit_pos_ = buffer_size();
  }
  const int error_code = owned_fd.Close();
  if (ABSL_PREDICT_FALSE(error_code != 0)) {
    FailOperation(internal::FdHolder::CloseFunctionName(), error_code);
  }
}

inline bool FdMMapReader::FailOperation(absl::string_view operation,
                                        int error_code) {
  error_code_ = error_code;
  return Fail(absl::StrCat(operation, " failed: ", StrError(error_code),
                           ", reading ", filename_));
}

inline Chain::BlockIterator FdMMapReader::iter() const {
  RIEGELI_ASSERT_EQ(contents_.blocks().size(), 1u)
      << "Failed precondition of FdMMapReader::iter(): single block expected";
  return contents_.blocks().begin();
}

bool FdMMapReader::PullSlow() {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of Reader::PullSlow(): "
         "data available, use Pull() instead";
  return false;
}

bool FdMMapReader::ReadSlow(Chain* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy()))
      << "Failed precondition of Reader::ReadSlow(Chain*): "
         "length too small, use Read(Chain*) instead";
  const size_t length_to_read = UnsignedMin(length, available());
  if (ABSL_PREDICT_TRUE(length_to_read > 0)) {  // iter() is undefined if
                                                // contents_.blocks().size()
                                                //     != 1.
    iter().AppendSubstrTo(absl::string_view(cursor_, length_to_read), dest);
    cursor_ += length_to_read;
  }
  return length_to_read == length;
}

bool FdMMapReader::CopyToSlow(Writer* dest, Position length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy()))
      << "Failed precondition of Reader::CopyToSlow(Writer*): "
         "length too small, use CopyTo(Writer*) instead";
  const size_t length_to_copy = UnsignedMin(length, available());
  bool ok = true;
  if (length_to_copy == contents_.size()) {
    cursor_ = limit_;
    ok = dest->Write(contents_);
  } else if (ABSL_PREDICT_TRUE(length_to_copy > 0)) {  // iter() is undefined if
                                                       // contents_.blocks()
                                                       //     .size() != 1.
    Chain data;
    iter().AppendSubstrTo(absl::string_view(cursor_, length_to_copy), &data,
                          length_to_copy);
    cursor_ += length_to_copy;
    ok = dest->Write(std::move(data));
  }
  return ok && length_to_copy == length;
}

bool FdMMapReader::CopyToSlow(BackwardWriter* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy()))
      << "Failed precondition of Reader::CopyToSlow(BackwardWriter*): "
         "length too small, use CopyTo(BackwardWriter*) instead";
  if (ABSL_PREDICT_FALSE(length > available())) {
    cursor_ = limit_;
    return false;
  }
  if (length == contents_.size()) {
    cursor_ = limit_;
    return dest->Write(contents_);
  }
  Chain data;
  iter().AppendSubstrTo(absl::string_view(cursor_, length), &data, length);
  cursor_ += length;
  return dest->Write(std::move(data));
}

bool FdMMapReader::Size(Position* size) const {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  *size = contents_.size();
  return true;
}

bool FdMMapReader::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT_EQ(start_pos(), 0u)
      << "Failed invariant of FdMMapReader: non-zero position of buffer start";
  RIEGELI_ASSERT(new_pos > limit_pos_)
      << "Failed precondition of Reader::SeekSlow(): "
         "position in the buffer, use Seek() instead";
  // Seeking forwards. Source ends.
  cursor_ = limit_;
  return false;
}

}  // namespace riegeli
