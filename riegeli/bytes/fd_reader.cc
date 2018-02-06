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
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <limits>
#include <string>
#include <utility>

#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
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

  string_view data() const {
    return string_view(static_cast<const char*>(data_), size_);
  }
  void AddUniqueTo(string_view data, MemoryEstimator* memory_estimator) const;
  void DumpStructure(string_view data, std::ostream& out) const;

 private:
  void* data_;
  size_t size_;
};

inline MMapRef::MMapRef(MMapRef&& src) noexcept
    : data_(riegeli::exchange(src.data_, nullptr)),
      size_(riegeli::exchange(src.size_, 0)) {}

inline MMapRef& MMapRef::operator=(MMapRef&& src) noexcept {
  // Exchange data_ early to support self-assignment.
  void* const data = riegeli::exchange(src.data_, nullptr);
  if (data_ != nullptr) {
    const int result = munmap(data_, size_);
    RIEGELI_CHECK_EQ(result, 0) << "munmap() failed";
  }
  data_ = data;
  size_ = riegeli::exchange(src.size_, 0);
  return *this;
}

inline MMapRef::~MMapRef() {
  if (data_ != nullptr) {
    const int result = munmap(data_, size_);
    RIEGELI_CHECK_EQ(result, 0) << "munmap() failed";
  }
}

void MMapRef::AddUniqueTo(string_view data,
                          MemoryEstimator* memory_estimator) const {
  memory_estimator->AddMemory(sizeof(*this));
}

void MMapRef::DumpStructure(string_view data, std::ostream& out) const {
  out << "mmap";
}

}  // namespace

namespace internal {

FdReaderBase::FdReaderBase(int fd, bool owns_fd, size_t buffer_size)
    : BufferedReader(UnsignedMin(buffer_size,
                                 Position{std::numeric_limits<off_t>::max()})),
      owned_fd_(owns_fd ? fd : -1),
      fd_(fd),
      filename_(fd == 0 ? "/dev/stdin"
                        : "/proc/self/fd/" + std::to_string(fd)) {
  RIEGELI_ASSERT_GE(fd, 0)
      << "Failed precondition of FdReaderBase::FdReaderBase(int): "
         "negative file descriptor";
}

FdReaderBase::FdReaderBase(std::string filename, int flags, size_t buffer_size)
    : BufferedReader(UnsignedMin(buffer_size,
                                 Position{std::numeric_limits<off_t>::max()})),
      filename_(std::move(filename)) {
  RIEGELI_ASSERT((flags & O_ACCMODE) == O_RDONLY ||
                 (flags & O_ACCMODE) == O_RDWR)
      << "Failed precondition of FdReaderBase::FdReaderBase(string): "
         "flags must include O_RDONLY or O_RDWR";
again:
  fd_ = open(filename_.c_str(), flags, 0666);
  if (RIEGELI_UNLIKELY(fd_ < 0)) {
    const int error_code = errno;
    if (error_code == EINTR) goto again;
    FailOperation("open()", error_code);
    return;
  }
  owned_fd_ = FdHolder(fd_);
}

FdReaderBase::FdReaderBase(FdReaderBase&& src) noexcept
    : BufferedReader(std::move(src)),
      owned_fd_(std::move(src.owned_fd_)),
      fd_(riegeli::exchange(src.fd_, -1)),
      filename_(riegeli::exchange(src.filename_, std::string())),
      error_code_(riegeli::exchange(src.error_code_, 0)) {}

FdReaderBase& FdReaderBase::operator=(FdReaderBase&& src) noexcept {
  BufferedReader::operator=(std::move(src));
  owned_fd_ = std::move(src.owned_fd_);
  fd_ = riegeli::exchange(src.fd_, -1);
  filename_ = riegeli::exchange(src.filename_, std::string());
  error_code_ = riegeli::exchange(src.error_code_, 0);
  return *this;
}

FdReaderBase::~FdReaderBase() = default;

void FdReaderBase::Done() {
  if (RIEGELI_LIKELY(healthy())) MaybeSyncPos();
  const int error_code = owned_fd_.Close();
  if (RIEGELI_UNLIKELY(error_code != 0) && RIEGELI_LIKELY(healthy())) {
    FailOperation(FdHolder::CloseFunctionName(), error_code);
  }
  // filename_ and error_code_ are not cleared.
  BufferedReader::Done();
}

bool FdReaderBase::FailOperation(const char* operation, int error_code) {
  error_code_ = error_code;
  char message[256];
  strerror_r(error_code, message, sizeof(message));
  message[sizeof(message) - 1] = '\0';
  return Fail(std::string(operation) + " failed: " + message + ", reading " +
              filename_);
}

}  // namespace internal

FdReader::FdReader() noexcept = default;

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
  if (RIEGELI_LIKELY(healthy())) InitializePos();
}

FdReader::FdReader(FdReader&& src) noexcept
    : internal::FdReaderBase(std::move(src)),
      sync_pos_(riegeli::exchange(src.sync_pos_, false)) {}

FdReader& FdReader::operator=(FdReader&& src) noexcept {
  internal::FdReaderBase::operator=(std::move(src));
  sync_pos_ = riegeli::exchange(src.sync_pos_, false);
  return *this;
}

void FdReader::Done() {
  internal::FdReaderBase::Done();
  sync_pos_ = false;
}

inline void FdReader::InitializePos() {
  if (sync_pos_) {
    const off_t result = lseek(fd_, 0, SEEK_CUR);
    if (RIEGELI_UNLIKELY(result < 0)) {
      FailOperation("lseek()", errno);
      return;
    }
    limit_pos_ = IntCast<Position>(result);
  }
}

bool FdReader::MaybeSyncPos() {
  if (sync_pos_) {
    if (RIEGELI_UNLIKELY(lseek(fd_, IntCast<off_t>(pos()), SEEK_SET) < 0)) {
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
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "Reader unhealthy";
  if (RIEGELI_UNLIKELY(max_length >
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
    if (RIEGELI_UNLIKELY(result < 0)) {
      const int error_code = errno;
      if (error_code == EINTR) goto again;
      return FailOperation("pread()", error_code);
    }
    if (RIEGELI_UNLIKELY(result == 0)) return false;
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
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  if (new_pos > limit_pos_) {
    // Seeking forwards.
    struct stat stat_info;
    if (RIEGELI_UNLIKELY(fstat(fd_, &stat_info) < 0)) {
      const int error_code = errno;
      return FailOperation("fstat()", error_code);
    }
    if (RIEGELI_UNLIKELY(new_pos > IntCast<Position>(stat_info.st_size))) {
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
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  struct stat stat_info;
  const int result = fstat(fd_, &stat_info);
  if (RIEGELI_UNLIKELY(result < 0)) return false;
  *size = IntCast<Position>(stat_info.st_size);
  return true;
}

FdStreamReader::FdStreamReader() noexcept = default;

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
  if (RIEGELI_UNLIKELY(!healthy())) return;
  limit_pos_ = options.assumed_pos_;
}

FdStreamReader::FdStreamReader(FdStreamReader&& src) noexcept
    : internal::FdReaderBase(std::move(src)) {}

FdStreamReader& FdStreamReader::operator=(FdStreamReader&& src) noexcept {
  internal::FdReaderBase::operator=(std::move(src));
  return *this;
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
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "Reader unhealthy";
  if (RIEGELI_UNLIKELY(max_length >
                       Position{std::numeric_limits<off_t>::max()} -
                           limit_pos_)) {
    return FailOverflow();
  }
  for (;;) {
  again:
    const ssize_t result = read(
        fd_, dest,
        UnsignedMin(max_length, size_t{std::numeric_limits<ssize_t>::max()}));
    if (RIEGELI_UNLIKELY(result < 0)) {
      const int error_code = errno;
      if (error_code == EINTR) goto again;
      return FailOperation("read()", error_code);
    }
    if (RIEGELI_UNLIKELY(result == 0)) return false;
    RIEGELI_ASSERT_LE(IntCast<size_t>(result), max_length)
        << "read() read more than requested";
    limit_pos_ += IntCast<size_t>(result);
    if (IntCast<size_t>(result) >= min_length) return true;
    dest += result;
    min_length -= IntCast<size_t>(result);
    max_length -= IntCast<size_t>(result);
  }
}

FdMMapReader::FdMMapReader() noexcept : Reader(State::kClosed) {}

FdMMapReader::FdMMapReader(int fd, Options options)
    : Reader(State::kOpen),
      filename_(fd == 0 ? "/dev/stdin"
                        : "/proc/self/fd/" + std::to_string(fd)) {
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
  if (RIEGELI_UNLIKELY(fd < 0)) {
    const int error_code = errno;
    if (error_code == EINTR) goto again;
    FailOperation("open()", error_code);
    return;
  }
  Initialize(fd, options);
}

FdMMapReader::FdMMapReader(FdMMapReader&& src) noexcept
    : Reader(std::move(src)),
      filename_(riegeli::exchange(src.filename_, std::string())),
      error_code_(riegeli::exchange(src.error_code_, 0)),
      contents_(riegeli::exchange(src.contents_, Chain())) {}

FdMMapReader& FdMMapReader::operator=(FdMMapReader&& src) noexcept {
  Reader::operator=(std::move(src)),
  filename_ = riegeli::exchange(src.filename_, std::string());
  error_code_ = riegeli::exchange(src.error_code_, 0);
  contents_ = riegeli::exchange(src.contents_, Chain());
  return *this;
}

void FdMMapReader::Done() {
  contents_ = Chain();
  // filename_ and error_code_ are not cleared.
  Reader::Done();
}

inline void FdMMapReader::Initialize(int fd, Options options) {
  internal::FdHolder owned_fd(options.owns_fd_ ? fd : -1);
  struct stat stat_info;
  if (RIEGELI_UNLIKELY(fstat(fd, &stat_info) < 0)) {
    const int error_code = errno;
    FailOperation("fstat()", error_code);
    return;
  }
  if (RIEGELI_UNLIKELY(IntCast<Position>(stat_info.st_size) >
                       std::numeric_limits<size_t>::max())) {
    Fail("File is too large for mmap()");
    return;
  }
  if (stat_info.st_size != 0) {
    void* const data = mmap(nullptr, IntCast<size_t>(stat_info.st_size),
                            PROT_READ, MAP_SHARED, fd, 0);
    if (RIEGELI_UNLIKELY(data == MAP_FAILED)) {
      const int error_code = errno;
      FailOperation("mmap()", error_code);
      return;
    }
    contents_.AppendExternal(MMapRef(data, IntCast<size_t>(stat_info.st_size)));
    start_ = iter()->data();
    cursor_ = iter()->data();
    limit_ = iter()->data() + iter()->size();
    limit_pos_ = iter()->size();
  }
  const int error_code = owned_fd.Close();
  if (RIEGELI_UNLIKELY(error_code != 0)) {
    FailOperation(internal::FdHolder::CloseFunctionName(), error_code);
  }
}

inline bool FdMMapReader::FailOperation(const char* operation, int error_code) {
  error_code_ = error_code;
  char message[256];
  strerror_r(error_code, message, sizeof(message));
  message[sizeof(message) - 1] = '\0';
  return Fail(std::string(operation) + " failed: " + message + ", reading " +
              filename_);
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
  if (length_to_read > 0) {  // iter() is undefined
                             // if contents_.blocks().size() != 1.
    const size_t size_hint = dest->size() + length_to_read;
    iter().AppendSubstrTo(string_view(cursor_, length_to_read), dest,
                          size_hint);
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
  } else if (length_to_copy > 0) {  // iter() is undefined
                                    // if contents_.blocks().size() != 1.
    Chain data;
    iter().AppendSubstrTo(string_view(cursor_, length_to_copy), &data,
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
  if (RIEGELI_UNLIKELY(length > available())) {
    cursor_ = limit_;
    return false;
  }
  if (length == contents_.size()) {
    cursor_ = limit_;
    return dest->Write(contents_);
  }
  Chain data;
  iter().AppendSubstrTo(string_view(cursor_, length), &data, length);
  cursor_ += length;
  return dest->Write(std::move(data));
}

bool FdMMapReader::Size(Position* size) const {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  *size = contents_.size();
  return true;
}

bool FdMMapReader::HopeForMoreSlow() const {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of Reader::HopeForMoreSlow(): "
         "data available, use HopeForMore() instead";
  return false;
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
