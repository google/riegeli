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

#ifndef RIEGELI_BYTES_FD_READER_H_
#define RIEGELI_BYTES_FD_READER_H_

#include <fcntl.h>
#include <stddef.h>
#include <string>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/fd_holder.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

namespace internal {

// Implementation shared between FdReader and FdStreamReader.
class FdReaderBase : public BufferedReader {
 public:
  const std::string& filename() const { return filename_; }
  int error_code() const { return error_code_; }

 protected:
  // Creates a closed FdReaderBase.
  FdReaderBase() noexcept {}

  // Will read from fd.
  FdReaderBase(int fd, bool owns_fd, size_t buffer_size);

  // Opens a file for reading.
  FdReaderBase(std::string filename, int flags, size_t buffer_size);

  FdReaderBase(FdReaderBase&& src) noexcept;
  FdReaderBase& operator=(FdReaderBase&& src) noexcept;

  void Done() override;
  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation,
                                         int error_code);
  virtual bool MaybeSyncPos() { return true; }

  FdHolder owned_fd_;
  int fd_ = -1;
  std::string filename_;
  // errno value from a failed operation, or 0 if none.
  //
  // Invariant: if healthy() then error_code_ == 0
  int error_code_ = 0;

  // Invariants:
  //   limit_pos_ <= numeric_limits<off_t>::max()
  //   buffer_size_ <= numeric_limits<off_t>::max()
};

}  // namespace internal

// A Reader which reads from a file descriptor. It supports random access; the
// file descriptor must support pread(), lseek(), and fstat().
//
// Multiple FdReaders can read concurrently from the same fd. Reads occur at the
// position managed by the FdReader (using pread()).
class FdReader final : public internal::FdReaderBase {
 public:
  class Options {
   public:
    // Not defaulted because of a C++ defect:
    // https://stackoverflow.com/questions/17430377
    Options() noexcept {}

    // If true, the fd will be owned by the FdReader and will be closed when the
    // FdReader is closed.
    //
    // If false, the fd must be kept alive until closing the FdReader.
    //
    // Default: true.
    Options& set_owns_fd(bool owns_fd) & {
      owns_fd_ = owns_fd;
      return *this;
    }
    Options&& set_owns_fd(bool owns_fd) && {
      return std::move(set_owns_fd(owns_fd));
    }

    Options& set_buffer_size(size_t buffer_size) & {
      RIEGELI_ASSERT_GT(buffer_size, 0u)
          << "Failed precondition of FdReader::Options::set_buffer_size()";
      buffer_size_ = buffer_size;
      return *this;
    }
    Options&& set_buffer_size(size_t buffer_size) && {
      return std::move(set_buffer_size(buffer_size));
    }

    // If true, FdReader will initially get the current file position, and will
    // set the final file position on Close().
    //
    // If false, file position is irrelevant for FdReader, and reading will
    // start at the beginning of file.
    //
    // Default: false.
    Options& set_sync_pos(bool sync_pos) & {
      sync_pos_ = sync_pos;
      return *this;
    }
    Options&& set_sync_pos(bool sync_pos) && {
      return std::move(set_sync_pos(sync_pos));
    }

   private:
    friend class FdReader;

    bool owns_fd_ = true;
    size_t buffer_size_ = kDefaultBufferSize();
    bool sync_pos_ = false;
  };

  // Creates a closed FdReader.
  FdReader() noexcept {}

  // Will read from fd, starting at its beginning (or current file position if
  // options.set_sync_pos(true) is used).
  explicit FdReader(int fd, Options options = Options());

  // Opens a file for reading.
  //
  // flags is the second argument of open, typically O_RDONLY.
  //
  // flags must include O_RDONLY or O_RDWR.
  // options.set_owns_fd(false) must not be used.
  FdReader(std::string filename, int flags, Options options = Options());

  FdReader(FdReader&& src) noexcept;
  FdReader& operator=(FdReader&& src) noexcept;

  bool SupportsRandomAccess() const override { return true; }
  bool Size(Position* size) const override;

 protected:
  void Done() override;
  bool MaybeSyncPos() override;
  bool ReadInternal(char* dest, size_t min_length, size_t max_length) override;
  bool SeekSlow(Position new_pos) override;

 private:
  void InitializePos();

  bool sync_pos_ = false;
};

// A Reader which reads from a file descriptor which does not have to support
// random access.
//
// Multiple FdStreamReaders may not be used with the same fd at the same time.
// Reads occur at the current file position (using read()).
class FdStreamReader final : public internal::FdReaderBase {
 public:
  class Options {
   public:
    // Not defaulted because of a C++ defect:
    // https://stackoverflow.com/questions/17430377
    Options() noexcept {}

    // There is no set_owns_fd() because it is impossible to unread what has
    // been buffered, so a non-owned fd would be left having an unpredictable
    // amount of extra data consumed, which would not be useful.

    Options& set_buffer_size(size_t buffer_size) & {
      RIEGELI_ASSERT_GT(buffer_size, 0u)
          << "Failed precondition of "
             "FdStreamReader::Options::set_buffer_size()";
      buffer_size_ = buffer_size;
      return *this;
    }
    Options&& set_buffer_size(size_t buffer_size) && {
      return std::move(set_buffer_size(buffer_size));
    }

    // Sets the file position assumed initially, used for reporting by pos().
    //
    // Default for constructor from fd: none, must be provided explicitly.
    //
    // Default for constructor from filename: 0.
    Options& set_assumed_pos(Position assumed_pos) & {
      has_assumed_pos_ = true;
      assumed_pos_ = assumed_pos;
      return *this;
    }
    Options&& set_assumed_pos(Position assumed_pos) && {
      return std::move(set_assumed_pos(assumed_pos));
    }

   private:
    friend class FdStreamReader;

    size_t buffer_size_ = kDefaultBufferSize();
    bool has_assumed_pos_ = false;
    Position assumed_pos_ = 0;
  };

  // Creates a closed FdStreamReader.
  FdStreamReader() noexcept {}

  // Will read from fd, starting at its current position.
  //
  // options.set_assumed_pos() must be used.
  FdStreamReader(int fd, Options options);

  // Opens a file for reading.
  //
  // flags is the second argument of open, typically O_RDONLY.
  //
  // flags must include O_RDONLY or O_RDWR.
  FdStreamReader(std::string filename, int flags, Options options = Options());

  FdStreamReader(FdStreamReader&& src) noexcept;
  FdStreamReader& operator=(FdStreamReader&& src) noexcept;

 protected:
  bool ReadInternal(char* dest, size_t min_length, size_t max_length) override;
};

// A Reader which reads from a file descriptor by mapping the whole file to
// memory. It supports random access; the file descriptor must support mmap()
// and fstat(). The file must not be changed while data read from the file is
// accessed.
//
// Multiple FdMMapReaders can read concurrently from the same fd.
class FdMMapReader final : public Reader {
 public:
  class Options {
   public:
    // Not defaulted because of a C++ defect:
    // https://stackoverflow.com/questions/17430377
    Options() noexcept {}

    // If true, the fd will be owned by the FdMMapReader and will be closed
    // after construction.
    //
    // If false, the fd will not be closed by the FdMMapReader.
    //
    // In any case, the memory mapped region is usable even after the fd is
    // closed.
    //
    // Default: true.
    Options& set_owns_fd(bool owns_fd) & {
      owns_fd_ = owns_fd;
      return *this;
    }
    Options&& set_owns_fd(bool owns_fd) && {
      return std::move(set_owns_fd(owns_fd));
    }

   private:
    friend class FdMMapReader;

    bool owns_fd_ = true;
  };

  // Creates a closed FdMMapReader.
  FdMMapReader() noexcept : Reader(State::kClosed) {}

  // Will read from fd, starting at its beginning.
  explicit FdMMapReader(int fd, Options options = Options());

  // Opens a file for reading.
  //
  // flags is the second argument of open, typically O_RDONLY.
  //
  // flags must include O_RDONLY or O_RDWR.
  // options.set_owns_fd(false) must not be used.
  FdMMapReader(std::string filename, int flags, Options options = Options());

  FdMMapReader(FdMMapReader&& src) noexcept;
  FdMMapReader& operator=(FdMMapReader&& src) noexcept;

  const std::string& filename() const { return filename_; }
  int error_code() const { return error_code_; }

  bool SupportsRandomAccess() const override { return true; }
  bool Size(Position* size) const override;

 protected:
  void Done() override;
  bool PullSlow() override;
  bool ReadSlow(Chain* dest, size_t length) override;
  bool CopyToSlow(Writer* dest, Position length) override;
  bool CopyToSlow(BackwardWriter* dest, size_t length) override;
  bool SeekSlow(Position new_pos) override;

 private:
  void Initialize(int fd, Options options);

  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation,
                                         int error_code);

  // Iterator pointing to the block of contents_ which holds the actual data.
  //
  // Precondition: contents_.blocks().size() == 1
  Chain::BlockIterator iter() const;

  std::string filename_;
  // errno value from a failed operation, or 0 if none.
  //
  // Invariant: if healthy() then error_code_ == 0
  int error_code_ = 0;
  Chain contents_;

  // Invariants:
  //   start_ == (contents_.blocks().empty() ? nullptr : iter()->data())
  //   buffer_size() == (contents_.blocks().empty() ? 0 : iter()->size())
  //   start_pos() == 0
};

// Implementation details follow.

namespace internal {

inline FdReaderBase::FdReaderBase(FdReaderBase&& src) noexcept
    : BufferedReader(std::move(src)),
      owned_fd_(std::move(src.owned_fd_)),
      fd_(riegeli::exchange(src.fd_, -1)),
      filename_(riegeli::exchange(src.filename_, std::string())),
      error_code_(riegeli::exchange(src.error_code_, 0)) {}

inline FdReaderBase& FdReaderBase::operator=(FdReaderBase&& src) noexcept {
  BufferedReader::operator=(std::move(src));
  owned_fd_ = std::move(src.owned_fd_);
  fd_ = riegeli::exchange(src.fd_, -1);
  filename_ = riegeli::exchange(src.filename_, std::string());
  error_code_ = riegeli::exchange(src.error_code_, 0);
  return *this;
}

}  // namespace internal

inline FdReader::FdReader(FdReader&& src) noexcept
    : internal::FdReaderBase(std::move(src)),
      sync_pos_(riegeli::exchange(src.sync_pos_, false)) {}

inline FdReader& FdReader::operator=(FdReader&& src) noexcept {
  internal::FdReaderBase::operator=(std::move(src));
  sync_pos_ = riegeli::exchange(src.sync_pos_, false);
  return *this;
}

inline FdStreamReader::FdStreamReader(FdStreamReader&& src) noexcept
    : internal::FdReaderBase(std::move(src)) {}

inline FdStreamReader& FdStreamReader::operator=(
    FdStreamReader&& src) noexcept {
  internal::FdReaderBase::operator=(std::move(src));
  return *this;
}

inline FdMMapReader::FdMMapReader(FdMMapReader&& src) noexcept
    : Reader(std::move(src)),
      filename_(riegeli::exchange(src.filename_, std::string())),
      error_code_(riegeli::exchange(src.error_code_, 0)),
      contents_(riegeli::exchange(src.contents_, Chain())) {}

inline FdMMapReader& FdMMapReader::operator=(FdMMapReader&& src) noexcept {
  Reader::operator=(std::move(src)),
  filename_ = riegeli::exchange(src.filename_, std::string());
  error_code_ = riegeli::exchange(src.error_code_, 0);
  contents_ = riegeli::exchange(src.contents_, Chain());
  return *this;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_FD_READER_H_
