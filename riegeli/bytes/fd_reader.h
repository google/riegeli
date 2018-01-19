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

#include "riegeli/base/assert.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/fd_holder.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

namespace internal {

// Implementation shared between FdReader and FdStreamReader.
class FdReaderBase : public BufferedReader {
 public:
  const std::string& filename() const { return filename_; }
  int error_code() const { return error_code_; }

 protected:
  FdReaderBase();

  FdReaderBase(int fd, bool owns_fd, size_t buffer_size);

  FdReaderBase(std::string filename, int flags, size_t buffer_size);

  FdReaderBase(FdReaderBase&& src) noexcept;
  void operator=(FdReaderBase&& src) noexcept;

  ~FdReaderBase();

  void Done() override;
  RIEGELI_ATTRIBUTE_COLD bool FailOperation(const char* operation,
                                            int error_code);
  virtual bool MaybeSyncPos() { return true; }

  FdHolder owned_fd_;
  int fd_;
  std::string filename_;
  // errno value from a failed operation, or 0 if none.
  //
  // Invariant: if healthy() then error_code_ == 0
  int error_code_ = 0;
};

}  // namespace internal

// FdReader::Options.
class FdReaderOptions {
 public:
  // If true, the fd will be owned by the FdReader and will be closed when the
  // FdReader is closed.
  //
  // If false, the fd must be kept alive until closing the FdReader.
  //
  // Default: true.
  FdReaderOptions& set_owns_fd(bool owns_fd) & {
    owns_fd_ = owns_fd;
    return *this;
  }
  FdReaderOptions&& set_owns_fd(bool owns_fd) && {
    return std::move(set_owns_fd(owns_fd));
  }

  FdReaderOptions& set_buffer_size(size_t buffer_size) & {
    RIEGELI_ASSERT_GT(buffer_size, 0u);
    buffer_size_ = buffer_size;
    return *this;
  }
  FdReaderOptions&& set_buffer_size(size_t buffer_size) && {
    return std::move(set_buffer_size(buffer_size));
  }

  // If true, FdReader will initially get the current file position, and will
  // set the final file position on Close().
  //
  // If false, file position is irrelevant for FdReader, and reading will start
  // at the beginning of file.
  //
  // Default: false.
  FdReaderOptions& set_sync_pos(bool sync_pos) & {
    sync_pos_ = sync_pos;
    return *this;
  }
  FdReaderOptions&& set_sync_pos(bool sync_pos) && {
    return std::move(set_sync_pos(sync_pos));
  }

 private:
  friend class FdReader;

  bool owns_fd_ = true;
  size_t buffer_size_ = kDefaultBufferSize();
  bool sync_pos_ = false;
};

// A Reader which reads from a file descriptor. It supports random access; the
// file descriptor must support pread(), lseek(), and fstat().
//
// Multiple FdReaders can read concurrently from the same fd. Reads occur at the
// position managed by the FdReader (using pread()).
class FdReader final : public internal::FdReaderBase {
 public:
  using Options = FdReaderOptions;

  // Creates a closed FdReader.
  FdReader();

  // Will read from the fd, starting at its beginning (or current file position
  // if options.set_sync_pos(true) is used).
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

  bool sync_pos_;
};

// FdStreamReader::Options.
class FdStreamReaderOptions {
 public:
  // There is no FdStreamReaderOptions::set_owns_fd() because it is impossible
  // to unread what has been buffered, so a non-owned fd would be left having an
  // unpredictable amount of extra data consumed, which would not be useful.

  FdStreamReaderOptions& set_buffer_size(size_t buffer_size) & {
    RIEGELI_ASSERT_GT(buffer_size, 0u);
    buffer_size_ = buffer_size;
    return *this;
  }
  FdStreamReaderOptions&& set_buffer_size(size_t buffer_size) && {
    return std::move(set_buffer_size(buffer_size));
  }

  // Sets the file position assumed initially, used for reporting by pos().
  //
  // Default for constructor from fd: none, must be provided explicitly.
  //
  // Default for constructor from filename: 0.
  FdStreamReaderOptions& set_assumed_pos(Position assumed_pos) & {
    has_assumed_pos_ = true;
    assumed_pos_ = assumed_pos;
    return *this;
  }
  FdStreamReaderOptions&& set_assumed_pos(Position assumed_pos) && {
    return std::move(set_assumed_pos(assumed_pos));
  }

 private:
  friend class FdStreamReader;

  size_t buffer_size_ = kDefaultBufferSize();
  bool has_assumed_pos_ = false;
  Position assumed_pos_ = 0;
};

// A Reader which reads from a file descriptor which does not have to support
// random access.
//
// Multiple FdStreamReaders may not be used with the same fd at the same time.
// Reads occur at the current file position (using read()).
class FdStreamReader final : public internal::FdReaderBase {
 public:
  using Options = FdStreamReaderOptions;

  // Creates a closed FdStreamReader.
  FdStreamReader();

  // Will read from the fd, starting at its current position.
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

}  // namespace riegeli

#endif  // RIEGELI_BYTES_FD_READER_H_
