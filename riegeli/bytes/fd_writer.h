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

#ifndef RIEGELI_BYTES_FD_WRITER_H_
#define RIEGELI_BYTES_FD_WRITER_H_

#include <fcntl.h>
#include <stddef.h>
#include <sys/types.h>
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

// Implementation shared between FdWriter and FdStreamWriter.
class FdWriterBase : public BufferedWriter {
 public:
  const std::string& filename() const { return filename_; }
  int error_code() const { return error_code_; }
  bool Flush(FlushType flush_type) override;

 protected:
  FdWriterBase();

  FdWriterBase(int fd, bool owns_fd, size_t buffer_size);

  FdWriterBase(std::string filename, int flags, mode_t permissions,
               size_t buffer_size);

  FdWriterBase(FdWriterBase&& src) noexcept;
  void operator=(FdWriterBase&& src) noexcept;

  ~FdWriterBase();

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

// FdWriter::Options.
class FdWriterOptions {
 public:
  // If true, the fd will be owned by the FdWriter and will be closed when the
  // FdWriter is closed.
  //
  // If false, the fd must be alive until closing the FdWriter.
  //
  // Default: true.
  FdWriterOptions& set_owns_fd(bool owns_fd) & {
    owns_fd_ = owns_fd;
    return *this;
  }
  FdWriterOptions&& set_owns_fd(bool owns_fd) && {
    return std::move(set_owns_fd(owns_fd));
  }

  // Permissions to use in case a new file is created (9 bits). The effective
  // permissions are modified by the process's umask.
  FdWriterOptions& set_permissions(mode_t permissions) & {
    permissions_ = permissions;
    return *this;
  }
  FdWriterOptions&& set_permissions(mode_t permissions) && {
    return std::move(set_permissions(permissions));
  }

  FdWriterOptions& set_buffer_size(size_t buffer_size) & {
    RIEGELI_ASSERT_GT(buffer_size, 0u);
    buffer_size_ = buffer_size;
    return *this;
  }
  FdWriterOptions&& set_buffer_size(size_t buffer_size) && {
    return std::move(set_buffer_size(buffer_size));
  }

  // If true, FdWriter will initially get the current file position, and will
  // set the final file position on Close() and Flush().
  //
  // If false, file position is irrelevant for FdWriter, and writing will start
  // at the end of file.
  //
  // Default: false.
  FdWriterOptions& set_sync_pos(bool sync_pos) & {
    sync_pos_ = sync_pos;
    return *this;
  }
  FdWriterOptions&& set_sync_pos(bool sync_pos) && {
    return std::move(set_sync_pos(sync_pos));
  }

 private:
  friend class FdWriter;

  bool owns_fd_ = true;
  mode_t permissions_ = 0666;
  size_t buffer_size_ = kDefaultBufferSize();
  bool sync_pos_ = false;
};

// A Writer which writes to a file descriptor. It supports random access; the
// file descriptor must support pwrite(), lseek(), fstat(), and ftruncate().
//
// Multiple FdWriters can write concurrently to the same fd, although this is
// rarely useful because they would need to write to disjoint regions. Writes
// occur at the position managed by the FdWriter (using pwrite()).
class FdWriter final : public internal::FdWriterBase {
 public:
  using Options = FdWriterOptions;

  // Creates a closed FdWriter.
  FdWriter();

  // Will write to the fd, starting at the end of file, or at the current fd
  // position if options.set_sync_pos(true) is used.
  explicit FdWriter(int fd, Options options = Options());

  // Opens a file for writing.
  //
  // flags is the second argument of open, typically one of:
  //  * O_WRONLY | O_CREAT | O_TRUNC
  //  * O_WRONLY | O_CREAT | O_APPEND
  //
  // flags must include O_WRONLY or O_RDWR.
  // options.set_owns_fd(false) must not be used.
  FdWriter(std::string filename, int flags, Options options = Options());

  FdWriter(FdWriter&& src) noexcept;
  FdWriter& operator=(FdWriter&& src) noexcept;

  bool SupportsRandomAccess() const override { return true; }
  bool Size(Position* size) const override;
  bool Truncate() override;

 protected:
  void Done() override;
  bool MaybeSyncPos() override;
  bool WriteInternal(string_view src) override;
  bool SeekSlow(Position new_pos) override;

 private:
  void InitializePos(int flags);

  bool sync_pos_;
};

// FdStreamWriter::Options.
class FdStreamWriterOptions {
 public:
  // If true, the fd will be owned by the FdStreamWriter and will be closed when
  // the FdStreamWriter is closed.
  //
  // If false, the fd must be alive until closing the FdStreamWriter.
  //
  // Default: true.
  FdStreamWriterOptions& set_owns_fd(bool owns_fd) & {
    owns_fd_ = owns_fd;
    return *this;
  }
  FdStreamWriterOptions&& set_owns_fd(bool owns_fd) && {
    return std::move(set_owns_fd(owns_fd));
  }

  // Permissions to use in case a new file is created (9 bits). The effective
  // permissions are modified by the process's umask.
  FdStreamWriterOptions& set_permissions(mode_t permissions) & {
    permissions_ = permissions;
    return *this;
  }
  FdStreamWriterOptions&& set_permissions(mode_t permissions) && {
    return std::move(set_permissions(permissions));
  }

  FdStreamWriterOptions& set_buffer_size(size_t buffer_size) & {
    RIEGELI_ASSERT_GT(buffer_size, 0u);
    buffer_size_ = buffer_size;
    return *this;
  }
  FdStreamWriterOptions&& set_buffer_size(size_t buffer_size) && {
    return std::move(set_buffer_size(buffer_size));
  }

  // Sets the file position assumed initially, used for reporting by pos().
  //
  // Default for constructor from fd: none, must be provided explicitly.
  //
  // Default for constructor from filename: 0 when opening for writing, or file
  // size when opening for appending.
  FdStreamWriterOptions& set_assumed_pos(Position assumed_pos) & {
    has_assumed_pos_ = true;
    assumed_pos_ = assumed_pos;
    return *this;
  }
  FdStreamWriterOptions&& set_assumed_pos(Position assumed_pos) && {
    return std::move(set_assumed_pos(assumed_pos));
  }

 private:
  friend class FdStreamWriter;

  bool owns_fd_ = true;
  mode_t permissions_ = 0666;
  size_t buffer_size_ = kDefaultBufferSize();
  bool has_assumed_pos_ = false;
  Position assumed_pos_ = 0;
};

// A Writer which writes to a file descriptor which does not have to support
// random access.
//
// Multiple FdStreamWriters can be used with the same fd, although this is
// rarely useful because they would need to avoid writing concurrently, and to
// Flush() before allowing another FdStreamWriter to write. Writes occur at the
// current file position (using write()).
class FdStreamWriter final : public internal::FdWriterBase {
 public:
  using Options = FdStreamWriterOptions;

  // Creates a closed FdStreamWriter.
  FdStreamWriter();

  // Will write to the fd, starting at its current position.
  //
  // options.set_assumed_pos() must be used.
  FdStreamWriter(int fd, Options options);

  // Opens a file for writing.
  //
  // flags is the second argument of open, typically one of:
  //  * O_WRONLY | O_CREAT | O_TRUNC
  //  * O_WRONLY | O_CREAT | O_APPEND
  //
  // flags must include O_WRONLY or O_RDWR.
  // options.set_owns_fd(false) must not be used.
  FdStreamWriter(std::string filename, int flags, Options options = Options());

  FdStreamWriter(FdStreamWriter&& src) noexcept;
  FdStreamWriter& operator=(FdStreamWriter&& src) noexcept;

 protected:
  bool WriteInternal(string_view src) override;
};

}  // namespace riegeli

#endif  // RIEGELI_BYTES_FD_WRITER_H_
