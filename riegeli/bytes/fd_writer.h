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

#include "absl/base/attributes.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/fd_holder.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

namespace internal {

// Implementation shared between FdWriter and FdStreamWriter.
class FdWriterBase : public BufferedWriter {
 public:
  // Returns the file descriptor being written to. Unchanged by Close() if the
  // fd was not owned, -1 if it was owned.
  int fd() const { return fd_; }
  // Returns the original name of the file being written to (or /dev/stdout,
  // /dev/stderr, or /proc/self/fd/<fd> if fd was given). Unchanged by Close().
  const std::string& filename() const { return filename_; }
  // Returns the errno value of the last fd operation, or 0 if none.
  // Unchanged by Close().
  int error_code() const { return error_code_; }

  bool Flush(FlushType flush_type) override;

 protected:
  // Creates a closed FdWriterBase.
  FdWriterBase() noexcept {}

  // Will write to fd.
  FdWriterBase(int fd, bool owns_fd, size_t buffer_size);

  // Opens a file for writing.
  FdWriterBase(std::string filename, int flags, mode_t permissions,
               bool owns_fd, size_t buffer_size);

  FdWriterBase(FdWriterBase&& src) noexcept;
  FdWriterBase& operator=(FdWriterBase&& src) noexcept;

  void Done() override;
  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation,
                                         int error_code);
  virtual bool MaybeSyncPos() { return true; }

  FdHolder owned_fd_;
  int fd_ = -1;
  std::string filename_;
  // errno value of the last fd operation, or 0 if none.
  //
  // Invariant: if healthy() then error_code_ == 0
  int error_code_ = 0;

  // Invariants:
  //   start_pos_ <= numeric_limits<off_t>::max()
  //   buffer_size_ <= numeric_limits<off_t>::max()
};

}  // namespace internal

// A Writer which writes to a file descriptor. It supports random access; the
// file descriptor must support pwrite(), lseek(), fstat(), and ftruncate().
//
// Multiple FdWriters can write concurrently to the same fd, although this is
// rarely useful because they would need to write to disjoint regions. Writes
// occur at the position managed by the FdWriter (using pwrite()).
class FdWriter : public internal::FdWriterBase {
 public:
  class Options {
   public:
    Options() noexcept {}

    // If true, the fd will be owned by the FdWriter and will be closed when the
    // FdWriter is closed.
    //
    // If false, the fd must be alive until closing the FdWriter.
    //
    // Default: true.
    Options& set_owns_fd(bool owns_fd) & {
      owns_fd_ = owns_fd;
      return *this;
    }
    Options&& set_owns_fd(bool owns_fd) && {
      return std::move(set_owns_fd(owns_fd));
    }

    // Permissions to use in case a new file is created (9 bits). The effective
    // permissions are modified by the process's umask.
    Options& set_permissions(mode_t permissions) & {
      permissions_ = permissions;
      return *this;
    }
    Options&& set_permissions(mode_t permissions) && {
      return std::move(set_permissions(permissions));
    }

    Options& set_buffer_size(size_t buffer_size) & {
      RIEGELI_ASSERT_GT(buffer_size, 0u)
          << "Failed precondition of FdWriter::Options::set_buffer_size(): "
             "zero buffer size";
      buffer_size_ = buffer_size;
      return *this;
    }
    Options&& set_buffer_size(size_t buffer_size) && {
      return std::move(set_buffer_size(buffer_size));
    }

    // If true, FdWriter will initially get the current file position, and will
    // set the final file position on Close() and Flush().
    //
    // If false, file position is irrelevant for FdWriter, and writing will
    // start at the end of file.
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
    friend class FdWriter;

    bool owns_fd_ = true;
    mode_t permissions_ = 0666;
    size_t buffer_size_ = kDefaultBufferSize();
    bool sync_pos_ = false;
  };

  // Creates a closed FdWriter.
  FdWriter() noexcept {}

  // Will write to fd, starting at the end of file, or at the current fd
  // position if options.set_sync_pos(true) is used.
  explicit FdWriter(int fd, Options options = Options());

  // Opens a file for writing.
  //
  // flags is the second argument of open, typically one of:
  //  * O_WRONLY | O_CREAT | O_TRUNC
  //  * O_WRONLY | O_CREAT | O_APPEND
  //
  // flags must include O_WRONLY or O_RDWR.
  FdWriter(std::string filename, int flags, Options options = Options());

  FdWriter(FdWriter&& src) noexcept;
  FdWriter& operator=(FdWriter&& src) noexcept;

  bool SupportsRandomAccess() const override { return true; }
  bool Size(Position* size) override;
  bool SupportsTruncate() const override { return true; }
  bool Truncate(Position new_size) override;

 protected:
  bool MaybeSyncPos() override;
  bool WriteInternal(absl::string_view src) override;
  bool SeekSlow(Position new_pos) override;

 private:
  void InitializePos(int flags);

  bool sync_pos_ = false;
};

// A Writer which writes to a file descriptor which does not have to support
// random access.
//
// Multiple FdStreamWriters can be used with the same fd, although this is
// rarely useful because they would need to avoid writing concurrently, and to
// Flush() before allowing another FdStreamWriter to write. Writes occur at the
// current file position (using write()).
class FdStreamWriter : public internal::FdWriterBase {
 public:
  class Options {
   public:
    Options() noexcept {}

    // If true, the fd will be owned by the FdStreamWriter and will be closed
    // when the FdStreamWriter is closed.
    //
    // If false, the fd must be alive until closing the FdStreamWriter.
    //
    // Default: true.
    Options& set_owns_fd(bool owns_fd) & {
      owns_fd_ = owns_fd;
      return *this;
    }
    Options&& set_owns_fd(bool owns_fd) && {
      return std::move(set_owns_fd(owns_fd));
    }

    // Permissions to use in case a new file is created (9 bits). The effective
    // permissions are modified by the process's umask.
    Options& set_permissions(mode_t permissions) & {
      permissions_ = permissions;
      return *this;
    }
    Options&& set_permissions(mode_t permissions) && {
      return std::move(set_permissions(permissions));
    }

    Options& set_buffer_size(size_t buffer_size) & {
      RIEGELI_ASSERT_GT(buffer_size, 0u)
          << "Failed precondition of "
             "FdStreamWriter::Options::set_buffer_size(): "
             "zero buffer size";
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
    // Default for constructor from filename: 0 when opening for writing, or
    // file size when opening for appending.
    Options& set_assumed_pos(Position assumed_pos) & {
      assumed_pos_ = assumed_pos;
      return *this;
    }
    Options&& set_assumed_pos(Position assumed_pos) && {
      return std::move(set_assumed_pos(assumed_pos));
    }

   private:
    friend class FdStreamWriter;

    bool owns_fd_ = true;
    mode_t permissions_ = 0666;
    size_t buffer_size_ = kDefaultBufferSize();
    absl::optional<Position> assumed_pos_;
  };

  // Creates a closed FdStreamWriter.
  FdStreamWriter() noexcept {}

  // Will write to fd, starting at its current position.
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
  FdStreamWriter(std::string filename, int flags, Options options = Options());

  FdStreamWriter(FdStreamWriter&& src) noexcept;
  FdStreamWriter& operator=(FdStreamWriter&& src) noexcept;

 protected:
  bool WriteInternal(absl::string_view src) override;
};

// Implementation details follow.

namespace internal {

inline FdWriterBase::FdWriterBase(FdWriterBase&& src) noexcept
    : BufferedWriter(std::move(src)),
      owned_fd_(std::move(src.owned_fd_)),
      fd_(riegeli::exchange(src.fd_, -1)),
      filename_(riegeli::exchange(src.filename_, std::string())),
      error_code_(riegeli::exchange(src.error_code_, 0)) {}

inline FdWriterBase& FdWriterBase::operator=(FdWriterBase&& src) noexcept {
  BufferedWriter::operator=(std::move(src));
  owned_fd_ = std::move(src.owned_fd_);
  fd_ = riegeli::exchange(src.fd_, -1);
  filename_ = riegeli::exchange(src.filename_, std::string());
  error_code_ = riegeli::exchange(src.error_code_, 0);
  return *this;
}

}  // namespace internal

inline FdWriter::FdWriter(FdWriter&& src) noexcept
    : internal::FdWriterBase(std::move(src)),
      sync_pos_(riegeli::exchange(src.sync_pos_, false)) {}

inline FdWriter& FdWriter::operator=(FdWriter&& src) noexcept {
  internal::FdWriterBase::operator=(std::move(src));
  sync_pos_ = riegeli::exchange(src.sync_pos_, false);
  return *this;
}

inline FdStreamWriter::FdStreamWriter(FdStreamWriter&& src) noexcept
    : internal::FdWriterBase(std::move(src)) {}

inline FdStreamWriter& FdStreamWriter::operator=(
    FdStreamWriter&& src) noexcept {
  internal::FdWriterBase::operator=(std::move(src));
  return *this;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_FD_WRITER_H_
