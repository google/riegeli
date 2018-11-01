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
#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "absl/utility/utility.h"
#include "riegeli/base/base.h"
#include "riegeli/base/dependency.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/fd_dependency.h"

namespace riegeli {

namespace internal {

// Implementation shared between FdWriter and FdStreamWriter.
class FdWriterCommon : public BufferedWriter {
 public:
  // Returns the fd being written to. If the fd is owned then changed to -1 by
  // Close(), otherwise unchanged.
  virtual int dest_fd() const = 0;

  // Returns the original name of the file being written to (or /dev/stdout,
  // /dev/stderr, or /proc/self/fd/<fd> if fd was given). Unchanged by Close().
  const std::string& filename() const { return filename_; }

  // Returns the errno value of the last fd operation, or 0 if none.
  // Unchanged by Close().
  int error_code() const { return error_code_; }

 protected:
  FdWriterCommon() noexcept {}

  explicit FdWriterCommon(size_t buffer_size);

  FdWriterCommon(FdWriterCommon&& that) noexcept;
  FdWriterCommon& operator=(FdWriterCommon&& that) noexcept;

  void SetFilename(int dest);
  int OpenFd(absl::string_view filename, int flags, mode_t permissions);
  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation);

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

// Template parameter invariant part of FdWriter.
class FdWriterBase : public internal::FdWriterCommon {
 public:
  class Options {
   public:
    Options() noexcept {}

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
          << "Failed precondition of FdWriterBase::Options::set_buffer_size(): "
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
    template <typename Dest>
    friend class FdWriter;

    mode_t permissions_ = 0666;
    size_t buffer_size_ = kDefaultBufferSize();
    bool sync_pos_ = false;
  };

  bool Flush(FlushType flush_type) override;
  bool SupportsRandomAccess() const override { return true; }
  bool Size(Position* size) override;
  bool SupportsTruncate() const override { return true; }
  bool Truncate(Position new_size) override;

 protected:
  FdWriterBase() noexcept {}

  explicit FdWriterBase(size_t buffer_size, bool sync_pos)
      : FdWriterCommon(buffer_size), sync_pos_(sync_pos) {}

  FdWriterBase(FdWriterBase&& that) noexcept;
  FdWriterBase& operator=(FdWriterBase&& that) noexcept;

  void Initialize(int flags, int dest);
  bool SyncPos(int dest);
  bool WriteInternal(absl::string_view src) override;
  bool SeekSlow(Position new_pos) override;

  bool sync_pos_ = false;
};

// Template parameter invariant part of FdStreamWriter.
class FdStreamWriterBase : public internal::FdWriterCommon {
 public:
  class Options {
   public:
    Options() noexcept {}

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
             "FdStreamWriterBase::Options::set_buffer_size(): "
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
    Options& set_assumed_pos(absl::optional<Position> assumed_pos) & {
      assumed_pos_ = assumed_pos;
      return *this;
    }
    Options&& set_assumed_pos(absl::optional<Position> assumed_pos) && {
      return std::move(set_assumed_pos(assumed_pos));
    }

   private:
    template <typename Dest>
    friend class FdStreamWriter;

    mode_t permissions_ = 0666;
    size_t buffer_size_ = kDefaultBufferSize();
    absl::optional<Position> assumed_pos_;
  };

  bool Flush(FlushType flush_type) override;

 protected:
  FdStreamWriterBase() noexcept {}

  explicit FdStreamWriterBase(size_t buffer_size)
      : FdWriterCommon(buffer_size) {}

  FdStreamWriterBase(FdStreamWriterBase&& that) noexcept;
  FdStreamWriterBase& operator=(FdStreamWriterBase&& that) noexcept;

  void Initialize(int flags, int dest);
  bool WriteInternal(absl::string_view src) override;
};

// A Writer which writes to a file descriptor. It supports random access; the
// fd must support pwrite(), lseek(), fstat(), and ftruncate(). Writes occur at
// the position managed by FdWriter.
//
// The Dest template parameter specifies the type of the object providing and
// possibly owning the fd being written to. Dest must support
// Dependency<int, Dest>, e.g. OwnedFd (owned, default), int (not owned).
//
// The fd must not be closed until the FdWriter is closed or no longer used.
template <typename Dest = OwnedFd>
class FdWriter : public FdWriterBase {
 public:
  // Creates a closed FdWriter.
  FdWriter() noexcept {}

  // Will write to the fd provided by dest, starting at the end of file, or at
  // the current fd position if options.set_sync_pos(true) is used.
  //
  // type_identity_t<Dest> disables template parameter deduction (C++17),
  // letting FdWriter(fd) mean FdWriter<OwnedFd>(fd) rather than
  // FdWriter<int>(fd).
  explicit FdWriter(type_identity_t<Dest> dest, Options options = Options());

  // Opens a file for writing.
  //
  // flags is the second argument of open, typically one of:
  //  * O_WRONLY | O_CREAT | O_TRUNC
  //  * O_WRONLY | O_CREAT | O_APPEND
  //
  // flags must include O_WRONLY or O_RDWR.
  explicit FdWriter(absl::string_view filename, int flags,
                    Options options = Options());

  FdWriter(FdWriter&& that) noexcept;
  FdWriter& operator=(FdWriter&& that) noexcept;

  // Returns the object providing and possibly owning the fd being written to.
  // If the fd is owned then changed to -1 by Close(), otherwise unchanged.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  int dest_fd() const override { return dest_.ptr(); }

 protected:
  void Done() override;

 private:
  // The object providing and possibly owning the fd being written to.
  Dependency<int, Dest> dest_;
};

// A Writer which writes to a fd which does not have to support random access.
// The fd must support write(). Writes occur at the current fd position.
//
// The Dest template parameter specifies the type of the object providing and
// possibly owning the fd being written to. Dest must support
// Dependency<int, Dest>, e.g. OwnedFd (owned, default), int (not owned).
//
// The fd must not be closed until the FdStreamWriter is closed or no longer
// used. Until then the fd may be accessed, but not concurrently, Flush() is
// needed before switching to another writer to the same fd, and pos() does not
// take other writers into account.
template <typename Dest = OwnedFd>
class FdStreamWriter : public FdStreamWriterBase {
 public:
  // Creates a closed FdStreamWriter.
  FdStreamWriter() noexcept {}

  // Will write to the fd provided by dest, starting at its current position.
  //
  // options.set_assumed_pos() must be used.
  //
  // type_identity_t<Dest> disables template parameter deduction (C++17),
  // letting FdStreamWriter(fd) mean FdStreamWriter<OwnedFd>(fd) rather than
  // FdStreamWriter<int>(fd).
  explicit FdStreamWriter(type_identity_t<Dest> dest, Options options);

  // Opens a file for writing.
  //
  // flags is the second argument of open, typically one of:
  //  * O_WRONLY | O_CREAT | O_TRUNC
  //  * O_WRONLY | O_CREAT | O_APPEND
  //
  // flags must include O_WRONLY or O_RDWR.
  explicit FdStreamWriter(absl::string_view filename, int flags,
                          Options options = Options());

  FdStreamWriter(FdStreamWriter&& that) noexcept;
  FdStreamWriter& operator=(FdStreamWriter&& that) noexcept;

  // Returns the object providing and possibly owning the fd being written to.
  // If the fd is owned then changed to -1 by Close(), otherwise unchanged.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  int dest_fd() const override { return dest_.ptr(); }

 protected:
  void Done() override;

 private:
  // The object providing and possibly owning the fd being written to.
  Dependency<int, Dest> dest_;
};

// Implementation details follow.

namespace internal {

inline FdWriterCommon::FdWriterCommon(FdWriterCommon&& that) noexcept
    : BufferedWriter(std::move(that)),
      filename_(absl::exchange(that.filename_, std::string())),
      error_code_(absl::exchange(that.error_code_, 0)) {}

inline FdWriterCommon& FdWriterCommon::operator=(
    FdWriterCommon&& that) noexcept {
  BufferedWriter::operator=(std::move(that));
  filename_ = absl::exchange(that.filename_, std::string());
  error_code_ = absl::exchange(that.error_code_, 0);
  return *this;
}

}  // namespace internal

inline FdWriterBase::FdWriterBase(FdWriterBase&& that) noexcept
    : FdWriterCommon(std::move(that)),
      sync_pos_(absl::exchange(that.sync_pos_, false)) {}

inline FdWriterBase& FdWriterBase::operator=(FdWriterBase&& that) noexcept {
  FdWriterCommon::operator=(std::move(that));
  sync_pos_ = absl::exchange(that.sync_pos_, false);
  return *this;
}

inline FdStreamWriterBase::FdStreamWriterBase(
    FdStreamWriterBase&& that) noexcept
    : FdWriterCommon(std::move(that)) {}

inline FdStreamWriterBase& FdStreamWriterBase::operator=(
    FdStreamWriterBase&& that) noexcept {
  FdWriterCommon::operator=(std::move(that));
  return *this;
}

template <typename Dest>
FdWriter<Dest>::FdWriter(type_identity_t<Dest> dest, Options options)
    : FdWriterBase(options.buffer_size_, options.sync_pos_),
      dest_(std::move(dest)) {
  RIEGELI_ASSERT_GE(dest_.ptr(), 0)
      << "Failed precondition of FdWriter<Dest>::FdWriter(Dest): "
         "negative file descriptor";
  SetFilename(dest_.ptr());
  Initialize(O_WRONLY | O_APPEND, dest_.ptr());
}

template <typename Dest>
FdWriter<Dest>::FdWriter(absl::string_view filename, int flags, Options options)
    : FdWriterBase(options.buffer_size_, options.sync_pos_) {
  RIEGELI_ASSERT((flags & O_ACCMODE) == O_WRONLY ||
                 (flags & O_ACCMODE) == O_RDWR)
      << "Failed precondition of FdWriter::FdWriter(string_view): "
         "flags must include O_WRONLY or O_RDWR";
  const int dest = OpenFd(filename, flags, options.permissions_);
  if (ABSL_PREDICT_TRUE(dest >= 0)) {
    dest_ = Dependency<int, Dest>(Dest(dest));
    Initialize(flags, dest_.ptr());
  }
}

template <typename Dest>
inline FdWriter<Dest>::FdWriter(FdWriter&& that) noexcept
    : FdWriterBase(std::move(that)), dest_(std::move(that.dest_)) {}

template <typename Dest>
inline FdWriter<Dest>& FdWriter<Dest>::operator=(FdWriter&& that) noexcept {
  FdWriterBase::operator=(std::move(that));
  dest_ = std::move(that.dest_);
  return *this;
}

template <typename Dest>
void FdWriter<Dest>::Done() {
  if (ABSL_PREDICT_TRUE(PushInternal())) SyncPos(dest_.ptr());
  FdWriterBase::Done();
  if (dest_.kIsOwning() && dest_.ptr() >= 0) {
    const int dest = dest_.Release();
    if (ABSL_PREDICT_FALSE(internal::CloseFd(dest) < 0) &&
        ABSL_PREDICT_TRUE(healthy())) {
      FailOperation(internal::CloseFunctionName());
    }
  }
}

template <typename Dest>
FdStreamWriter<Dest>::FdStreamWriter(type_identity_t<Dest> dest,
                                     Options options)
    : FdStreamWriterBase(options.buffer_size_), dest_(std::move(dest)) {
  RIEGELI_ASSERT_GE(dest_.ptr(), 0)
      << "Failed precondition of FdStreamWriter<Dest>::FdStreamWriter(Dest): "
         "negative file descriptor";
  RIEGELI_ASSERT(options.assumed_pos_.has_value())
      << "Failed precondition of FdStreamWriter<Dest>::FdStreamWriter(Dest): "
         "assumed file position must be specified "
         "if FdStreamWriter does not open the file";
  SetFilename(dest_.ptr());
  start_pos_ = *options.assumed_pos_;
}

template <typename Dest>
FdStreamWriter<Dest>::FdStreamWriter(absl::string_view filename, int flags,
                                     Options options)
    : FdStreamWriterBase(options.buffer_size_) {
  RIEGELI_ASSERT((flags & O_ACCMODE) == O_WRONLY ||
                 (flags & O_ACCMODE) == O_RDWR)
      << "Failed precondition of FdStreamWriter::FdStreamWriter(string_view): "
         "flags must include O_WRONLY or O_RDWR";
  const int dest = OpenFd(filename, flags, options.permissions_);
  if (ABSL_PREDICT_TRUE(dest >= 0)) {
    dest_ = Dependency<int, Dest>(Dest(dest));
    if (options.assumed_pos_.has_value()) {
      start_pos_ = *options.assumed_pos_;
    } else {
      Initialize(flags, dest_.ptr());
    }
  }
}

template <typename Dest>
inline FdStreamWriter<Dest>::FdStreamWriter(FdStreamWriter&& that) noexcept
    : FdStreamWriterBase(std::move(that)), dest_(std::move(that.dest_)) {}

template <typename Dest>
inline FdStreamWriter<Dest>& FdStreamWriter<Dest>::operator=(
    FdStreamWriter&& that) noexcept {
  FdStreamWriterBase::operator=(std::move(that));
  dest_ = std::move(that.dest_);
  return *this;
}

template <typename Dest>
void FdStreamWriter<Dest>::Done() {
  PushInternal();
  FdStreamWriterBase::Done();
  if (dest_.kIsOwning() && dest_.ptr() >= 0) {
    const int dest = dest_.Release();
    if (ABSL_PREDICT_FALSE(internal::CloseFd(dest) < 0) &&
        ABSL_PREDICT_TRUE(healthy())) {
      FailOperation(internal::CloseFunctionName());
    }
  }
}

extern template class FdWriter<OwnedFd>;
extern template class FdWriter<int>;
extern template class FdStreamWriter<OwnedFd>;
extern template class FdStreamWriter<int>;

}  // namespace riegeli

#endif  // RIEGELI_BYTES_FD_WRITER_H_
