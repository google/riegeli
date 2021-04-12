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
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/dependency.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/fd_dependency.h"

namespace riegeli {

namespace internal {

// Implementation shared between `FdWriter` and `FdStreamWriter`.
class FdWriterCommon : public BufferedWriter {
 public:
  // Returns the fd being written to. If the fd is owned then changed to -1 by
  // `Close()`, otherwise unchanged.
  virtual int dest_fd() const = 0;

  // Returns the original name of the file being written to (or "/dev/stdout",
  // "/dev/stderr", or "/proc/self/fd/<fd>" if fd was given). Unchanged by
  // `Close()`.
  const std::string& filename() const { return filename_; }

  using BufferedWriter::Fail;
  bool Fail(absl::Status status) override;

 protected:
  FdWriterCommon() noexcept {}

  explicit FdWriterCommon(size_t buffer_size);

  FdWriterCommon(FdWriterCommon&& that) noexcept;
  FdWriterCommon& operator=(FdWriterCommon&& that) noexcept;

  void Reset();
  void Reset(size_t buffer_size);
  void SetFilename(int dest);
  int OpenFd(absl::string_view filename, int flags, mode_t permissions);
  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation);

  std::string filename_;
};

}  // namespace internal

// Template parameter independent part of `FdWriter`.
class FdWriterBase : public internal::FdWriterCommon {
 public:
  class Options {
   public:
    Options() noexcept {}

    // Permissions to use in case a new file is created (9 bits). The effective
    // permissions are modified by the process's umask.
    //
    // Default: `0666`.
    Options& set_permissions(mode_t permissions) & {
      permissions_ = permissions;
      return *this;
    }
    Options&& set_permissions(mode_t permissions) && {
      return std::move(set_permissions(permissions));
    }
    mode_t permissions() const { return permissions_; }

    // If `absl::nullopt`, the current position reported by `pos()` corresponds
    // to the current fd position if possible, otherwise 0 is assumed as the
    // initial position. Random access is supported if the fd supports random
    // access.
    //
    // If not `absl::nullopt`, this position is assumed initially, to be
    // reported by `pos()`. It does not need to correspond to the current fd
    // position. Random access is not supported.
    //
    // `assumed_pos()` and `independent_pos()` must not be both set.
    //
    // Default: `absl::nullopt`.
    Options& set_assumed_pos(absl::optional<Position> assumed_pos) & {
      assumed_pos_ = assumed_pos;
      return *this;
    }
    Options&& set_assumed_pos(absl::optional<Position> assumed_pos) && {
      return std::move(set_assumed_pos(assumed_pos));
    }
    absl::optional<Position> assumed_pos() const { return assumed_pos_; }

    // If `absl::nullopt`, `FdWriter` writes at the current fd position.
    //
    // If not `absl::nullopt`, `FdWriter` writes starting from this position,
    // without disturbing the current fd position. This is useful for multiple
    // writers concurrently writing to disjoint regions of the same file. The fd
    // must support `pwrite()`.
    //
    // `assumed_pos()` and `independent_pos()` must not be both set.
    //
    // Default: `absl::nullopt`.
    Options& set_independent_pos(absl::optional<Position> independent_pos) & {
      independent_pos_ = independent_pos;
      return *this;
    }
    Options&& set_independent_pos(absl::optional<Position> independent_pos) && {
      return std::move(set_independent_pos(independent_pos));
    }
    absl::optional<Position> independent_pos() const {
      return independent_pos_;
    }

    ABSL_DEPRECATED("Use set_independent_pos() instead")
    Options& set_initial_pos(absl::optional<Position> initial_pos) & {
      return set_independent_pos(initial_pos);
    }
    ABSL_DEPRECATED("Use set_independent_pos() instead")
    Options&& set_initial_pos(absl::optional<Position> initial_pos) && {
      return std::move(set_initial_pos(initial_pos));
    }

    // Tunes how much data is buffered before writing to the file.
    //
    // Default: `kDefaultBufferSize` (64K).
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
    size_t buffer_size() const { return buffer_size_; }

   private:
    mode_t permissions_ = 0666;
    absl::optional<Position> assumed_pos_;
    absl::optional<Position> independent_pos_;
    size_t buffer_size_ = kDefaultBufferSize;
  };

  bool Flush(FlushType flush_type) override;
  bool SupportsRandomAccess() override { return supports_random_access_; }
  absl::optional<Position> Size() override;
  bool SupportsTruncate() override { return supports_random_access_; }
  bool Truncate(Position new_size) override;

 protected:
  FdWriterBase() noexcept {}

  explicit FdWriterBase(size_t buffer_size);

  FdWriterBase(FdWriterBase&& that) noexcept;
  FdWriterBase& operator=(FdWriterBase&& that) noexcept;

  void Reset();
  void Reset(size_t buffer_size);
  void Initialize(int dest, absl::optional<Position> assumed_pos,
                  absl::optional<Position> independent_pos);
  void InitializePos(int dest, absl::optional<Position> assumed_pos,
                     absl::optional<Position> independent_pos);
  void InitializePos(int dest, int flags, absl::optional<Position> assumed_pos,
                     absl::optional<Position> independent_pos);
  bool SyncPos(int dest);

  void Done() override;
  bool WriteInternal(absl::string_view src) override;
  bool SeekSlow(Position new_pos) override;

  bool supports_random_access_ = false;
  bool has_independent_pos_ = false;

  // Invariant: `start_pos() <= std::numeric_limits<off_t>::max()`
};

// Template parameter independent part of `FdStreamWriter`.
class FdStreamWriterBase : public internal::FdWriterCommon {
 public:
  class Options {
   public:
    Options() noexcept {}

    // Permissions to use in case a new file is created (9 bits). The effective
    // permissions are modified by the process's umask.
    //
    // Default: `0666`.
    Options& set_permissions(mode_t permissions) & {
      permissions_ = permissions;
      return *this;
    }
    Options&& set_permissions(mode_t permissions) && {
      return std::move(set_permissions(permissions));
    }
    mode_t permissions() const { return permissions_; }

    // If not `absl::nullopt`, this position will be assumed initially, to be
    // reported by `pos()`.
    //
    // If `absl::nullopt`, in the constructor from filename, the position will
    // be assumed to be 0 when not appending, or file size when appending.
    //
    // If `absl::nullopt`, in the constructor from fd, `FdStreamWriter` will
    // initially get the current fd position.
    //
    // In any case writing will start from the current position.
    //
    // Default: `absl::nullopt`.
    Options& set_assumed_pos(absl::optional<Position> assumed_pos) & {
      assumed_pos_ = assumed_pos;
      return *this;
    }
    Options&& set_assumed_pos(absl::optional<Position> assumed_pos) && {
      return std::move(set_assumed_pos(assumed_pos));
    }
    absl::optional<Position> assumed_pos() const { return assumed_pos_; }

    // Tunes how much data is buffered before writing to the file.
    //
    // Default: `kDefaultBufferSize` (64K).
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
    size_t buffer_size() const { return buffer_size_; }

   private:
    mode_t permissions_ = 0666;
    absl::optional<Position> assumed_pos_;
    size_t buffer_size_ = kDefaultBufferSize;
  };

  bool Flush(FlushType flush_type) override;

 protected:
  FdStreamWriterBase() noexcept {}

  explicit FdStreamWriterBase(size_t buffer_size)
      : FdWriterCommon(buffer_size) {}

  FdStreamWriterBase(FdStreamWriterBase&& that) noexcept;
  FdStreamWriterBase& operator=(FdStreamWriterBase&& that) noexcept;

  void Initialize(int dest, absl::optional<Position> assumed_pos);
  void InitializePos(int dest, absl::optional<Position> assumed_pos);
  void InitializePos(int dest, int flags, absl::optional<Position> assumed_pos);

  void Done() override;
  bool WriteInternal(absl::string_view src) override;
};

// A `Writer` which writes to a file descriptor.
//
// The fd must support:
//  * `fcntl()`     - for the constructor from fd,
//                    if `Options::assumed_pos() == absl::nullopt`
//                    and `Options::independent_pos() == absl::nullopt`
//  * `close()`     - if the fd is owned
//  * `write()`     - if `Options::independent_pos() == absl::nullopt`
//  * `pwrite()`    - if `Options::independent_pos() != absl::nullopt`
//  * `lseek()`     - for `Seek()`, `Size()`, or `Truncate()`
//                    if `Options::independent_pos() == absl::nullopt`
//  * `fstat()`     - for `Seek()`, `Size()`, or `Truncate()`
//  * `fsync()`     - for `Flush(FlushType::kFromMachine)`
//  * `ftruncate()` - for `Truncate()`
//
// `FdWriter` supports random access if
// `Options::assumed_pos() == absl::nullopt` and the fd supports random access
// (this is assumed if `Options::independent_pos() != absl::nullopt`, otherwise
// this is checked by calling `lseek()`).
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the fd being written to. `Dest` must support
// `Dependency<int, Dest>`, e.g. `OwnedFd` (owned, default), `UnownedFd`
// (not owned).
//
// By relying on CTAD the template argument can be deduced as `OwnedFd` if the
// first constructor argument is a filename or an `int`, otherwise as the value
// type of the first constructor argument. This requires C++17.
//
// Until the `FdWriter` is closed or no longer used, the fd must not be closed;
// additionally, if `Options::independent_pos() == absl::nullopt`, the fd should
// not have its position changed, except that if random access is not used,
// careful interleaving of multiple writers is possible: `Flush()` is needed
// before switching to another writer, and `pos()` does not take other writers
// into account.
template <typename Dest = OwnedFd>
class FdWriter : public FdWriterBase {
 public:
  // Creates a closed `FdWriter`.
  FdWriter() noexcept {}

  // Will write to the fd provided by `dest`.
  explicit FdWriter(const Dest& dest, Options options = Options());
  explicit FdWriter(Dest&& dest, Options options = Options());

  // Will write to the fd provided by a `Dest` constructed from elements of
  // `dest_args`. This avoids constructing a temporary `Dest` and moving from
  // it.
  template <typename... DestArgs>
  explicit FdWriter(std::tuple<DestArgs...> dest_args,
                    Options options = Options());

  // Opens a file for writing.
  //
  // `flags` is the second argument of `open()`, typically one of:
  //  * `O_WRONLY | O_CREAT | O_TRUNC`
  //  * `O_WRONLY | O_CREAT | O_APPEND`
  //
  // `flags` must include either `O_WRONLY` or `O_RDWR`.
  explicit FdWriter(absl::string_view filename, int flags,
                    Options options = Options());

  FdWriter(FdWriter&& that) noexcept;
  FdWriter& operator=(FdWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `FdWriter`. This avoids
  // constructing a temporary `FdWriter` and moving from it.
  void Reset();
  void Reset(const Dest& dest, Options options = Options());
  void Reset(Dest&& dest, Options options = Options());
  template <typename... DestArgs>
  void Reset(std::tuple<DestArgs...> dest_args, Options options = Options());
  void Reset(absl::string_view filename, int flags,
             Options options = Options());

  // Returns the object providing and possibly owning the fd being written to.
  // If the fd is owned then changed to -1 by `Close()`, otherwise unchanged.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  int dest_fd() const override { return dest_.get(); }

 protected:
  void Done() override;

 private:
  using FdWriterBase::Initialize;
  void Initialize(absl::string_view filename, int flags, mode_t permissions,
                  absl::optional<Position> assumed_pos,
                  absl::optional<Position> independent_pos);

  // The object providing and possibly owning the fd being written to.
  Dependency<int, Dest> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
FdWriter()->FdWriter<DeleteCtad<>>;
template <typename Dest>
explicit FdWriter(const Dest& dest,
                  FdWriterBase::Options options = FdWriterBase::Options())
    -> FdWriter<std::conditional_t<std::is_convertible<const Dest&, int>::value,
                                   OwnedFd, std::decay_t<Dest>>>;
template <typename Dest>
explicit FdWriter(Dest&& dest,
                  FdWriterBase::Options options = FdWriterBase::Options())
    -> FdWriter<std::conditional_t<std::is_convertible<Dest&&, int>::value,
                                   OwnedFd, std::decay_t<Dest>>>;
template <typename... DestArgs>
explicit FdWriter(std::tuple<DestArgs...> dest_args,
                  FdWriterBase::Options options = FdWriterBase::Options())
    -> FdWriter<DeleteCtad<std::tuple<DestArgs...>>>;
explicit FdWriter(absl::string_view filename, int flags,
                  FdWriterBase::Options options = FdWriterBase::Options())
    ->FdWriter<>;
#endif

// A `Writer` which writes to a fd which does not have to support random access.
//
// The fd must support:
//  * `fcntl()` - for the constructor from fd,
//                if `Options::independent_pos() == absl::nullopt`
//  * `close()` - if the fd is owned
//  * `write()`
//  * `lseek()` - for the constructor from fd,
//                if `Options::assumed_pos() == absl::nullopt`
//  * `fstat()` - when opening for appending,
//                if `Options::assumed_pos() == absl::nullopt`
//  * `fsync()` - for Flush(FlushType::kFromMachine)`
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the fd being written to. `Dest` must support
// `Dependency<int, Dest>`, e.g. `OwnedFd` (owned, default),
// `UnownedFd` (not owned).
//
// By relying on CTAD the template argument can be deduced as `OwnedFd` if the
// first constructor argument is a filename or an `int`, otherwise as the value
// type of the first constructor argument. This requires C++17.
//
// The fd must not be closed until the `FdStreamWriter` is closed or no longer
// used. Until then the fd may be accessed, but not concurrently, `Flush()` is
// needed before switching to another writer to the same fd, and `pos()` does
// not take other writers into account.
template <typename Dest = OwnedFd>
ABSL_DEPRECATED("Use FdWriter instead")
class FdStreamWriter : public FdStreamWriterBase {
 public:
  // Creates a closed `FdStreamWriter`.
  FdStreamWriter() noexcept {}

  // Will write to the fd provided by `dest`.
  explicit FdStreamWriter(const Dest& dest, Options options = Options());
  explicit FdStreamWriter(Dest&& dest, Options options = Options());

  // Will write to the fd provided by a `Dest` constructed from elements of
  // `dest_args`. This avoids constructing a temporary `Dest` and moving from
  // it.
  template <typename... DestArgs>
  explicit FdStreamWriter(std::tuple<DestArgs...> dest_args,
                          Options options = Options());

  // Opens a file for writing.
  //
  // `flags` is the second argument of `open()`, typically one of:
  //  * `O_WRONLY | O_CREAT | O_TRUNC`
  //  * `O_WRONLY | O_CREAT | O_APPEND`
  //
  // `flags` must include either `O_WRONLY` or `O_RDWR`.
  explicit FdStreamWriter(absl::string_view filename, int flags,
                          Options options = Options());

  FdStreamWriter(FdStreamWriter&& that) noexcept;
  FdStreamWriter& operator=(FdStreamWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `FdStreamWriter`. This
  // avoids constructing a temporary `FdStreamWriter` and moving from it.
  void Reset();
  void Reset(const Dest& dest, Options options = Options());
  void Reset(Dest&& dest, Options options = Options());
  template <typename... DestArgs>
  void Reset(std::tuple<DestArgs...> dest_args, Options options = Options());
  void Reset(absl::string_view filename, int flags,
             Options options = Options());

  // Returns the object providing and possibly owning the fd being written to.
  // If the fd is owned then changed to -1 by `Close()`, otherwise unchanged.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  int dest_fd() const override { return dest_.get(); }

 protected:
  void Done() override;

 private:
  using FdStreamWriterBase::Initialize;
  void Initialize(absl::string_view filename, int flags, mode_t permissions,
                  absl::optional<Position> assumed_pos);

  // The object providing and possibly owning the fd being written to.
  Dependency<int, Dest> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
FdStreamWriter()->FdStreamWriter<DeleteCtad<>>;
template <typename Dest>
explicit FdStreamWriter(const Dest& dest, FdStreamWriterBase::Options options =
                                              FdStreamWriterBase::Options())
    -> FdStreamWriter<
        std::conditional_t<std::is_convertible<const Dest&, int>::value,
                           OwnedFd, std::decay_t<Dest>>>;
template <typename Dest>
explicit FdStreamWriter(Dest&& dest, FdStreamWriterBase::Options options =
                                         FdStreamWriterBase::Options())
    -> FdStreamWriter<std::conditional_t<
        std::is_convertible<Dest&&, int>::value, OwnedFd, std::decay_t<Dest>>>;
template <typename... DestArgs>
explicit FdStreamWriter(
    std::tuple<DestArgs...> dest_args,
    FdStreamWriterBase::Options options = FdStreamWriterBase::Options())
    -> FdStreamWriter<DeleteCtad<std::tuple<DestArgs...>>>;
explicit FdStreamWriter(
    absl::string_view filename, int flags,
    FdStreamWriterBase::Options options = FdStreamWriterBase::Options())
    ->FdStreamWriter<>;
#endif

// Implementation details follow.

namespace internal {

inline FdWriterCommon::FdWriterCommon(size_t buffer_size)
    : BufferedWriter(buffer_size) {}

inline FdWriterCommon::FdWriterCommon(FdWriterCommon&& that) noexcept
    : BufferedWriter(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      filename_(std::move(that.filename_)) {}

inline FdWriterCommon& FdWriterCommon::operator=(
    FdWriterCommon&& that) noexcept {
  BufferedWriter::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  filename_ = std::move(that.filename_);
  return *this;
}

inline void FdWriterCommon::Reset() {
  BufferedWriter::Reset();
  filename_.clear();
}

inline void FdWriterCommon::Reset(size_t buffer_size) {
  BufferedWriter::Reset(buffer_size);
  // `filename_` will be set by `Initialize()`.
}

}  // namespace internal

inline FdWriterBase::FdWriterBase(size_t buffer_size)
    : FdWriterCommon(buffer_size) {}

inline FdWriterBase::FdWriterBase(FdWriterBase&& that) noexcept
    : FdWriterCommon(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      supports_random_access_(that.supports_random_access_),
      has_independent_pos_(that.has_independent_pos_) {}

inline FdWriterBase& FdWriterBase::operator=(FdWriterBase&& that) noexcept {
  FdWriterCommon::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  supports_random_access_ = that.supports_random_access_;
  has_independent_pos_ = that.has_independent_pos_;
  return *this;
}

inline void FdWriterBase::Reset() {
  FdWriterCommon::Reset();
  supports_random_access_ = false;
  has_independent_pos_ = false;
}

inline void FdWriterBase::Reset(size_t buffer_size) {
  FdWriterCommon::Reset(buffer_size);
  supports_random_access_ = false;
  has_independent_pos_ = false;
}

inline void FdWriterBase::Initialize(int dest,
                                     absl::optional<Position> assumed_pos,
                                     absl::optional<Position> independent_pos) {
  RIEGELI_ASSERT_GE(dest, 0)
      << "Failed precondition of FdWriter: negative file descriptor";
  SetFilename(dest);
  InitializePos(dest, assumed_pos, independent_pos);
}

inline FdStreamWriterBase::FdStreamWriterBase(
    FdStreamWriterBase&& that) noexcept
    : FdWriterCommon(std::move(that)) {}

inline FdStreamWriterBase& FdStreamWriterBase::operator=(
    FdStreamWriterBase&& that) noexcept {
  FdWriterCommon::operator=(std::move(that));
  return *this;
}

inline void FdStreamWriterBase::Initialize(
    int dest, absl::optional<Position> assumed_pos) {
  RIEGELI_ASSERT_GE(dest, 0)
      << "Failed precondition of FdStreamWriter: negative file descriptor";
  SetFilename(dest);
  InitializePos(dest, assumed_pos);
}

template <typename Dest>
inline FdWriter<Dest>::FdWriter(const Dest& dest, Options options)
    : FdWriterBase(options.buffer_size()), dest_(dest) {
  Initialize(dest_.get(), options.assumed_pos(), options.independent_pos());
}

template <typename Dest>
inline FdWriter<Dest>::FdWriter(Dest&& dest, Options options)
    : FdWriterBase(options.buffer_size()), dest_(std::move(dest)) {
  Initialize(dest_.get(), options.assumed_pos(), options.independent_pos());
}

template <typename Dest>
template <typename... DestArgs>
inline FdWriter<Dest>::FdWriter(std::tuple<DestArgs...> dest_args,
                                Options options)
    : FdWriterBase(options.buffer_size()), dest_(std::move(dest_args)) {
  Initialize(dest_.get(), options.assumed_pos(), options.independent_pos());
}

template <typename Dest>
inline FdWriter<Dest>::FdWriter(absl::string_view filename, int flags,
                                Options options)
    : FdWriterBase(options.buffer_size()) {
  Initialize(filename, flags, options.permissions(), options.assumed_pos(),
             options.independent_pos());
}

template <typename Dest>
inline FdWriter<Dest>::FdWriter(FdWriter&& that) noexcept
    : FdWriterBase(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      dest_(std::move(that.dest_)) {}

template <typename Dest>
inline FdWriter<Dest>& FdWriter<Dest>::operator=(FdWriter&& that) noexcept {
  FdWriterBase::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  dest_ = std::move(that.dest_);
  return *this;
}

template <typename Dest>
inline void FdWriter<Dest>::Reset() {
  FdWriterBase::Reset();
  dest_.Reset();
}

template <typename Dest>
inline void FdWriter<Dest>::Reset(const Dest& dest, Options options) {
  FdWriterBase::Reset(options.buffer_size());
  dest_.Reset(dest);
  Initialize(dest_.get(), options.assumed_pos(), options.independent_pos());
}

template <typename Dest>
inline void FdWriter<Dest>::Reset(Dest&& dest, Options options) {
  FdWriterBase::Reset(options.buffer_size());
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), options.assumed_pos(), options.independent_pos());
}

template <typename Dest>
template <typename... DestArgs>
inline void FdWriter<Dest>::Reset(std::tuple<DestArgs...> dest_args,
                                  Options options) {
  FdWriterBase::Reset(options.buffer_size());
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get(), options.assumed_pos(), options.independent_pos());
}

template <typename Dest>
inline void FdWriter<Dest>::Reset(absl::string_view filename, int flags,
                                  Options options) {
  FdWriterBase::Reset(options.buffer_size());
  dest_.Reset();  // In case `OpenFd()` fails.
  Initialize(filename, flags, options.permissions(), options.assumed_pos(),
             options.independent_pos());
}

template <typename Dest>
inline void FdWriter<Dest>::Initialize(
    absl::string_view filename, int flags, mode_t permissions,
    absl::optional<Position> assumed_pos,
    absl::optional<Position> independent_pos) {
  RIEGELI_ASSERT((flags & O_ACCMODE) == O_WRONLY ||
                 (flags & O_ACCMODE) == O_RDWR)
      << "Failed precondition of FdWriter: "
         "flags must include either O_WRONLY or O_RDWR";
  const int dest = OpenFd(filename, flags, permissions);
  if (ABSL_PREDICT_FALSE(dest < 0)) return;
  dest_.Reset(std::forward_as_tuple(dest));
  InitializePos(dest_.get(), flags, assumed_pos, independent_pos);
}

template <typename Dest>
void FdWriter<Dest>::Done() {
  FdWriterBase::Done();
  if (dest_.is_owning()) {
    const int dest = dest_.Release();
    if (ABSL_PREDICT_FALSE(internal::CloseFd(dest) < 0) &&
        ABSL_PREDICT_TRUE(healthy())) {
      FailOperation(internal::CloseFunctionName());
    }
  }
}

template <typename Dest>
inline FdStreamWriter<Dest>::FdStreamWriter(const Dest& dest, Options options)
    : FdStreamWriterBase(options.buffer_size()), dest_(dest) {
  Initialize(dest_.get(), options.assumed_pos());
}

template <typename Dest>
inline FdStreamWriter<Dest>::FdStreamWriter(Dest&& dest, Options options)
    : FdStreamWriterBase(options.buffer_size()), dest_(std::move(dest)) {
  Initialize(dest_.get(), options.assumed_pos());
}

template <typename Dest>
template <typename... DestArgs>
inline FdStreamWriter<Dest>::FdStreamWriter(std::tuple<DestArgs...> dest_args,
                                            Options options)
    : FdStreamWriterBase(options.buffer_size()), dest_(std::move(dest_args)) {
  Initialize(dest_.get(), options.assumed_pos());
}

template <typename Dest>
inline FdStreamWriter<Dest>::FdStreamWriter(absl::string_view filename,
                                            int flags, Options options)
    : FdStreamWriterBase(options.buffer_size()) {
  Initialize(filename, flags, options.permissions(), options.assumed_pos());
}

template <typename Dest>
inline FdStreamWriter<Dest>::FdStreamWriter(FdStreamWriter&& that) noexcept
    : FdStreamWriterBase(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      dest_(std::move(that.dest_)) {}

template <typename Dest>
inline FdStreamWriter<Dest>& FdStreamWriter<Dest>::operator=(
    FdStreamWriter&& that) noexcept {
  FdStreamWriterBase::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  dest_ = std::move(that.dest_);
  return *this;
}

template <typename Dest>
inline void FdStreamWriter<Dest>::Reset() {
  FdStreamWriterBase::Reset();
  dest_.Reset();
}

template <typename Dest>
inline void FdStreamWriter<Dest>::Reset(const Dest& dest, Options options) {
  FdStreamWriterBase::Reset(options.buffer_size());
  dest_.Reset(dest);
  Initialize(dest_.get(), options.assumed_pos());
}

template <typename Dest>
inline void FdStreamWriter<Dest>::Reset(Dest&& dest, Options options) {
  FdStreamWriterBase::Reset(options.buffer_size());
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), options.assumed_pos());
}

template <typename Dest>
template <typename... DestArgs>
inline void FdStreamWriter<Dest>::Reset(std::tuple<DestArgs...> dest_args,
                                        Options options) {
  FdStreamWriterBase::Reset(options.buffer_size());
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get(), options.assumed_pos());
}

template <typename Dest>
inline void FdStreamWriter<Dest>::Reset(absl::string_view filename, int flags,
                                        Options options) {
  FdStreamWriterBase::Reset(options.buffer_size());
  dest_.Reset();  // In case `OpenFd()` fails.
  Initialize(filename, flags, options.permissions(), options.assumed_pos());
}

template <typename Dest>
inline void FdStreamWriter<Dest>::Initialize(
    absl::string_view filename, int flags, mode_t permissions,
    absl::optional<Position> assumed_pos) {
  RIEGELI_ASSERT((flags & O_ACCMODE) == O_WRONLY ||
                 (flags & O_ACCMODE) == O_RDWR)
      << "Failed precondition of FdStreamWriter: "
         "flags must include either O_WRONLY or O_RDWR";
  const int dest = OpenFd(filename, flags, permissions);
  if (ABSL_PREDICT_FALSE(dest < 0)) return;
  dest_.Reset(std::forward_as_tuple(dest));
  InitializePos(dest_.get(), flags, assumed_pos);
}

template <typename Dest>
void FdStreamWriter<Dest>::Done() {
  FdStreamWriterBase::Done();
  if (dest_.is_owning()) {
    const int dest = dest_.Release();
    if (ABSL_PREDICT_FALSE(internal::CloseFd(dest) < 0) &&
        ABSL_PREDICT_TRUE(healthy())) {
      FailOperation(internal::CloseFunctionName());
    }
  }
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_FD_WRITER_H_
