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
#include <stdint.h>
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
#include "riegeli/base/object.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/fd_dependency.h"
#include "riegeli/bytes/fd_internal.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

template <typename Src>
class FdReader;
class Reader;

// Template parameter independent part of `FdWriter`.
class FdWriterBase : public BufferedWriter {
 public:
  class Options : public BufferOptionsBase<Options> {
   public:
    Options() noexcept {}

    // If `FdWriter` writes to an already open fd, `assumed_filename()` allows
    // to override the filename which is included in failure messages and
    // returned by `filename()`.
    //
    // If this is `absl::nullopt`, then "/dev/stdin", "/dev/stdout",
    // "/dev/stderr", or "/proc/self/fd/<fd>" is assumed.
    //
    // If `FdWriter` opens a fd with a filename, `assumed_filename()` has no
    // effect.
    //
    // Default: `absl::nullopt`.
    Options& set_assumed_filename(
        absl::optional<absl::string_view> assumed_filename) & {
      if (assumed_filename == absl::nullopt) {
        assumed_filename_ = absl::nullopt;
      } else {
        // TODO: When `absl::string_view` becomes C++17
        // `std::string_view`: `assumed_filename_.emplace(*assumed_filename)`
        assumed_filename_.emplace(assumed_filename->data(),
                                  assumed_filename->size());
      }
      return *this;
    }
    Options&& set_assumed_filename(
        absl::optional<absl::string_view> assumed_filename) && {
      return std::move(set_assumed_filename(assumed_filename));
    }
    absl::optional<std::string>& assumed_filename() {
      return assumed_filename_;
    }
    const absl::optional<std::string>& assumed_filename() const {
      return assumed_filename_;
    }

    // If `FdWriter` opens a fd with a filename, `mode()` is the second argument
    // of `open()` and specifies the open mode and flags, typically one of:
    //  * `O_WRONLY | O_CREAT | O_TRUNC`
    //  * `O_WRONLY | O_CREAT | O_APPEND`
    //
    // It must include either `O_WRONLY` or `O_RDWR`.
    //
    // If `FdWriter` reads from an already open fd, `mode()` has no effect.
    //
    // Default: `O_WRONLY | O_CREAT | O_TRUNC`.
    Options& set_mode(int mode) & {
      mode_ = mode;
      return *this;
    }
    Options&& set_mode(int mode) && { return std::move(set_mode(mode)); }
    int mode() const { return mode_; }

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

   private:
    absl::optional<std::string> assumed_filename_;
    int mode_ = O_WRONLY | O_CREAT | O_TRUNC;
    mode_t permissions_ = 0666;
    absl::optional<Position> assumed_pos_;
    absl::optional<Position> independent_pos_;
  };

  // Returns the fd being written to. If the fd is owned then changed to -1 by
  // `Close()`, otherwise unchanged.
  virtual int dest_fd() const = 0;

  // Returns the original name of the file being written to. Unchanged by
  // `Close()`.
  absl::string_view filename() const { return filename_; }

  bool SupportsRandomAccess() override { return supports_random_access(); }
  bool SupportsReadMode() override {
    return supports_read_mode_ && supports_random_access();
  }

 protected:
  explicit FdWriterBase(Closed) noexcept : BufferedWriter(kClosed) {}

  explicit FdWriterBase(const BufferOptions& buffer_options);

  FdWriterBase(FdWriterBase&& that) noexcept;
  FdWriterBase& operator=(FdWriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset(const BufferOptions& buffer_options);
  void Initialize(int dest, absl::optional<std::string>&& assumed_filename,
                  absl::optional<Position> assumed_pos,
                  absl::optional<Position> independent_pos);
  int OpenFd(absl::string_view filename, int mode, mode_t permissions);
  void InitializePos(int dest, absl::optional<Position> assumed_pos,
                     absl::optional<Position> independent_pos);
  void InitializePos(int dest, int flags, absl::optional<Position> assumed_pos,
                     absl::optional<Position> independent_pos);
  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation);
  bool supports_random_access();

  void Done() override;
  absl::Status AnnotateStatusImpl(absl::Status status) override;
  bool WriteInternal(absl::string_view src) override;
  bool FlushImpl(FlushType flush_type) override;
  bool FlushBehindBuffer(absl::string_view src, FlushType flush_type) override;
  bool SeekBehindBuffer(Position new_pos) override;
  absl::optional<Position> SizeBehindBuffer() override;
  bool TruncateBehindBuffer(Position new_size) override;
  Reader* ReadModeBehindBuffer(Position initial_pos) override;

 private:
  // Encodes a `bool` or a marker that the value is not fully resolved yet.
  enum class LazyBoolState : uint8_t { kFalse, kTrue, kUnknown };

  bool WriteMode();
  bool SeekInternal(int dest, Position new_pos);

  std::string filename_;
  bool has_independent_pos_ = false;
  LazyBoolState supports_random_access_ = LazyBoolState::kFalse;
  bool supports_read_mode_ = false;

  AssociatedReader<FdReader<UnownedFd>> associated_reader_;
  bool read_mode_ = false;

  // Invariant: `start_pos() <= std::numeric_limits<off_t>::max()`
};

// A `Writer` which writes to a file descriptor.
//
// The fd must support:
//  * `fcntl()`     - for the constructor from fd,
//                    if `Options::assumed_pos() == absl::nullopt`
//  * `close()`     - if the fd is owned
//  * `write()`     - if `Options::independent_pos() == absl::nullopt`
//  * `pwrite()`    - if `Options::independent_pos() != absl::nullopt`
//  * `lseek()`     - for `Seek()`, `Size()`, or `Truncate()`,
//                    if `Options::independent_pos() == absl::nullopt`
//  * `fstat()`     - for `Seek()`, `Size()`, or `Truncate()`
//  * `fsync()`     - for `Flush(FlushType::kFromMachine)`
//  * `ftruncate()` - for `Truncate()`
//  * `read()`      - for `ReadMode()`
//                    if `Options::independent_pos() == absl::nullopt`
//                    (fd must be opened with `O_RDWR`)
//  * `pread()`     - for `ReadMode()`
//                    if `Options::independent_pos() != absl::nullopt`
//                    (fd must be opened with `O_RDWR`)
//
// `FdWriter` supports random access if
// `Options::assumed_pos() == absl::nullopt` and the fd supports random access
// (this is assumed if `Options::independent_pos() != absl::nullopt`, otherwise
// this is checked by calling `lseek(SEEK_END)`).
//
// On Linux, some virtual file systems ("/proc", "/sys") contain files with
// contents generated on the fly when the files are read. The files appear as
// regular files, with an apparent size of 0 or 4096, and random access is only
// partially supported. `FdWriter` properly detects lack of random access for
// "/proc" files; for "/sys" files this is detected only if the filename seen by
// `FdWriter` starts with "/sys/". An explicit
// `FdWriterBase::Options().set_assumed_pos(0)` can be used to disable random
// access for such files.
//
// `FdWriter` supports `ReadMode()` if it supports random access and the fd was
// opened with `O_RDWR`.
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
  explicit FdWriter(Closed) noexcept : FdWriterBase(kClosed) {}

  // Will write to the fd provided by `dest`.
  explicit FdWriter(const Dest& dest, Options options = Options());
  explicit FdWriter(Dest&& dest, Options options = Options());
  explicit FdWriter(int dest, Options options = Options());

  // Will write to the fd provided by a `Dest` constructed from elements of
  // `dest_args`. This avoids constructing a temporary `Dest` and moving from
  // it.
  template <typename... DestArgs>
  explicit FdWriter(std::tuple<DestArgs...> dest_args,
                    Options options = Options());

  // Opens a file for writing.
  //
  // If opening the file fails, `FdWriter` will be failed and closed.
  //
  // This constructor is present only if `Dest` is `OwnedFd`.
  template <
      typename DependentDest = Dest,
      std::enable_if_t<std::is_same<DependentDest, OwnedFd>::value, int> = 0>
  explicit FdWriter(absl::string_view filename, Options options = Options());

  FdWriter(FdWriter&& that) noexcept;
  FdWriter& operator=(FdWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `FdWriter`. This avoids
  // constructing a temporary `FdWriter` and moving from it.
  void Reset(Closed);
  void Reset(const Dest& dest, Options options = Options());
  void Reset(Dest&& dest, Options options = Options());
  void Reset(int dest, Options options = Options());
  template <typename... DestArgs>
  void Reset(std::tuple<DestArgs...> dest_args, Options options = Options());
  template <
      typename DependentDest = Dest,
      std::enable_if_t<std::is_same<DependentDest, OwnedFd>::value, int> = 0>
  void Reset(absl::string_view filename, Options options = Options());

  // Returns the object providing and possibly owning the fd being written to.
  // If the fd is owned then changed to -1 by `Close()`, otherwise unchanged.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  int dest_fd() const override { return dest_.get(); }

 protected:
  using FdWriterBase::Initialize;
  void Initialize(absl::string_view filename, Options&& options);

  void Done() override;

 private:
  // The object providing and possibly owning the fd being written to.
  Dependency<int, Dest> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit FdWriter(Closed)->FdWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit FdWriter(const Dest& dest,
                  FdWriterBase::Options options = FdWriterBase::Options())
    -> FdWriter<std::conditional_t<
        std::is_convertible<const Dest&, int>::value ||
            std::is_convertible<const Dest&, absl::string_view>::value,
        OwnedFd, std::decay_t<Dest>>>;
template <typename Dest>
explicit FdWriter(Dest&& dest,
                  FdWriterBase::Options options = FdWriterBase::Options())
    -> FdWriter<std::conditional_t<
        std::is_convertible<Dest&&, int>::value ||
            std::is_convertible<Dest&&, absl::string_view>::value,
        OwnedFd, std::decay_t<Dest>>>;
template <typename... DestArgs>
explicit FdWriter(std::tuple<DestArgs...> dest_args,
                  FdWriterBase::Options options = FdWriterBase::Options())
    -> FdWriter<DeleteCtad<std::tuple<DestArgs...>>>;
#endif

// Implementation details follow.

inline FdWriterBase::FdWriterBase(const BufferOptions& buffer_options)
    : BufferedWriter(buffer_options) {}

inline FdWriterBase::FdWriterBase(FdWriterBase&& that) noexcept
    : BufferedWriter(static_cast<BufferedWriter&&>(that)),
      filename_(std::exchange(that.filename_, std::string())),
      has_independent_pos_(that.has_independent_pos_),
      supports_random_access_(that.supports_random_access_),
      supports_read_mode_(that.supports_read_mode_),
      associated_reader_(std::move(that.associated_reader_)),
      read_mode_(that.read_mode_) {}

inline FdWriterBase& FdWriterBase::operator=(FdWriterBase&& that) noexcept {
  BufferedWriter::operator=(static_cast<BufferedWriter&&>(that));
  filename_ = std::exchange(that.filename_, std::string());
  has_independent_pos_ = that.has_independent_pos_;
  supports_random_access_ = that.supports_random_access_;
  supports_read_mode_ = that.supports_read_mode_;
  associated_reader_ = std::move(that.associated_reader_);
  read_mode_ = that.read_mode_;
  return *this;
}

inline void FdWriterBase::Reset(Closed) {
  BufferedWriter::Reset(kClosed);
  filename_ = std::string();
  has_independent_pos_ = false;
  supports_random_access_ = LazyBoolState::kFalse;
  supports_read_mode_ = false;
  associated_reader_.Reset();
  read_mode_ = false;
}

inline void FdWriterBase::Reset(const BufferOptions& buffer_options) {
  BufferedWriter::Reset(buffer_options);
  // `filename_` will be set by `Initialize()` or `OpenFd()`.
  has_independent_pos_ = false;
  supports_random_access_ = LazyBoolState::kFalse;
  supports_read_mode_ = false;
  associated_reader_.Reset();
  read_mode_ = false;
}

template <typename Dest>
inline FdWriter<Dest>::FdWriter(const Dest& dest, Options options)
    : FdWriterBase(options.buffer_options()), dest_(dest) {
  Initialize(dest_.get(), std::move(options.assumed_filename()),
             options.assumed_pos(), options.independent_pos());
}

template <typename Dest>
inline FdWriter<Dest>::FdWriter(Dest&& dest, Options options)
    : FdWriterBase(options.buffer_options()), dest_(std::move(dest)) {
  Initialize(dest_.get(), std::move(options.assumed_filename()),
             options.assumed_pos(), options.independent_pos());
}

template <typename Dest>
inline FdWriter<Dest>::FdWriter(int dest, Options options)
    : FdWriter(std::forward_as_tuple(dest), std::move(options)) {}

template <typename Dest>
template <typename... DestArgs>
inline FdWriter<Dest>::FdWriter(std::tuple<DestArgs...> dest_args,
                                Options options)
    : FdWriterBase(options.buffer_options()), dest_(std::move(dest_args)) {
  Initialize(dest_.get(), std::move(options.assumed_filename()),
             options.assumed_pos(), options.independent_pos());
}

template <typename Dest>
template <typename DependentDest,
          std::enable_if_t<std::is_same<DependentDest, OwnedFd>::value, int>>
inline FdWriter<Dest>::FdWriter(absl::string_view filename, Options options)
    : FdWriterBase(options.buffer_options()) {
  Initialize(filename, std::move(options));
}

template <typename Dest>
inline FdWriter<Dest>::FdWriter(FdWriter&& that) noexcept
    : FdWriterBase(static_cast<FdWriterBase&&>(that)),
      dest_(std::move(that.dest_)) {}

template <typename Dest>
inline FdWriter<Dest>& FdWriter<Dest>::operator=(FdWriter&& that) noexcept {
  FdWriterBase::operator=(static_cast<FdWriterBase&&>(that));
  dest_ = std::move(that.dest_);
  return *this;
}

template <typename Dest>
inline void FdWriter<Dest>::Reset(Closed) {
  FdWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void FdWriter<Dest>::Reset(const Dest& dest, Options options) {
  FdWriterBase::Reset(options.buffer_options());
  dest_.Reset(dest);
  Initialize(dest_.get(), std::move(options.assumed_filename()),
             options.assumed_pos(), options.independent_pos());
}

template <typename Dest>
inline void FdWriter<Dest>::Reset(Dest&& dest, Options options) {
  FdWriterBase::Reset(options.buffer_options());
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), std::move(options.assumed_filename()),
             options.assumed_pos(), options.independent_pos());
}

template <typename Dest>
inline void FdWriter<Dest>::Reset(int dest, Options options) {
  Reset(std::forward_as_tuple(dest), std::move(options));
}

template <typename Dest>
template <typename... DestArgs>
inline void FdWriter<Dest>::Reset(std::tuple<DestArgs...> dest_args,
                                  Options options) {
  FdWriterBase::Reset(options.buffer_options());
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get(), std::move(options.assumed_filename()),
             options.assumed_pos(), options.independent_pos());
}

template <typename Dest>
template <typename DependentDest,
          std::enable_if_t<std::is_same<DependentDest, OwnedFd>::value, int>>
inline void FdWriter<Dest>::Reset(absl::string_view filename, Options options) {
  FdWriterBase::Reset(options.buffer_options());
  Initialize(filename, std::move(options));
}

template <typename Dest>
void FdWriter<Dest>::Initialize(absl::string_view filename, Options&& options) {
  const int dest = OpenFd(filename, options.mode(), options.permissions());
  if (ABSL_PREDICT_FALSE(dest < 0)) return;
  dest_.Reset(std::forward_as_tuple(dest));
  InitializePos(dest_.get(), options.mode(), options.assumed_pos(),
                options.independent_pos());
}

template <typename Dest>
void FdWriter<Dest>::Done() {
  FdWriterBase::Done();
  {
    const int dest = dest_.Release();
    if (dest >= 0) {
      if (ABSL_PREDICT_FALSE(fd_internal::Close(dest) < 0) &&
          ABSL_PREDICT_TRUE(ok())) {
        FailOperation(fd_internal::kCloseFunctionName);
      }
    } else {
      RIEGELI_ASSERT(!dest_.is_owning())
          << "The dependency type does not support closing the fd";
    }
  }
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_FD_WRITER_H_
