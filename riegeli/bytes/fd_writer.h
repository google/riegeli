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
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/fd_dependency.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

template <typename Src>
class FdReader;

// Template parameter independent part of `FdWriter`.
class FdWriterBase : public BufferedWriter {
 public:
  class Options {
   public:
    Options() noexcept {}

    // If `FdWriter` writes to an already open fd, `set_assumed_filename()`
    // allows to override the filename which is included in failure messages and
    // returned by `filename()`.
    //
    // If this is `absl::nullopt`, then "/dev/stdin", "/dev/stdout",
    // "/dev/stderr", or "/proc/self/fd/<fd>" is assumed.
    //
    // If `FdWriter` writes to a filename, `set_assumed_filename()` has no
    // effect.
    //
    // Default: `absl::nullopt`
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
    absl::optional<std::string> assumed_filename_;
    mode_t permissions_ = 0666;
    absl::optional<Position> assumed_pos_;
    absl::optional<Position> independent_pos_;
    size_t buffer_size_ = kDefaultBufferSize;
  };

  // Returns the fd being written to. If the fd is owned then changed to -1 by
  // `Close()`, otherwise unchanged.
  virtual int dest_fd() const = 0;

  // Returns the original name of the file being written to. Unchanged by
  // `Close()`.
  const std::string& filename() const { return filename_; }

  bool SupportsRandomAccess() override { return supports_random_access_; }
  bool SupportsReadMode() override { return supports_read_mode_; }

 protected:
  explicit FdWriterBase(Closed) noexcept : BufferedWriter(kClosed) {}

  explicit FdWriterBase(size_t buffer_size);

  FdWriterBase(FdWriterBase&& that) noexcept;
  FdWriterBase& operator=(FdWriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset(size_t buffer_size);
  void Initialize(int dest, absl::optional<std::string>&& assumed_filename,
                  absl::optional<Position> assumed_pos,
                  absl::optional<Position> independent_pos);
  int OpenFd(absl::string_view filename, int flags, mode_t permissions);
  void InitializePos(int dest, absl::optional<Position> assumed_pos,
                     absl::optional<Position> independent_pos);
  void InitializePos(int dest, int flags, absl::optional<Position> assumed_pos,
                     absl::optional<Position> independent_pos);
  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation);

  void Done() override;
  void DefaultAnnotateStatus() override;
  bool WriteInternal(absl::string_view src) override;
  bool FlushImpl(FlushType flush_type) override;
  bool SeekBehindBuffer(Position new_pos) override;
  absl::optional<Position> SizeBehindBuffer() override;
  bool TruncateBehindBuffer(Position new_size) override;
  Reader* ReadModeBehindBuffer(Position initial_pos) override;
  bool WriteModeImpl() override;

 private:
  bool SeekInternal(int dest, Position new_pos);

  std::string filename_;
  bool supports_random_access_ = false;
  bool has_independent_pos_ = false;
  bool supports_read_mode_ = false;

  AssociatedReader<FdReader<UnownedFd>> associated_reader_;

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
// this is checked by calling `lseek()`).
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

  ABSL_DEPRECATED("Use kClosed constructor instead")
  FdWriter() noexcept : FdWriter(kClosed) {}

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
  //
  // If opening the file fails, `FdWriter` will be failed and closed.
  explicit FdWriter(absl::string_view filename, int flags,
                    Options options = Options());

  FdWriter(FdWriter&& that) noexcept;
  FdWriter& operator=(FdWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `FdWriter`. This avoids
  // constructing a temporary `FdWriter` and moving from it.
  void Reset(Closed);
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
  using FdWriterBase::Initialize;
  void Initialize(absl::string_view filename, int flags, Options&& options);

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

// Implementation details follow.

inline FdWriterBase::FdWriterBase(size_t buffer_size)
    : BufferedWriter(buffer_size) {}

inline FdWriterBase::FdWriterBase(FdWriterBase&& that) noexcept
    : BufferedWriter(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      filename_(std::move(that.filename_)),
      supports_random_access_(that.supports_random_access_),
      has_independent_pos_(that.has_independent_pos_),
      supports_read_mode_(that.supports_read_mode_),
      associated_reader_(std::move(that.associated_reader_)) {}

inline FdWriterBase& FdWriterBase::operator=(FdWriterBase&& that) noexcept {
  BufferedWriter::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  filename_ = std::move(that.filename_);
  supports_random_access_ = that.supports_random_access_;
  has_independent_pos_ = that.has_independent_pos_;
  supports_read_mode_ = that.supports_read_mode_;
  associated_reader_ = std::move(that.associated_reader_);
  return *this;
}

inline void FdWriterBase::Reset(Closed) {
  BufferedWriter::Reset(kClosed);
  filename_ = std::string();
  supports_random_access_ = false;
  has_independent_pos_ = false;
  supports_read_mode_ = false;
  associated_reader_.Reset();
}

inline void FdWriterBase::Reset(size_t buffer_size) {
  BufferedWriter::Reset(buffer_size);
  // `filename_` was set by `OpenFd()` or will be set by `Initialize()`.
  supports_random_access_ = false;
  has_independent_pos_ = false;
  supports_read_mode_ = false;
  associated_reader_.Reset();
}

template <typename Dest>
inline FdWriter<Dest>::FdWriter(const Dest& dest, Options options)
    : FdWriterBase(options.buffer_size()), dest_(dest) {
  Initialize(dest_.get(), std::move(options.assumed_filename()),
             options.assumed_pos(), options.independent_pos());
}

template <typename Dest>
inline FdWriter<Dest>::FdWriter(Dest&& dest, Options options)
    : FdWriterBase(options.buffer_size()), dest_(std::move(dest)) {
  Initialize(dest_.get(), std::move(options.assumed_filename()),
             options.assumed_pos(), options.independent_pos());
}

template <typename Dest>
template <typename... DestArgs>
inline FdWriter<Dest>::FdWriter(std::tuple<DestArgs...> dest_args,
                                Options options)
    : FdWriterBase(options.buffer_size()), dest_(std::move(dest_args)) {
  Initialize(dest_.get(), std::move(options.assumed_filename()),
             options.assumed_pos(), options.independent_pos());
}

template <typename Dest>
inline FdWriter<Dest>::FdWriter(absl::string_view filename, int flags,
                                Options options)
    : FdWriterBase(kClosed) {
  Initialize(filename, flags, std::move(options));
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
inline void FdWriter<Dest>::Reset(Closed) {
  FdWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void FdWriter<Dest>::Reset(const Dest& dest, Options options) {
  FdWriterBase::Reset(options.buffer_size());
  dest_.Reset(dest);
  Initialize(dest_.get(), std::move(options.assumed_filename()),
             options.assumed_pos(), options.independent_pos());
}

template <typename Dest>
inline void FdWriter<Dest>::Reset(Dest&& dest, Options options) {
  FdWriterBase::Reset(options.buffer_size());
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), std::move(options.assumed_filename()),
             options.assumed_pos(), options.independent_pos());
}

template <typename Dest>
template <typename... DestArgs>
inline void FdWriter<Dest>::Reset(std::tuple<DestArgs...> dest_args,
                                  Options options) {
  FdWriterBase::Reset(options.buffer_size());
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get(), std::move(options.assumed_filename()),
             options.assumed_pos(), options.independent_pos());
}

template <typename Dest>
inline void FdWriter<Dest>::Reset(absl::string_view filename, int flags,
                                  Options options) {
  Reset(kClosed);
  Initialize(filename, flags, std::move(options));
}

template <typename Dest>
void FdWriter<Dest>::Initialize(absl::string_view filename, int flags,
                                Options&& options) {
  const int dest = OpenFd(filename, flags, options.permissions());
  if (ABSL_PREDICT_FALSE(dest < 0)) return;
  FdWriterBase::Reset(options.buffer_size());
  dest_.Reset(std::forward_as_tuple(dest));
  InitializePos(dest_.get(), flags, options.assumed_pos(),
                options.independent_pos());
}

template <typename Dest>
void FdWriter<Dest>::Done() {
  FdWriterBase::Done();
  if (dest_.is_owning()) {
    const int dest = dest_.Release();
    if (ABSL_PREDICT_FALSE(internal::CloseFd(dest) < 0) &&
        ABSL_PREDICT_TRUE(healthy())) {
      FailOperation(internal::kCloseFunctionName);
    }
  }
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_FD_WRITER_H_
