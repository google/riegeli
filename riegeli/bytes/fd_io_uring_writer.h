#ifndef RIEGELI_BYTES_FD_IO_URING_WRITER_H_
#define RIEGELI_BYTES_FD_IO_URING_WRITER_H_

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

#include "riegeli/iouring/fd_sync_io_uring.h"

namespace riegeli {

// Template parameter independent part of `FdIoUringWriter`.
class FdIoUringWriterBase : public BufferedWriter {
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

    // If `absl::nullopt`, `FdIoUringWriter` writes at the current fd position.
    //
    // If not `absl::nullopt`, `FdIoUringWriter` writes starting from this position,
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
          << "Failed precondition of FdIoUringWriterBase::Options::set_buffer_size(): "
             "zero buffer size";
      buffer_size_ = buffer_size;
      return *this;
    }
    Options&& set_buffer_size(size_t buffer_size) && {
      return std::move(set_buffer_size(buffer_size));
    }
    size_t buffer_size() const { return buffer_size_; }

    // The option for Io_Uring operation.
    //
    // The option includes: async or sync, fd_register or not, and size. 
    Options& set_io_uring_option(FdIoUringOptions io_uring_option) & {
      io_uring_option_ = io_uring_option;
      return *this;
    }

    Options&& set_io_uring_option(FdIoUringOptions io_uring_option) && {
      return std::move(set_io_uring_option(io_uring_option));
    }

    FdIoUringOptions io_uring_option() const {
      return io_uring_option_;
    }

   private:
    mode_t permissions_ = 0666;
    absl::optional<Position> assumed_pos_;
    absl::optional<Position> independent_pos_;
    size_t buffer_size_ = kDefaultBufferSize;
    FdIoUringOptions io_uring_option_;
  };

  // Returns the fd being written to. If the fd is owned then changed to -1 by
  // `Close()`, otherwise unchanged.
  virtual int dest_fd() const = 0;

  // Returns the original name of the file being written to (or "/dev/stdout",
  // "/dev/stderr", or "/proc/self/fd/<fd>" if fd was given). Unchanged by
  // `Close()`.
  const std::string& filename() const { return filename_; }

  bool SupportsRandomAccess() override { return supports_random_access_; }

 protected:
  FdIoUringWriterBase() noexcept {}

  explicit FdIoUringWriterBase(size_t buffer_size);

  FdIoUringWriterBase(FdIoUringWriterBase&& that) noexcept;
  FdIoUringWriterBase& operator=(FdIoUringWriterBase&& that) noexcept;

  void Reset();
  void Reset(size_t buffer_size);
  void Initialize(int dest, absl::optional<Position> assumed_pos,
                  absl::optional<Position> independent_pos, FdIoUringOptions io_uring_option);
  int OpenFd(absl::string_view filename, int flags, mode_t permissions);
  void InitializePos(int dest, absl::optional<Position> assumed_pos,
                     absl::optional<Position> independent_pos);
  void InitializePos(int dest, int flags, absl::optional<Position> assumed_pos,
                     absl::optional<Position> independent_pos);
  void InitializeFdIoUring(FdIoUringOptions options, int fd);
  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation);

  void AnnotateFailure(absl::Status& status) override;
  bool WriteInternal(absl::string_view src) override;
  bool FlushImpl(FlushType flush_type) override;
  bool SeekBehindBuffer(Position new_pos) override;
  absl::optional<Position> SizeBehindBuffer() override;
  bool TruncateBehindBuffer(Position new_size) override;

  bool SyncWriteInternal(absl::string_view src);
  bool AsyncWriteInternal(absl::string_view src);

 private:
  void SetFilename(int dest);
  bool SeekInternal(int dest, Position new_pos);

  std::string filename_;
  bool supports_random_access_ = false;
  bool has_independent_pos_ = false;
  bool async_ = false;

 protected:
  std::unique_ptr<FdIoUring> fd_io_uring_;
  // Invariant: `start_pos() <= std::numeric_limits<off_t>::max()`
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
// `FdIoUringWriter` supports random access if
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
// Until the `FdIoUringWriter` is closed or no longer used, the fd must not be closed;
// additionally, if `Options::independent_pos() == absl::nullopt`, the fd should
// not have its position changed, except that if random access is not used,
// careful interleaving of multiple writers is possible: `Flush()` is needed
// before switching to another writer, and `pos()` does not take other writers
// into account.
template <typename Dest = OwnedFd>
class FdIoUringWriter : public FdIoUringWriterBase {
 public:
  // Creates a closed `FdIoUringWriter`.
  FdIoUringWriter() noexcept {}

  // Will write to the fd provided by `dest`.
  explicit FdIoUringWriter(const Dest& dest, Options options = Options());
  explicit FdIoUringWriter(Dest&& dest, Options options = Options());

  // Will write to the fd provided by a `Dest` constructed from elements of
  // `dest_args`. This avoids constructing a temporary `Dest` and moving from
  // it.
  template <typename... DestArgs>
  explicit FdIoUringWriter(std::tuple<DestArgs...> dest_args,
                    Options options = Options());

  // Opens a file for writing.
  //
  // `flags` is the second argument of `open()`, typically one of:
  //  * `O_WRONLY | O_CREAT | O_TRUNC`
  //  * `O_WRONLY | O_CREAT | O_APPEND`
  //
  // `flags` must include either `O_WRONLY` or `O_RDWR`.
  //
  // If opening the file fails, `FdIoUringWriter` will be failed and closed.
  explicit FdIoUringWriter(absl::string_view filename, int flags,
                    Options options = Options());

  FdIoUringWriter(FdIoUringWriter&& that) noexcept;
  FdIoUringWriter& operator=(FdIoUringWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `FdIoUringWriter`. This avoids
  // constructing a temporary `FdIoUringWriter` and moving from it.
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
  using FdIoUringWriterBase::Initialize;
  void Initialize(absl::string_view filename, int flags, Options&& options);

  void Done() override;

 private:
  // The object providing and possibly owning the fd being written to.
  Dependency<int, Dest> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
FdIoUringWriter()->FdIoUringWriter<DeleteCtad<>>;
template <typename Dest>
explicit FdIoUringWriter(const Dest& dest,
                  FdIoUringWriterBase::Options options = FdIoUringWriterBase::Options())
    -> FdIoUringWriter<std::conditional_t<std::is_convertible<const Dest&, int>::value,
                                   OwnedFd, std::decay_t<Dest>>>;
template <typename Dest>
explicit FdIoUringWriter(Dest&& dest,
                  FdIoUringWriterBase::Options options = FdIoUringWriterBase::Options())
    -> FdIoUringWriter<std::conditional_t<std::is_convertible<Dest&&, int>::value,
                                   OwnedFd, std::decay_t<Dest>>>;
template <typename... DestArgs>
explicit FdIoUringWriter(std::tuple<DestArgs...> dest_args,
                  FdIoUringWriterBase::Options options = FdIoUringWriterBase::Options())
    -> FdIoUringWriter<DeleteCtad<std::tuple<DestArgs...>>>;
explicit FdIoUringWriter(absl::string_view filename, int flags,
                  FdIoUringWriterBase::Options options = FdIoUringWriterBase::Options())
    ->FdIoUringWriter<>;
#endif

// Implementation details follow.

inline FdIoUringWriterBase::FdIoUringWriterBase(size_t buffer_size)
    : BufferedWriter(buffer_size) {}

inline FdIoUringWriterBase::FdIoUringWriterBase(FdIoUringWriterBase&& that) noexcept
    : BufferedWriter(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      filename_(std::move(that.filename_)),
      supports_random_access_(that.supports_random_access_),
      has_independent_pos_(that.has_independent_pos_), async_(that.async_), fd_io_uring_(std::move(that.fd_io_uring_)) {}

inline FdIoUringWriterBase& FdIoUringWriterBase::operator=(FdIoUringWriterBase&& that) noexcept {
  BufferedWriter::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  filename_ = std::move(that.filename_);
  supports_random_access_ = that.supports_random_access_;
  has_independent_pos_ = that.has_independent_pos_;
  async_ = that.async_;
  fd_io_uring_ = std::move(that.fd_io_uring_);
  return *this;
}

inline void FdIoUringWriterBase::Reset() {
  BufferedWriter::Reset();
  filename_.clear();
  supports_random_access_ = false;
  has_independent_pos_ = false;
  async_ = false;
  fd_io_uring_.reset();
}

inline void FdIoUringWriterBase::Reset(size_t buffer_size) {
  BufferedWriter::Reset(buffer_size);
  // `filename_` was set by `OpenFd()` or will be set by `Initialize()`.
  supports_random_access_ = false;
  has_independent_pos_ = false;
  async_ = false;
  fd_io_uring_.reset();
}

template <typename Dest>
inline FdIoUringWriter<Dest>::FdIoUringWriter(const Dest& dest, Options options)
    : FdIoUringWriterBase(options.buffer_size()), dest_(dest) {
  Initialize(dest_.get(), options.assumed_pos(), options.independent_pos(), options.io_uring_option());
}

template <typename Dest>
inline FdIoUringWriter<Dest>::FdIoUringWriter(Dest&& dest, Options options)
    : FdIoUringWriterBase(options.buffer_size()), dest_(std::move(dest)) {
  Initialize(dest_.get(), options.assumed_pos(), options.independent_pos(), options.io_uring_option());
}

template <typename Dest>
template <typename... DestArgs>
inline FdIoUringWriter<Dest>::FdIoUringWriter(std::tuple<DestArgs...> dest_args,
                                Options options)
    : FdIoUringWriterBase(options.buffer_size()), dest_(std::move(dest_args)) {
  Initialize(dest_.get(), options.assumed_pos(), options.independent_pos(), options.io_uring_option());
}

template <typename Dest>
inline FdIoUringWriter<Dest>::FdIoUringWriter(absl::string_view filename, int flags,
                                Options options) {
  Initialize(filename, flags, std::move(options));
}

template <typename Dest>
inline FdIoUringWriter<Dest>::FdIoUringWriter(FdIoUringWriter&& that) noexcept
    : FdIoUringWriterBase(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      dest_(std::move(that.dest_)) {}

template <typename Dest>
inline FdIoUringWriter<Dest>& FdIoUringWriter<Dest>::operator=(FdIoUringWriter&& that) noexcept {
  FdIoUringWriterBase::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  dest_ = std::move(that.dest_);
  return *this;
}

template <typename Dest>
inline void FdIoUringWriter<Dest>::Reset() {
  FdIoUringWriterBase::Reset();
  dest_.Reset();
}

template <typename Dest>
inline void FdIoUringWriter<Dest>::Reset(const Dest& dest, Options options) {
  FdIoUringWriterBase::Reset(options.buffer_size());
  dest_.Reset(dest);
  Initialize(dest_.get(), options.assumed_pos(), options.independent_pos(), options.io_uring_option());
}

template <typename Dest>
inline void FdIoUringWriter<Dest>::Reset(Dest&& dest, Options options) {
  FdIoUringWriterBase::Reset(options.buffer_size());
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), options.assumed_pos(), options.independent_pos(), options.io_uring_option());
}

template <typename Dest>
template <typename... DestArgs>
inline void FdIoUringWriter<Dest>::Reset(std::tuple<DestArgs...> dest_args,
                                  Options options) {
  FdIoUringWriterBase::Reset(options.buffer_size());
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get(), options.assumed_pos(), options.independent_pos(), options.io_uring_option());
}

template <typename Dest>
inline void FdIoUringWriter<Dest>::Reset(absl::string_view filename, int flags,
                                  Options options) {
  Reset();
  Initialize(filename, flags, std::move(options));
}

template <typename Dest>
void FdIoUringWriter<Dest>::Initialize(absl::string_view filename, int flags,
                                Options&& options) {
  const int dest = OpenFd(filename, flags, options.permissions());
  if (ABSL_PREDICT_FALSE(dest < 0)) return;
  FdIoUringWriterBase::Reset(options.buffer_size());
  dest_.Reset(std::forward_as_tuple(dest));
  int fd = dest_.get();
  InitializeFdIoUring(options.io_uring_option(), fd);
  InitializePos(dest_.get(), flags, options.assumed_pos(),
                options.independent_pos());
}

template <typename Dest>
void FdIoUringWriter<Dest>::Done() {
  FdIoUringWriterBase::Done();
  fd_io_uring_.reset();

  if (dest_.is_owning()) {
    const int dest = dest_.Release();
    if (ABSL_PREDICT_FALSE(internal::CloseFd(dest) < 0) &&
        ABSL_PREDICT_TRUE(healthy())) {
      FailOperation(internal::kCloseFunctionName);
    }
  }
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_FD_IO_URING_WRITER_H_