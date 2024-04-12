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
#ifdef _WIN32
#include <sys/stat.h>
#endif
#include <sys/types.h>

#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/meta/type_traits.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/object.h"
#include "riegeli/base/type_id.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/fd_handle.h"
#include "riegeli/bytes/fd_internal_for_headers.h"
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

    // `assumed_filename()` allows to override the filename which is included in
    // failure messages and returned by `filename()`.
    //
    // If this is `absl::nullopt` and `FdWriter` opens a fd with a filename,
    // then that filename is used.
    //
    // If this is `absl::nullopt` and `FdWriter` writes to an already open fd,
    // then "/dev/stdin", "/dev/stdout", "/dev/stderr", or
    // `absl::StrCat("/proc/self/fd/", fd)` is inferred from the fd (on Windows:
    // "CONIN$", "CONOUT$", "CONERR$", or `absl::StrCat("<fd ", fd, ">")`).
    //
    // Default: `absl::nullopt`.
    Options& set_assumed_filename(
        Initializer<absl::optional<std::string>>::AllowingExplicit
            assumed_filename) & {
      std::move(assumed_filename).AssignTo(assumed_filename_);
      return *this;
    }
    Options&& set_assumed_filename(
        Initializer<absl::optional<std::string>>::AllowingExplicit
            assumed_filename) && {
      return std::move(set_assumed_filename(std::move(assumed_filename)));
    }
    absl::optional<std::string>& assumed_filename() {
      return assumed_filename_;
    }
    const absl::optional<std::string>& assumed_filename() const {
      return assumed_filename_;
    }

    // If `FdWriter` opens a fd with a filename, `mode()` is the second argument
    // of `open()` (on Windows: `_open()`) and specifies the open mode and
    // flags, typically one of:
    //  * `O_WRONLY | O_CREAT | O_TRUNC`
    //    (on Windows: `_O_WRONLY | _O_CREAT | _O_TRUNC | _O_BINARY`)
    //  * `O_WRONLY | O_CREAT | O_APPEND`
    //    (on Windows: `_O_WRONLY | _O_CREAT | _O_APPEND | _O_BINARY`)
    //
    // It must include either `O_WRONLY` or `O_RDWR` (on Windows: `_O_WRONLY` or
    // `_O_RDWR`).
    //
    // `mode()` can also be changed with `set_existing()`, `set_read()`,
    // `set_append()`, `set_exclusive()`, `set_inheritable()`, and `set_text()`.
    //
    // Default: `O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC`
    // (on Windows: `_O_WRONLY | _O_CREAT | _O_TRUNC | _O_BINARY |
    //               _O_NOINHERIT`).
    Options& set_mode(int mode) & {
      mode_ = mode;
      return *this;
    }
    Options&& set_mode(int mode) && { return std::move(set_mode(mode)); }
    int mode() const { return mode_; }

    // If `false`, the file will be created if it does not exist, or it will be
    // truncated to empty if it exists. This implies `set_read(false)` and
    // `set_append(false)` unless overwritten later.
    //
    // If `true`, the file must already exist, and its contents will not be
    // truncated. Writing will start from the beginning, with random access
    // supported. This implies `set_read(true)` unless overwritten later.
    //
    // If `FdWriter` writes to an already open fd, `existing()` has no effect.
    //
    // `set_existing()` affects `mode()`.
    //
    // Default: `false`.
    Options& set_existing(bool existing) & {
#ifndef _WIN32
      mode_ = (mode_ & ~(O_ACCMODE | O_CREAT | O_TRUNC | O_APPEND)) |
              (existing ? O_RDWR : O_WRONLY | O_CREAT | O_TRUNC);
#else
      mode_ = (mode_ & ~(_O_RDONLY | _O_WRONLY | _O_RDWR | _O_CREAT | _O_TRUNC |
                         _O_APPEND)) |
              (existing ? _O_RDWR : _O_WRONLY | _O_CREAT | _O_TRUNC);
#endif
      return *this;
    }
    Options&& set_existing(bool existing) && {
      return std::move(set_existing(existing));
    }
    bool existing() const {
#ifndef _WIN32
      return (mode_ & O_CREAT) == 0;
#else
      return (mode_ & _O_CREAT) == 0;
#endif
    }

    // If `false`, the fd will be open for writing.
    //
    // If `true`, the fd will be open for writing and reading (using
    // `ReadMode()`).
    //
    // If `FdWriter` writes to an already open fd, `read()` has no effect.
    //
    // `set_read()` affects `mode()`.
    //
    // Default: `false`.
    Options& set_read(bool read) & {
#ifndef _WIN32
      mode_ = (mode_ & ~O_ACCMODE) | (read ? O_RDWR : O_WRONLY);
#else
      mode_ = (mode_ & ~(_O_RDONLY | _O_WRONLY | _O_RDWR)) |
              (read ? _O_RDWR : _O_WRONLY);
#endif
      return *this;
    }
    Options&& set_read(bool read) && { return std::move(set_read(read)); }
    bool read() const {
#ifndef _WIN32
      return (mode_ & O_ACCMODE) == O_RDWR;
#else
      return (mode_ & (_O_RDONLY | _O_WRONLY | _O_RDWR)) == _O_RDWR;
#endif
    }

    // If `false`, the file will be truncated to empty if it exists.
    //
    // If `true`, the file will not be truncated if it exists, and writing will
    // always happen at its end.
    //
    // If `FdWriter` writes to an already open fd, `append()` has effect only on
    // Windows. If `assumed_pos()` is not set, `append()` should be `true` if
    // the fd was originally open in append mode. This allows to determine the
    // effective initial position and lets `SupportsRandomAccess()` correctly
    // return `false`.
    //
    // `set_append()` affects `mode()`.
    //
    // Default: `false`.
    Options& set_append(bool append) & {
#ifndef _WIN32
      mode_ = (mode_ & ~(O_TRUNC | O_APPEND)) | (append ? O_APPEND : O_TRUNC);
#else
      mode_ =
          (mode_ & ~(_O_TRUNC | _O_APPEND)) | (append ? _O_APPEND : _O_TRUNC);
#endif
      return *this;
    }
    Options&& set_append(bool append) && {
      return std::move(set_append(append));
    }
    bool append() const {
#ifndef _WIN32
      return (mode_ & O_APPEND) != 0;
#else
      return (mode_ & _O_APPEND) != 0;
#endif
    }

    // If `false`, the file will be created if it does not exist, or it will be
    // opened if it exists (truncated to empty by default, or left unchanged if
    // `set_existing(true)` or `set_append(true)` was used).
    //
    // If `true`, the file will be created if it does not exist, or opening will
    // fail if it exists.
    //
    // If `FdWriter` writes to an already open fd, `exclusive()` has no effect.
    //
    // `set_exclusive()` affects `mode()`.
    //
    // Default: `false`.
    Options& set_exclusive(bool exclusive) & {
#ifndef _WIN32
      mode_ = (mode_ & ~O_EXCL) | (exclusive ? O_EXCL : 0);
#else
      mode_ = (mode_ & ~_O_EXCL) | (exclusive ? _O_EXCL : 0);
#endif
      return *this;
    }
    Options&& set_exclusive(bool exclusive) && {
      return std::move(set_exclusive(exclusive));
    }
    bool exclusive() const {
#ifndef _WIN32
      return (mode_ & O_EXCL) != 0;
#else
      return (mode_ & _O_EXCL) != 0;
#endif
    }

    // If `false`, `execve()` (`CreateProcess()` on Windows) will close the fd.
    //
    // If `true`, the fd will remain open across `execve()` (`CreateProcess()`
    // on Windows).
    //
    // If `FdWriter` writes to an already open fd, `inheritable()` has no
    // effect.
    //
    // `set_inheritable()` affects `mode()`.
    //
    // Default: `false`.
    Options& set_inheritable(bool inheritable) & {
      mode_ = (mode_ & ~fd_internal::kCloseOnExec) |
              (inheritable ? 0 : fd_internal::kCloseOnExec);
      return *this;
    }
    Options&& set_inheritable(bool inheritable) && {
      return std::move(set_inheritable(inheritable));
    }
    bool inheritable() const {
      return (mode_ & fd_internal::kCloseOnExec) == 0;
    }

    // If `false`, data will be written directly to the file. This is called the
    // binary mode.
    //
    // If `true`, text mode translation will be applied on Windows:
    // LF characters are translated to CR-LF.
    //
    // It is recommended to use `WriteLine()` or `TextWriter` instead, which
    // expect a binary mode `Writer`.
    //
    // `set_text()` has an effect only on Windows. It is applicable whenever
    // `FdWriter` opens a fd with a filename or writes to an already open fd.
    //
    // `set_text()` affects `mode()`.
    //
    // Default: `false`.
    Options& set_text(ABSL_ATTRIBUTE_UNUSED bool text) & {
#ifdef _WIN32
      mode_ =
          (mode_ & ~(_O_BINARY | _O_TEXT | _O_WTEXT | _O_U16TEXT | _O_U8TEXT)) |
          (text ? _O_TEXT : _O_BINARY);
#endif
      return *this;
    }
    Options&& set_text(bool text) && { return std::move(set_text(text)); }
    // No `text()` getter is provided. On Windows `mode()` can have unspecified
    // text mode, resolved using `_get_fmode()`. Not on Windows the concept does
    // not exist.

    // Permissions to use in case a new file is created (9 bits, except on
    // Windows: `_S_IREAD`, `_S_IWRITE`, or `_S_IREAD | _S_IWRITE`). The
    // effective permissions are modified by the process' umask.
    //
    // Default: `0666` (on Windows: `_S_IREAD | _S_IWRITE`).
    Options& set_permissions(OwnedFd::Permissions permissions) & {
      permissions_ = permissions;
      return *this;
    }
    Options&& set_permissions(OwnedFd::Permissions permissions) && {
      return std::move(set_permissions(permissions));
    }
    OwnedFd::Permissions permissions() const { return permissions_; }

    // If `absl::nullopt`, the current position reported by `pos()` corresponds
    // to the current fd position if possible, otherwise 0 is assumed as the
    // initial position. Random access is supported if the fd supports random
    // access. On Windows binary mode is also required.
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
    // If not `absl::nullopt`, `FdWriter` writes starting from this position.
    // The current fd position is not disturbed except on Windows, where seeking
    // and writing is nevertheless atomic. This is useful for multiple writers
    // concurrently writing to disjoint regions of the same file. The fd must
    // support `pwrite()` (`_get_osfhandle()` and `WriteFile()` with
    // `OVERLAPPED*` on Windows). On Windows binary mode is also required.
    //
    // `assumed_pos()` and `independent_pos()` must not be both set.
    //
    // If the original open mode of the fd includes `O_APPEND` then
    // `independent_pos()` must not be set.
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
#ifndef _WIN32
    int mode_ = O_WRONLY | O_CREAT | O_TRUNC | fd_internal::kCloseOnExec;
#else
    int mode_ =
        _O_WRONLY | _O_CREAT | _O_TRUNC | _O_BINARY | fd_internal::kCloseOnExec;
#endif
    OwnedFd::Permissions permissions_ = OwnedFd::kDefaultPermissions;
    absl::optional<Position> assumed_pos_;
    absl::optional<Position> independent_pos_;
  };

  // Returns the `FdHandle` being written to. Unchanged by `Close()`.
  virtual FdHandle DestFdHandle() const = 0;

  // Returns the fd being written to. If the fd is owned then changed to -1 by
  // `Close()`, otherwise unchanged.
  virtual int DestFd() const = 0;

  TypeId GetTypeId() const override;

  // Returns the original name of the file being written to. Unchanged by
  // `Close()`.
  absl::string_view filename() const { return filename_; }

  bool SupportsRandomAccess() override;
  bool SupportsReadMode() override;

 protected:
  explicit FdWriterBase(Closed) noexcept : BufferedWriter(kClosed) {}

  explicit FdWriterBase(BufferOptions buffer_options);

  FdWriterBase(FdWriterBase&& that) noexcept;
  FdWriterBase& operator=(FdWriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset(BufferOptions buffer_options);
  void Initialize(int dest, Options&& options);
  const std::string& InitializeFilename(
      Initializer<std::string>::AllowingExplicit filename);
  bool InitializeAssumedFilename(Options& options);
  void InitializePos(int dest, Options&& options, bool mode_was_passed_to_open);
  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation);
#ifdef _WIN32
  ABSL_ATTRIBUTE_COLD bool FailWindowsOperation(absl::string_view operation);
#endif

  void Done() override;
  absl::Status AnnotateStatusImpl(absl::Status status) override;
  bool WriteInternal(absl::string_view src) override;
  bool WriteZerosSlow(Position length) override;
  bool FlushImpl(FlushType flush_type) override;
  bool FlushBehindBuffer(absl::string_view src, FlushType flush_type) override;
  bool SeekBehindBuffer(Position new_pos) override;
  absl::optional<Position> SizeBehindBuffer() override;
  bool TruncateBehindBuffer(Position new_size) override;
  Reader* ReadModeBehindBuffer(Position initial_pos) override;

 private:
  friend class FdReaderBase;  // For `has_independent_pos_`.

  // Encodes a `bool` or a marker that the value is not resolved yet.
  enum class LazyBoolState : uint8_t { kUnknown, kTrue, kFalse };

  absl::Status FailedOperationStatus(absl::string_view operation);
#ifdef _WIN32
  absl::Status FailedWindowsOperationStatus(absl::string_view operation);
#endif
  // Lazily determined condition shared by `SupportsRandomAccess()` and
  // `SupportsReadMode()`.
  absl::Status SizeStatus();

  bool WriteMode();
  bool SeekInternal(int dest, Position new_pos);
  bool TruncateInternal(int dest, Position new_size);

  std::string filename_;
  bool has_independent_pos_ = false;
  // Invariant except on Windows:
  //   if `supports_read_mode_ == LazyBoolState::kUnknown` then
  //       `supports_random_access_ == LazyBoolState::kUnknown`
  LazyBoolState supports_random_access_ = LazyBoolState::kUnknown;
  // If `supports_read_mode_ == LazyBoolState::kUnknown`,
  // then at least size is known to be supported
  // when `supports_random_access_ != LazyBoolState::kUnknown`
  // (no matter whether `LazyBoolState::kTrue` or LazyBoolState::kFalse`).
  //
  // This is useful on Windows, otherwise this is trivially true
  // (`supports_random_access_ == LazyBoolState::kUnknown`).
  LazyBoolState supports_read_mode_ = LazyBoolState::kUnknown;
  absl::Status random_access_status_;
  absl::Status read_mode_status_;
#ifdef _WIN32
  absl::optional<int> original_mode_;
#endif

  AssociatedReader<FdReader<UnownedFd>> associated_reader_;
  bool read_mode_ = false;

  // Invariant: `start_pos() <= std::numeric_limits<fd_internal::Offset>::max()`
};

// A `Writer` which writes to a file descriptor.
//
// The fd must support:
#ifndef _WIN32
//  * `fcntl()`     - for the constructor from fd
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
#else
//  * `_close()`    - if the fd is owned
//  * `_write()`    - if `Options::independent_pos() == absl::nullopt`
//  * `_get_osfhandle()`, `WriteFile()` with `OVERLAPPED*`
//                  - if `Options::independent_pos() != absl::nullopt`
//  * `_lseeki64()` - for `Seek()`, `Size()`, or `Truncate(),
//                    if `Options::independent_pos() == absl::nullopt`
//  * `_fstat64()`  - for `Seek()`, `Size()`, or `Truncate(),
//  * `_commit()`   - for `Flush(FlushType::kFromMachine)`
//  * `_chsize_s()` - for `Truncate()`
//  * `_read()`     - for `ReadMode()`
//                    if `Options::independent_pos() == absl::nullopt`
//                    (fd must be opened with `_O_RDWR`)
//  * `_get_osfhandle()`, `ReadFile()` with `OVERLAPPED*`
//                  - for `ReadMode()`
//                    if `Options::independent_pos() != absl::nullopt`
//                    (fd must be opened with `_O_RDWR`)
#endif
//
// `FdWriter` supports random access if
// `Options::assumed_pos() == absl::nullopt` and the fd supports random access
// (this is assumed if `Options::independent_pos() != absl::nullopt`, otherwise
// this is checked by calling `lseek(SEEK_END)`, or `_lseeki64()` on Windows).
// On Windows binary mode is also required.
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
// opened with `O_RDWR` (`_O_RDWR` on Windows).
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the fd being written to. `Dest` must support
// `Dependency<FdHandle, Dest>`, e.g. `OwnedFd` (owned, default),
// `UnownedFd` (not owned), `AnyDependency<FdHandle>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as `OwnedFd` if the
// first constructor argument is a filename or an `int`, otherwise as
// `InitializerTargetT` of the type of the first constructor argument.
// This requires C++17.
//
// Until the `FdWriter` is closed or no longer used, the fd must not be closed.
// Additionally, if `Options::independent_pos() == absl::nullopt`
// (or unconditionally on Windows), the fd should not have its position changed,
// except that if random access is not used, careful interleaving of multiple
// writers is possible: `Flush()` is needed before switching to another writer,
// and `pos()` does not take other writers into account.
template <typename Dest = OwnedFd>
class FdWriter : public FdWriterBase {
 public:
  // Creates a closed `FdWriter`.
  explicit FdWriter(Closed) noexcept : FdWriterBase(kClosed) {}

  // Will write to the fd provided by `dest`.
  explicit FdWriter(Initializer<Dest> dest, Options options = Options());

  // Will write to `dest`.
  template <typename DependentDest = Dest,
            std::enable_if_t<std::is_constructible<DependentDest, int>::value,
                             int> = 0>
  explicit FdWriter(int dest, Options options = Options());

  // Opens a file for writing.
  //
  // If opening the file fails, `FdWriter` will be failed and closed.
  //
  // This constructor is present only if `Dest` supports `Open()`.
  template <typename DependentDest = Dest,
            std::enable_if_t<FdTargetHasOpen<DependentDest>::value, int> = 0>
  explicit FdWriter(Initializer<std::string>::AllowingExplicit filename,
                    Options options = Options());

  // Opens a file for writing, with the filename interpreted relatively to the
  // directory specified by an existing fd.
  //
  // If opening the file fails, `FdWriter` will be failed and closed.
  //
  // This constructor is present only if `Dest` supports `OpenAt()`.
  template <typename DependentDest = Dest,
            std::enable_if_t<FdTargetHasOpenAt<DependentDest>::value, int> = 0>
  explicit FdWriter(int dir_fd,
                    Initializer<std::string>::AllowingExplicit filename,
                    Options options = Options());

  FdWriter(FdWriter&& that) noexcept;
  FdWriter& operator=(FdWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `FdWriter`. This avoids
  // constructing a temporary `FdWriter` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Dest> dest,
                                          Options options = Options());
  template <typename DependentDest = Dest,
            std::enable_if_t<std::is_constructible<DependentDest, int>::value,
                             int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(int dest,
                                          Options options = Options());
  template <typename DependentDest = Dest,
            std::enable_if_t<FdTargetHasOpen<DependentDest>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      Initializer<std::string>::AllowingExplicit filename,
      Options options = Options());
  template <typename DependentDest = Dest,
            std::enable_if_t<FdTargetHasOpenAt<DependentDest>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      int dir_fd, Initializer<std::string>::AllowingExplicit filename,
      Options options = Options());

  // Returns the object providing and possibly owning the fd being written to.
  // Unchanged by `Close()`.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  FdHandle DestFdHandle() const override { return dest_.get(); }
  int DestFd() const override { return *dest_; }

 protected:
  void Done() override;

 private:
  // The object providing and possibly owning the fd being written to.
  Dependency<FdHandle, Dest> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit FdWriter(Closed) -> FdWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit FdWriter(Dest&& dest,
                  FdWriterBase::Options options = FdWriterBase::Options())
    -> FdWriter<std::conditional_t<
        absl::disjunction<
            std::is_convertible<Dest&&, int>,
            std::is_convertible<
                Dest&&, Initializer<std::string>::AllowingExplicit>>::value,
        OwnedFd, InitializerTargetT<Dest>>>;
explicit FdWriter(int dir_fd,
                  Initializer<std::string>::AllowingExplicit filename,
                  FdWriterBase::Options options = FdWriterBase::Options())
    -> FdWriter<OwnedFd>;
#endif

// Implementation details follow.

inline FdWriterBase::FdWriterBase(BufferOptions buffer_options)
    : BufferedWriter(buffer_options) {}

inline FdWriterBase::FdWriterBase(FdWriterBase&& that) noexcept
    : BufferedWriter(static_cast<BufferedWriter&&>(that)),
      filename_(std::exchange(that.filename_, std::string())),
      has_independent_pos_(that.has_independent_pos_),
      supports_random_access_(
          std::exchange(that.supports_random_access_, LazyBoolState::kUnknown)),
      supports_read_mode_(
          std::exchange(that.supports_read_mode_, LazyBoolState::kUnknown)),
      random_access_status_(std::move(that.random_access_status_)),
      read_mode_status_(std::move(that.read_mode_status_)),
#ifdef _WIN32
      original_mode_(that.original_mode_),
#endif
      associated_reader_(std::move(that.associated_reader_)),
      read_mode_(that.read_mode_) {
}

inline FdWriterBase& FdWriterBase::operator=(FdWriterBase&& that) noexcept {
  BufferedWriter::operator=(static_cast<BufferedWriter&&>(that));
  filename_ = std::exchange(that.filename_, std::string());
  has_independent_pos_ = that.has_independent_pos_;
  supports_random_access_ =
      std::exchange(that.supports_random_access_, LazyBoolState::kUnknown),
  supports_read_mode_ =
      std::exchange(that.supports_read_mode_, LazyBoolState::kUnknown),
  random_access_status_ = std::move(that.random_access_status_);
  read_mode_status_ = std::move(that.read_mode_status_);
#ifdef _WIN32
  original_mode_ = that.original_mode_;
#endif
  associated_reader_ = std::move(that.associated_reader_);
  read_mode_ = that.read_mode_;
  return *this;
}

inline void FdWriterBase::Reset(Closed) {
  BufferedWriter::Reset(kClosed);
  filename_ = std::string();
  has_independent_pos_ = false;
  supports_random_access_ = LazyBoolState::kUnknown;
  supports_read_mode_ = LazyBoolState::kUnknown;
  random_access_status_ = absl::OkStatus();
  read_mode_status_ = absl::OkStatus();
#ifdef _WIN32
  original_mode_ = absl::nullopt;
#endif
  associated_reader_.Reset();
  read_mode_ = false;
}

inline void FdWriterBase::Reset(BufferOptions buffer_options) {
  BufferedWriter::Reset(buffer_options);
  // `filename_` will be set by `Initialize()`, `InitializeFilename()`, or
  // `InitializeAssumedFilename()`.
  has_independent_pos_ = false;
  supports_random_access_ = LazyBoolState::kUnknown;
  supports_read_mode_ = LazyBoolState::kUnknown;
  random_access_status_ = absl::OkStatus();
  read_mode_status_ = absl::OkStatus();
#ifdef _WIN32
  original_mode_ = absl::nullopt;
#endif
  associated_reader_.Reset();
  read_mode_ = false;
}

inline const std::string& FdWriterBase::InitializeFilename(
    Initializer<std::string>::AllowingExplicit filename) {
  std::move(filename).AssignTo(filename_);
  return filename_;
}

inline bool FdWriterBase::InitializeAssumedFilename(Options& options) {
  if (options.assumed_filename() != absl::nullopt) {
    filename_ = *std::move(options.assumed_filename());
    return true;
  } else {
    return false;
  }
}

template <typename Dest>
inline FdWriter<Dest>::FdWriter(Initializer<Dest> dest, Options options)
    : FdWriterBase(options.buffer_options()), dest_(std::move(dest)) {
  Initialize(*dest_, std::move(options));
}

template <typename Dest>
template <
    typename DependentDest,
    std::enable_if_t<std::is_constructible<DependentDest, int>::value, int>>
inline FdWriter<Dest>::FdWriter(int dest, Options options)
    : FdWriter(riegeli::Maker(dest), std::move(options)) {}

template <typename Dest>
template <typename DependentDest,
          std::enable_if_t<FdTargetHasOpen<DependentDest>::value, int>>
inline FdWriter<Dest>::FdWriter(
    Initializer<std::string>::AllowingExplicit filename, Options options)
    : FdWriterBase(options.buffer_options()) {
  absl::Status status =
      dest_.manager().Open(InitializeFilename(std::move(filename)),
                           options.mode(), options.permissions());
  InitializeAssumedFilename(options);
  if (ABSL_PREDICT_FALSE(!status.ok())) {
    // Not `FdWriterBase::Reset()` to preserve `filename()`.
    BufferedWriter::Reset(kClosed);
    FailWithoutAnnotation(std::move(status));
    return;
  }
  InitializePos(*dest_, std::move(options), /*mode_was_passed_to_open=*/true);
}

template <typename Dest>
template <typename DependentDest,
          std::enable_if_t<FdTargetHasOpenAt<DependentDest>::value, int>>
inline FdWriter<Dest>::FdWriter(
    int dir_fd, Initializer<std::string>::AllowingExplicit filename,
    Options options)
    : FdWriterBase(options.buffer_options()) {
  absl::Status status =
      dest_.manager().OpenAt(dir_fd, InitializeFilename(std::move(filename)),
                             options.mode(), options.permissions());
  InitializeAssumedFilename(options);
  if (ABSL_PREDICT_FALSE(!status.ok())) {
    // Not `FdWriterBase::Reset()` to preserve `filename()`.
    BufferedWriter::Reset(kClosed);
    FailWithoutAnnotation(std::move(status));
    return;
  }
  InitializePos(*dest_, std::move(options), /*mode_was_passed_to_open=*/true);
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
inline void FdWriter<Dest>::Reset(Initializer<Dest> dest, Options options) {
  FdWriterBase::Reset(options.buffer_options());
  dest_.Reset(std::move(dest));
  Initialize(*dest_, std::move(options));
}

template <typename Dest>
template <
    typename DependentDest,
    std::enable_if_t<std::is_constructible<DependentDest, int>::value, int>>
inline void FdWriter<Dest>::Reset(int dest, Options options) {
  Reset(riegeli::Maker(dest), std::move(options));
}

template <typename Dest>
template <typename DependentDest,
          std::enable_if_t<FdTargetHasOpen<DependentDest>::value, int>>
inline void FdWriter<Dest>::Reset(
    Initializer<std::string>::AllowingExplicit filename, Options options) {
  FdWriterBase::Reset(options.buffer_options());
  absl::Status status =
      dest_.manager().Open(InitializeFilename(std::move(filename)),
                           options.mode(), options.permissions());
  InitializeAssumedFilename(options);
  if (ABSL_PREDICT_FALSE(!status.ok())) {
    // Not `FdWriterBase::Reset()` to preserve `filename()`.
    BufferedWriter::Reset(kClosed);
    FailWithoutAnnotation(std::move(status));
    return;
  }
  InitializePos(*dest_, std::move(options), /*mode_was_passed_to_open=*/true);
}

template <typename Dest>
template <typename DependentDest,
          std::enable_if_t<FdTargetHasOpenAt<DependentDest>::value, int>>
inline void FdWriter<Dest>::Reset(
    int dir_fd, Initializer<std::string>::AllowingExplicit filename,
    Options options) {
  FdWriterBase::Reset(options.buffer_options());
  absl::Status status =
      dest_.manager().OpenAt(dir_fd, InitializeFilename(std::move(filename)),
                             options.mode(), options.permissions());
  InitializeAssumedFilename(options);
  if (ABSL_PREDICT_FALSE(!status.ok())) {
    // Not `FdWriterBase::Reset()` to preserve `filename()`.
    BufferedWriter::Reset(kClosed);
    FailWithoutAnnotation(std::move(status));
    return;
  }
  InitializePos(*dest_, std::move(options), /*mode_was_passed_to_open=*/true);
}

template <typename Dest>
void FdWriter<Dest>::Done() {
  FdWriterBase::Done();
  if (dest_.IsOwning()) {
    {
      absl::Status status = dest_.get().Close();
      if (ABSL_PREDICT_FALSE(!status.ok())) {
        Fail(std::move(status));
      }
    }
  }
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_FD_WRITER_H_
