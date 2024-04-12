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

#include <memory>
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
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/fd_handle.h"
#include "riegeli/bytes/fd_internal_for_headers.h"
#include "riegeli/bytes/reader.h"
#ifndef _WIN32
#include "riegeli/bytes/writer.h"
#endif

namespace riegeli {

// Template parameter independent part of `FdReader`.
class FdReaderBase : public BufferedReader {
 public:
  class Options : public BufferOptionsBase<Options> {
   public:
    Options() noexcept {}

    // `assumed_filename()` allows to override the filename which is included in
    // failure messages and returned by `filename()`.
    //
    // If this is `absl::nullopt` and `FdReader` opens a fd with a filename,
    // then that filename is used.
    //
    // If this is `absl::nullopt` and `FdReader` reads from an already open fd,
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

    // If `FdReader` opens a fd with a filename, `mode()` is the second argument
    // of `open()` (on Windows: `_open()`) and specifies the open mode and
    // flags, typically `O_RDONLY` (on Windows: `_O_RDONLY | _O_BINARY`).
    // It must include either `O_RDONLY` or `O_RDWR` (on Windows: `_O_RDONLY` or
    // `_O_RDWR`).
    //
    // `mode()` can also be changed with `set_inheritable()` and `set_text()`.
    //
    // Default: `O_RDONLY | O_CLOEXEC`
    // (on Windows: `_O_RDONLY | _O_BINARY | _O_NOINHERIT`).
    Options& set_mode(int mode) & {
      mode_ = mode;
      return *this;
    }
    Options&& set_mode(int mode) && { return std::move(set_mode(mode)); }
    int mode() const { return mode_; }

    // If `false`, `execve()` (`CreateProcess()` on Windows) will close the fd.
    //
    // If `true`, the fd will remain open across `execve()` (`CreateProcess()`
    // on Windows).
    //
    // If `FdReader` reads from an already open fd, `inheritable()` has no
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

    // If `false`, data will be read directly from the file. This is called the
    // binary mode.
    //
    // If `true`, text mode translation will be applied on Windows:
    // CR-LF character pairs are translated to LF, and a ^Z character is
    // interpreted as end of file.
    //
    // It is recommended to use `ReadLine()` or `TextReader` instead, which
    // expect a binary mode `Reader`.
    //
    // `set_text()` has an effect only on Windows. It is applicable whenever
    // `FdReader` opens a fd with a filename or reads from an already open fd.
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

    // If `absl::nullopt`, `FdReader` reads at the current fd position.
    //
    // If not `absl::nullopt`, `FdReader` reads starting from this position.
    // The current fd position is not disturbed except on Windows, where seeking
    // and reading is nevertheless atomic. This is useful for multiple readers
    // concurrently reading from the same fd. The fd must support `pread()`
    // (`_get_osfhandle()` and `ReadFile()` with `OVERLAPPED*` on Windows).
    // On Windows binary mode is also required.
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

    // If `true`, supports reading up to the end of the file, then retrying when
    // the file has grown. This disables caching the file size.
    //
    // Default: `false`.
    Options& set_growing_source(bool growing_source) & {
      growing_source_ = growing_source;
      return *this;
    }
    Options&& set_growing_source(bool growing_source) && {
      return std::move(set_growing_source(growing_source));
    }
    bool growing_source() const { return growing_source_; }

   private:
    absl::optional<std::string> assumed_filename_;
#ifndef _WIN32
    int mode_ = O_RDONLY | fd_internal::kCloseOnExec;
#else
    int mode_ = _O_RDONLY | _O_BINARY | fd_internal::kCloseOnExec;
#endif
    absl::optional<Position> assumed_pos_;
    absl::optional<Position> independent_pos_;
    bool growing_source_ = false;
  };

  // Returns the `FdHandle` being read from. Unchanged by `Close()`.
  virtual FdHandle SrcFdHandle() const = 0;

  // Returns the fd being read from. If the fd is owned then changed to -1 by
  // `Close()`, otherwise unchanged.
  virtual int SrcFd() const = 0;

  // Returns the original name of the file being read from. Unchanged by
  // `Close()`.
  absl::string_view filename() const { return filename_; }

  bool ToleratesReadingAhead() override {
    return BufferedReader::ToleratesReadingAhead() ||
           FdReaderBase::SupportsRandomAccess();
  }
  bool SupportsRandomAccess() override { return supports_random_access_; }
  bool SupportsNewReader() override {
#ifndef _WIN32
    return FdReaderBase::SupportsRandomAccess();
#else
    return has_independent_pos_ && FdReaderBase::SupportsRandomAccess();
#endif
  }

 protected:
  explicit FdReaderBase(Closed) noexcept : BufferedReader(kClosed) {}

  explicit FdReaderBase(BufferOptions buffer_options, bool growing_source);

  FdReaderBase(FdReaderBase&& that) noexcept;
  FdReaderBase& operator=(FdReaderBase&& that) noexcept;

  void Reset(Closed);
  void Reset(BufferOptions buffer_options, bool growing_source);
  void Initialize(int src, Options&& options);
  const std::string& InitializeFilename(
      Initializer<std::string>::AllowingExplicit filename);
  bool InitializeAssumedFilename(Options& options);
  void InitializePos(int src, Options&& options
#ifdef _WIN32
                     ,
                     bool mode_was_passed_to_open
#endif
  );
  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation);
#ifdef _WIN32
  ABSL_ATTRIBUTE_COLD bool FailWindowsOperation(absl::string_view operation);
#endif

  void Done() override;
  absl::Status AnnotateStatusImpl(absl::Status status) override;
#ifndef _WIN32
  void SetReadAllHintImpl(bool read_all_hint) override;
#endif
  bool ReadInternal(size_t min_length, size_t max_length, char* dest) override;
#ifndef _WIN32
  bool CopyInternal(Position length, Writer& dest) override;
#endif
  bool SeekBehindBuffer(Position new_pos) override;
  absl::optional<Position> SizeImpl() override;
  std::unique_ptr<Reader> NewReaderImpl(Position initial_pos) override;

 private:
  absl::Status FailedOperationStatus(absl::string_view operation);
  void CheckRandomAccess(int src);

  bool SeekInternal(int src, Position new_pos);

  std::string filename_;
  bool has_independent_pos_ = false;
  bool growing_source_ = false;
  bool supports_random_access_ = false;
  absl::Status random_access_status_;
#ifdef _WIN32
  absl::optional<int> original_mode_;
#endif

  // Invariant: `limit_pos() <= std::numeric_limits<fd_internal::Offset>::max()`
};

// A `Reader` which reads from a file descriptor.
//
// The fd must support:
#ifndef _WIN32
//  * `close()` - if the fd is owned
//  * `read()`  - if `Options::independent_pos() == absl::nullopt`
//  * `pread()` - if `Options::independent_pos() != absl::nullopt`,
//                or for `NewReader()`
//  * `lseek()` - for `Seek()` or `Size()`
//                if `Options::independent_pos() == absl::nullopt`
//  * `fstat()` - for `Seek()` or `Size()`
#else
//  * `_close()`    - if the fd is owned
//  * `_read()`     - if `Options::independent_pos() == absl::nullopt`
//  * `_get_osfhandle()`, `ReadFile()` with `OVERLAPPED*`
//                  - if `Options::independent_pos() != absl::nullopt`
//  * `_lseeki64()` - for `Seek()` or `Size()`
//                    if `Options::independent_pos() == absl::nullopt`
//  * `_fstat64()`  - for `Seek()` or `Size()`
#endif
//
// `FdReader` supports random access if
// `Options::assumed_pos() == absl::nullopt` and the fd supports random access
// (this is assumed if `Options::independent_pos() != absl::nullopt`, otherwise
// this is checked by calling `lseek(SEEK_END)`, or `_lseeki64()` on Windows).
// On Windows binary mode is also required.
//
// On Linux, some virtual file systems ("/proc", "/sys") contain files with
// contents generated on the fly when the files are read. The files appear as
// regular files, with an apparent size of 0 or 4096, and random access is only
// partially supported. `FdReader` properly detects lack of random access for
// "/proc" files; for "/sys" files this is detected only if the filename seen by
// `FdReader` starts with "/sys/". An explicit
// `FdReaderBase::Options().set_assumed_pos(0)` can be used to disable random
// access for such files.
//
// `FdReader` supports `NewReader()` if it supports random access. On Windows
// `independent_pos() != absl::nullopt` is also required.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the fd being read from. `Src` must support
// `Dependency<FdHandle, Src>`, e.g. `OwnedFd` (owned, default),
// `UnownedFd` (not owned), `AnyDependency<FdHandle>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as `OwnedFd` if the
// first constructor argument is a filename or an `int`, otherwise as
// `InitializerTargetT` of the type of the first constructor argument.
// This requires C++17.
//
// Warning: if random access is not supported and the fd is not owned, it will
// have an unpredictable amount of extra data consumed because of buffering.
//
// Until the `FdReader` is closed or no longer used, the fd must not be closed.
// Additionally, if `Options::independent_pos() == absl::nullopt`
// (or unconditionally on Windows), the fd must not have its position changed.
template <typename Src = OwnedFd>
class FdReader : public FdReaderBase {
 public:
  // Creates a closed `FdReader`.
  explicit FdReader(Closed) noexcept : FdReaderBase(kClosed) {}

  // Will read from the fd provided by `src`.
  explicit FdReader(Initializer<Src> src, Options options = Options());

  // Will read from `src`.
  template <typename DependentSrc = Src,
            std::enable_if_t<std::is_constructible<DependentSrc, int>::value,
                             int> = 0>
  explicit FdReader(int src, Options options = Options());

  // Opens a file for reading.
  //
  // If opening the file fails, `FdReader` will be failed and closed.
  //
  // This constructor is present only if `Src` supports `Open()`.
  template <typename DependentSrc = Src,
            std::enable_if_t<FdTargetHasOpen<DependentSrc>::value, int> = 0>
  explicit FdReader(Initializer<std::string>::AllowingExplicit filename,
                    Options options = Options());

  // Opens a file for reading, with the filename interpreted relatively to the
  // directory specified by an existing fd.
  //
  // If opening the file fails, `FdReader` will be failed and closed.
  //
  // This constructor is present only if `Src` supports `OpenAt()`.
  template <typename DependentSrc = Src,
            std::enable_if_t<FdTargetHasOpenAt<DependentSrc>::value, int> = 0>
  explicit FdReader(int dir_fd,
                    Initializer<std::string>::AllowingExplicit filename,
                    Options options = Options());

  FdReader(FdReader&& that) noexcept;
  FdReader& operator=(FdReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `FdReader`. This avoids
  // constructing a temporary `FdReader` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Src> src,
                                          Options options = Options());
  template <typename DependentSrc = Src,
            std::enable_if_t<std::is_constructible<DependentSrc, int>::value,
                             int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(int src, Options options = Options());
  template <typename DependentSrc = Src,
            std::enable_if_t<FdTargetHasOpen<DependentSrc>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      Initializer<std::string>::AllowingExplicit filename,
      Options options = Options());
  template <typename DependentSrc = Src,
            std::enable_if_t<FdTargetHasOpenAt<DependentSrc>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      int dir_fd, Initializer<std::string>::AllowingExplicit filename,
      Options options = Options());

  // Returns the object providing and possibly owning the fd being read from.
  // Unchanged by `Close()`.
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  FdHandle SrcFdHandle() const override { return src_.get(); }
  int SrcFd() const override { return *src_; }

 protected:
  void Done() override;

 private:
  // The object providing and possibly owning the fd being read from.
  Dependency<FdHandle, Src> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit FdReader(Closed) -> FdReader<DeleteCtad<Closed>>;
template <typename Src>
explicit FdReader(Src&& src,
                  FdReaderBase::Options options = FdReaderBase::Options())
    -> FdReader<std::conditional_t<
        absl::disjunction<
            std::is_convertible<Src&&, int>,
            std::is_convertible<
                Src&&, Initializer<std::string>::AllowingExplicit>>::value,
        OwnedFd, InitializerTargetT<Src>>>;
explicit FdReader(int dir_fd,
                  Initializer<std::string>::AllowingExplicit filename,
                  FdReaderBase::Options options = FdReaderBase::Options())
    -> FdReader<OwnedFd>;
#endif

// Implementation details follow.

inline FdReaderBase::FdReaderBase(BufferOptions buffer_options,
                                  bool growing_source)
    : BufferedReader(buffer_options), growing_source_(growing_source) {}

inline FdReaderBase::FdReaderBase(FdReaderBase&& that) noexcept
    : BufferedReader(static_cast<BufferedReader&&>(that)),
      filename_(std::exchange(that.filename_, std::string())),
      has_independent_pos_(that.has_independent_pos_),
      growing_source_(that.growing_source_),
      supports_random_access_(
          std::exchange(that.supports_random_access_, false)),
      random_access_status_(std::move(that.random_access_status_))
#ifdef _WIN32
      ,
      original_mode_(that.original_mode_)
#endif
{
}

inline FdReaderBase& FdReaderBase::operator=(FdReaderBase&& that) noexcept {
  BufferedReader::operator=(static_cast<BufferedReader&&>(that));
  filename_ = std::exchange(that.filename_, std::string());
  has_independent_pos_ = that.has_independent_pos_;
  growing_source_ = that.growing_source_;
  supports_random_access_ = std::exchange(that.supports_random_access_, false);
  random_access_status_ = std::move(that.random_access_status_);
#ifdef _WIN32
  original_mode_ = that.original_mode_;
#endif
  return *this;
}

inline void FdReaderBase::Reset(Closed) {
  BufferedReader::Reset(kClosed);
  filename_ = std::string();
  has_independent_pos_ = false;
  growing_source_ = false;
  supports_random_access_ = false;
  random_access_status_ = absl::OkStatus();
#ifdef _WIN32
  original_mode_ = absl::nullopt;
#endif
}

inline void FdReaderBase::Reset(BufferOptions buffer_options,
                                bool growing_source) {
  BufferedReader::Reset(buffer_options);
  // `filename_` will be set by `Initialize()`, `InitializeFilename()`, or
  // `InitializeAssumedFilename()`.
  has_independent_pos_ = false;
  growing_source_ = growing_source;
  supports_random_access_ = false;
  random_access_status_ = absl::OkStatus();
#ifdef _WIN32
  original_mode_ = absl::nullopt;
#endif
}

inline const std::string& FdReaderBase::InitializeFilename(
    Initializer<std::string>::AllowingExplicit filename) {
  std::move(filename).AssignTo(filename_);
  return filename_;
}

inline bool FdReaderBase::InitializeAssumedFilename(Options& options) {
  if (options.assumed_filename() != absl::nullopt) {
    filename_ = *std::move(options.assumed_filename());
    return true;
  } else {
    return false;
  }
}

template <typename Src>
inline FdReader<Src>::FdReader(Initializer<Src> src, Options options)
    : FdReaderBase(options.buffer_options(), options.growing_source()),
      src_(std::move(src)) {
  Initialize(*src_, std::move(options));
}

template <typename Src>
template <
    typename DependentSrc,
    std::enable_if_t<std::is_constructible<DependentSrc, int>::value, int>>
inline FdReader<Src>::FdReader(int src, Options options)
    : FdReader(riegeli::Maker(src), std::move(options)) {}

template <typename Src>
template <typename DependentSrc,
          std::enable_if_t<FdTargetHasOpen<DependentSrc>::value, int>>
inline FdReader<Src>::FdReader(
    Initializer<std::string>::AllowingExplicit filename, Options options)
    : FdReaderBase(options.buffer_options(), options.growing_source()) {
  absl::Status status =
      src_.manager().Open(InitializeFilename(std::move(filename)),
                          options.mode(), OwnedFd::kDefaultPermissions);
  InitializeAssumedFilename(options);
  if (ABSL_PREDICT_FALSE(!status.ok())) {
    // Not `FdReaderBase::Reset()` to preserve `filename()`.
    BufferedReader::Reset(kClosed);
    FailWithoutAnnotation(std::move(status));
    return;
  }
  InitializePos(*src_, std::move(options)
#ifdef _WIN32
                           ,
                /*mode_was_passed_to_open=*/true
#endif
  );
}

template <typename Src>
template <typename DependentSrc,
          std::enable_if_t<FdTargetHasOpenAt<DependentSrc>::value, int>>
inline FdReader<Src>::FdReader(
    int dir_fd, Initializer<std::string>::AllowingExplicit filename,
    Options options)
    : FdReaderBase(options.buffer_options(), options.growing_source()) {
  absl::Status status =
      src_.manager().OpenAt(dir_fd, InitializeFilename(std::move(filename)),
                            options.mode(), OwnedFd::kDefaultPermissions);
  InitializeAssumedFilename(options);
  if (ABSL_PREDICT_FALSE(!status.ok())) {
    // Not `FdReaderBase::Reset()` to preserve `filename()`.
    BufferedReader::Reset(kClosed);
    FailWithoutAnnotation(std::move(status));
    return;
  }
  InitializePos(*src_, std::move(options)
#ifdef _WIN32
                           ,
                /*mode_was_passed_to_open=*/true
#endif
  );
}

template <typename Src>
inline FdReader<Src>::FdReader(FdReader&& that) noexcept
    : FdReaderBase(static_cast<FdReaderBase&&>(that)),
      src_(std::move(that.src_)) {}

template <typename Src>
inline FdReader<Src>& FdReader<Src>::operator=(FdReader&& that) noexcept {
  FdReaderBase::operator=(static_cast<FdReaderBase&&>(that));
  src_ = std::move(that.src_);
  return *this;
}

template <typename Src>
inline void FdReader<Src>::Reset(Closed) {
  FdReaderBase::Reset(kClosed);
  src_.Reset();
}

template <typename Src>
inline void FdReader<Src>::Reset(Initializer<Src> src, Options options) {
  FdReaderBase::Reset(options.buffer_options(), options.growing_source());
  src_.Reset(std::move(src));
  Initialize(*src_, std::move(options));
}

template <typename Src>
template <
    typename DependentSrc,
    std::enable_if_t<std::is_constructible<DependentSrc, int>::value, int>>
inline void FdReader<Src>::Reset(int src, Options options) {
  Reset(riegeli::Maker(src), std::move(options));
}

template <typename Src>
template <typename DependentSrc,
          std::enable_if_t<FdTargetHasOpen<DependentSrc>::value, int>>
inline void FdReader<Src>::Reset(
    Initializer<std::string>::AllowingExplicit filename, Options options) {
  FdReaderBase::Reset(options.buffer_options(), options.growing_source());
  absl::Status status =
      src_.manager().Open(InitializeFilename(std::move(filename)),
                          options.mode(), OwnedFd::kDefaultPermissions);
  InitializeAssumedFilename(options);
  if (ABSL_PREDICT_FALSE(!status.ok())) {
    // Not `FdReaderBase::Reset()` to preserve `filename()`.
    BufferedReader::Reset(kClosed);
    FailWithoutAnnotation(std::move(status));
    return;
  }
  InitializePos(*src_, std::move(options)
#ifdef _WIN32
                           ,
                /*mode_was_passed_to_open=*/true
#endif
  );
}

template <typename Src>
template <typename DependentSrc,
          std::enable_if_t<FdTargetHasOpenAt<DependentSrc>::value, int>>
inline void FdReader<Src>::Reset(
    int dir_fd, Initializer<std::string>::AllowingExplicit filename,
    Options options) {
  FdReaderBase::Reset(options.buffer_options(), options.growing_source());
  absl::Status status =
      src_.manager().OpenAt(dir_fd, InitializeFilename(std::move(filename)),
                            options.mode(), OwnedFd::kDefaultPermissions);
  InitializeAssumedFilename(options);
  if (ABSL_PREDICT_FALSE(!status.ok())) {
    // Not `FdReaderBase::Reset()` to preserve `filename()`.
    BufferedReader::Reset(kClosed);
    FailWithoutAnnotation(std::move(status));
    return;
  }
  InitializePos(*src_, std::move(options)
#ifdef _WIN32
                           ,
                /*mode_was_passed_to_open=*/true
#endif
  );
}

template <typename Src>
void FdReader<Src>::Done() {
  FdReaderBase::Done();
  if (src_.IsOwning()) {
    {
      absl::Status status = src_.get().Close();
      if (ABSL_PREDICT_FALSE(!status.ok())) {
        Fail(std::move(status));
      }
    }
  }
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_FD_READER_H_
