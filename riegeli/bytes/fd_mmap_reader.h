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

#ifndef RIEGELI_BYTES_FD_MMAP_READER_H_
#define RIEGELI_BYTES_FD_MMAP_READER_H_

#include <fcntl.h>

#include <limits>
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
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/object.h"
#include "riegeli/base/reset.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/fd_handle.h"
#include "riegeli/bytes/fd_internal_for_headers.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

// Template parameter independent part of `FdMMapReader`.
class FdMMapReaderBase : public ChainReader<Chain> {
 public:
  class Options {
   public:
    Options() noexcept {}

    // If `FdMMapReader` opens a fd with a filename, `mode()` is the second
    // argument of `open()` (on Windows: `_open()`) and specifies the open mode
    // and flags, typically `O_RDONLY` (on Windows: `_O_RDONLY | _O_BINARY`).
    // It must include either `O_RDONLY` or `O_RDWR` (on Windows: `_O_RDONLY` or
    // `_O_RDWR`).
    //
    // If `FdMMapReader` reads from an already open fd, `mode()` has no effect.
    //
    // `mode()` can also be changed with `set_inheritable()`.
    //
    // Default: `O_RDONLY | O_CLOEXEC`
    // (on Windows: `_O_RDONLY | _O_BINARY | _O_NOINHERIT`).
    Options& set_mode(int mode) & ABSL_ATTRIBUTE_LIFETIME_BOUND {
      mode_ = mode;
      return *this;
    }
    Options&& set_mode(int mode) && ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_mode(mode));
    }
    int mode() const { return mode_; }

    // If `false`, `execve()` (`CreateProcess()` on Windows) will close the fd.
    //
    // If `true`, the fd will remain open across `execve()` (`CreateProcess()`
    // on Windows).
    //
    // If `FdMMapReader` reads from an already open fd, `inheritable()` has no
    // effect.
    //
    // `set_inheritable()` affects `mode()`.
    //
    // Default: `false`.
    Options& set_inheritable(bool inheritable) & ABSL_ATTRIBUTE_LIFETIME_BOUND {
      mode_ = (mode_ & ~fd_internal::kCloseOnExec) |
              (inheritable ? 0 : fd_internal::kCloseOnExec);
      return *this;
    }
    Options&& set_inheritable(bool inheritable) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_inheritable(inheritable));
    }
    bool inheritable() const {
      return (mode_ & fd_internal::kCloseOnExec) == 0;
    }

    // If `absl::nullopt`, `FdMMapReader` reads starting from the current fd
    // position. The `FdMMapReader` position is synchronized back to the fd by
    // `Close()` and `Sync()`.
    //
    // If not `absl::nullopt`, `FdMMapReader` reads starting from this position,
    // without disturbing the current fd position. This is useful for multiple
    // readers concurrently reading from the same fd.
    //
    // Default: `absl::nullopt`.
    Options& set_independent_pos(absl::optional<Position> independent_pos) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      independent_pos_ = independent_pos;
      return *this;
    }
    Options&& set_independent_pos(absl::optional<Position> independent_pos) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_independent_pos(independent_pos));
    }
    absl::optional<Position> independent_pos() const {
      return independent_pos_;
    }

    // If `absl::nullopt`, the whole file is mapped into memory. `pos()`
    // corresponds to original file positions.
    //
    // If not `absl::nullopt`, only the range of this length starting from the
    // current position or `independent_pos()` is mapped into memory, or the
    // remaining part of the file if that is shorter. `pos()` starts from 0.
    //
    // Default: `absl::nullopt`.
    Options& set_max_length(absl::optional<Position> max_length) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      max_length_ = max_length;
      return *this;
    }
    Options&& set_max_length(absl::optional<Position> max_length) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_max_length(max_length));
    }
    absl::optional<Position> max_length() const { return max_length_; }

    // Sets `max_length()` to the remaining part of the file.
    Options& set_remaining_length() & ABSL_ATTRIBUTE_LIFETIME_BOUND {
      set_max_length(std::numeric_limits<Position>::max());
      return *this;
    }
    Options&& set_remaining_length() && ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_remaining_length());
    }

   private:
#ifndef _WIN32
    int mode_ = O_RDONLY | fd_internal::kCloseOnExec;
#else
    int mode_ = _O_RDONLY | _O_BINARY | fd_internal::kCloseOnExec;
#endif
    absl::optional<Position> independent_pos_;
    absl::optional<Position> max_length_;
  };

  // Returns the `FdHandle` being read from. Unchanged by `Close()`.
  virtual FdHandle SrcFdHandle() const ABSL_ATTRIBUTE_LIFETIME_BOUND = 0;

  // Returns the fd being read from. If the fd is owned then changed to -1 by
  // `Close()`, otherwise unchanged.
  virtual int SrcFd() const ABSL_ATTRIBUTE_LIFETIME_BOUND = 0;

  // Returns the original name of the file being read from. Unchanged by
  // `Close()`.
  absl::string_view filename() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return filename_;
  }

  bool SupportsNewReader() override { return true; }

 protected:
  explicit FdMMapReaderBase(Closed) noexcept : ChainReader(kClosed) {}

  explicit FdMMapReaderBase();

  FdMMapReaderBase(FdMMapReaderBase&& that) noexcept;
  FdMMapReaderBase& operator=(FdMMapReaderBase&& that) noexcept;

  void Reset(Closed);
  void Reset();
  void Initialize(int src, Options&& options);
  const std::string& InitializeFilename(
      Initializer<std::string>::AllowingExplicit filename);
  void InitializePos(int src, Options&& options);
  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation);
#ifdef _WIN32
  ABSL_ATTRIBUTE_COLD bool FailWindowsOperation(absl::string_view operation);
#endif

  void Done() override;
  absl::Status AnnotateStatusImpl(absl::Status status) override;
#ifndef _WIN32
  void SetReadAllHintImpl(bool read_all_hint) override;
#endif
  bool SyncImpl(SyncType sync_type) override;
  std::unique_ptr<Reader> NewReaderImpl(Position initial_pos) override;

 private:
  std::string filename_;
  absl::optional<Position> base_pos_to_sync_;
};

// A `Reader` which reads from a file descriptor by mapping the whole file to
// memory.
//
// The fd must support:
#ifndef _WIN32
//  * `close()` - if the fd is owned
//  * `fstat()`
//  * `mmap()`
//  * `lseek()` - if `Options::independent_pos() == absl::nullopt`
#else
//  * `_close()`    - if the fd is owned
//  * `_fstat64()`
//  * `_get_osfhandle()`, `CreateFileMappingW()`, `MapViewOfFile()`
//  * `_lseeki64()` - if `Options::independent_pos() == absl::nullopt`
#endif
//
// `FdMMapReader` supports random access and `NewReader()`.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the fd being read from. `Src` must support
// `Dependency<FdHandle, Src>`, e.g. `OwnedFd` (owned, default),
// `UnownedFd` (not owned), `AnyFd` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as `OwnedFd` if the
// first constructor argument is a filename or an `int`, otherwise as
// `InitializerTargetT` of the type of the first constructor argument.
// This requires C++17.
//
// The fd must not be closed until the `FdMMapReader` is closed or no longer
// used. File contents must not be changed while data read from the file is
// accessed without a memory copy.
template <typename Src = OwnedFd>
class FdMMapReader : public FdMMapReaderBase {
 public:
  // Creates a closed `FdMMapReader`.
  explicit FdMMapReader(Closed) noexcept : FdMMapReaderBase(kClosed) {}

  // Will read from the fd provided by `src`.
  explicit FdMMapReader(Initializer<Src> src, Options options = Options());

  // Will read from `src`.
  template <typename DependentSrc = Src,
            std::enable_if_t<std::is_constructible<DependentSrc, int>::value,
                             int> = 0>
  explicit FdMMapReader(int src, Options options = Options());

  // Opens a file for reading.
  //
  // If opening the file fails, `FdMMapReader` will be failed and closed.
  //
  // This constructor is present only if `Src` supports `Open()`.
  template <typename DependentSrc = Src,
            std::enable_if_t<FdTargetHasOpen<DependentSrc>::value, int> = 0>
  explicit FdMMapReader(Initializer<std::string>::AllowingExplicit filename,
                        Options options = Options());

  // Opens a file for reading, with the filename interpreted relatively to the
  // directory specified by an existing fd.
  //
  // If opening the file fails, `FdMMapReader` will be failed and closed.
  //
  // This constructor is present only if `Src` supports `Open()`.
  template <typename DependentSrc = Src,
            std::enable_if_t<FdTargetHasOpenAt<DependentSrc>::value, int> = 0>
  explicit FdMMapReader(int dir_fd,
                        Initializer<std::string>::AllowingExplicit filename,
                        Options options = Options());

  FdMMapReader(FdMMapReader&& that) = default;
  FdMMapReader& operator=(FdMMapReader&& that) = default;

  // Makes `*this` equivalent to a newly constructed `FdMMapReader`. This avoids
  // constructing a temporary `FdMMapReader` and moving from it.
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
  Src& src() ABSL_ATTRIBUTE_LIFETIME_BOUND { return src_.manager(); }
  const Src& src() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return src_.manager();
  }
  FdHandle SrcFdHandle() const ABSL_ATTRIBUTE_LIFETIME_BOUND override {
    return src_.get();
  }
  int SrcFd() const ABSL_ATTRIBUTE_LIFETIME_BOUND override { return *src_; }

 protected:
  void Done() override;

 private:
  friend class FdMMapReaderBase;  // For `InitializeWithExistingData()`.

  void InitializeWithExistingData(
      int src, Initializer<std::string>::AllowingExplicit filename,
      const Chain& data);

  // The object providing and possibly owning the fd being read from.
  Dependency<FdHandle, Src> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit FdMMapReader(Closed) -> FdMMapReader<DeleteCtad<Closed>>;
template <typename Src>
explicit FdMMapReader(
    Src&& src, FdMMapReaderBase::Options options = FdMMapReaderBase::Options())
    -> FdMMapReader<std::conditional_t<
        absl::disjunction<
            std::is_convertible<Src&&, int>,
            std::is_convertible<
                Src&&, Initializer<std::string>::AllowingExplicit>>::value,
        OwnedFd, InitializerTargetT<Src>>>;
explicit FdMMapReader(int dir_fd,
                      Initializer<std::string>::AllowingExplicit filename,
                      FdMMapReaderBase::Options options =
                          FdMMapReaderBase::Options()) -> FdMMapReader<OwnedFd>;
#endif

// Implementation details follow.

inline FdMMapReaderBase::FdMMapReaderBase()
    // The `Chain` to read from is not known yet. `ChainReader` will be reset in
    // `Initialize()` to read from the `Chain` when it is known.
    : ChainReader(kClosed) {}

inline FdMMapReaderBase::FdMMapReaderBase(FdMMapReaderBase&& that) noexcept
    : ChainReader(static_cast<ChainReader&&>(that)),
      filename_(std::exchange(that.filename_, std::string())),
      base_pos_to_sync_(that.base_pos_to_sync_) {}

inline FdMMapReaderBase& FdMMapReaderBase::operator=(
    FdMMapReaderBase&& that) noexcept {
  ChainReader::operator=(static_cast<ChainReader&&>(that));
  filename_ = std::exchange(that.filename_, std::string());
  base_pos_to_sync_ = that.base_pos_to_sync_;
  return *this;
}

inline void FdMMapReaderBase::Reset(Closed) {
  ChainReader::Reset(kClosed);
  filename_ = std::string();
  base_pos_to_sync_ = absl::nullopt;
}

inline void FdMMapReaderBase::Reset() {
  // The `Chain` to read from is not known yet. `ChainReader` will be reset in
  // `Initialize()` to read from the `Chain` when it is known.
  ChainReader::Reset(kClosed);
  // `filename_` will be set by `Initialize()` or `InitializeFilename()`.
  base_pos_to_sync_ = absl::nullopt;
}

inline const std::string& FdMMapReaderBase::InitializeFilename(
    Initializer<std::string>::AllowingExplicit filename) {
  riegeli::Reset(filename_, std::move(filename));
  return filename_;
}

template <typename Src>
inline FdMMapReader<Src>::FdMMapReader(Initializer<Src> src, Options options)
    : src_(std::move(src)) {
  Initialize(*src_, std::move(options));
}

template <typename Src>
template <
    typename DependentSrc,
    std::enable_if_t<std::is_constructible<DependentSrc, int>::value, int>>
inline FdMMapReader<Src>::FdMMapReader(int src, Options options)
    : FdMMapReader(riegeli::Maker(src), std::move(options)) {}

template <typename Src>
template <typename DependentSrc,
          std::enable_if_t<FdTargetHasOpen<DependentSrc>::value, int>>
inline FdMMapReader<Src>::FdMMapReader(
    Initializer<std::string>::AllowingExplicit filename, Options options) {
  absl::Status status =
      src_.manager().Open(InitializeFilename(std::move(filename)),
                          options.mode(), OwnedFd::kDefaultPermissions);
  if (ABSL_PREDICT_FALSE(!status.ok())) {
    // Not `FdMMapReaderBase::Reset()` to preserve `filename()`.
    ChainReader::Reset(kClosed);
    FailWithoutAnnotation(std::move(status));
    return;
  }
  InitializePos(*src_, std::move(options));
}

template <typename Src>
template <typename DependentSrc,
          std::enable_if_t<FdTargetHasOpenAt<DependentSrc>::value, int>>
inline FdMMapReader<Src>::FdMMapReader(
    int dir_fd, Initializer<std::string>::AllowingExplicit filename,
    Options options) {
  absl::Status status =
      src_.manager().OpenAt(dir_fd, InitializeFilename(std::move(filename)),
                            options.mode(), OwnedFd::kDefaultPermissions);
  if (ABSL_PREDICT_FALSE(!status.ok())) {
    // Not `FdMMapReaderBase::Reset()` to preserve `filename()`.
    ChainReader::Reset(kClosed);
    FailWithoutAnnotation(std::move(status));
    return;
  }
  InitializePos(*src_, std::move(options));
}

template <typename Src>
inline void FdMMapReader<Src>::Reset(Closed) {
  FdMMapReaderBase::Reset(kClosed);
  src_.Reset();
}

template <typename Src>
inline void FdMMapReader<Src>::Reset(Initializer<Src> src, Options options) {
  FdMMapReaderBase::Reset();
  src_.Reset(std::move(src));
  Initialize(*src_, std::move(options));
}

template <typename Src>
template <
    typename DependentSrc,
    std::enable_if_t<std::is_constructible<DependentSrc, int>::value, int>>
inline void FdMMapReader<Src>::Reset(int src, Options options) {
  Reset(riegeli::Maker(src), std::move(options));
}

template <typename Src>
template <typename DependentSrc,
          std::enable_if_t<FdTargetHasOpen<DependentSrc>::value, int>>
inline void FdMMapReader<Src>::Reset(
    Initializer<std::string>::AllowingExplicit filename, Options options) {
  FdMMapReaderBase::Reset();
  absl::Status status =
      src_.manager().Open(InitializeFilename(std::move(filename)),
                          options.mode(), OwnedFd::kDefaultPermissions);
  if (ABSL_PREDICT_FALSE(!status.ok())) {
    // Not `FdMMapReaderBase::Reset()` to preserve `filename()`.
    ChainReader::Reset(kClosed);
    FailWithoutAnnotation(std::move(status));
    return;
  }
  InitializePos(*src_, std::move(options));
}

template <typename Src>
template <typename DependentSrc,
          std::enable_if_t<FdTargetHasOpenAt<DependentSrc>::value, int>>
inline void FdMMapReader<Src>::Reset(
    int dir_fd, Initializer<std::string>::AllowingExplicit filename,
    Options options) {
  FdMMapReaderBase::Reset();
  absl::Status status =
      src_.manager().OpenAt(dir_fd, InitializeFilename(std::move(filename)),
                            options.mode(), OwnedFd::kDefaultPermissions);
  if (ABSL_PREDICT_FALSE(!status.ok())) {
    // Not `FdMMapReaderBase::Reset()` to preserve `filename()`.
    ChainReader::Reset(kClosed);
    FailWithoutAnnotation(std::move(status));
    return;
  }
  InitializePos(*src_, std::move(options));
}

template <typename Src>
void FdMMapReader<Src>::InitializeWithExistingData(
    int src, Initializer<std::string>::AllowingExplicit filename,
    const Chain& data) {
  FdMMapReaderBase::Reset();
  src_.Reset(riegeli::Maker(src));
  InitializeFilename(std::move(filename));
  ChainReader::Reset(data);
}

template <typename Src>
void FdMMapReader<Src>::Done() {
  FdMMapReaderBase::Done();
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

#endif  // RIEGELI_BYTES_FD_MMAP_READER_H_
