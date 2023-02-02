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
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/meta/type_traits.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/fd_close.h"
#include "riegeli/bytes/fd_dependency.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

// Template parameter independent part of `FdMMapReader`.
class FdMMapReaderBase : public ChainReader<Chain> {
 public:
  class Options {
   public:
    Options() noexcept {}

    // If `FdMMapReader` reads from an already open fd, `assumed_filename()`
    // allows to override the filename which is included in failure messages and
    // returned by `filename()`.
    //
    // If this is `absl::nullopt`, then "/dev/stdin", "/dev/stdout",
    // "/dev/stderr", or `absl::StrCat("/proc/self/fd/", fd)` is inferred from
    // the fd (on Windows: "CONIN$", "CONOUT$", "CONERR$", or
    // `absl::StrCat("<fd ", fd, ">")`).
    //
    // If `FdMMapReader` opens a fd with a filename, `assumed_filename()` has no
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

    // If `FdMMapReader` opens a fd with a filename, `mode()` is the second
    // argument of `open()` (on Windows: `_open()`) and specifies the open mode
    // and flags, typically `O_RDONLY` (on Windows: `_O_RDONLY | _O_BINARY`).
    // It must include either `O_RDONLY` or `O_RDWR` (on Windows: `_O_RDONLY` or
    // `_O_RDWR`).
    //
    // If `FdMMapReader` reads from an already open fd, `mode()` has no effect.
    //
    // Default: `O_RDONLY` (on Windows: `_O_RDONLY | _O_BINARY`).
    Options& set_mode(int mode) & {
      mode_ = mode;
      return *this;
    }
    Options&& set_mode(int mode) && { return std::move(set_mode(mode)); }
    int mode() const { return mode_; }

    // If `absl::nullopt`, `FdMMapReader` reads starting from the current fd
    // position. The `FdMMapReader` position is synchronized back to the fd by
    // `Close()` and `Sync()`.
    //
    // If not `absl::nullopt`, `FdMMapReader` reads starting from this position,
    // without disturbing the current fd position. This is useful for multiple
    // readers concurrently reading from the same fd.
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

    // If `absl::nullopt`, the whole file is mapped into memory. `pos()`
    // corresponds to original file positions.
    //
    // If not `absl::nullopt`, only the range of this length starting from the
    // current position or `independent_pos()` is mapped into memory, or the
    // remaining part of the file if that is shorter. `pos()` starts from 0.
    //
    // Default: `absl::nullopt`.
    Options& set_max_length(absl::optional<Position> max_length) & {
      max_length_ = max_length;
      return *this;
    }
    Options&& set_max_length(absl::optional<Position> max_length) && {
      return std::move(set_max_length(max_length));
    }
    absl::optional<Position> max_length() const { return max_length_; }

    // Sets `max_length()` to the remaining part of the file.
    Options& set_remaining_length() & {
      set_max_length(std::numeric_limits<Position>::max());
      return *this;
    }
    Options&& set_remaining_length() && {
      return std::move(set_remaining_length());
    }

   private:
    absl::optional<std::string> assumed_filename_;
#ifndef _WIN32
    int mode_ = O_RDONLY;
#else
    int mode_ = _O_RDONLY | _O_BINARY;
#endif
    absl::optional<Position> independent_pos_;
    absl::optional<Position> max_length_;
  };

  // Returns the fd being read from. If the fd is owned then changed to -1 by
  // `Close()`, otherwise unchanged.
  virtual int SrcFd() const = 0;

  // Returns the original name of the file being read from. Unchanged by
  // `Close()`.
  absl::string_view filename() const { return filename_; }

  absl::Status AnnotateStatusImpl(absl::Status status) override;
  bool SupportsNewReader() override { return true; }

 protected:
  explicit FdMMapReaderBase(Closed) noexcept : ChainReader(kClosed) {}

  explicit FdMMapReaderBase();

  FdMMapReaderBase(FdMMapReaderBase&& that) noexcept;
  FdMMapReaderBase& operator=(FdMMapReaderBase&& that) noexcept;

  void Reset(Closed);
  void Reset();
  void Initialize(int src, absl::optional<std::string>&& assumed_filename,
                  absl::optional<Position> independent_pos,
                  absl::optional<Position> max_length);
  int OpenFd(absl::string_view filename, int mode);
  void InitializePos(int src, absl::optional<Position> independent_pos,
                     absl::optional<Position> max_length);
  void InitializeWithExistingData(int src, absl::string_view filename,
                                  const Chain& data);
  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation);
#ifdef _WIN32
  ABSL_ATTRIBUTE_COLD bool FailWindowsOperation(absl::string_view operation);
#endif

  void Done() override;
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
// `Dependency<int, Src>`, e.g. `OwnedFd` (owned, default),
// `UnownedFd` (not owned), `AnyDependency<int>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as `OwnedFd` if the
// first constructor argument is a filename or an `int`, otherwise as the value
// type of the first constructor argument. This requires C++17.
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
  explicit FdMMapReader(const Src& src, Options options = Options());
  explicit FdMMapReader(Src&& src, Options options = Options());
  explicit FdMMapReader(int src, Options options = Options());

  // Will read from the fd provided by a `Src` constructed from elements of
  // `src_args`. This avoids constructing a temporary `Src` and moving from it.
  template <typename... SrcArgs>
  explicit FdMMapReader(std::tuple<SrcArgs...> src_args,
                        Options options = Options());

  // Opens a file for reading.
  //
  // If opening the file fails, `FdMMapReader` will be failed and closed.
  //
  // This constructor is present only if `Src` is `OwnedFd`.
  template <
      typename DependentSrc = Src,
      std::enable_if_t<std::is_same<DependentSrc, OwnedFd>::value, int> = 0>
  explicit FdMMapReader(absl::string_view filename,
                        Options options = Options());

  FdMMapReader(FdMMapReader&& that) noexcept;
  FdMMapReader& operator=(FdMMapReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `FdMMapReader`. This avoids
  // constructing a temporary `FdMMapReader` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(const Src& src,
                                          Options options = Options());
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Src&& src,
                                          Options options = Options());
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(int src, Options options = Options());
  template <typename... SrcArgs>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(std::tuple<SrcArgs...> src_args,
                                          Options options = Options());
  template <
      typename DependentSrc = Src,
      std::enable_if_t<std::is_same<DependentSrc, OwnedFd>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(absl::string_view filename,
                                          Options options = Options());

  // Returns the object providing and possibly owning the fd being read from. If
  // the fd is owned then changed to -1 by `Close()`, otherwise unchanged.
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  int SrcFd() const override { return src_.get(); }

 protected:
  void Done() override;

 private:
  friend class FdMMapReaderBase;  // For `InitializeWithExistingData()`.

  using FdMMapReaderBase::Initialize;
  void Initialize(absl::string_view filename, Options&& options);
  using FdMMapReaderBase::InitializeWithExistingData;
  void InitializeWithExistingData(int src, absl::string_view filename,
                                  const Chain& data);

  // The object providing and possibly owning the fd being read from.
  Dependency<int, Src> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit FdMMapReader(Closed) -> FdMMapReader<DeleteCtad<Closed>>;
template <typename Src>
explicit FdMMapReader(const Src& src, FdMMapReaderBase::Options options =
                                          FdMMapReaderBase::Options())
    -> FdMMapReader<std::conditional_t<
        absl::disjunction<
            std::is_convertible<const Src&, int>,
            std::is_convertible<const Src&, absl::string_view>>::value,
        OwnedFd, std::decay_t<Src>>>;
template <typename Src>
explicit FdMMapReader(
    Src&& src, FdMMapReaderBase::Options options = FdMMapReaderBase::Options())
    -> FdMMapReader<std::conditional_t<
        absl::disjunction<std::is_convertible<Src&&, int>,
                          std::is_convertible<Src&&, absl::string_view>>::value,
        OwnedFd, std::decay_t<Src>>>;
template <typename... SrcArgs>
explicit FdMMapReader(
    std::tuple<SrcArgs...> src_args,
    FdMMapReaderBase::Options options = FdMMapReaderBase::Options())
    -> FdMMapReader<DeleteCtad<std::tuple<SrcArgs...>>>;
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
  // `filename_` will be set by `Initialize()` or `OpenFd()`.
  base_pos_to_sync_ = absl::nullopt;
}

template <typename Src>
inline FdMMapReader<Src>::FdMMapReader(const Src& src, Options options)
    : src_(src) {
  Initialize(src_.get(), std::move(options.assumed_filename()),
             options.independent_pos(), options.max_length());
}

template <typename Src>
inline FdMMapReader<Src>::FdMMapReader(Src&& src, Options options)
    : src_(std::move(src)) {
  Initialize(src_.get(), std::move(options.assumed_filename()),
             options.independent_pos(), options.max_length());
}

template <typename Src>
inline FdMMapReader<Src>::FdMMapReader(int src, Options options)
    : FdMMapReader(std::forward_as_tuple(src), std::move(options)) {}

template <typename Src>
template <typename... SrcArgs>
inline FdMMapReader<Src>::FdMMapReader(std::tuple<SrcArgs...> src_args,
                                       Options options)
    : src_(std::move(src_args)) {
  Initialize(src_.get(), std::move(options.assumed_filename()),
             options.independent_pos(), options.max_length());
}

template <typename Src>
template <typename DependentSrc,
          std::enable_if_t<std::is_same<DependentSrc, OwnedFd>::value, int>>
inline FdMMapReader<Src>::FdMMapReader(absl::string_view filename,
                                       Options options) {
  Initialize(filename, std::move(options));
}

template <typename Src>
inline FdMMapReader<Src>::FdMMapReader(FdMMapReader&& that) noexcept
    : FdMMapReaderBase(static_cast<FdMMapReaderBase&&>(that)),
      src_(std::move(that.src_)) {}

template <typename Src>
inline FdMMapReader<Src>& FdMMapReader<Src>::operator=(
    FdMMapReader&& that) noexcept {
  FdMMapReaderBase::operator=(static_cast<FdMMapReaderBase&&>(that));
  src_ = std::move(that.src_);
  return *this;
}

template <typename Src>
inline void FdMMapReader<Src>::Reset(Closed) {
  FdMMapReaderBase::Reset(kClosed);
  src_.Reset();
}

template <typename Src>
inline void FdMMapReader<Src>::Reset(const Src& src, Options options) {
  FdMMapReaderBase::Reset();
  src_.Reset(src);
  Initialize(src_.get(), std::move(options.assumed_filename()),
             options.independent_pos(), options.max_length());
}

template <typename Src>
inline void FdMMapReader<Src>::Reset(Src&& src, Options options) {
  FdMMapReaderBase::Reset();
  src_.Reset(std::move(src));
  Initialize(src_.get(), std::move(options.assumed_filename()),
             options.independent_pos(), options.max_length());
}

template <typename Src>
inline void FdMMapReader<Src>::Reset(int src, Options options) {
  Reset(std::forward_as_tuple(src), std::move(options));
}

template <typename Src>
template <typename... SrcArgs>
inline void FdMMapReader<Src>::Reset(std::tuple<SrcArgs...> src_args,
                                     Options options) {
  FdMMapReaderBase::Reset();
  src_.Reset(std::move(src_args));
  Initialize(src_.get(), std::move(options.assumed_filename()),
             options.independent_pos(), options.max_length());
}

template <typename Src>
template <typename DependentSrc,
          std::enable_if_t<std::is_same<DependentSrc, OwnedFd>::value, int>>
inline void FdMMapReader<Src>::Reset(absl::string_view filename,
                                     Options options) {
  FdMMapReaderBase::Reset();
  Initialize(filename, std::move(options));
}

template <typename Src>
void FdMMapReader<Src>::Initialize(absl::string_view filename,
                                   Options&& options) {
  const int src = OpenFd(filename, options.mode());
  if (ABSL_PREDICT_FALSE(src < 0)) return;
  src_.Reset(std::forward_as_tuple(src));
  InitializePos(src_.get(), options.independent_pos(), options.max_length());
}

template <typename Src>
void FdMMapReader<Src>::InitializeWithExistingData(int src,
                                                   absl::string_view filename,
                                                   const Chain& data) {
  FdMMapReaderBase::Reset();
  src_.Reset(std::forward_as_tuple(src));
  FdMMapReaderBase::InitializeWithExistingData(src, filename, data);
}

template <typename Src>
void FdMMapReader<Src>::Done() {
  FdMMapReaderBase::Done();
  {
    const int src = src_.Release();
    if (src >= 0) {
      if (ABSL_PREDICT_FALSE(fd_internal::Close(src) < 0) &&
          ABSL_PREDICT_TRUE(ok())) {
        FailOperation(fd_internal::kCloseFunctionName);
      }
    } else {
      RIEGELI_ASSERT(!src_.is_owning())
          << "The dependency type does not support closing the fd";
    }
  }
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_FD_MMAP_READER_H_
