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
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/fd_dependency.h"
#include "riegeli/bytes/fd_internal.h"
// TODO: Temporary.
#include "riegeli/bytes/fd_mmap_reader.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

// Template parameter independent part of `FdReader`.
class FdReaderBase : public BufferedReader {
 public:
  class Options : public BufferOptionsBase<Options> {
   public:
    Options() noexcept {}

    // If `FdReader` reads from an already open fd, `assumed_filename()` allows
    // to override the filename which is included in failure messages and
    // returned by `filename()`.
    //
    // If this is `absl::nullopt`, then "/dev/stdin", "/dev/stdout",
    // "/dev/stderr", or "/proc/self/fd/<fd>" is assumed.
    //
    // If `FdReader` opens a fd with a filename, `assumed_filename()` has no
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

    // If `FdReader` opens a fd with a filename, `mode()` is the second argument
    // of `open()` and specifies the open mode and flags, typically `O_RDONLY`.
    // It must include either `O_RDONLY` or `O_RDWR`.
    //
    // If `FdReader` reads from an already open fd, `mode()` has no effect.
    //
    // Default: `O_RDONLY`.
    Options& set_mode(int mode) & {
      mode_ = mode;
      return *this;
    }
    Options&& set_mode(int mode) && { return std::move(set_mode(mode)); }
    int mode() const { return mode_; }

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

    // If `absl::nullopt`, `FdReader` reads at the current fd position.
    //
    // If not `absl::nullopt`, `FdReader` reads starting from this position,
    // without disturbing the current fd position. This is useful for multiple
    // readers concurrently reading from the same fd. The fd must support
    // `pread()`.
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
    int mode_ = O_RDONLY;
    absl::optional<Position> assumed_pos_;
    absl::optional<Position> independent_pos_;
    bool growing_source_ = false;
  };

  // Returns the fd being read from. If the fd is owned then changed to -1 by
  // `Close()`, otherwise unchanged.
  virtual int SrcFd() const = 0;

  // Returns the original name of the file being read from. Unchanged by
  // `Close()`.
  absl::string_view filename() const { return filename_; }

  bool ToleratesReadingAhead() override {
    return read_all_hint() || supports_random_access();
  }
  bool SupportsRandomAccess() override { return supports_random_access(); }
  bool SupportsNewReader() override { return supports_random_access(); }

 protected:
  explicit FdReaderBase(Closed) noexcept : BufferedReader(kClosed) {}

  explicit FdReaderBase(const BufferOptions& buffer_options,
                        bool growing_source);

  FdReaderBase(FdReaderBase&& that) noexcept;
  FdReaderBase& operator=(FdReaderBase&& that) noexcept;

  void Reset(Closed);
  void Reset(const BufferOptions& buffer_options, bool growing_source);
  void Initialize(int src, absl::optional<std::string>&& assumed_filename,
                  absl::optional<Position> assumed_pos,
                  absl::optional<Position> independent_pos);
  int OpenFd(absl::string_view filename, int mode);
  void InitializePos(int src, absl::optional<Position> assumed_pos,
                     absl::optional<Position> independent_pos);
  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation);
  bool supports_random_access() const { return supports_random_access_; }

  absl::Status AnnotateStatusImpl(absl::Status status) override;
  bool ReadInternal(size_t min_length, size_t max_length, char* dest) override;
  bool SeekBehindBuffer(Position new_pos) override;
  absl::optional<Position> SizeImpl() override;
  std::unique_ptr<Reader> NewReaderImpl(Position initial_pos) override;

 private:
  bool SeekInternal(int src, Position new_pos);

  std::string filename_;
  bool has_independent_pos_ = false;
  bool growing_source_ = false;
  bool supports_random_access_ = false;

  // Invariant: `limit_pos() <= std::numeric_limits<off_t>::max()`
};

// A `Reader` which reads from a file descriptor.
//
// The fd must support:
//  * `close()` - if the fd is owned
//  * `read()`  - if `Options::independent_pos() == absl::nullopt`
//  * `pread()` - if `Options::independent_pos() != absl::nullopt`,
//                or for `NewReader()`
//  * `lseek()` - for `Seek()` or `Size()`
//                if `Options::independent_pos() == absl::nullopt`
//  * `fstat()` - for `Seek()` or `Size()`
//
// `FdReader` supports random access if
// `Options::assumed_pos() == absl::nullopt` and the fd supports random access
// (this is assumed if `Options::independent_pos() != absl::nullopt`, otherwise
// this is checked by calling `lseek(SEEK_END)`).
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
// `FdReader` supports `NewReader()` if it supports random access.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the fd being read from. `Src` must support
// `Dependency<int, Src>`, e.g. `OwnedFd` (owned, default), `UnownedFd`
// (not owned).
//
// By relying on CTAD the template argument can be deduced as `OwnedFd` if the
// first constructor argument is a filename or an `int`, otherwise as the value
// type of the first constructor argument. This requires C++17.
//
// Warning: if random access is not supported and the fd is not owned, it will
// have an unpredictable amount of extra data consumed because of buffering.
//
// Until the `FdReader` is closed or no longer used, the fd must not be closed;
// additionally, if `Options::independent_pos() == absl::nullopt`, the fd must
// not have its position changed.
template <typename Src = OwnedFd>
class FdReader : public FdReaderBase {
 public:
  // Creates a closed `FdReader`.
  explicit FdReader(Closed) noexcept : FdReaderBase(kClosed) {}

  // Will read from the fd provided by `src`.
  explicit FdReader(const Src& src, Options options = Options());
  explicit FdReader(Src&& src, Options options = Options());
  explicit FdReader(int src, Options options = Options());

  // Will read from the fd provided by a `Src` constructed from elements of
  // `src_args`. This avoids constructing a temporary `Src` and moving from it.
  template <typename... SrcArgs>
  explicit FdReader(std::tuple<SrcArgs...> src_args,
                    Options options = Options());

  // Opens a file for reading.
  //
  // If opening the file fails, `FdReader` will be failed and closed.
  //
  // This constructor is present only if `Src` is `OwnedFd`.
  template <
      typename DependentSrc = Src,
      std::enable_if_t<std::is_same<DependentSrc, OwnedFd>::value, int> = 0>
  explicit FdReader(absl::string_view filename, Options options = Options());

  FdReader(FdReader&& that) noexcept;
  FdReader& operator=(FdReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `FdReader`. This avoids
  // constructing a temporary `FdReader` and moving from it.
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
  using FdReaderBase::Initialize;
  void Initialize(absl::string_view filename, Options&& options);

  // The object providing and possibly owning the fd being read from.
  Dependency<int, Src> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit FdReader(Closed)->FdReader<DeleteCtad<Closed>>;
template <typename Src>
explicit FdReader(const Src& src,
                  FdReaderBase::Options options = FdReaderBase::Options())
    -> FdReader<std::conditional_t<
        std::is_convertible<const Src&, int>::value ||
            std::is_convertible<const Src&, absl::string_view>::value,
        OwnedFd, std::decay_t<Src>>>;
template <typename Src>
explicit FdReader(Src&& src,
                  FdReaderBase::Options options = FdReaderBase::Options())
    -> FdReader<std::conditional_t<
        std::is_convertible<Src&&, int>::value ||
            std::is_convertible<Src&&, absl::string_view>::value,
        OwnedFd, std::decay_t<Src>>>;
template <typename... SrcArgs>
explicit FdReader(std::tuple<SrcArgs...> src_args,
                  FdReaderBase::Options options = FdReaderBase::Options())
    -> FdReader<DeleteCtad<std::tuple<SrcArgs...>>>;
#endif

// Implementation details follow.

inline FdReaderBase::FdReaderBase(const BufferOptions& buffer_options,
                                  bool growing_source)
    : BufferedReader(buffer_options), growing_source_(growing_source) {}

inline FdReaderBase::FdReaderBase(FdReaderBase&& that) noexcept
    : BufferedReader(static_cast<BufferedReader&&>(that)),
      filename_(std::exchange(that.filename_, std::string())),
      has_independent_pos_(that.has_independent_pos_),
      growing_source_(that.growing_source_),
      supports_random_access_(that.supports_random_access_) {}

inline FdReaderBase& FdReaderBase::operator=(FdReaderBase&& that) noexcept {
  BufferedReader::operator=(static_cast<BufferedReader&&>(that));
  filename_ = std::exchange(that.filename_, std::string());
  has_independent_pos_ = that.has_independent_pos_;
  growing_source_ = that.growing_source_;
  supports_random_access_ = that.supports_random_access_;
  return *this;
}

inline void FdReaderBase::Reset(Closed) {
  BufferedReader::Reset(kClosed);
  filename_ = std::string();
  has_independent_pos_ = false;
  growing_source_ = false;
  supports_random_access_ = false;
}

inline void FdReaderBase::Reset(const BufferOptions& buffer_options,
                                bool growing_source) {
  BufferedReader::Reset(buffer_options);
  // `filename_` will be set by `Initialize()` or `OpenFd()`.
  has_independent_pos_ = false;
  growing_source_ = growing_source;
  supports_random_access_ = false;
}

template <typename Src>
inline FdReader<Src>::FdReader(const Src& src, Options options)
    : FdReaderBase(options.buffer_options(), options.growing_source()),
      src_(src) {
  Initialize(src_.get(), std::move(options.assumed_filename()),
             options.assumed_pos(), options.independent_pos());
}

template <typename Src>
inline FdReader<Src>::FdReader(Src&& src, Options options)
    : FdReaderBase(options.buffer_options(), options.growing_source()),
      src_(std::move(src)) {
  Initialize(src_.get(), std::move(options.assumed_filename()),
             options.assumed_pos(), options.independent_pos());
}

template <typename Src>
inline FdReader<Src>::FdReader(int src, Options options)
    : FdReader(std::forward_as_tuple(src), std::move(options)) {}

template <typename Src>
template <typename... SrcArgs>
inline FdReader<Src>::FdReader(std::tuple<SrcArgs...> src_args, Options options)
    : FdReaderBase(options.buffer_options(), options.growing_source()),
      src_(std::move(src_args)) {
  Initialize(src_.get(), std::move(options.assumed_filename()),
             options.assumed_pos(), options.independent_pos());
}

template <typename Src>
template <typename DependentSrc,
          std::enable_if_t<std::is_same<DependentSrc, OwnedFd>::value, int>>
inline FdReader<Src>::FdReader(absl::string_view filename, Options options)
    : FdReaderBase(options.buffer_options(), options.growing_source()) {
  Initialize(filename, std::move(options));
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
inline void FdReader<Src>::Reset(const Src& src, Options options) {
  FdReaderBase::Reset(options.buffer_options(), options.growing_source());
  src_.Reset(src);
  Initialize(src_.get(), std::move(options.assumed_filename()),
             options.assumed_pos(), options.independent_pos());
}

template <typename Src>
inline void FdReader<Src>::Reset(Src&& src, Options options) {
  FdReaderBase::Reset(options.buffer_options(), options.growing_source());
  src_.Reset(std::move(src));
  Initialize(src_.get(), std::move(options.assumed_filename()),
             options.assumed_pos(), options.independent_pos());
}

template <typename Src>
inline void FdReader<Src>::Reset(int src, Options options) {
  Reset(std::forward_as_tuple(src), std::move(options));
}

template <typename Src>
template <typename... SrcArgs>
inline void FdReader<Src>::Reset(std::tuple<SrcArgs...> src_args,
                                 Options options) {
  FdReaderBase::Reset(options.buffer_options(), options.growing_source());
  src_.Reset(std::move(src_args));
  Initialize(src_.get(), std::move(options.assumed_filename()),
             options.assumed_pos(), options.independent_pos());
}

template <typename Src>
template <typename DependentSrc,
          std::enable_if_t<std::is_same<DependentSrc, OwnedFd>::value, int>>
inline void FdReader<Src>::Reset(absl::string_view filename, Options options) {
  FdReaderBase::Reset(options.buffer_options(), options.growing_source());
  Initialize(filename, std::move(options));
}

template <typename Src>
void FdReader<Src>::Initialize(absl::string_view filename, Options&& options) {
  const int src = OpenFd(filename, options.mode());
  if (ABSL_PREDICT_FALSE(src < 0)) return;
  src_.Reset(std::forward_as_tuple(src));
  InitializePos(src_.get(), options.assumed_pos(), options.independent_pos());
}

template <typename Src>
void FdReader<Src>::Done() {
  FdReaderBase::Done();
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

#endif  // RIEGELI_BYTES_FD_READER_H_
