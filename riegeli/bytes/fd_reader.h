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
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/fd_dependency.h"

namespace riegeli {

namespace internal {

// Implementation shared between `FdReader` and `FdStreamReader`.
class FdReaderCommon : public BufferedReader {
 public:
  // Returns the fd being read from. If the fd is owned then changed to -1 by
  // `Close()`, otherwise unchanged.
  virtual int src_fd() const = 0;

  // Returns the original name of the file being read from (or "/dev/stdin" or
  // "/proc/self/fd/<fd>" if fd was given). Unchanged by `Close()`.
  const std::string& filename() const { return filename_; }

  using BufferedReader::Fail;
  bool Fail(absl::Status status) override;

 protected:
  FdReaderCommon() noexcept {}

  explicit FdReaderCommon(size_t buffer_size);

  FdReaderCommon(FdReaderCommon&& that) noexcept;
  FdReaderCommon& operator=(FdReaderCommon&& that) noexcept;

  void Reset();
  void Reset(size_t buffer_size);
  void SetFilename(int src);
  int OpenFd(absl::string_view filename, int flags);
  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation);

  std::string filename_;
};

}  // namespace internal

// Template parameter independent part of `FdReader`.
class FdReaderBase : public internal::FdReaderCommon {
 public:
  class Options {
   public:
    Options() noexcept {}

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

    ABSL_DEPRECATED("Use set_independent_pos() instead")
    Options& set_initial_pos(absl::optional<Position> initial_pos) & {
      return set_independent_pos(initial_pos);
    }
    ABSL_DEPRECATED("Use set_independent_pos() instead")
    Options&& set_initial_pos(absl::optional<Position> initial_pos) && {
      return std::move(set_initial_pos(initial_pos));
    }

    // Tunes how much data is buffered after reading from the file.
    //
    // Default: `kDefaultBufferSize` (64K).
    Options& set_buffer_size(size_t buffer_size) & {
      RIEGELI_ASSERT_GT(buffer_size, 0u)
          << "Failed precondition of FdReaderBase::Options::set_buffer_size(): "
             "zero buffer size";
      buffer_size_ = buffer_size;
      return *this;
    }
    Options&& set_buffer_size(size_t buffer_size) && {
      return std::move(set_buffer_size(buffer_size));
    }
    size_t buffer_size() const { return buffer_size_; }

   private:
    absl::optional<Position> assumed_pos_;
    absl::optional<Position> independent_pos_;
    size_t buffer_size_ = kDefaultBufferSize;
  };

  bool Sync() override;
  bool SupportsRandomAccess() override { return supports_random_access_; }
  bool SupportsSize() override { return supports_random_access_; }
  absl::optional<Position> Size() override;

 protected:
  FdReaderBase() noexcept {}

  explicit FdReaderBase(size_t buffer_size);

  FdReaderBase(FdReaderBase&& that) noexcept;
  FdReaderBase& operator=(FdReaderBase&& that) noexcept;

  void Reset();
  void Reset(size_t buffer_size);
  void Initialize(int src, absl::optional<Position> assumed_pos,
                  absl::optional<Position> independent_pos);
  void InitializePos(int src, absl::optional<Position> assumed_pos,
                     absl::optional<Position> independent_pos);
  bool SyncPos(int src);

  void Done() override;
  bool ReadInternal(size_t min_length, size_t max_length, char* dest) override;
  bool SeekSlow(Position new_pos) override;

  bool supports_random_access_ = false;
  bool has_independent_pos_ = false;

  // Invariant: `limit_pos() <= std::numeric_limits<off_t>::max()`
};

// Template parameter independent part of `FdStreamReader`.
class FdStreamReaderBase : public internal::FdReaderCommon {
 public:
  class Options {
   public:
    Options() noexcept {}

    // If not `absl::nullopt`, this position will be assumed initially, to be
    // reported by `pos()`.
    //
    // If `absl::nullopt`, in the constructor from filename, the position will
    // be assumed to be 0.
    //
    // If `absl::nullopt`, in the constructor from fd, `FdStreamReader` will
    // initially get the current fd position.
    //
    // In any case reading will start from the current position.
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

    // Tunes how much data is buffered after reading from the file.
    //
    // Default: `kDefaultBufferSize` (64K).
    Options& set_buffer_size(size_t buffer_size) & {
      RIEGELI_ASSERT_GT(buffer_size, 0u)
          << "Failed precondition of "
             "FdStreamReaderBase::Options::set_buffer_size()";
      buffer_size_ = buffer_size;
      return *this;
    }
    Options&& set_buffer_size(size_t buffer_size) && {
      return std::move(set_buffer_size(buffer_size));
    }
    size_t buffer_size() const { return buffer_size_; }

   private:
    absl::optional<Position> assumed_pos_;
    size_t buffer_size_ = kDefaultBufferSize;
  };

 protected:
  FdStreamReaderBase() noexcept {}

  explicit FdStreamReaderBase(size_t buffer_size)
      : FdReaderCommon(buffer_size) {}

  FdStreamReaderBase(FdStreamReaderBase&& that) noexcept;
  FdStreamReaderBase& operator=(FdStreamReaderBase&& that) noexcept;

  void Initialize(int src, absl::optional<Position> assumed_pos);
  void InitializePos(int src, absl::optional<Position> assumed_pos);

  bool ReadInternal(size_t min_length, size_t max_length, char* dest) override;
};

// Template parameter independent part of `FdMMapReader`.
class FdMMapReaderBase : public ChainReader<Chain> {
 public:
  class Options {
   public:
    Options() noexcept {}

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

   private:
    absl::optional<Position> independent_pos_;
  };

  // Returns the fd being read from. If the fd is owned then changed to -1 by
  // `Close()`, otherwise unchanged.
  virtual int src_fd() const = 0;

  // Returns the original name of the file being read from (or "/dev/stdin" or
  // "/proc/self/fd/<fd>" if fd was given). Unchanged by `Close()`.
  const std::string& filename() const { return filename_; }

  using ChainReader::Fail;
  bool Fail(absl::Status status) override;
  bool Sync() override;

 protected:
  FdMMapReaderBase() noexcept {}

  explicit FdMMapReaderBase(bool has_independent_pos);

  FdMMapReaderBase(FdMMapReaderBase&& that) noexcept;
  FdMMapReaderBase& operator=(FdMMapReaderBase&& that) noexcept;

  void Reset();
  void Reset(bool has_independent_pos);
  void Initialize(int src, absl::optional<Position> independent_pos);
  void SetFilename(int src);
  int OpenFd(absl::string_view filename, int flags);
  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation);
  void InitializePos(int src, absl::optional<Position> independent_pos);
  bool SyncPos(int src);

  void Done() override;

  std::string filename_;
  bool has_independent_pos_ = false;
};

// A `Reader` which reads from a file descriptor.
//
// The fd must support:
//  * `close()` - if the fd is owned
//  * `read()`  - if `Options::independent_pos() == absl::nullopt`
//  * `pread()` - if `Options::independent_pos() != absl::nullopt`
//  * `lseek()` - for `Seek()` or `Size()`
//                if `Options::independent_pos() == absl::nullopt`
//  * `fstat()` - for `Seek()` or `Size()`
//
// `FdReader` supports random access if
// `Options::assumed_pos() == absl::nullopt` and the fd supports random access
// (this is assumed if `Options::independent_pos() != absl::nullopt`, otherwise
// this is checked by calling `lseek()`).
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
  FdReader() noexcept {}

  // Will read from the fd provided by `src`.
  explicit FdReader(const Src& src, Options options = Options());
  explicit FdReader(Src&& src, Options options = Options());

  // Will read from the fd provided by a `Src` constructed from elements of
  // `src_args`. This avoids constructing a temporary `Src` and moving from it.
  template <typename... SrcArgs>
  explicit FdReader(std::tuple<SrcArgs...> src_args,
                    Options options = Options());

  // Opens a file for reading.
  //
  // `flags` is the second argument of `open()`, typically `O_RDONLY`.
  //
  // `flags` must include either `O_RDONLY` or `O_RDWR`.
  explicit FdReader(absl::string_view filename, int flags,
                    Options options = Options());

  FdReader(FdReader&& that) noexcept;
  FdReader& operator=(FdReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `FdReader`. This avoids
  // constructing a temporary `FdReader` and moving from it.
  void Reset();
  void Reset(const Src& src, Options options = Options());
  void Reset(Src&& src, Options options = Options());
  template <typename... SrcArgs>
  void Reset(std::tuple<SrcArgs...> src_args, Options options = Options());
  void Reset(absl::string_view filename, int flags,
             Options options = Options());

  // Returns the object providing and possibly owning the fd being read from. If
  // the fd is owned then changed to -1 by `Close()`, otherwise unchanged.
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  int src_fd() const override { return src_.get(); }

 protected:
  void Done() override;

 private:
  using FdReaderBase::Initialize;
  void Initialize(absl::string_view filename, int flags,
                  absl::optional<Position> assumed_pos,
                  absl::optional<Position> independent_pos);

  // The object providing and possibly owning the fd being read from.
  Dependency<int, Src> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
FdReader()->FdReader<DeleteCtad<>>;
template <typename Src>
explicit FdReader(const Src& src,
                  FdReaderBase::Options options = FdReaderBase::Options())
    -> FdReader<std::conditional_t<std::is_convertible<const Src&, int>::value,
                                   OwnedFd, std::decay_t<Src>>>;
template <typename Src>
explicit FdReader(Src&& src,
                  FdReaderBase::Options options = FdReaderBase::Options())
    -> FdReader<std::conditional_t<std::is_convertible<Src&&, int>::value,
                                   OwnedFd, std::decay_t<Src>>>;
template <typename... SrcArgs>
explicit FdReader(std::tuple<SrcArgs...> src_args,
                  FdReaderBase::Options options = FdReaderBase::Options())
    -> FdReader<DeleteCtad<std::tuple<SrcArgs...>>>;
explicit FdReader(absl::string_view filename, int flags,
                  FdReaderBase::Options options = FdReaderBase::Options())
    ->FdReader<>;
#endif

// A `Reader` which reads from a file descriptor which does not have to support
// random access.
//
// The fd must support:
//  * `close()` - if the fd is owned
//  * `read()`
//  * `lseek()` - for the constructor from fd,
//                if `Options::assumed_pos() == absl::nullopt`
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
// Warning: if the fd is not owned, it will have an unpredictable amount of
// extra data consumed because of buffering.
//
// The fd must not be closed nor have its position changed until the
// `FdStreamReader` is closed or no longer used.
template <typename Src = OwnedFd>
ABSL_DEPRECATED("Use FdReader instead")
class FdStreamReader : public FdStreamReaderBase {
 public:
  // Creates a closed `FdStreamReader`.
  FdStreamReader() noexcept {}

  // Will read from the fd provided by `src`.
  explicit FdStreamReader(const Src& src, Options options = Options());
  explicit FdStreamReader(Src&& src, Options options = Options());

  // Will read from the fd provided by a `Src` constructed from elements of
  // `src_args`. This avoids constructing a temporary `Src `and moving from it.
  template <typename... SrcArgs>
  explicit FdStreamReader(std::tuple<SrcArgs...> src_args,
                          Options options = Options());

  // Opens a file for reading.
  //
  // `flags` is the second argument of `open()`, typically `O_RDONLY`.
  //
  // `flags` must include either `O_RDONLY` or `O_RDWR`.
  explicit FdStreamReader(absl::string_view filename, int flags,
                          Options options = Options());

  FdStreamReader(FdStreamReader&& that) noexcept;
  FdStreamReader& operator=(FdStreamReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `FdStreamReader`. This
  // avoids constructing a temporary `FdStreamReader` and moving from it.
  void Reset();
  void Reset(const Src& src, Options options = Options());
  void Reset(Src&& src, Options options = Options());
  template <typename... SrcArgs>
  void Reset(std::tuple<SrcArgs...> src_args, Options options = Options());
  void Reset(absl::string_view filename, int flags,
             Options options = Options());

  // Returns the object providing and possibly owning the fd being read from. If
  // the fd is owned then changed to -1 by `Close()`, otherwise unchanged.
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  int src_fd() const override { return src_.get(); }

 protected:
  void Done() override;

 private:
  using FdStreamReaderBase::Initialize;
  void Initialize(absl::string_view filename, int flags,
                  absl::optional<Position> assumed_pos);

  // The object providing and possibly owning the fd being read from.
  Dependency<int, Src> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
FdStreamReader()->FdStreamReader<DeleteCtad<>>;
template <typename Src>
explicit FdStreamReader(const Src& src, FdStreamReaderBase::Options options =
                                            FdStreamReaderBase::Options())
    -> FdStreamReader<
        std::conditional_t<std::is_convertible<const Src&, int>::value, OwnedFd,
                           std::decay_t<Src>>>;
template <typename Src>
explicit FdStreamReader(Src&& src, FdStreamReaderBase::Options options =
                                       FdStreamReaderBase::Options())
    -> FdStreamReader<std::conditional_t<std::is_convertible<Src&&, int>::value,
                                         OwnedFd, std::decay_t<Src>>>;
template <typename... SrcArgs>
explicit FdStreamReader(
    std::tuple<SrcArgs...> src_args,
    FdStreamReaderBase::Options options = FdStreamReaderBase::Options())
    -> FdStreamReader<DeleteCtad<std::tuple<SrcArgs...>>>;
explicit FdStreamReader(
    absl::string_view filename, int flags,
    FdStreamReaderBase::Options options = FdStreamReaderBase::Options())
    ->FdStreamReader<>;
#endif

// A `Reader` which reads from a file descriptor by mapping the whole file to
// memory. It supports random access.
//
// The fd must support:
//  * `close()` - if the fd is owned
//  * `fstat()`
//  * `mmap()`
//  * `lseek()` - if `Options::independent_pos() == absl::nullopt`
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
// The fd must not be closed until the `FdMMapReader` is closed or no longer
// used. `File` contents must not be changed while data read from the file is
// accessed without a memory copy.
template <typename Src = OwnedFd>
class FdMMapReader : public FdMMapReaderBase {
 public:
  // Creates a closed `FdMMapReader`.
  FdMMapReader() noexcept {}

  // Will read from the fd provided by `src`.
  explicit FdMMapReader(const Src& src, Options options = Options());
  explicit FdMMapReader(Src&& src, Options options = Options());

  // Will read from the fd provided by a `Src` constructed from elements of
  // `src_args`. This avoids constructing a temporary `Src` and moving from it.
  template <typename... SrcArgs>
  explicit FdMMapReader(std::tuple<SrcArgs...> src_args,
                        Options options = Options());

  // Opens a file for reading.
  //
  // `flags` is the second argument of `open()`, typically `O_RDONLY`.
  //
  // `flags` must include either `O_RDONLY` or `O_RDWR`.
  explicit FdMMapReader(absl::string_view filename, int flags,
                        Options options = Options());

  FdMMapReader(FdMMapReader&& that) noexcept;
  FdMMapReader& operator=(FdMMapReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `FdMMapReader`. This avoids
  // constructing a temporary `FdMMapReader` and moving from it.
  void Reset();
  void Reset(const Src& src, Options options = Options());
  void Reset(Src&& src, Options options = Options());
  template <typename... SrcArgs>
  void Reset(std::tuple<SrcArgs...> src_args, Options options = Options());
  void Reset(absl::string_view filename, int flags,
             Options options = Options());

  // Returns the object providing and possibly owning the fd being read from. If
  // the fd is owned then changed to -1 by `Close()`, otherwise unchanged.
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  int src_fd() const override { return src_.get(); }

 protected:
  void Done() override;

 private:
  using FdMMapReaderBase::Initialize;
  void Initialize(absl::string_view filename, int flags,
                  absl::optional<Position> independent_pos);

  // The object providing and possibly owning the fd being read from.
  Dependency<int, Src> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
FdMMapReader()->FdMMapReader<DeleteCtad<>>;
template <typename Src>
explicit FdMMapReader(const Src& src, FdMMapReaderBase::Options options =
                                          FdMMapReaderBase::Options())
    -> FdMMapReader<
        std::conditional_t<std::is_convertible<const Src&, int>::value, OwnedFd,
                           std::decay_t<Src>>>;
template <typename Src>
explicit FdMMapReader(
    Src&& src, FdMMapReaderBase::Options options = FdMMapReaderBase::Options())
    -> FdMMapReader<std::conditional_t<std::is_convertible<Src&&, int>::value,
                                       OwnedFd, std::decay_t<Src>>>;
template <typename... SrcArgs>
explicit FdMMapReader(
    std::tuple<SrcArgs...> src_args,
    FdMMapReaderBase::Options options = FdMMapReaderBase::Options())
    -> FdMMapReader<DeleteCtad<std::tuple<SrcArgs...>>>;
explicit FdMMapReader(
    absl::string_view filename, int flags,
    FdMMapReaderBase::Options options = FdMMapReaderBase::Options())
    ->FdMMapReader<>;
#endif

// Implementation details follow.

namespace internal {

inline FdReaderCommon::FdReaderCommon(size_t buffer_size)
    : BufferedReader(buffer_size) {}

inline FdReaderCommon::FdReaderCommon(FdReaderCommon&& that) noexcept
    : BufferedReader(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      filename_(std::move(that.filename_)) {}

inline FdReaderCommon& FdReaderCommon::operator=(
    FdReaderCommon&& that) noexcept {
  BufferedReader::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  filename_ = std::move(that.filename_);
  return *this;
}

inline void FdReaderCommon::Reset() {
  BufferedReader::Reset();
  filename_.clear();
}

inline void FdReaderCommon::Reset(size_t buffer_size) {
  BufferedReader::Reset(buffer_size);
  // `filename_` will be set by `Initialize()`.
}

}  // namespace internal

inline FdReaderBase::FdReaderBase(size_t buffer_size)
    : FdReaderCommon(buffer_size) {}

inline FdReaderBase::FdReaderBase(FdReaderBase&& that) noexcept
    : FdReaderCommon(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      supports_random_access_(that.supports_random_access_),
      has_independent_pos_(that.has_independent_pos_) {}

inline FdReaderBase& FdReaderBase::operator=(FdReaderBase&& that) noexcept {
  FdReaderCommon::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  supports_random_access_ = that.supports_random_access_;
  has_independent_pos_ = that.has_independent_pos_;
  return *this;
}

inline void FdReaderBase::Reset() {
  FdReaderCommon::Reset();
  supports_random_access_ = false;
  has_independent_pos_ = false;
}

inline void FdReaderBase::Reset(size_t buffer_size) {
  FdReaderCommon::Reset(buffer_size);
  supports_random_access_ = false;
  has_independent_pos_ = false;
}

inline void FdReaderBase::Initialize(int src,
                                     absl::optional<Position> assumed_pos,
                                     absl::optional<Position> independent_pos) {
  RIEGELI_ASSERT_GE(src, 0)
      << "Failed precondition of FdReader: negative file descriptor";
  SetFilename(src);
  InitializePos(src, assumed_pos, independent_pos);
}

inline FdStreamReaderBase::FdStreamReaderBase(
    FdStreamReaderBase&& that) noexcept
    : FdReaderCommon(std::move(that)) {}

inline FdStreamReaderBase& FdStreamReaderBase::operator=(
    FdStreamReaderBase&& that) noexcept {
  FdReaderCommon::operator=(std::move(that));
  return *this;
}

inline void FdStreamReaderBase::Initialize(
    int src, absl::optional<Position> assumed_pos) {
  RIEGELI_ASSERT_GE(src, 0)
      << "Failed precondition of FdStreamReader: negative file descriptor";
  SetFilename(src);
  InitializePos(src, assumed_pos);
}

inline FdMMapReaderBase::FdMMapReaderBase(bool has_independent_pos)
    // Empty `Chain` as the `ChainReader` source is a placeholder, it will be
    // set by `Initialize()`.
    : ChainReader(std::forward_as_tuple()),
      has_independent_pos_(has_independent_pos) {}

inline FdMMapReaderBase::FdMMapReaderBase(FdMMapReaderBase&& that) noexcept
    : ChainReader(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      filename_(std::move(that.filename_)),
      has_independent_pos_(that.has_independent_pos_) {}

inline FdMMapReaderBase& FdMMapReaderBase::operator=(
    FdMMapReaderBase&& that) noexcept {
  ChainReader::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  filename_ = std::move(that.filename_);
  has_independent_pos_ = that.has_independent_pos_;
  return *this;
}

inline void FdMMapReaderBase::Reset() {
  ChainReader::Reset();
  filename_.clear();
  has_independent_pos_ = false;
}

inline void FdMMapReaderBase::Reset(bool has_independent_pos) {
  // Empty `Chain` as the `ChainReader` source is a placeholder, it will be set
  // by `Initialize()`.
  ChainReader::Reset(std::forward_as_tuple());
  // `filename_` will be set by `Initialize()`.
  has_independent_pos_ = has_independent_pos;
}

inline void FdMMapReaderBase::Initialize(
    int src, absl::optional<Position> independent_pos) {
  RIEGELI_ASSERT_GE(src, 0)
      << "Failed precondition of FdMMapReader: negative file descriptor";
  SetFilename(src);
  InitializePos(src, independent_pos);
}

template <typename Src>
inline FdReader<Src>::FdReader(const Src& src, Options options)
    : FdReaderBase(options.buffer_size()), src_(src) {
  Initialize(src_.get(), options.assumed_pos(), options.independent_pos());
}

template <typename Src>
inline FdReader<Src>::FdReader(Src&& src, Options options)
    : FdReaderBase(options.buffer_size()), src_(std::move(src)) {
  Initialize(src_.get(), options.assumed_pos(), options.independent_pos());
}

template <typename Src>
template <typename... SrcArgs>
inline FdReader<Src>::FdReader(std::tuple<SrcArgs...> src_args, Options options)
    : FdReaderBase(options.buffer_size()), src_(std::move(src_args)) {
  Initialize(src_.get(), options.assumed_pos(), options.independent_pos());
}

template <typename Src>
inline FdReader<Src>::FdReader(absl::string_view filename, int flags,
                               Options options)
    : FdReaderBase(options.buffer_size()) {
  Initialize(filename, flags, options.assumed_pos(), options.independent_pos());
}

template <typename Src>
inline FdReader<Src>::FdReader(FdReader&& that) noexcept
    : FdReaderBase(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      src_(std::move(that.src_)) {}

template <typename Src>
inline FdReader<Src>& FdReader<Src>::operator=(FdReader&& that) noexcept {
  FdReaderBase::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  src_ = std::move(that.src_);
  return *this;
}

template <typename Src>
inline void FdReader<Src>::Reset() {
  FdReaderBase::Reset();
  src_.Reset();
}

template <typename Src>
inline void FdReader<Src>::Reset(const Src& src, Options options) {
  FdReaderBase::Reset(options.buffer_size());
  src_.Reset(src);
  Initialize(src_.get(), options.assumed_pos(), options.independent_pos());
}

template <typename Src>
inline void FdReader<Src>::Reset(Src&& src, Options options) {
  FdReaderBase::Reset(options.buffer_size());
  src_.Reset(std::move(src));
  Initialize(src_.get(), options.assumed_pos(), options.independent_pos());
}

template <typename Src>
template <typename... SrcArgs>
inline void FdReader<Src>::Reset(std::tuple<SrcArgs...> src_args,
                                 Options options) {
  FdReaderBase::Reset(options.buffer_size());
  src_.Reset(std::move(src_args));
  Initialize(src_.get(), options.assumed_pos(), options.independent_pos());
}

template <typename Src>
inline void FdReader<Src>::Reset(absl::string_view filename, int flags,
                                 Options options) {
  FdReaderBase::Reset(options.buffer_size());
  src_.Reset();  // In case `OpenFd()` fails.
  Initialize(filename, flags, options.assumed_pos(), options.independent_pos());
}

template <typename Src>
inline void FdReader<Src>::Initialize(
    absl::string_view filename, int flags, absl::optional<Position> assumed_pos,
    absl::optional<Position> independent_pos) {
  RIEGELI_ASSERT((flags & O_ACCMODE) == O_RDONLY ||
                 (flags & O_ACCMODE) == O_RDWR)
      << "Failed precondition of FdReader: "
         "flags must include either O_RDONLY or O_RDWR";
  const int src = OpenFd(filename, flags);
  if (ABSL_PREDICT_FALSE(src < 0)) return;
  src_.Reset(std::forward_as_tuple(src));
  InitializePos(src_.get(), assumed_pos, independent_pos);
}

template <typename Src>
void FdReader<Src>::Done() {
  FdReaderBase::Done();
  if (src_.is_owning()) {
    const int src = src_.Release();
    if (ABSL_PREDICT_FALSE(internal::CloseFd(src) < 0) &&
        ABSL_PREDICT_TRUE(healthy())) {
      FailOperation(internal::CloseFunctionName());
    }
  }
}

template <typename Src>
inline FdStreamReader<Src>::FdStreamReader(const Src& src, Options options)
    : FdStreamReaderBase(options.buffer_size()), src_(src) {
  Initialize(src_.get(), options.assumed_pos());
}

template <typename Src>
inline FdStreamReader<Src>::FdStreamReader(Src&& src, Options options)
    : FdStreamReaderBase(options.buffer_size()), src_(std::move(src)) {
  Initialize(src_.get(), options.assumed_pos());
}

template <typename Src>
template <typename... SrcArgs>
inline FdStreamReader<Src>::FdStreamReader(std::tuple<SrcArgs...> src_args,
                                           Options options)
    : FdStreamReaderBase(options.buffer_size()), src_(std::move(src_args)) {
  Initialize(src_.get(), options.assumed_pos());
}

template <typename Src>
inline FdStreamReader<Src>::FdStreamReader(absl::string_view filename,
                                           int flags, Options options)
    : FdStreamReaderBase(options.buffer_size()) {
  Initialize(filename, flags, options.assumed_pos());
}

template <typename Src>
inline FdStreamReader<Src>::FdStreamReader(FdStreamReader&& that) noexcept
    : FdStreamReaderBase(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      src_(std::move(that.src_)) {}

template <typename Src>
inline FdStreamReader<Src>& FdStreamReader<Src>::operator=(
    FdStreamReader&& that) noexcept {
  FdStreamReaderBase::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  src_ = std::move(that.src_);
  return *this;
}

template <typename Src>
inline void FdStreamReader<Src>::Reset() {
  FdStreamReaderBase::Reset();
  src_.Reset();
}

template <typename Src>
inline void FdStreamReader<Src>::Reset(const Src& src, Options options) {
  FdStreamReaderBase::Reset(options.buffer_size());
  src_.Reset(src);
  Initialize(src_.get(), options.assumed_pos());
}

template <typename Src>
inline void FdStreamReader<Src>::Reset(Src&& src, Options options) {
  FdStreamReaderBase::Reset(options.buffer_size());
  src_.Reset(std::move(src));
  Initialize(src_.get(), options.assumed_pos());
}

template <typename Src>
template <typename... SrcArgs>
inline void FdStreamReader<Src>::Reset(std::tuple<SrcArgs...> src_args,
                                       Options options) {
  FdStreamReaderBase::Reset(options.buffer_size());
  src_.Reset(std::move(src_args));
  Initialize(src_.get(), options.assumed_pos());
}

template <typename Src>
inline void FdStreamReader<Src>::Reset(absl::string_view filename, int flags,
                                       Options options) {
  FdStreamReaderBase::Reset(options.buffer_size());
  src_.Reset();  // In case `OpenFd()` fails.
  Initialize(filename, flags, options.assumed_pos());
}

template <typename Src>
inline void FdStreamReader<Src>::Initialize(
    absl::string_view filename, int flags,
    absl::optional<Position> assumed_pos) {
  RIEGELI_ASSERT((flags & O_ACCMODE) == O_RDONLY ||
                 (flags & O_ACCMODE) == O_RDWR)
      << "Failed precondition of FdStreamReader: "
         "flags must include either O_RDONLY or O_RDWR";
  const int src = OpenFd(filename, flags);
  if (ABSL_PREDICT_FALSE(src < 0)) return;
  src_.Reset(std::forward_as_tuple(src));
  if (assumed_pos != absl::nullopt) set_limit_pos(*assumed_pos);
}

template <typename Src>
void FdStreamReader<Src>::Done() {
  FdStreamReaderBase::Done();
  if (src_.is_owning()) {
    const int src = src_.Release();
    if (ABSL_PREDICT_FALSE(internal::CloseFd(src) < 0) &&
        ABSL_PREDICT_TRUE(healthy())) {
      FailOperation(internal::CloseFunctionName());
    }
  }
}

template <typename Src>
inline FdMMapReader<Src>::FdMMapReader(const Src& src, Options options)
    : FdMMapReaderBase(options.independent_pos() != absl::nullopt), src_(src) {
  Initialize(src_.get(), options.independent_pos());
}

template <typename Src>
inline FdMMapReader<Src>::FdMMapReader(Src&& src, Options options)
    : FdMMapReaderBase(options.independent_pos() != absl::nullopt),
      src_(std::move(src)) {
  Initialize(src_.get(), options.independent_pos());
}

template <typename Src>
template <typename... SrcArgs>
inline FdMMapReader<Src>::FdMMapReader(std::tuple<SrcArgs...> src_args,
                                       Options options)
    : FdMMapReaderBase(options.independent_pos() != absl::nullopt),
      src_(std::move(src_args)) {
  Initialize(src_.get(), options.independent_pos());
}

template <typename Src>
inline FdMMapReader<Src>::FdMMapReader(absl::string_view filename, int flags,
                                       Options options)
    : FdMMapReaderBase(options.independent_pos() != absl::nullopt) {
  Initialize(filename, flags, options.independent_pos());
}

template <typename Src>
inline FdMMapReader<Src>::FdMMapReader(FdMMapReader&& that) noexcept
    : FdMMapReaderBase(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      src_(std::move(that.src_)) {}

template <typename Src>
inline FdMMapReader<Src>& FdMMapReader<Src>::operator=(
    FdMMapReader&& that) noexcept {
  FdMMapReaderBase::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  src_ = std::move(that.src_);
  return *this;
}

template <typename Src>
inline void FdMMapReader<Src>::Reset() {
  FdMMapReaderBase::Reset();
  src_.Reset();
}

template <typename Src>
inline void FdMMapReader<Src>::Reset(const Src& src, Options options) {
  FdMMapReaderBase::Reset(options.independent_pos() != absl::nullopt);
  src_.Reset(src);
  Initialize(src_.get(), options.independent_pos());
}

template <typename Src>
inline void FdMMapReader<Src>::Reset(Src&& src, Options options) {
  FdMMapReaderBase::Reset(options.independent_pos() != absl::nullopt);
  src_.Reset(std::move(src));
  Initialize(src_.get(), options.independent_pos());
}

template <typename Src>
template <typename... SrcArgs>
inline void FdMMapReader<Src>::Reset(std::tuple<SrcArgs...> src_args,
                                     Options options) {
  FdMMapReaderBase::Reset(options.independent_pos() != absl::nullopt);
  src_.Reset(std::move(src_args));
  Initialize(src_.get(), options.independent_pos());
}

template <typename Src>
inline void FdMMapReader<Src>::Reset(absl::string_view filename, int flags,
                                     Options options) {
  FdMMapReaderBase::Reset(options.independent_pos() != absl::nullopt);
  src_.Reset();  // In case `OpenFd()` fails.
  Initialize(filename, flags, options.independent_pos());
}

template <typename Src>
inline void FdMMapReader<Src>::Initialize(
    absl::string_view filename, int flags,
    absl::optional<Position> independent_pos) {
  RIEGELI_ASSERT((flags & O_ACCMODE) == O_RDONLY ||
                 (flags & O_ACCMODE) == O_RDWR)
      << "Failed precondition of FdMMapReader: "
         "flags must include either O_RDONLY or O_RDWR";
  const int src = OpenFd(filename, flags);
  if (ABSL_PREDICT_FALSE(src < 0)) return;
  src_.Reset(std::forward_as_tuple(src));
  InitializePos(src_.get(), independent_pos);
}

template <typename Src>
void FdMMapReader<Src>::Done() {
  FdMMapReaderBase::Done();
  if (src_.is_owning()) {
    const int src = src_.Release();
    if (ABSL_PREDICT_FALSE(internal::CloseFd(src) < 0) &&
        ABSL_PREDICT_TRUE(healthy())) {
      FailOperation(internal::CloseFunctionName());
    }
  }
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_FD_READER_H_
