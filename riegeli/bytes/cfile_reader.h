// Copyright 2022 Google LLC
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

#ifndef RIEGELI_BYTES_CFILE_READER_H_
#define RIEGELI_BYTES_CFILE_READER_H_

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

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
#include "riegeli/bytes/cfile_dependency.h"

namespace riegeli {

// Template parameter independent part of `CFileReader`.
class CFileReaderBase : public BufferedReader {
 public:
  class Options : public BufferOptionsBase<Options> {
   public:
    Options() noexcept {}

    // If `CFileReader` reads from an already open `FILE`, `assumed_filename()`
    // allows to override the filename which is included in failure messages and
    // returned by `filename()`.
    //
    // If `CFileReader` opens a `FILE` with a filename, `assumed_filename()`
    // has no effect.
    //
    // Default: "".
    Options& set_assumed_filename(absl::string_view assumed_filename) & {
      // TODO: When `absl::string_view` becomes C++17
      // `std::string_view`: `assumed_filename_ = assumed_filename`
      assumed_filename_.assign(assumed_filename.data(),
                               assumed_filename.size());
      return *this;
    }
    Options&& set_assumed_filename(absl::string_view assumed_filename) && {
      return std::move(set_assumed_filename(assumed_filename));
    }
    std::string& assumed_filename() { return assumed_filename_; }
    const std::string& assumed_filename() const { return assumed_filename_; }

    // If `CFileReader` opens a `FILE` with a filename, `mode()` is the second
    // argument of `fopen()` and specifies the open mode, typically "r".
    //
    // If `CFileReader` reads from an already open `FILE`, `mode()` has no
    // effect.
    //
    // Default: "r".
    Options& set_mode(absl::string_view mode) & {
      // TODO: When `absl::string_view` becomes C++17
      // `std::string_view`: `mode_ = mode`
      mode_.assign(mode.data(), mode.size());
      return *this;
    }
    Options&& set_mode(absl::string_view mode) && {
      return std::move(set_mode(mode));
    }
    const std::string& mode() const { return mode_; }

    // If `absl::nullopt`, the current position reported by `pos()` corresponds
    // to the current `FILE` position if possible, otherwise 0 is assumed as the
    // initial position. Random access is supported if the `FILE` supports
    // random access.
    //
    // If not `absl::nullopt`, this position is assumed initially, to be
    // reported by `pos()`. It does not need to correspond to the current `FILE`
    // position. Random access is not supported.
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
    std::string assumed_filename_;
    std::string mode_ = "r";
    absl::optional<Position> assumed_pos_;
    bool growing_source_ = false;
  };

  // Returns the `FILE` being read from. If the `FILE` is owned then changed to
  // `nullptr` by `Close()`, otherwise unchanged.
  virtual FILE* src_file() const = 0;

  // Returns the original name of the file being read from. Unchanged by
  // `Close()`.
  absl::string_view filename() const { return filename_; }

  bool ToleratesReadingAhead() override {
    return read_all_hint() || supports_random_access();
  }
  bool SupportsRandomAccess() override { return supports_random_access(); }

 protected:
  explicit CFileReaderBase(Closed) noexcept : BufferedReader(kClosed) {}

  explicit CFileReaderBase(const BufferOptions& buffer_options,
                           bool growing_source);

  CFileReaderBase(CFileReaderBase&& that) noexcept;
  CFileReaderBase& operator=(CFileReaderBase&& that) noexcept;

  void Reset(Closed);
  void Reset(const BufferOptions& buffer_options, bool growing_source);
  void Initialize(FILE* src, std::string&& assumed_filename,
                  absl::optional<Position> assumed_pos);
  FILE* OpenFile(absl::string_view filename, const char* mode);
  void InitializePos(FILE* src, absl::optional<Position> assumed_pos);
  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation);
  bool supports_random_access();

  absl::Status AnnotateStatusImpl(absl::Status status) override;
  bool ReadInternal(size_t min_length, size_t max_length, char* dest) override;
  bool SeekBehindBuffer(Position new_pos) override;
  absl::optional<Position> SizeImpl() override;

 private:
  // Encodes a `bool` or a marker that the value is not fully resolved yet.
  enum class LazyBoolState : uint8_t { kFalse, kTrue, kUnknown };

  std::string filename_;
  bool growing_source_ = false;
  LazyBoolState supports_random_access_ = LazyBoolState::kFalse;

  // Invariant: `limit_pos() <= std::numeric_limits<off_t>::max()`
};

// A `Reader` which reads from a `FILE`.
//
// `CFileReader` supports random access if
// `Options::assumed_pos() == absl::nullopt` and the `FILE` supports random
// access (this is checked by calling `ftell()` and `fseek(SEEK_END)`).
//
// On Linux, some virtual file systems ("/proc", "/sys") contain files with
// contents generated on the fly when the files are read. The files appear as
// regular files, with an apparent size of 0 or 4096, and random access is only
// partially supported. `CFileReader` detects lack of random access for them
// only if the filename seen `CFileReader` starts with "/proc/" (for zero-sized
// files) or "/sys/". An explicit
// `CFileReaderBase::Options().set_assumed_pos(0)` can be used to disable random
// access for such files.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the `FILE` being read from. `Src` must support
// `Dependency<FILE*, Src>`, e.g. `OwnedCFile` (owned, default),
// `UnownedCFile` (not owned).
//
// By relying on CTAD the template argument can be deduced as `OwnedCFile` if
// the first constructor argument is a filename or a `FILE*`, otherwise as the
// value type of the first constructor argument. This requires C++17.
//
// Warning: if random access is not supported and the `FILE` is not owned, it
// will have an unpredictable amount of extra data consumed because of
// buffering.
//
// Until the `CFileReader` is closed or no longer used, the `FILE` must not be
// closed nor have its position changed.
template <typename Src = OwnedCFile>
class CFileReader : public CFileReaderBase {
 public:
  // Creates a closed `CFileReader`.
  explicit CFileReader(Closed) noexcept : CFileReaderBase(kClosed) {}

  // Will read from the `FILE` provided by `src`.
  explicit CFileReader(const Src& src, Options options = Options());
  explicit CFileReader(Src&& src, Options options = Options());
  explicit CFileReader(FILE* src, Options options = Options());

  // Will read from the `FILE` provided by a `Src` constructed from elements of
  // `src_args`. This avoids constructing a temporary `Src` and moving from it.
  template <typename... SrcArgs>
  explicit CFileReader(std::tuple<SrcArgs...> src_args,
                       Options options = Options());

  // Opens a file for reading.
  //
  // If opening the file fails, `CFileReader` will be failed and closed.
  //
  // This constructor is present only if `Src` is `OwnedCFile`.
  template <
      typename DependentSrc = Src,
      std::enable_if_t<std::is_same<DependentSrc, OwnedCFile>::value, int> = 0>
  explicit CFileReader(absl::string_view filename, Options options = Options());

  CFileReader(CFileReader&& that) noexcept;
  CFileReader& operator=(CFileReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `CFileReader`. This avoids
  // constructing a temporary `CFileReader` and moving from it.
  void Reset(Closed);
  void Reset(const Src& src, Options options = Options());
  void Reset(Src&& src, Options options = Options());
  void Reset(FILE* src, Options options = Options());
  template <typename... SrcArgs>
  void Reset(std::tuple<SrcArgs...> src_args, Options options = Options());
  template <
      typename DependentSrc = Src,
      std::enable_if_t<std::is_same<DependentSrc, OwnedCFile>::value, int> = 0>
  void Reset(absl::string_view filename, Options options = Options());

  // Returns the object providing and possibly owning the `FILE` being read
  // from. If the `FILE` is owned then changed to `nullptr` by `Close()`,
  // otherwise unchanged.
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  FILE* src_file() const override { return src_.get(); }

 protected:
  void Done() override;

 private:
  using CFileReaderBase::Initialize;
  void Initialize(absl::string_view filename, Options&& options);

  // The object providing and possibly owning the `FILE` being read from.
  Dependency<FILE*, Src> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit CFileReader(Closed)->CFileReader<DeleteCtad<Closed>>;
template <typename Src>
explicit CFileReader(const Src& src, CFileReaderBase::Options options =
                                         CFileReaderBase::Options())
    -> CFileReader<std::conditional_t<
        std::is_convertible<const Src&, FILE*>::value ||
            std::is_convertible<const Src&, absl::string_view>::value,
        OwnedCFile, std::decay_t<Src>>>;
template <typename Src>
explicit CFileReader(
    Src&& src, CFileReaderBase::Options options = CFileReaderBase::Options())
    -> CFileReader<std::conditional_t<
        std::is_convertible<Src&&, FILE*>::value ||
            std::is_convertible<Src&&, absl::string_view>::value,
        OwnedCFile, std::decay_t<Src>>>;
template <typename... SrcArgs>
explicit CFileReader(
    std::tuple<SrcArgs...> src_args,
    CFileReaderBase::Options options = CFileReaderBase::Options())
    -> CFileReader<DeleteCtad<std::tuple<SrcArgs...>>>;
#endif

// Implementation details follow.

inline CFileReaderBase::CFileReaderBase(const BufferOptions& buffer_options,
                                        bool growing_source)
    : BufferedReader(buffer_options), growing_source_(growing_source) {}

inline CFileReaderBase::CFileReaderBase(CFileReaderBase&& that) noexcept
    : BufferedReader(static_cast<BufferedReader&&>(that)),
      filename_(std::exchange(that.filename_, std::string())),
      growing_source_(that.growing_source_),
      supports_random_access_(that.supports_random_access_) {}

inline CFileReaderBase& CFileReaderBase::operator=(
    CFileReaderBase&& that) noexcept {
  BufferedReader::operator=(static_cast<BufferedReader&&>(that));
  filename_ = std::exchange(that.filename_, std::string());
  growing_source_ = that.growing_source_;
  supports_random_access_ = that.supports_random_access_;
  return *this;
}

inline void CFileReaderBase::Reset(Closed) {
  BufferedReader::Reset(kClosed);
  filename_ = std::string();
  growing_source_ = false;
  supports_random_access_ = LazyBoolState::kFalse;
}

inline void CFileReaderBase::Reset(const BufferOptions& buffer_options,
                                   bool growing_source) {
  BufferedReader::Reset(buffer_options);
  // `filename_` will be set by `Initialize()` or `OpenFile()`.
  growing_source_ = growing_source;
  supports_random_access_ = LazyBoolState::kFalse;
}

template <typename Src>
inline CFileReader<Src>::CFileReader(const Src& src, Options options)
    : CFileReaderBase(options.buffer_options(), options.growing_source()),
      src_(src) {
  Initialize(src_.get(), std::move(options.assumed_filename()),
             options.assumed_pos());
}

template <typename Src>
inline CFileReader<Src>::CFileReader(Src&& src, Options options)
    : CFileReaderBase(options.buffer_options(), options.growing_source()),
      src_(std::move(src)) {
  Initialize(src_.get(), std::move(options.assumed_filename()),
             options.assumed_pos());
}

template <typename Src>
inline CFileReader<Src>::CFileReader(FILE* src, Options options)
    : CFileReader(std::forward_as_tuple(src), std::move(options)) {}

template <typename Src>
template <typename... SrcArgs>
inline CFileReader<Src>::CFileReader(std::tuple<SrcArgs...> src_args,
                                     Options options)
    : CFileReaderBase(options.buffer_options(), options.growing_source()),
      src_(std::move(src_args)) {
  Initialize(src_.get(), std::move(options.assumed_filename()),
             options.assumed_pos());
}

template <typename Src>
template <typename DependentSrc,
          std::enable_if_t<std::is_same<DependentSrc, OwnedCFile>::value, int>>
inline CFileReader<Src>::CFileReader(absl::string_view filename,
                                     Options options)
    : CFileReaderBase(options.buffer_options(), options.growing_source()) {
  Initialize(filename, std::move(options));
}

template <typename Src>
inline CFileReader<Src>::CFileReader(CFileReader&& that) noexcept
    : CFileReaderBase(static_cast<CFileReaderBase&&>(that)),
      src_(std::move(that.src_)) {}

template <typename Src>
inline CFileReader<Src>& CFileReader<Src>::operator=(
    CFileReader&& that) noexcept {
  CFileReaderBase::operator=(static_cast<CFileReaderBase&&>(that));
  src_ = std::move(that.src_);
  return *this;
}

template <typename Src>
inline void CFileReader<Src>::Reset(Closed) {
  CFileReaderBase::Reset(kClosed);
  src_.Reset();
}

template <typename Src>
inline void CFileReader<Src>::Reset(const Src& src, Options options) {
  CFileReaderBase::Reset(options.buffer_options(), options.growing_source());
  src_.Reset(src);
  Initialize(src_.get(), std::move(options.assumed_filename()),
             options.assumed_pos());
}

template <typename Src>
inline void CFileReader<Src>::Reset(Src&& src, Options options) {
  CFileReaderBase::Reset(options.buffer_options(), options.growing_source());
  src_.Reset(std::move(src));
  Initialize(src_.get(), std::move(options.assumed_filename()),
             options.assumed_pos());
}

template <typename Src>
inline void CFileReader<Src>::Reset(FILE* src, Options options) {
  Reset(std::forward_as_tuple(src), std::move(options));
}

template <typename Src>
template <typename... SrcArgs>
inline void CFileReader<Src>::Reset(std::tuple<SrcArgs...> src_args,
                                    Options options) {
  CFileReaderBase::Reset(options.buffer_options(), options.growing_source());
  src_.Reset(std::move(src_args));
  Initialize(src_.get(), std::move(options.assumed_filename()),
             options.assumed_pos());
}

template <typename Src>
template <typename DependentSrc,
          std::enable_if_t<std::is_same<DependentSrc, OwnedCFile>::value, int>>
inline void CFileReader<Src>::Reset(absl::string_view filename,
                                    Options options) {
  CFileReaderBase::Reset(options.buffer_options(), options.growing_source());
  Initialize(filename, std::move(options));
}

template <typename Src>
void CFileReader<Src>::Initialize(absl::string_view filename,
                                  Options&& options) {
  FILE* const src = OpenFile(filename, options.mode().c_str());
  if (ABSL_PREDICT_FALSE(src == nullptr)) return;
  src_.Reset(std::forward_as_tuple(src));
  InitializePos(src_.get(), options.assumed_pos());
}

template <typename Src>
void CFileReader<Src>::Done() {
  CFileReaderBase::Done();
  {
    FILE* const src = src_.Release();
    if (src != nullptr) {
      if (ABSL_PREDICT_FALSE((fclose(src)) != 0) && ABSL_PREDICT_TRUE(ok())) {
        FailOperation("fclose()");
      }
    } else {
      RIEGELI_ASSERT(!src_.is_owning())
          << "The dependency type does not support closing the FILE";
    }
  }
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_CFILE_READER_H_
