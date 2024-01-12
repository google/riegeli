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
#include <stdio.h>

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
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/cfile_dependency.h"  // IWYU pragma: export
#include "riegeli/bytes/file_mode_string.h"

namespace riegeli {

// Template parameter independent part of `CFileReader`.
class CFileReaderBase : public BufferedReader {
 public:
  class Options : public BufferOptionsBase<Options> {
   public:
    Options() noexcept {}

    // `assumed_filename()` allows to override the filename which is included in
    // failure messages and returned by `filename()`.
    //
    // If this is `absl::nullopt` and `CFileReader` opens a `FILE` with a
    // filename, then that filename is used.
    //
    // If this is `absl::nullopt` and `CFileReader` reads from an already open
    // `FILE`, then "/dev/stdin", "/dev/stdout", "/dev/stderr", or
    // `absl::StrCat("/proc/self/fd/", fd)` is inferred from the fd
    // corresponding to the `FILE` (on Windows: "CONIN$", "CONOUT$", "CONERR$",
    // or `absl::StrCat("<fd ", fd, ">")`), or "<unknown>" if there is no
    // corresponding fd.
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

    // If `CFileReader` opens a `FILE` with a filename, `mode()` is the second
    // argument of `fopen()` and specifies the open mode, typically "r" (on
    // Windows: "rb").
    //
    // `mode()` can also be changed with `set_inheritable()` and `set_text()`.
    //
    // Default: "re" (on Windows: "rbN").
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

    // If `false`, `execve()` (`CreateProcess()` on Windows) will close the
    // file. This is not supported by all systems, but it is supported at least
    // on Linux, Windows, FreeBSD, OpenBSD, NetBSD, and it is planned for POSIX
    // (https://www.austingroupbugs.net/view.php?id=411). For MacOS X this is
    // emulated by `CFileReader`.
    //
    // If `true`, the file will remain open across `execve()` (`CreateProcess()`
    // on Windows).
    //
    // If `CFileReader` reads from an already open file, `inheritable()` has no
    // effect.
    //
    // `set_inheritable()` affects `mode()`.
    //
    // Default: `false`.
    Options& set_inheritable(bool inheritable) & {
      file_internal::SetInheritableReading(inheritable, mode_);
      return *this;
    }
    Options&& set_inheritable(bool inheritable) && {
      return std::move(set_inheritable(inheritable));
    }
    bool inheritable() const { return file_internal::GetInheritable(mode_); }

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
    // `CFileReader` opens a `FILE` with a filename or reads from an already
    // open `FILE`.
    //
    // `set_text()` affects `mode()`.
    //
    // Default: `false`.
    Options& set_text(bool text) & {
      file_internal::SetTextReading(text, mode_);
      return *this;
    }
    Options&& set_text(bool text) && { return std::move(set_text(text)); }
    // No `text()` getter is provided. On Windows `mode()` can have unspecified
    // text mode, resolved using `_get_fmode()`. Not on Windows the concept does
    // not exist.

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
    absl::optional<std::string> assumed_filename_;
#ifndef _WIN32
    std::string mode_ = "re";
#else
    std::string mode_ = "rbN";
#endif
    absl::optional<Position> assumed_pos_;
    bool growing_source_ = false;
  };

  // Returns the `FILE` being read from. If the `FILE` is owned then changed to
  // `nullptr` by `Close()`, otherwise unchanged.
  virtual FILE* SrcFile() const = 0;

  // Returns the original name of the file being read from. Unchanged by
  // `Close()`.
  absl::string_view filename() const { return filename_; }

  bool ToleratesReadingAhead() override {
    return BufferedReader::ToleratesReadingAhead() ||
           CFileReaderBase::SupportsRandomAccess();
  }
  bool SupportsRandomAccess() override { return supports_random_access_; }

 protected:
  explicit CFileReaderBase(Closed) noexcept : BufferedReader(kClosed) {}

  explicit CFileReaderBase(const BufferOptions& buffer_options,
                           bool growing_source);

  CFileReaderBase(CFileReaderBase&& that) noexcept;
  CFileReaderBase& operator=(CFileReaderBase&& that) noexcept;

  void Reset(Closed);
  void Reset(const BufferOptions& buffer_options, bool growing_source);
  void Initialize(FILE* src, Options&& options);
  FILE* OpenFile(absl::string_view filename, const std::string& mode);
  bool InitializeAssumedFilename(Options& options);
  void InitializePos(FILE* src, Options&& options
#ifdef _WIN32
                     ,
                     bool mode_was_passed_to_fopen
#endif
  );
  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation);

  void Done() override;
  absl::Status AnnotateStatusImpl(absl::Status status) override;
  bool ReadInternal(size_t min_length, size_t max_length, char* dest) override;
  bool SeekBehindBuffer(Position new_pos) override;
  absl::optional<Position> SizeImpl() override;

 private:
  absl::Status FailedOperationStatus(absl::string_view operation);

  std::string filename_;
  bool growing_source_ = false;
  bool supports_random_access_ = false;
  absl::Status random_access_status_;
#ifdef _WIN32
  absl::optional<int> original_mode_;
#endif

  // Invariant:
  //   `limit_pos() <= std::numeric_limits<cfile_internal::Offset>::max()`
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
// `UnownedCFile` (not owned), `AnyDependency<FILE*>` (maybe owned).
// The only supported owning `Src` is `OwnedCFile`, possibly wrapped in
// `AnyDependency`.
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
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(const Src& src,
                                          Options options = Options());
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Src&& src,
                                          Options options = Options());
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(FILE* src,
                                          Options options = Options());
  template <typename... SrcArgs>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(std::tuple<SrcArgs...> src_args,
                                          Options options = Options());
  template <
      typename DependentSrc = Src,
      std::enable_if_t<std::is_same<DependentSrc, OwnedCFile>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(absl::string_view filename,
                                          Options options = Options());

  // Returns the object providing and possibly owning the `FILE` being read
  // from. If the `FILE` is owned then changed to `nullptr` by `Close()`,
  // otherwise unchanged.
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  FILE* SrcFile() const override { return src_.get(); }

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
explicit CFileReader(Closed) -> CFileReader<DeleteCtad<Closed>>;
template <typename Src>
explicit CFileReader(const Src& src, CFileReaderBase::Options options =
                                         CFileReaderBase::Options())
    -> CFileReader<std::conditional_t<
        absl::disjunction<
            std::is_convertible<const Src&, FILE*>,
            std::is_convertible<const Src&, absl::string_view>>::value,
        OwnedCFile, std::decay_t<Src>>>;
template <typename Src>
explicit CFileReader(
    Src&& src, CFileReaderBase::Options options = CFileReaderBase::Options())
    -> CFileReader<std::conditional_t<
        absl::disjunction<std::is_convertible<Src&&, FILE*>,
                          std::is_convertible<Src&&, absl::string_view>>::value,
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
      supports_random_access_(
          std::exchange(that.supports_random_access_, false)),
      random_access_status_(std::move(that.random_access_status_))
#ifdef _WIN32
      ,
      original_mode_(that.original_mode_)
#endif
{
}

inline CFileReaderBase& CFileReaderBase::operator=(
    CFileReaderBase&& that) noexcept {
  BufferedReader::operator=(static_cast<BufferedReader&&>(that));
  filename_ = std::exchange(that.filename_, std::string());
  growing_source_ = that.growing_source_;
  supports_random_access_ = std::exchange(that.supports_random_access_, false);
  random_access_status_ = std::move(that.random_access_status_);
#ifdef _WIN32
  original_mode_ = that.original_mode_;
#endif
  return *this;
}

inline void CFileReaderBase::Reset(Closed) {
  BufferedReader::Reset(kClosed);
  filename_ = std::string();
  growing_source_ = false;
  supports_random_access_ = false;
  random_access_status_ = absl::OkStatus();
#ifdef _WIN32
  original_mode_ = absl::nullopt;
#endif
}

inline void CFileReaderBase::Reset(const BufferOptions& buffer_options,
                                   bool growing_source) {
  BufferedReader::Reset(buffer_options);
  // `filename_` will be set by `Initialize()`, `OpenFile()`, or
  // `InitializeAssumedFilename()`.
  growing_source_ = growing_source;
  supports_random_access_ = false;
  random_access_status_ = absl::OkStatus();
#ifdef _WIN32
  original_mode_ = absl::nullopt;
#endif
}

inline bool CFileReaderBase::InitializeAssumedFilename(Options& options) {
  if (options.assumed_filename() != absl::nullopt) {
    filename_ = *std::move(options.assumed_filename());
    return true;
  } else {
    return false;
  }
}

template <typename Src>
inline CFileReader<Src>::CFileReader(const Src& src, Options options)
    : CFileReaderBase(options.buffer_options(), options.growing_source()),
      src_(src) {
  Initialize(src_.get(), std::move(options));
}

template <typename Src>
inline CFileReader<Src>::CFileReader(Src&& src, Options options)
    : CFileReaderBase(options.buffer_options(), options.growing_source()),
      src_(std::move(src)) {
  Initialize(src_.get(), std::move(options));
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
  Initialize(src_.get(), std::move(options));
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
  Initialize(src_.get(), std::move(options));
}

template <typename Src>
inline void CFileReader<Src>::Reset(Src&& src, Options options) {
  CFileReaderBase::Reset(options.buffer_options(), options.growing_source());
  src_.Reset(std::move(src));
  Initialize(src_.get(), std::move(options));
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
  Initialize(src_.get(), std::move(options));
}

template <typename Src>
template <typename DependentSrc,
          std::enable_if_t<std::is_same<DependentSrc, OwnedCFile>::value, int>>
inline void CFileReader<Src>::Reset(absl::string_view filename,
                                    Options options) {
  CFileReaderBase::Reset(options.buffer_options(), options.growing_source());
  src_.Reset();
  Initialize(filename, std::move(options));
}

template <typename Src>
void CFileReader<Src>::Initialize(absl::string_view filename,
                                  Options&& options) {
  FILE* const src = OpenFile(filename, options.mode());
  InitializeAssumedFilename(options);
  if (ABSL_PREDICT_FALSE(src == nullptr)) return;
  src_.Reset(std::forward_as_tuple(src));
  InitializePos(src_.get(), std::move(options)
#ifdef _WIN32
                                ,
                /*mode_was_passed_to_fopen=*/true
#endif
  );
}

template <typename Src>
void CFileReader<Src>::Done() {
  CFileReaderBase::Done();
  {
    OwnedCFile* const src = src_.template GetIf<OwnedCFile>();
    if (src != nullptr) {
      if (ABSL_PREDICT_FALSE((fclose(src->Release())) != 0) &&
          ABSL_PREDICT_TRUE(ok())) {
        FailOperation("fclose()");
      }
    } else if (src_.is_owning()) {
      Fail(
          absl::InvalidArgumentError("CFileReader dependency owns the FILE "
                                     "but does not contain OwnedCFile"));
    }
  }
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_CFILE_READER_H_
