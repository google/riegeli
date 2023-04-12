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

#ifndef RIEGELI_BYTES_CFILE_WRITER_H_
#define RIEGELI_BYTES_CFILE_WRITER_H_

#include <stdint.h>
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
#include "riegeli/base/assert.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/cfile_dependency.h"  // IWYU pragma: export
#include "riegeli/bytes/file_mode_string.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

template <typename Src>
class CFileReader;
class Reader;

// Template parameter independent part of `CFileWriter`.
class CFileWriterBase : public BufferedWriter {
 public:
  class Options : public BufferOptionsBase<Options> {
   public:
    Options() noexcept {}

    // If `CFileWriter` writes to an already open `FILE`, `assumed_filename()`
    // allows to override the filename which is included in failure messages and
    // returned by `filename()`.
    //
    // If `CFileWriter` opens a `FILE` with a filename, `assumed_filename()` has
    // no effect.
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

    // If `CFileWriter` opens a `FILE` with a filename, `mode()` is the second
    // argument of `fopen()` and specifies the open mode, typically "w" or "a"
    // (on Windows: "wb" or "ab").
    //
    // `mode()` can also be changed with `set_existing(), `set_read()`,
    // `set_append()`, `set_exclusive()`, `set_inheritable()`, and `set_text()`.
    //
    // Default: "we" (on Windows: "wbN").
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

    // If `false`, the file will be created if it does not exist, or it will be
    // truncated to empty if it exists. This implies `set_read(false)` and
    // `set_append(false)` unless overwritten later.
    //
    // If `true`, the file must already exist, and its contents will not be
    // truncated. Writing will start from the beginning, with random access
    // supported. This implies `set_read(true)`.
    //
    // If `CFileWriter` writes to an already open `FILE`, `existing()` has no
    // effect.
    //
    // `set_existing()` affects `mode()`.
    //
    // Default: `false`.
    Options& set_existing(bool existing) & {
      file_internal::SetExisting(existing, mode_);
      return *this;
    }
    Options&& set_existing(bool existing) && {
      return std::move(set_existing(existing));
    }
    bool existing() const { return file_internal::GetExisting(mode_); }

    // If `false`, the `FILE` will be open for writing, except that
    // `set_read(false)` has no effect after `set_existing(true)`.
    //
    // If `true`, the `FILE` will be open for writing and reading (using
    // `ReadMode()`).
    //
    // If `CFileWriter` writes to an already open `FILE`, `read()` has no
    // effect.
    //
    // `set_read()` affects `mode()`.
    //
    // Default: `false`.
    Options& set_read(bool read) & {
      file_internal::SetRead(read, mode_);
      return *this;
    }
    Options&& set_read(bool read) && { return std::move(set_read(read)); }
    bool read() const { return file_internal::GetRead(mode_); }

    // If `false`, the file will be truncated to empty if it exists.
    //
    // If `true`, the file will not be truncated if it exists, and writing will
    // always happen at its end.
    //
    // Calling `set_append()` with any argument after `set_existing(true)`
    // undoes the effect if the file does not exist: it will be created.
    //
    // If `CFileWriter` writes to an already open `FILE` and `assumed_pos()` is
    // not set, `append()` should be `true` if the `FILE` was originally open
    // in append mode. This allows to determine the effective initial position
    // and lets `SupportsRandomAccess()` correctly return `false`.
    //
    // `set_append()` affects `mode()`.
    //
    // Default: `false`.
    Options& set_append(bool append) & {
      file_internal::SetAppend(append, mode_);
      return *this;
    }
    Options&& set_append(bool append) && {
      return std::move(set_append(append));
    }
    bool append() const { return file_internal::GetAppend(mode_); }

    // If `false`, the file will be created if it does not exist, or it will be
    // opened if it exists (truncated to empty by default, or left unchanged if
    // `set_existing(true)` or `set_append(true)` was used).
    //
    // If `true`, the file will be created if it does not exist, or opening will
    // fail if it exists. This is not supported by all systems, but it is
    // specified by C++17 and supported at least on Linux, Windows, FreeBSD,
    // OpenBSD, NetBSD, and MacOS X.
    //
    // If `CFileWriter` writes to an already open `FILE`, `exclusive()` has no
    // effect.
    //
    // `set_exclusive()` affects `mode()`.
    //
    // Default: `false`.
    Options& set_exclusive(bool exclusive) & {
      file_internal::SetExclusive(exclusive, mode_);
      return *this;
    }
    Options&& set_exclusive(bool exclusive) && {
      return std::move(set_exclusive(exclusive));
    }
    bool exclusive() const { return file_internal::GetExclusive(mode_); }

    // If `false`, `execve()` (`CreateProcess()` on Windows) will close the
    // file. This is not supported by all systems, but it is supported at least
    // on Linux, Windows, FreeBSD, OpenBSD, NetBSD, and it is planned for POSIX
    // (https://www.austingroupbugs.net/view.php?id=411). For MacOS X this is
    // emulated by `CFileWriter`.
    //
    // If `true`, the file will remain open across `execve()` (`CreateProcess()`
    // on Windows).
    //
    // If `CFileWriter` writes to an already open file, `inheritable()` has no
    // effect.
    //
    // `set_inheritable()` affects `mode()`.
    //
    // Default: `false`.
    Options& set_inheritable(bool inheritable) & {
      file_internal::SetInheritableWriting(inheritable, mode_);
      return *this;
    }
    Options&& set_inheritable(bool inheritable) && {
      return std::move(set_inheritable(inheritable));
    }
    bool inheritable() const { return file_internal::GetInheritable(mode_); }

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
    // `CFileWriter` opens a `FILE` with a filename or reads from an already
    // open `FILE`.
    //
    // `set_text()` affects `mode()`.
    //
    // Default: `false`.
    Options& set_text(bool text) & {
      file_internal::SetTextWriting(text, mode_);
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

   private:
    std::string assumed_filename_;
#ifndef _WIN32
    std::string mode_ = "we";
#else
    std::string mode_ = "wbN";
#endif
    absl::optional<Position> assumed_pos_;
  };

  // Returns the `FILE` being written to. If the `FILE` is owned then changed to
  // `nullptr` by `Close()`, otherwise unchanged.
  virtual FILE* DestFile() const = 0;

  // Returns the original name of the file being written to. Unchanged by
  // `Close()`.
  absl::string_view filename() const { return filename_; }

  bool SupportsRandomAccess() override;
  bool SupportsTruncate() override;
  bool SupportsReadMode() override;

 protected:
  explicit CFileWriterBase(Closed) noexcept : BufferedWriter(kClosed) {}

  explicit CFileWriterBase(const BufferOptions& buffer_options);

  CFileWriterBase(CFileWriterBase&& that) noexcept;
  CFileWriterBase& operator=(CFileWriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset(const BufferOptions& buffer_options);
  void Initialize(FILE* dest, std::string&& assumed_filename,
                  absl::string_view mode, absl::optional<Position> assumed_pos);
  FILE* OpenFile(absl::string_view filename, const std::string& mode);
  void InitializePos(FILE* dest, absl::string_view mode,
                     bool mode_was_passed_to_fopen,
                     absl::optional<Position> assumed_pos);
  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation);

  void Done() override;
  absl::Status AnnotateStatusImpl(absl::Status status) override;
  bool WriteInternal(absl::string_view src) override;
  bool FlushBehindBuffer(absl::string_view src, FlushType flush_type) override;
  bool SeekBehindBuffer(Position new_pos) override;
  absl::optional<Position> SizeBehindBuffer() override;
  bool TruncateBehindBuffer(Position new_size) override;
  Reader* ReadModeBehindBuffer(Position initial_pos) override;

 private:
  // Encodes a `bool` or a marker that the value is not resolved yet.
  enum class LazyBoolState : uint8_t { kUnknown, kTrue, kFalse };

  absl::Status FailedOperationStatus(absl::string_view operation);
  // Lazily determined condition shared by `SupportsRandomAccess()` and
  // `SupportsReadMode()`.
  absl::Status SizeStatus();

  bool WriteMode();

  std::string filename_;
  LazyBoolState supports_random_access_ = LazyBoolState::kUnknown;
  // If `supports_read_mode_ == LazyBoolState::kUnknown`,
  // then at least size is known to be supported
  // when `supports_random_access_ != LazyBoolState::kUnknown`
  // (no matter whether `LazyBoolState::kTrue` or LazyBoolState::kFalse`).
  //
  // Invariant:
  //   if `supports_random_access_ == LazyBoolState::kUnknown` then
  //       `supports_read_mode_ != LazyBoolState::kTrue`
  LazyBoolState supports_read_mode_ = LazyBoolState::kUnknown;
  absl::Status random_access_status_;
  absl::Status read_mode_status_;
#ifdef _WIN32
  absl::optional<int> original_mode_;
#endif

  AssociatedReader<CFileReader<UnownedCFile>> associated_reader_;
  bool read_mode_ = false;

  // Invariant:
  //   `start_pos() <= std::numeric_limits<cfile_internal::Offset>::max()`
};

// A `Writer` which writes to a `FILE`.
//
// `CFileWriter` supports random access if
// `Options::assumed_pos() == absl::nullopt` and the `FILE` supports random
// access (this is checked by calling `ftell()` and `fseek(SEEK_END)`).
//
// On Linux, some virtual file systems ("/proc", "/sys") contain files with
// contents generated on the fly when the files are read. The files appear as
// regular files, with an apparent size of 0 or 4096, and random access is only
// partially supported. `CFileWriter` detects lack of random access for them
// only if the filename seen `CFileWriter` starts with "/proc/" (for zero-sized
// files) or "/sys/". An explicit
// `CFileWriterBase::Options().set_assumed_pos(0)` can be used to disable random
// access for such files.
//
// `CFileWriter` supports `ReadMode()` if it supports random access and the
// `FILE` supports reading (this is checked by calling `fread()`).
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the `FILE` being written to. `Dest` must support
// `Dependency<FILE*, Dest>`, e.g. `OwnedCFile` (owned, default),
// `UnownedCFile` (not owned), `AnyDependency<FILE*>` (maybe owned).
// The only supported owning `Dest` is `OwnedCFile`, possibly wrapped in
// `AnyDependency`.
//
// By relying on CTAD the template argument can be deduced as `OwnedCFile` if
// the first constructor argument is a filename or a `FILE*`, otherwise as the
// value type of the first constructor argument. This requires C++17.
//
// Until the `CFileWriter` is closed or no longer used, the `FILE` must not be
// closed nor have its position changed, except that if random access is not
// used, careful interleaving of multiple writers is possible: `Flush()` is
// needed before switching to another writer, and `pos()` does not take other
// writers into account.
template <typename Dest = OwnedCFile>
class CFileWriter : public CFileWriterBase {
 public:
  // Creates a closed `CFileWriter`.
  explicit CFileWriter(Closed) noexcept : CFileWriterBase(kClosed) {}

  // Will write to the `FILE` provided by `dest`.
  explicit CFileWriter(const Dest& dest, Options options = Options());
  explicit CFileWriter(Dest&& dest, Options options = Options());
  explicit CFileWriter(FILE* dest, Options options = Options());

  // Will write to the `FILE` provided by a `Dest` constructed from elements of
  // `dest_args`. This avoids constructing a temporary `Dest` and moving from
  // it.
  template <typename... DestArgs>
  explicit CFileWriter(std::tuple<DestArgs...> dest_args,
                       Options options = Options());

  // Opens a file for writing.
  //
  // If opening the file fails, `CFileWriter` will be failed and closed.
  //
  // This constructor is present only if `Dest` is `OwnedCFile`.
  template <
      typename DependentDest = Dest,
      std::enable_if_t<std::is_same<DependentDest, OwnedCFile>::value, int> = 0>
  explicit CFileWriter(absl::string_view filename, Options options = Options());

  CFileWriter(CFileWriter&& that) noexcept;
  CFileWriter& operator=(CFileWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `CFileWriter`. This avoids
  // constructing a temporary `CFileWriter` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(const Dest& dest,
                                          Options options = Options());
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Dest&& dest,
                                          Options options = Options());
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(FILE* dest,
                                          Options options = Options());
  template <typename... DestArgs>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(std::tuple<DestArgs...> dest_args,
                                          Options options = Options());
  template <
      typename DependentDest = Dest,
      std::enable_if_t<std::is_same<DependentDest, OwnedCFile>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(absl::string_view filename,
                                          Options options = Options());

  // Returns the object providing and possibly owning the `FILE` being written
  // to. If the `FILE` is owned then changed to `nullptr` by `Close()`,
  // otherwise unchanged.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  FILE* DestFile() const override { return dest_.get(); }

 protected:
  using CFileWriterBase::Initialize;
  void Initialize(absl::string_view filename, Options&& options);

  void Done() override;
  bool FlushImpl(FlushType flush_type) override;

 private:
  // The object providing and possibly owning the `FILE` being written to.
  Dependency<FILE*, Dest> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit CFileWriter(Closed) -> CFileWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit CFileWriter(const Dest& dest, CFileWriterBase::Options options =
                                           CFileWriterBase::Options())
    -> CFileWriter<std::conditional_t<
        absl::disjunction<
            std::is_convertible<const Dest&, FILE*>,
            std::is_convertible<const Dest&, absl::string_view>>::value,
        OwnedCFile, std::decay_t<Dest>>>;
template <typename Dest>
explicit CFileWriter(
    Dest&& dest, CFileWriterBase::Options options = CFileWriterBase::Options())
    -> CFileWriter<std::conditional_t<
        absl::disjunction<
            std::is_convertible<Dest&&, FILE*>,
            std::is_convertible<Dest&&, absl::string_view>>::value,
        OwnedCFile, std::decay_t<Dest>>>;
template <typename... DestArgs>
explicit CFileWriter(
    std::tuple<DestArgs...> dest_args,
    CFileWriterBase::Options options = CFileWriterBase::Options())
    -> CFileWriter<DeleteCtad<std::tuple<DestArgs...>>>;
#endif

// Implementation details follow.

inline CFileWriterBase::CFileWriterBase(const BufferOptions& buffer_options)
    : BufferedWriter(buffer_options) {}

inline CFileWriterBase::CFileWriterBase(CFileWriterBase&& that) noexcept
    : BufferedWriter(static_cast<BufferedWriter&&>(that)),
      filename_(std::exchange(that.filename_, std::string())),
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

inline CFileWriterBase& CFileWriterBase::operator=(
    CFileWriterBase&& that) noexcept {
  BufferedWriter::operator=(static_cast<BufferedWriter&&>(that));
  filename_ = std::exchange(that.filename_, std::string());
  supports_random_access_ =
      std::exchange(that.supports_random_access_, LazyBoolState::kUnknown);
  supports_read_mode_ =
      std::exchange(that.supports_read_mode_, LazyBoolState::kUnknown);
  random_access_status_ = std::move(that.random_access_status_);
  read_mode_status_ = std::move(that.read_mode_status_);
#ifdef _WIN32
  original_mode_ = that.original_mode_;
#endif
  associated_reader_ = std::move(that.associated_reader_);
  read_mode_ = that.read_mode_;
  return *this;
}

inline void CFileWriterBase::Reset(Closed) {
  BufferedWriter::Reset(kClosed);
  filename_ = std::string();
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

inline void CFileWriterBase::Reset(const BufferOptions& buffer_options) {
  BufferedWriter::Reset(buffer_options);
  // `filename_` will be set by `Initialize()` or `OpenFile()`.
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

template <typename Dest>
inline CFileWriter<Dest>::CFileWriter(const Dest& dest, Options options)
    : CFileWriterBase(options.buffer_options()), dest_(dest) {
  Initialize(dest_.get(), std::move(options.assumed_filename()), options.mode(),
             options.assumed_pos());
}

template <typename Dest>
inline CFileWriter<Dest>::CFileWriter(Dest&& dest, Options options)
    : CFileWriterBase(options.buffer_options()), dest_(std::move(dest)) {
  Initialize(dest_.get(), std::move(options.assumed_filename()), options.mode(),
             options.assumed_pos());
}

template <typename Dest>
inline CFileWriter<Dest>::CFileWriter(FILE* dest, Options options)
    : CFileWriter(std::forward_as_tuple(dest), std::move(options)) {}

template <typename Dest>
template <typename... DestArgs>
inline CFileWriter<Dest>::CFileWriter(std::tuple<DestArgs...> dest_args,
                                      Options options)
    : CFileWriterBase(options.buffer_options()), dest_(std::move(dest_args)) {
  Initialize(dest_.get(), std::move(options.assumed_filename()), options.mode(),
             options.assumed_pos());
}

template <typename Dest>
template <typename DependentDest,
          std::enable_if_t<std::is_same<DependentDest, OwnedCFile>::value, int>>
inline CFileWriter<Dest>::CFileWriter(absl::string_view filename,
                                      Options options)
    : CFileWriterBase(options.buffer_options()) {
  Initialize(filename, std::move(options));
}

template <typename Dest>
inline CFileWriter<Dest>::CFileWriter(CFileWriter&& that) noexcept
    : CFileWriterBase(static_cast<CFileWriterBase&&>(that)),
      dest_(std::move(that.dest_)) {}

template <typename Dest>
inline CFileWriter<Dest>& CFileWriter<Dest>::operator=(
    CFileWriter&& that) noexcept {
  CFileWriterBase::operator=(static_cast<CFileWriterBase&&>(that));
  dest_ = std::move(that.dest_);
  return *this;
}

template <typename Dest>
inline void CFileWriter<Dest>::Reset(Closed) {
  CFileWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void CFileWriter<Dest>::Reset(const Dest& dest, Options options) {
  CFileWriterBase::Reset(options.buffer_options());
  dest_.Reset(dest);
  Initialize(dest_.get(), std::move(options.assumed_filename()), options.mode(),
             options.assumed_pos());
}

template <typename Dest>
inline void CFileWriter<Dest>::Reset(Dest&& dest, Options options) {
  CFileWriterBase::Reset(options.buffer_options());
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), std::move(options.assumed_filename()), options.mode(),
             options.assumed_pos());
}

template <typename Dest>
inline void CFileWriter<Dest>::Reset(FILE* dest, Options options) {
  Reset(std::forward_as_tuple(dest), std::move(options));
}

template <typename Dest>
template <typename... DestArgs>
inline void CFileWriter<Dest>::Reset(std::tuple<DestArgs...> dest_args,
                                     Options options) {
  CFileWriterBase::Reset(options.buffer_options());
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get(), std::move(options.assumed_filename()), options.mode(),
             options.assumed_pos());
}

template <typename Dest>
template <typename DependentDest,
          std::enable_if_t<std::is_same<DependentDest, OwnedCFile>::value, int>>
inline void CFileWriter<Dest>::Reset(absl::string_view filename,
                                     Options options) {
  CFileWriterBase::Reset(options.buffer_options());
  Initialize(filename, std::move(options));
}

template <typename Dest>
void CFileWriter<Dest>::Initialize(absl::string_view filename,
                                   Options&& options) {
  FILE* const dest = OpenFile(filename, options.mode());
  if (ABSL_PREDICT_FALSE(dest == nullptr)) return;
  dest_.Reset(std::forward_as_tuple(dest));
  InitializePos(dest_.get(), options.mode(),
                /*mode_was_passed_to_fopen=*/true, options.assumed_pos());
}

template <typename Dest>
void CFileWriter<Dest>::Done() {
  CFileWriterBase::Done();
  {
    OwnedCFile* const dest = dest_.template GetIf<OwnedCFile>();
    if (dest != nullptr) {
      if (ABSL_PREDICT_FALSE((fclose(dest->Release())) != 0) &&
          ABSL_PREDICT_TRUE(ok())) {
        FailOperation("fclose()");
      }
    } else if (dest_.is_owning()) {
      Fail(
          absl::InvalidArgumentError("CFileWriter dependency owns the FILE "
                                     "but does not contain OwnedCFile"));
    }
  }
}

template <typename Dest>
bool CFileWriter<Dest>::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!CFileWriterBase::FlushImpl(flush_type))) return false;
  switch (flush_type) {
    case FlushType::kFromObject:
      if (!dest_.is_owning()) return true;
      ABSL_FALLTHROUGH_INTENDED;
    case FlushType::kFromProcess:
    case FlushType::kFromMachine:
      if (ABSL_PREDICT_FALSE(fflush(dest_.get()) != 0)) {
        return FailOperation("fflush()");
      }
      return true;
  }
  RIEGELI_ASSERT_UNREACHABLE()
      << "Unknown flush type: " << static_cast<int>(flush_type);
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_CFILE_WRITER_H_
