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

#include <optional>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/compact_string.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/object.h"
#include "riegeli/base/reset.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/cfile_handle.h"
#include "riegeli/bytes/file_mode_string.h"
#include "riegeli/bytes/path_ref.h"
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

    // If `CFileWriter` opens a `FILE` with a filename, `mode()` is the second
    // argument of `fopen()` and specifies the open mode, typically "w" or "a"
    // (on Windows: "wb" or "ab").
    //
    // `mode()` can also be changed with `set_existing(), `set_read()`,
    // `set_append()`, `set_exclusive()`, `set_inheritable()`, and `set_text()`.
    //
    // Default: "we" (on Windows: "wbN").
    Options& set_mode(Initializer<std::string>::AllowingExplicit mode) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      riegeli::Reset(mode_, std::move(mode));
      return *this;
    }
    Options&& set_mode(Initializer<std::string>::AllowingExplicit mode) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_mode(std::move(mode)));
    }
    const std::string& mode() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return mode_;
    }

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
    Options& set_existing(bool existing) & ABSL_ATTRIBUTE_LIFETIME_BOUND {
      file_internal::SetExisting(existing, mode_);
      return *this;
    }
    Options&& set_existing(bool existing) && ABSL_ATTRIBUTE_LIFETIME_BOUND {
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
    Options& set_read(bool read) & ABSL_ATTRIBUTE_LIFETIME_BOUND {
      file_internal::SetRead(read, mode_);
      return *this;
    }
    Options&& set_read(bool read) && ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_read(read));
    }
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
    Options& set_append(bool append) & ABSL_ATTRIBUTE_LIFETIME_BOUND {
      file_internal::SetAppend(append, mode_);
      return *this;
    }
    Options&& set_append(bool append) && ABSL_ATTRIBUTE_LIFETIME_BOUND {
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
    Options& set_exclusive(bool exclusive) & ABSL_ATTRIBUTE_LIFETIME_BOUND {
      file_internal::SetExclusive(exclusive, mode_);
      return *this;
    }
    Options&& set_exclusive(bool exclusive) && ABSL_ATTRIBUTE_LIFETIME_BOUND {
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
    Options& set_inheritable(bool inheritable) & ABSL_ATTRIBUTE_LIFETIME_BOUND {
      file_internal::SetInheritableWriting(inheritable, mode_);
      return *this;
    }
    Options&& set_inheritable(bool inheritable) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
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
    Options& set_text(bool text) & ABSL_ATTRIBUTE_LIFETIME_BOUND {
      file_internal::SetTextWriting(text, mode_);
      return *this;
    }
    Options&& set_text(bool text) && ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_text(text));
    }
    // No `text()` getter is provided. On Windows `mode()` can have unspecified
    // text mode, resolved using `_get_fmode()`. Not on Windows the concept does
    // not exist.

    // If `std::nullopt`, the current position reported by `pos()` corresponds
    // to the current `FILE` position if possible, otherwise 0 is assumed as the
    // initial position. Random access is supported if the `FILE` supports
    // random access.
    //
    // If not `std::nullopt`, this position is assumed initially, to be reported
    // by `pos()`. It does not need to correspond to the current `FILE`
    // position. Random access is not supported.
    //
    // Default: `std::nullopt`.
    Options& set_assumed_pos(std::optional<Position> assumed_pos) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      assumed_pos_ = assumed_pos;
      return *this;
    }
    Options&& set_assumed_pos(std::optional<Position> assumed_pos) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_assumed_pos(assumed_pos));
    }
    std::optional<Position> assumed_pos() const { return assumed_pos_; }

   private:
#ifndef _WIN32
    std::string mode_ = "we";
#else
    std::string mode_ = "wbN";
#endif
    std::optional<Position> assumed_pos_;
  };

  // Returns the `CFileHandle` being written to. Unchanged by `Close()`.
  virtual CFileHandle DestCFileHandle() const ABSL_ATTRIBUTE_LIFETIME_BOUND = 0;

  // Returns the `FILE*` being written to. If the `FILE*` is owned then changed
  // to `nullptr` by `Close()`, otherwise unchanged.
  virtual FILE* DestFile() const ABSL_ATTRIBUTE_LIFETIME_BOUND = 0;

  // Returns the filename of the `FILE*` being written to, or "<none>" for
  // closed-constructed or moved-from `CFileWriter`. Unchanged by `Close()`.
  //
  // If the constructor from filename was used, this is the filename passed to
  // the constructor, otherwise a filename is inferred from the `FILE*`. This
  // can be a placeholder instead of a real filename if the `FILE*` does not
  // refer to a named file or inferring the filename is not supported.
  //
  // If `Dest` does not support `filename()`, returns "<unsupported>".
  absl::string_view filename() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return DestCFileHandle().filename();
  }

  bool SupportsRandomAccess() override;
  bool SupportsTruncate() override;
  bool SupportsReadMode() override;

 protected:
  explicit CFileWriterBase(Closed) noexcept : BufferedWriter(kClosed) {}

  explicit CFileWriterBase(BufferOptions buffer_options);

  CFileWriterBase(CFileWriterBase&& that) noexcept;
  CFileWriterBase& operator=(CFileWriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset(BufferOptions buffer_options);
  void Initialize(FILE* dest, Options&& options);
  void InitializePos(FILE* dest, Options&& options,
                     bool mode_was_passed_to_fopen);
  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation);

  void Done() override;
  absl::Status AnnotateStatusImpl(absl::Status status) override;
  bool WriteInternal(absl::string_view src) override;
  bool FlushBehindBuffer(absl::string_view src, FlushType flush_type) override;
  bool SeekBehindBuffer(Position new_pos) override;
  std::optional<Position> SizeBehindBuffer() override;
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
  std::optional<int> original_mode_;
#endif

  AssociatedReader<CFileReader<UnownedCFile>> associated_reader_;
  bool read_mode_ = false;

  // Invariant:
  //   `start_pos() <= std::numeric_limits<cfile_internal::Offset>::max()`
};

// A `Writer` which writes to a `FILE`.
//
// `CFileWriter` supports random access if
// `Options::assumed_pos() == std::nullopt` and the `FILE` supports random
// access (this is checked by calling `ftell()` and `fseek(SEEK_END)`).
//
// `CFileWriter` supports `ReadMode()` if it supports random access and the
// `FILE` supports reading (this is checked by calling `fread()`).
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the `FILE` being written to. `Dest` must support
// `Dependency<CFileHandle, Dest>`, e.g. `OwnedCFile` (owned, default),
// `UnownedCFile` (not owned), `AnyCFile` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as `OwnedCFile` if
// the first constructor argument is a filename or a `FILE*`, otherwise as
// `TargetT` of the type of the first constructor argument.
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
  explicit CFileWriter(Initializer<Dest> dest, Options options = Options());

  // Will write to `dest`.
  template <
      typename DependentDest = Dest,
      std::enable_if_t<std::is_constructible_v<DependentDest, FILE*>, int> = 0>
  explicit CFileWriter(FILE* dest ABSL_ATTRIBUTE_LIFETIME_BOUND,
                       Options options = Options());

  // Opens a file for writing.
  //
  // If opening the file fails, `CFileWriter` will be failed and closed.
  //
  // This constructor is present only if `Dest` supports `Open()`.
  template <typename DependentDest = Dest,
            std::enable_if_t<std::conjunction_v<
                                 CFileSupportsOpen<DependentDest>,
                                 std::is_default_constructible<DependentDest>>,
                             int> = 0>
  explicit CFileWriter(PathRef filename, Options options = Options());

  CFileWriter(CFileWriter&& that) = default;
  CFileWriter& operator=(CFileWriter&& that) = default;

  // Makes `*this` equivalent to a newly constructed `CFileWriter`. This avoids
  // constructing a temporary `CFileWriter` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Dest> dest,
                                          Options options = Options());
  template <
      typename DependentDest = Dest,
      std::enable_if_t<std::is_constructible_v<DependentDest, FILE*>, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(FILE* dest,
                                          Options options = Options());
  template <
      typename DependentDest = Dest,
      std::enable_if_t<std::conjunction_v<CFileSupportsOpen<DependentDest>,
                                          SupportsReset<DependentDest>>,
                       int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(PathRef filename,
                                          Options options = Options());

  // Returns the object providing and possibly owning the `FILE` being written
  // to. Unchanged by `Close()`.
  Dest& dest() ABSL_ATTRIBUTE_LIFETIME_BOUND { return dest_.manager(); }
  const Dest& dest() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return dest_.manager();
  }
  CFileHandle DestCFileHandle() const ABSL_ATTRIBUTE_LIFETIME_BOUND override {
    return dest_.get();
  }
  FILE* DestFile() const ABSL_ATTRIBUTE_LIFETIME_BOUND override {
    return dest_.get().get();
  }

  // An optimized implementation in a derived class, avoiding a virtual call.
  absl::string_view filename() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return dest_.get().filename();
  }

 protected:
  void Done() override;
  bool FlushImpl(FlushType flush_type) override;

 private:
  template <typename DependentDest = Dest,
            std::enable_if_t<CFileSupportsOpen<DependentDest>::value, int> = 0>
  void OpenImpl(CompactString filename, Options&& options);

  // The object providing and possibly owning the `FILE` being written to.
  Dependency<CFileHandle, Dest> dest_;
};

explicit CFileWriter(Closed) -> CFileWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit CFileWriter(
    Dest&& dest, CFileWriterBase::Options options = CFileWriterBase::Options())
    -> CFileWriter<std::conditional_t<
        std::disjunction_v<std::is_convertible<Dest&&, FILE*>,
                           std::is_convertible<Dest&&, PathRef>>,
        OwnedCFile, TargetT<Dest>>>;

// Implementation details follow.

inline CFileWriterBase::CFileWriterBase(BufferOptions buffer_options)
    : BufferedWriter(buffer_options) {}

inline CFileWriterBase::CFileWriterBase(CFileWriterBase&& that) noexcept
    : BufferedWriter(static_cast<BufferedWriter&&>(that)),
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
  supports_random_access_ = LazyBoolState::kUnknown;
  supports_read_mode_ = LazyBoolState::kUnknown;
  random_access_status_ = absl::OkStatus();
  read_mode_status_ = absl::OkStatus();
#ifdef _WIN32
  original_mode_ = std::nullopt;
#endif
  associated_reader_.Reset();
  read_mode_ = false;
}

inline void CFileWriterBase::Reset(BufferOptions buffer_options) {
  BufferedWriter::Reset(buffer_options);
  supports_random_access_ = LazyBoolState::kUnknown;
  supports_read_mode_ = LazyBoolState::kUnknown;
  random_access_status_ = absl::OkStatus();
  read_mode_status_ = absl::OkStatus();
#ifdef _WIN32
  original_mode_ = std::nullopt;
#endif
  associated_reader_.Reset();
  read_mode_ = false;
}

template <typename Dest>
inline CFileWriter<Dest>::CFileWriter(Initializer<Dest> dest, Options options)
    : CFileWriterBase(options.buffer_options()), dest_(std::move(dest)) {
  Initialize(dest_.get().get(), std::move(options));
}

template <typename Dest>
template <typename DependentDest,
          std::enable_if_t<std::is_constructible_v<DependentDest, FILE*>, int>>
inline CFileWriter<Dest>::CFileWriter(FILE* dest ABSL_ATTRIBUTE_LIFETIME_BOUND,
                                      Options options)
    : CFileWriter(riegeli::Maker(dest), std::move(options)) {}

template <typename Dest>
template <typename DependentDest,
          std::enable_if_t<
              std::conjunction_v<CFileSupportsOpen<DependentDest>,
                                 std::is_default_constructible<DependentDest>>,
              int>>
inline CFileWriter<Dest>::CFileWriter(PathRef filename, Options options)
    : CFileWriterBase(options.buffer_options()), dest_(riegeli::Maker()) {
  OpenImpl(CompactString::ForCStr(filename), std::move(options));
}

template <typename Dest>
inline void CFileWriter<Dest>::Reset(Closed) {
  CFileWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void CFileWriter<Dest>::Reset(Initializer<Dest> dest, Options options) {
  CFileWriterBase::Reset(options.buffer_options());
  dest_.Reset(std::move(dest));
  Initialize(dest_.get().get(), std::move(options));
}

template <typename Dest>
template <typename DependentDest,
          std::enable_if_t<std::is_constructible_v<DependentDest, FILE*>, int>>
inline void CFileWriter<Dest>::Reset(FILE* dest, Options options) {
  Reset(riegeli::Maker(dest), std::move(options));
}

template <typename Dest>
template <typename DependentDest,
          std::enable_if_t<std::conjunction_v<CFileSupportsOpen<DependentDest>,
                                              SupportsReset<DependentDest>>,
                           int>>
inline void CFileWriter<Dest>::Reset(PathRef filename, Options options) {
  CompactString filename_copy =
      CompactString::ForCStr(filename);  // In case it gets invalidated.
  riegeli::Reset(dest_.manager());
  CFileWriterBase::Reset(options.buffer_options());
  OpenImpl(std::move(filename_copy), std::move(options));
}

template <typename Dest>
template <typename DependentDest,
          std::enable_if_t<CFileSupportsOpen<DependentDest>::value, int>>
void CFileWriter<Dest>::OpenImpl(CompactString filename, Options&& options) {
  absl::Status status =
      dest_.manager().Open(std::move(filename), options.mode());
  if (ABSL_PREDICT_FALSE(!status.ok())) {
    CFileWriterBase::Reset(kClosed);
    FailWithoutAnnotation(std::move(status));
    return;
  }
  InitializePos(dest_.get().get(), std::move(options),
                /*mode_was_passed_to_fopen=*/true);
}

template <typename Dest>
void CFileWriter<Dest>::Done() {
  CFileWriterBase::Done();
  if (dest_.IsOwning()) {
    if (absl::Status status = dest_.get().Close();
        ABSL_PREDICT_FALSE(!status.ok())) {
      Fail(std::move(status));
    }
  }
}

template <typename Dest>
bool CFileWriter<Dest>::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!CFileWriterBase::FlushImpl(flush_type))) return false;
  switch (flush_type) {
    case FlushType::kFromObject:
      if (!dest_.IsOwning() || !dest_.get().IsOwning()) return true;
      ABSL_FALLTHROUGH_INTENDED;
    case FlushType::kFromProcess:
    case FlushType::kFromMachine:
      if (ABSL_PREDICT_FALSE(fflush(dest_.get().get()) != 0)) {
        return FailOperation("fflush()");
      }
      return true;
  }
  RIEGELI_ASSUME_UNREACHABLE()
      << "Unknown flush type: " << static_cast<int>(flush_type);
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_CFILE_WRITER_H_
