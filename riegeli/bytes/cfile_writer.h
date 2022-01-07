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

#include <stddef.h>
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
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/cfile_dependency.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

template <typename Src>
class CFileReader;

// Template parameter independent part of `CFileWriter`.
class CFileWriterBase : public BufferedWriter {
 public:
  class Options {
   public:
    Options() noexcept {}

    // If `CFileWriter` writes to an already open `FILE`,
    // `set_assumed_filename()` allows to override the filename which is
    // included in failure messages and returned by `filename()`.
    //
    // If `CFileWriter` writes to a filename, `set_assumed_filename()` has no
    // effect.
    //
    // Default: ""
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

    // If `true`, writing will start at the end of file instead of at the
    // current `FILE` position.
    //
    // If `assumed_pos()` is set, `append()` has no effect. If `CFileWriter`
    // opens the file and the open mode starts with "a", `append()` is implied
    // to be `true`. If `CFileWriter` writes to an already open file and the
    // actual open mode was "a", `ftell()` reports the file size so the behavior
    // is the same in either case. But if the actual open mode was "a+",
    // `ftell()` reports 0 until the first write, so `set_append(true)` is
    // required.
    //
    // Default: `false`
    Options& set_append(bool append) & {
      append_ = append;
      return *this;
    }
    Options&& set_append(bool append) && {
      return std::move(set_append(append));
    }
    bool append() const { return append_; }

    // Tunes how much data is buffered before writing to the file.
    //
    // Default: `kDefaultBufferSize` (64K).
    Options& set_buffer_size(size_t buffer_size) & {
      RIEGELI_ASSERT_GT(buffer_size, 0u)
          << "Failed precondition of "
             "CFileWriterBase::Options::set_buffer_size(): "
             "zero buffer size";
      buffer_size_ = buffer_size;
      return *this;
    }
    Options&& set_buffer_size(size_t buffer_size) && {
      return std::move(set_buffer_size(buffer_size));
    }
    size_t buffer_size() const { return buffer_size_; }

   private:
    std::string assumed_filename_;
    absl::optional<Position> assumed_pos_;
    bool append_ = false;
    size_t buffer_size_ = kDefaultBufferSize;
  };

  // Returns the `FILE` being written to. If the `FILE` is owned then changed to
  // `nullptr` by `Close()`, otherwise unchanged.
  virtual FILE* dest_file() const = 0;

  // Returns the original name of the file being written to. Unchanged by
  // `Close()`.
  const std::string& filename() const { return filename_; }

  bool SupportsRandomAccess() override { return supports_random_access(); }
  bool SupportsTruncate() override { return false; }
  bool SupportsReadMode() override { return supports_read_mode(); }

 protected:
  explicit CFileWriterBase(Closed) noexcept : BufferedWriter(kClosed) {}

  explicit CFileWriterBase(size_t buffer_size);

  CFileWriterBase(CFileWriterBase&& that) noexcept;
  CFileWriterBase& operator=(CFileWriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset(size_t buffer_size);
  void Initialize(FILE* dest, std::string&& assumed_filename,
                  absl::optional<Position> assumed_pos, bool append);
  FILE* OpenFile(absl::string_view filename, const char* mode);
  void InitializePos(FILE* dest, absl::optional<Position> assumed_pos,
                     bool append);
  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation);
  bool supports_random_access();
  bool supports_read_mode();

  void Done() override;
  absl::Status AnnotateStatusImpl(absl::Status status) override;
  bool WriteInternal(absl::string_view src) override;
  bool FlushBehindBuffer(absl::string_view src, FlushType flush_type) override;
  bool SeekBehindBuffer(Position new_pos) override;
  absl::optional<Position> SizeBehindBuffer() override;
  Reader* ReadModeBehindBuffer(Position initial_pos) override;

 private:
  // Encodes a `bool` or a marker that the value is not fully resolved yet.
  enum class LazyBoolState { kFalse, kTrue, kUnknown };

  bool WriteMode();

  std::string filename_;
  // Invariant:
  //   if `is_open()` then `supports_random_access_ != LazyBoolState::kUnknown`
  LazyBoolState supports_random_access_ = LazyBoolState::kFalse;
  // Invariant:
  //   if `is_open()` then `supports_read_mode_ != LazyBoolState::kUnknown`
  LazyBoolState supports_read_mode_ = LazyBoolState::kFalse;

  AssociatedReader<CFileReader<UnownedCFile>> associated_reader_;
  bool read_mode_ = false;

  // Invariant: `start_pos() <= std::numeric_limits<off_t>::max()`
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
// `Dependency<FILE*, Dest>`, e.g. `OwnedCFile` (owned, default), `UnownedCFile`
// (not owned).
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

  // Will write to the `FILE` provided by a `Dest` constructed from elements of
  // `dest_args`. This avoids constructing a temporary `Dest` and moving from
  // it.
  template <typename... DestArgs>
  explicit CFileWriter(std::tuple<DestArgs...> dest_args,
                       Options options = Options());

  // Opens a file for writing.
  //
  // `mode` is the second argument of `fopen()`, typically "w" or "a".
  //
  // If opening the file fails, `CFileWriter` will be failed and closed.
  explicit CFileWriter(absl::string_view filename, const char* mode,
                       Options options = Options());

  CFileWriter(CFileWriter&& that) noexcept;
  CFileWriter& operator=(CFileWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `CFileWriter`. This avoids
  // constructing a temporary `CFileWriter` and moving from it.
  void Reset(Closed);
  void Reset(const Dest& dest, Options options = Options());
  void Reset(Dest&& dest, Options options = Options());
  template <typename... DestArgs>
  void Reset(std::tuple<DestArgs...> dest_args, Options options = Options());
  void Reset(absl::string_view filename, const char* mode,
             Options options = Options());

  // Returns the object providing and possibly owning the `FILE` being written
  // to. If the `FILE` is owned then changed to `nullptr` by `Close()`,
  // otherwise unchanged.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  FILE* dest_file() const override { return dest_.get(); }

 protected:
  using CFileWriterBase::Initialize;
  void Initialize(absl::string_view filename, const char* mode,
                  Options&& options);

  void Done() override;
  bool FlushImpl(FlushType flush_type) override;

 private:
  // The object providing and possibly owning the `FILE` being written to.
  Dependency<FILE*, Dest> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit CFileWriter(Closed)->CFileWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit CFileWriter(const Dest& dest, CFileWriterBase::Options options =
                                           CFileWriterBase::Options())
    -> CFileWriter<
        std::conditional_t<std::is_convertible<const Dest&, FILE*>::value,
                           OwnedCFile, std::decay_t<Dest>>>;
template <typename Dest>
explicit CFileWriter(
    Dest&& dest, CFileWriterBase::Options options = CFileWriterBase::Options())
    -> CFileWriter<std::conditional_t<std::is_convertible<Dest&&, FILE*>::value,
                                      OwnedCFile, std::decay_t<Dest>>>;
template <typename... DestArgs>
explicit CFileWriter(
    std::tuple<DestArgs...> dest_args,
    CFileWriterBase::Options options = CFileWriterBase::Options())
    -> CFileWriter<DeleteCtad<std::tuple<DestArgs...>>>;
explicit CFileWriter(
    absl::string_view filename, const char* mode,
    CFileWriterBase::Options options = CFileWriterBase::Options())
    ->CFileWriter<>;
#endif

// Implementation details follow.

inline CFileWriterBase::CFileWriterBase(size_t buffer_size)
    : BufferedWriter(buffer_size) {}

inline CFileWriterBase::CFileWriterBase(CFileWriterBase&& that) noexcept
    : BufferedWriter(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      filename_(std::move(that.filename_)),
      supports_random_access_(that.supports_random_access_),
      supports_read_mode_(that.supports_read_mode_),
      associated_reader_(std::move(that.associated_reader_)),
      read_mode_(that.read_mode_) {}

inline CFileWriterBase& CFileWriterBase::operator=(
    CFileWriterBase&& that) noexcept {
  BufferedWriter::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  filename_ = std::move(that.filename_);
  supports_random_access_ = that.supports_random_access_;
  supports_read_mode_ = that.supports_read_mode_;
  associated_reader_ = std::move(that.associated_reader_);
  read_mode_ = that.read_mode_;
  return *this;
}

inline void CFileWriterBase::Reset(Closed) {
  BufferedWriter::Reset(kClosed);
  filename_ = std::string();
  supports_random_access_ = LazyBoolState::kFalse;
  supports_read_mode_ = LazyBoolState::kFalse;
  associated_reader_.Reset();
  read_mode_ = false;
}

inline void CFileWriterBase::Reset(size_t buffer_size) {
  BufferedWriter::Reset(buffer_size);
  // `filename_` was set by `OpenFile()` or will be set by `Initialize()`.
  supports_random_access_ = LazyBoolState::kFalse;
  supports_read_mode_ = LazyBoolState::kFalse;
  associated_reader_.Reset();
  read_mode_ = false;
}

template <typename Dest>
inline CFileWriter<Dest>::CFileWriter(const Dest& dest, Options options)
    : CFileWriterBase(options.buffer_size()), dest_(dest) {
  Initialize(dest_.get(), std::move(options.assumed_filename()),
             options.assumed_pos(), options.append());
}

template <typename Dest>
inline CFileWriter<Dest>::CFileWriter(Dest&& dest, Options options)
    : CFileWriterBase(options.buffer_size()), dest_(std::move(dest)) {
  Initialize(dest_.get(), std::move(options.assumed_filename()),
             options.assumed_pos(), options.append());
}

template <typename Dest>
template <typename... DestArgs>
inline CFileWriter<Dest>::CFileWriter(std::tuple<DestArgs...> dest_args,
                                      Options options)
    : CFileWriterBase(options.buffer_size()), dest_(std::move(dest_args)) {
  Initialize(dest_.get(), std::move(options.assumed_filename()),
             options.assumed_pos(), options.append());
}

template <typename Dest>
inline CFileWriter<Dest>::CFileWriter(absl::string_view filename,
                                      const char* mode, Options options)
    : CFileWriterBase(kClosed) {
  Initialize(filename, mode, std::move(options));
}

template <typename Dest>
inline CFileWriter<Dest>::CFileWriter(CFileWriter&& that) noexcept
    : CFileWriterBase(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      dest_(std::move(that.dest_)) {}

template <typename Dest>
inline CFileWriter<Dest>& CFileWriter<Dest>::operator=(
    CFileWriter&& that) noexcept {
  CFileWriterBase::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
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
  CFileWriterBase::Reset(options.buffer_size());
  dest_.Reset(dest);
  Initialize(dest_.get(), std::move(options.assumed_filename()),
             options.assumed_pos(), options.append());
}

template <typename Dest>
inline void CFileWriter<Dest>::Reset(Dest&& dest, Options options) {
  CFileWriterBase::Reset(options.buffer_size());
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), std::move(options.assumed_filename()),
             options.assumed_pos(), options.append());
}

template <typename Dest>
template <typename... DestArgs>
inline void CFileWriter<Dest>::Reset(std::tuple<DestArgs...> dest_args,
                                     Options options) {
  CFileWriterBase::Reset(options.buffer_size());
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get(), std::move(options.assumed_filename()),
             options.assumed_pos(), options.append());
}

template <typename Dest>
inline void CFileWriter<Dest>::Reset(absl::string_view filename,
                                     const char* mode, Options options) {
  Reset(kClosed);
  Initialize(filename, mode, std::move(options));
}

template <typename Dest>
void CFileWriter<Dest>::Initialize(absl::string_view filename, const char* mode,
                                   Options&& options) {
  FILE* const dest = OpenFile(filename, mode);
  if (ABSL_PREDICT_FALSE(dest == nullptr)) return;
  CFileWriterBase::Reset(options.buffer_size());
  dest_.Reset(std::forward_as_tuple(dest));
  InitializePos(dest_.get(), options.assumed_pos(),
                options.append() || mode[0] == 'a');
}

template <typename Dest>
void CFileWriter<Dest>::Done() {
  CFileWriterBase::Done();
  {
    FILE* const dest = dest_.Release();
    if (dest != nullptr) {
      if (ABSL_PREDICT_FALSE((fclose(dest)) != 0) &&
          ABSL_PREDICT_TRUE(healthy())) {
        FailOperation("fclose()");
      }
    } else {
      RIEGELI_ASSERT(!dest_.is_owning())
          << "The dependency type does not support closing the FILE";
    }
  }
}

template <typename Dest>
bool CFileWriter<Dest>::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!CFileWriterBase::FlushImpl(flush_type))) {
    return false;
  }
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
