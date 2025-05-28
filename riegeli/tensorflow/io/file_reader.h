// Copyright 2019 Google LLC
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

#ifndef RIEGELI_TENSORFLOW_IO_FILE_READER_H_
#define RIEGELI_TENSORFLOW_IO_FILE_READER_H_

#include <stddef.h>

#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/object.h"
#include "riegeli/base/sized_shared_buffer.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/path_ref.h"
#include "riegeli/bytes/reader.h"
#include "tensorflow/core/platform/env.h"
#include "tensorflow/core/platform/file_system.h"

namespace riegeli {

class BackwardWriter;
class Writer;

namespace tensorflow {

// Template parameter independent part of `FileReader`.
class FileReaderBase : public Reader {
 public:
  class Options : public BufferOptionsBase<Options> {
   public:
    Options() noexcept {}

    // `FileReader` has a larger `kDefaultMaxBufferSize` (1M) because remote
    // file access may have high latency of each operation.
    static constexpr size_t kDefaultMaxBufferSize = size_t{1} << 20;

    // Overrides the TensorFlow environment.
    //
    // `nullptr` is interpreted as `::tensorflow::Env::Default()`.
    //
    // Default: `nullptr`.
    Options& set_env(::tensorflow::Env* env) & ABSL_ATTRIBUTE_LIFETIME_BOUND {
      env_ = env;
      return *this;
    }
    Options&& set_env(::tensorflow::Env* env) && ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_env(env));
    }
    ::tensorflow::Env* env() const { return env_; }

    // Reading will start from this position.
    //
    // Default: 0.
    Options& set_initial_pos(Position initial_pos) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      initial_pos_ = initial_pos;
      return *this;
    }
    Options&& set_initial_pos(Position initial_pos) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_initial_pos(initial_pos));
    }
    Position initial_pos() const { return initial_pos_; }

    // If `true`, supports reading up to the end of the file, then retrying when
    // the file has grown. This disables caching the file size.
    //
    // Default: `false`.
    Options& set_growing_source(bool growing_source) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      growing_source_ = growing_source;
      return *this;
    }
    Options&& set_growing_source(bool growing_source) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_growing_source(growing_source));
    }
    bool growing_source() const { return growing_source_; }

   private:
    ::tensorflow::Env* env_ = nullptr;
    Position initial_pos_ = 0;
    bool growing_source_ = false;
  };

  // Returns the `::tensorflow::RandomAccessFile` being read from. If the
  // `::tensorflow::RandomAccessFile` is owned then changed to `nullptr` by
  // `Close()`, otherwise unchanged.
  virtual ::tensorflow::RandomAccessFile* SrcFile() const
      ABSL_ATTRIBUTE_LIFETIME_BOUND = 0;

  // Returns the name of the `::tensorflow::RandomAccessFile` being read from.
  // Unchanged by `Close()`.
  absl::string_view filename() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return filename_;
  }

  bool ToleratesReadingAhead() override {
    return buffer_sizer_.read_all_hint() ||
           FileReaderBase::SupportsRandomAccess();
  }
  bool SupportsRandomAccess() override { return file_system_ != nullptr; }
  bool SupportsNewReader() override {
    return FileReaderBase::SupportsRandomAccess();
  }

 protected:
  explicit FileReaderBase(Closed) noexcept : Reader(kClosed) {}

  explicit FileReaderBase(BufferOptions buffer_options, ::tensorflow::Env* env,
                          bool growing_source);

  FileReaderBase(FileReaderBase&& that) noexcept;
  FileReaderBase& operator=(FileReaderBase&& that) noexcept;

  void Reset(Closed);
  void Reset(BufferOptions buffer_options, ::tensorflow::Env* env,
             bool growing_source);
  void Initialize(::tensorflow::RandomAccessFile* src, Position initial_pos);
  bool InitializeFilename(::tensorflow::RandomAccessFile* src);
  bool InitializeFilename(Initializer<std::string>::AllowingExplicit filename);
  std::unique_ptr<::tensorflow::RandomAccessFile> OpenFile();
  void InitializePos(Position initial_pos);

  void Done() override;
  absl::Status AnnotateStatusImpl(absl::Status status) override;
  void SetReadAllHintImpl(bool read_all_hint) override;
  bool PullSlow(size_t min_length, size_t recommended_length) override;
  using Reader::ReadSlow;
  bool ReadSlow(size_t length, char* dest) override;
  bool ReadSlow(size_t length, Chain& dest) override;
  bool ReadSlow(size_t length, absl::Cord& dest) override;
  using Reader::CopySlow;
  bool CopySlow(Position length, Writer& dest) override;
  bool CopySlow(size_t length, BackwardWriter& dest) override;
  using Reader::ReadOrPullSomeSlow;
  bool ReadOrPullSomeSlow(size_t max_length,
                          absl::FunctionRef<char*(size_t&)> get_dest) override;
  bool SyncImpl(SyncType sync_type) override;
  bool SeekSlow(Position new_pos) override;
  std::optional<Position> SizeImpl() override;
  std::unique_ptr<Reader> NewReaderImpl(Position initial_pos) override;

 private:
  ABSL_ATTRIBUTE_COLD bool FailOperation(const absl::Status& status,
                                         absl::string_view operation);
  ABSL_ATTRIBUTE_COLD static absl::Status NoRandomAccessStatus();

  void set_exact_size(std::optional<Position> exact_size) {
    buffer_sizer_.set_exact_size(exact_size);
  }
  std::optional<Position> exact_size() const {
    return buffer_sizer_.exact_size();
  }

  // Discards buffer contents.
  void SyncBuffer();

  // Clears `buffer_`. Reads `length` bytes from `*src`, from the physical file
  // position which is `limit_pos()`, to `dest[]`.
  //
  // Increments `limit_pos()` by the length read. Returns `true` on success.
  bool ReadToDest(size_t length, ::tensorflow::RandomAccessFile* src,
                  char* dest);

  // Reads `flat_buffer.size()` bytes from `*src`, from the physical file
  // position which is `limit_pos()`, preferably to `flat_buffer.data()`. Newly
  // read data are adjacent to previously available data in `buffer_`, if any.
  // `cursor_index` is the amount of already read data before previously
  // available data.
  //
  // Increments `limit_pos()` by the length read. Sets buffer pointers. Returns
  // `true` on success.
  //
  // Precondition: `flat_buffer` is a suffix of `buffer_`
  bool ReadToBuffer(size_t cursor_index, ::tensorflow::RandomAccessFile* src,
                    absl::Span<char> flat_buffer);

  // Implementation of `CopySlow(Writer&)` in terms of `Writer::Push()` and
  // `ReadToDest()`. Does not use buffer pointers.
  //
  // Precondition: `length > 0`
  bool CopyUsingPush(Position length, ::tensorflow::RandomAccessFile* src,
                     Writer& dest);

  std::string filename_{kDefaultFilename};
  // Invariant: if `is_open()` then `env_ != nullptr`
  ::tensorflow::Env* env_ = nullptr;
  ::tensorflow::FileSystem* file_system_ = nullptr;
  bool growing_source_ = false;
  ReadBufferSizer buffer_sizer_;
  // If `buffer_` is not empty, it contains buffered data, read directly before
  // the physical source position which is `limit_pos()`. Otherwise buffered
  // data are in memory managed by the `::tensorflow::RandomAccessFile`. In any
  // case `start()` points to them.
  SizedSharedBuffer buffer_;

  // Invariants if `!buffer_.empty()`:
  //   `start() == buffer_.data()`
  //   `start_to_limit() == buffer_.size()`
};

// A `Reader` which reads from a `::tensorflow::RandomAccessFile`.
//
// It supports random access and `NewReader()` if the
// `::tensorflow::RandomAccessFile` supports
// `::tensorflow::RandomAccessFile::Name()` and the name is not empty.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the `::tensorflow::RandomAccessFile` being read from. `Src`
// must support `Dependency<::tensorflow::RandomAccessFile*, Src>`, e.g.
// `std::unique_ptr<::tensorflow::RandomAccessFile>` (owned, default),
// `::tensorflow::RandomAccessFile*` (not owned),
// `Any<::tensorflow::RandomAccessFile*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as `TargetT` of the
// type of the first constructor argument.
//
// The `::tensorflow::RandomAccessFile` must not be closed until the
// `FileReader` is closed or no longer used.
template <typename Src = std::unique_ptr<::tensorflow::RandomAccessFile>>
class FileReader : public FileReaderBase {
 public:
  // Creates a closed `FileReader`.
  explicit FileReader(Closed) noexcept : FileReaderBase(kClosed) {}

  // Will read from the `::tensorflow::RandomAccessFile` provided by `src`.
  explicit FileReader(Initializer<Src> src, Options options = Options());

  // Opens a `::tensorflow::RandomAccessFile` for reading.
  //
  // If opening the file fails, `FileReader` will be failed and closed.
  explicit FileReader(Initializer<std::string>::AllowingExplicit filename,
                      Options options = Options());

  FileReader(FileReader&& that) = default;
  FileReader& operator=(FileReader&& that) = default;

  // Makes `*this` equivalent to a newly constructed `FileReader`. This avoids
  // constructing a temporary `FileReader` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Src> src,
                                          Options options = Options());
  ABSL_ATTRIBUTE_REINITIALIZES
  void Reset(Initializer<std::string>::AllowingExplicit filename,
             Options options = Options());

  // Returns the object providing and possibly owning the
  // `::tensorflow::RandomAccessFile` being read from. If the
  // `::tensorflow::RandomAccessFile` is owned then changed to `nullptr` by
  // `Close()`, otherwise unchanged.
  Src& src() ABSL_ATTRIBUTE_LIFETIME_BOUND { return src_.manager(); }
  const Src& src() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return src_.manager();
  }
  ::tensorflow::RandomAccessFile* SrcFile() const
      ABSL_ATTRIBUTE_LIFETIME_BOUND override {
    return src_.get();
  }

 protected:
  void Done() override;

 private:
  using FileReaderBase::Initialize;
  void Initialize(Initializer<std::string>::AllowingExplicit filename,
                  Options&& options);

  // The object providing and possibly owning the
  // `::tensorflow::RandomAccessFile` being read from.
  Dependency<::tensorflow::RandomAccessFile*, Src> src_;
};

explicit FileReader(Closed) -> FileReader<DeleteCtad<Closed>>;
template <typename Src>
explicit FileReader(Src&& src,
                    FileReaderBase::Options options = FileReaderBase::Options())
    -> FileReader<std::conditional_t<
        std::is_convertible_v<Src&&,
                              Initializer<std::string>::AllowingExplicit>,
        std::unique_ptr<::tensorflow::RandomAccessFile>, TargetT<Src>>>;

// Implementation details follow.

inline FileReaderBase::FileReaderBase(BufferOptions buffer_options,
                                      ::tensorflow::Env* env,
                                      bool growing_source)
    : env_(env != nullptr ? env : ::tensorflow::Env::Default()),
      growing_source_(growing_source),
      buffer_sizer_(buffer_options) {}

inline FileReaderBase::FileReaderBase(FileReaderBase&& that) noexcept
    : Reader(static_cast<Reader&&>(that)),
      filename_(std::exchange(that.filename_, std::string(kDefaultFilename))),
      env_(that.env_),
      file_system_(that.file_system_),
      growing_source_(that.growing_source_),
      buffer_sizer_(that.buffer_sizer_),
      buffer_(std::move(that.buffer_)) {}

inline FileReaderBase& FileReaderBase::operator=(
    FileReaderBase&& that) noexcept {
  Reader::operator=(static_cast<Reader&&>(that));
  filename_ = std::exchange(that.filename_, std::string(kDefaultFilename));
  env_ = that.env_;
  file_system_ = that.file_system_;
  growing_source_ = that.growing_source_;
  buffer_sizer_ = that.buffer_sizer_;
  buffer_ = std::move(that.buffer_);
  return *this;
}

inline void FileReaderBase::Reset(Closed) {
  Reader::Reset(kClosed);
  filename_ = std::string(kDefaultFilename);
  env_ = nullptr;
  file_system_ = nullptr;
  growing_source_ = false;
  buffer_sizer_.Reset();
  buffer_ = SizedSharedBuffer();
}

inline void FileReaderBase::Reset(BufferOptions buffer_options,
                                  ::tensorflow::Env* env, bool growing_source) {
  Reader::Reset();
  // `filename_` will be set by `InitializeFilename()`.
  env_ = env != nullptr ? env : ::tensorflow::Env::Default();
  file_system_ = nullptr;
  growing_source_ = growing_source;
  buffer_sizer_.Reset(buffer_options);
  buffer_.Clear();
}

inline void FileReaderBase::Initialize(::tensorflow::RandomAccessFile* src,
                                       Position initial_pos) {
  RIEGELI_ASSERT_NE(src, nullptr)
      << "Failed precondition of FileReader: null RandomAccessFile pointer";
  if (ABSL_PREDICT_FALSE(!InitializeFilename(src))) return;
  InitializePos(initial_pos);
}

template <typename Src>
inline FileReader<Src>::FileReader(Initializer<Src> src, Options options)
    : FileReaderBase(options.buffer_options(), options.env(),
                     options.growing_source()),
      src_(std::move(src)) {
  Initialize(src_.get(), options.initial_pos());
}

template <typename Src>
inline FileReader<Src>::FileReader(
    Initializer<std::string>::AllowingExplicit filename, Options options)
    : FileReaderBase(options.buffer_options(), options.env(),
                     options.growing_source()) {
  Initialize(std::move(filename), std::move(options));
}

template <typename Src>
inline void FileReader<Src>::Reset(Closed) {
  FileReaderBase::Reset(kClosed);
  src_.Reset();
}

template <typename Src>
inline void FileReader<Src>::Reset(Initializer<Src> src, Options options) {
  FileReaderBase::Reset(options.buffer_options(), options.env(),
                        options.growing_source());
  src_.Reset(std::move(src));
  Initialize(src_.get(), options.initial_pos());
}

template <typename Src>
inline void FileReader<Src>::Reset(
    Initializer<std::string>::AllowingExplicit filename, Options options) {
  FileReaderBase::Reset(options.buffer_options(), options.env(),
                        options.growing_source());
  Initialize(std::move(filename), std::move(options));
}

template <typename Src>
inline void FileReader<Src>::Initialize(
    Initializer<std::string>::AllowingExplicit filename, Options&& options) {
  if (ABSL_PREDICT_FALSE(!InitializeFilename(std::move(filename)))) return;
  std::unique_ptr<::tensorflow::RandomAccessFile> src = OpenFile();
  if (ABSL_PREDICT_FALSE(src == nullptr)) return;
  src_.Reset(riegeli::Maker(src.release()));
  InitializePos(options.initial_pos());
}

template <typename Src>
void FileReader<Src>::Done() {
  FileReaderBase::Done();
  if (src_.IsOwning()) {
    // The only way to close a `::tensorflow::RandomAccessFile` is to delete it.
    src_.Reset();
  }
}

}  // namespace tensorflow

}  // namespace riegeli

#endif  // RIEGELI_TENSORFLOW_IO_FILE_READER_H_
