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
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"
#include "tensorflow/core/platform/env.h"
#include "tensorflow/core/platform/file_system.h"
#include "tensorflow/core/platform/status.h"

namespace riegeli {
namespace tensorflow {

// Template parameter independent part of `FileReader`.
class FileReaderBase : public Reader {
 public:
  class Options {
   public:
    Options() noexcept {}

    // Overrides the TensorFlow environment.
    //
    // `nullptr` is interpreted as `::tensorflow::Env::Default()`.
    //
    // Default: `nullptr`.
    Options& set_env(::tensorflow::Env* env) & {
      env_ = env;
      return *this;
    }
    Options&& set_env(::tensorflow::Env* env) && {
      return std::move(set_env(env));
    }
    ::tensorflow::Env* env() const { return env_; }

    // Reading will start from this position.
    //
    // Default: 0.
    Options& set_initial_pos(Position initial_pos) & {
      initial_pos_ = initial_pos;
      return *this;
    }
    Options&& set_initial_pos(Position initial_pos) && {
      return std::move(set_initial_pos(initial_pos));
    }
    Position initial_pos() const { return initial_pos_; }

    // Tunes how much data is buffered after reading from the file.
    //
    // Default: `kDefaultBufferSize` (64K).
    Options& set_buffer_size(size_t buffer_size) & {
      RIEGELI_ASSERT_GT(buffer_size, 0u)
          << "Failed precondition of "
             "FileReaderBase::Options::set_buffer_size(): "
             "zero buffer size";
      buffer_size_ = buffer_size;
      return *this;
    }
    Options&& set_buffer_size(size_t buffer_size) && {
      return std::move(set_buffer_size(buffer_size));
    }
    size_t buffer_size() const { return buffer_size_; }

   private:
    ::tensorflow::Env* env_ = nullptr;
    Position initial_pos_ = 0;
    size_t buffer_size_ = kDefaultBufferSize;
  };

  // Returns the `::tensorflow::RandomAccessFile` being read from. If the
  // `::tensorflow::RandomAccessFile` is owned then changed to `nullptr` by
  // `Close()`, otherwise unchanged.
  virtual ::tensorflow::RandomAccessFile* src_file() const = 0;

  // Returns the name of the `::tensorflow::RandomAccessFile` being read from.
  // Unchanged by `Close()`.
  const std::string& filename() const { return filename_; }

  using Reader::Fail;
  bool Fail(absl::Status status) override;
  bool SupportsRandomAccess() const override { return !filename_.empty(); }
  bool SupportsSize() const override { return !filename_.empty(); }
  absl::optional<Position> Size() override;

 protected:
  FileReaderBase() noexcept : Reader(kInitiallyClosed) {}

  explicit FileReaderBase(size_t buffer_size);

  FileReaderBase(FileReaderBase&& that) noexcept;
  FileReaderBase& operator=(FileReaderBase&& that) noexcept;

  void Reset();
  void Reset(size_t buffer_size);
  void Initialize(::tensorflow::RandomAccessFile* src, ::tensorflow::Env* env,
                  Position initial_pos);
  bool InitializeFilename(::tensorflow::RandomAccessFile* src,
                          ::tensorflow::Env* env);
  bool InitializeFilename(absl::string_view filename, ::tensorflow::Env* env);
  std::unique_ptr<::tensorflow::RandomAccessFile> OpenFile();
  void InitializePos(Position initial_pos);
  ABSL_ATTRIBUTE_COLD bool FailOperation(const ::tensorflow::Status& status,
                                         absl::string_view operation);

  bool PullSlow(size_t min_length, size_t recommended_length) override;
  using Reader::ReadSlow;
  bool ReadSlow(size_t length, char* dest) override;
  bool ReadSlow(size_t length, Chain& dest) override;
  bool ReadSlow(size_t length, absl::Cord& dest) override;
  bool SeekSlow(Position new_pos) override;
  using Reader::CopyToSlow;
  bool CopyToSlow(Position length, Writer& dest) override;
  bool CopyToSlow(size_t length, BackwardWriter& dest) override;

 private:
  // Minimum length for which it is better to append current contents of
  // `buffer_` and read the remaining data directly than to read the data
  // through `buffer_`.
  size_t LengthToReadDirectly() const;

  // Clears `buffer_`. Reads `length` bytes from `*src`, from the physical file
  // position which is `limit_pos()`, to `dest[]`.
  //
  // Sets `length_read` to the length read.
  //
  // Increments `limit_pos()` by the length read.
  bool ReadToDest(size_t length, ::tensorflow::RandomAccessFile* src,
                  char* dest, size_t& length_read);

  // Reads `flat_buffer.size()` bytes from `*src`, from the physical file
  // position which is `limit_pos()`, preferably to `flat_buffer.data()`. Newly
  // read data are adjacent to previously available data in `buffer_`, if any.
  // `cursor_index` is the amount of already read data before previously
  // available data.
  //
  // Increments `limit_pos()` by the length read. Sets buffer pointers.
  //
  // Precondition: `flat_buffer` is a suffix of `buffer_`
  bool ReadToBuffer(size_t cursor_index, ::tensorflow::RandomAccessFile* src,
                    absl::Span<char> flat_buffer);

  // Discards buffer contents.
  void ClearBuffer();

  std::string filename_;
  // Invariant:
  //   if `healthy() && !filename_.empty()` then `file_system_ != nullptr`
  ::tensorflow::FileSystem* file_system_ = nullptr;
  // Invariant: if `is_open()` then `buffer_size_ > 0`
  size_t buffer_size_ = 0;
  // If `buffer_` is not empty, it contains buffered data, read directly before
  // the physical source position which is `limit_pos()`. Otherwise buffered
  // data are in memory managed by the `::tensorflow::RandomAccessFile`. In any
  // case `start()` points to them.
  ChainBlock buffer_;

  // Invariants if `!buffer_.empty()`:
  //   `start() == buffer_.data()`
  //   `buffer_size() == buffer_.size()`
};

// A `Reader` which reads from a `::tensorflow::RandomAccessFile`. It supports
// random access if `::tensorflow::RandomAccessFile::Name()` is supported.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the `::tensorflow::RandomAccessFile` being read from. `Src`
// must support `Dependency<::tensorflow::RandomAccessFile*, Src>`, e.g.
// `std::unique_ptr<::tensorflow::RandomAccessFile>` (owned, default),
// `::tensorflow::RandomAccessFile*` (not owned).
//
// By relying on CTAD the template argument can be deduced as the value type of
// the first constructor argument. This requires C++17.
//
// The `::tensorflow::RandomAccessFile` must not be closed until the
// `FileReader` is closed or no longer used.
template <typename Src = std::unique_ptr<::tensorflow::RandomAccessFile>>
class FileReader : public FileReaderBase {
 public:
  // Creates a closed `FileReader`.
  FileReader() noexcept {}

  // Will read from the `::tensorflow::RandomAccessFile` provided by `src`.
  explicit FileReader(const Src& src, Options options = Options());
  explicit FileReader(Src&& src, Options options = Options());

  // Will read from the `::tensorflow::RandomAccessFile` provided by a `Src`
  // constructed from elements of `src_args`. This avoids constructing a
  // temporary `Src` and moving from it.
  template <typename... SrcArgs>
  explicit FileReader(std::tuple<SrcArgs...> src_args,
                      Options options = Options());

  // Opens a `::tensorflow::RandomAccessFile` for reading.
  explicit FileReader(absl::string_view filename, Options options = Options());

  FileReader(FileReader&& that) noexcept;
  FileReader& operator=(FileReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `FileReader`. This avoids
  // constructing a temporary `FileReader` and moving from it.
  void Reset();
  void Reset(const Src& src, Options options = Options());
  void Reset(Src&& src, Options options = Options());
  template <typename... SrcArgs>
  void Reset(std::tuple<SrcArgs...> src_args, Options options = Options());
  void Reset(absl::string_view filename, Options options = Options());

  // Returns the object providing and possibly owning the
  // `::tensorflow::RandomAccessFile` being read from. If the
  // `::tensorflow::RandomAccessFile` is owned then changed to `nullptr` by
  // `Close()`, otherwise unchanged.
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  ::tensorflow::RandomAccessFile* src_file() const override {
    return src_.get();
  }

 protected:
  void Done() override;

 private:
  using FileReaderBase::Initialize;
  void Initialize(absl::string_view filename, ::tensorflow::Env* env,
                  Position initial_pos);

  // The object providing and possibly owning the
  // `::tensorflow::RandomAccessFile` being read from.
  Dependency<::tensorflow::RandomAccessFile*, Src> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
FileReader()->FileReader<DeleteCtad<>>;
template <typename Src>
explicit FileReader(const Src& src,
                    FileReaderBase::Options options = FileReaderBase::Options())
    -> FileReader<std::decay_t<Src>>;
template <typename Src>
explicit FileReader(Src&& src,
                    FileReaderBase::Options options = FileReaderBase::Options())
    -> FileReader<std::decay_t<Src>>;
template <typename... SrcArgs>
explicit FileReader(std::tuple<SrcArgs...> src_args,
                    FileReaderBase::Options options = FileReaderBase::Options())
    -> FileReader<DeleteCtad<std::tuple<SrcArgs...>>>;
explicit FileReader(absl::string_view filename,
                    FileReaderBase::Options options = FileReaderBase::Options())
    ->FileReader<>;
#endif

// Implementation details follow.

inline FileReaderBase::FileReaderBase(size_t buffer_size)
    : Reader(kInitiallyOpen), buffer_size_(buffer_size) {}

inline FileReaderBase::FileReaderBase(FileReaderBase&& that) noexcept
    : Reader(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      filename_(std::move(that.filename_)),
      file_system_(that.file_system_),
      buffer_size_(that.buffer_size_),
      buffer_(std::move(that.buffer_)) {}

inline FileReaderBase& FileReaderBase::operator=(
    FileReaderBase&& that) noexcept {
  Reader::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  filename_ = std::move(that.filename_);
  file_system_ = that.file_system_;
  buffer_size_ = that.buffer_size_;
  buffer_ = std::move(that.buffer_);
  return *this;
}

inline void FileReaderBase::Reset() {
  Reader::Reset(kInitiallyClosed);
  filename_.clear();
  file_system_ = nullptr;
  buffer_size_ = 0;
  buffer_.Clear();
}

inline void FileReaderBase::Reset(size_t buffer_size) {
  Reader::Reset(kInitiallyOpen);
  filename_.clear();
  file_system_ = nullptr;
  buffer_size_ = buffer_size;
  buffer_.Clear();
}

inline void FileReaderBase::Initialize(::tensorflow::RandomAccessFile* src,
                                       ::tensorflow::Env* env,
                                       Position initial_pos) {
  RIEGELI_ASSERT(src != nullptr)
      << "Failed precondition of FileReader: null RandomAccessFile pointer";
  if (ABSL_PREDICT_FALSE(!InitializeFilename(src, env))) return;
  InitializePos(initial_pos);
}

template <typename Src>
inline FileReader<Src>::FileReader(const Src& src, Options options)
    : FileReaderBase(options.buffer_size()), src_(src) {
  Initialize(src_.get(), options.env(), options.initial_pos());
}

template <typename Src>
inline FileReader<Src>::FileReader(Src&& src, Options options)
    : FileReaderBase(options.buffer_size()), src_(std::move(src)) {
  Initialize(src_.get(), options.env(), options.initial_pos());
}

template <typename Src>
template <typename... SrcArgs>
inline FileReader<Src>::FileReader(std::tuple<SrcArgs...> src_args,
                                   Options options)
    : FileReaderBase(options.buffer_size()), src_(std::move(src_args)) {
  Initialize(src_.get(), options.env(), options.initial_pos());
}

template <typename Src>
inline FileReader<Src>::FileReader(absl::string_view filename, Options options)
    : FileReaderBase(options.buffer_size()) {
  Initialize(filename, options.env(), options.initial_pos());
}

template <typename Src>
inline void FileReader<Src>::Reset() {
  FileReaderBase::Reset();
  src_.Reset();
}

template <typename Src>
inline void FileReader<Src>::Reset(const Src& src, Options options) {
  FileReaderBase::Reset(options.buffer_size());
  src_.Reset(src);
  Initialize(src_.get(), options.env(), options.initial_pos());
}

template <typename Src>
inline void FileReader<Src>::Reset(Src&& src, Options options) {
  FileReaderBase::Reset(options.buffer_size());
  src_.Reset(std::move(src));
  Initialize(src_.get(), options.env(), options.initial_pos());
}

template <typename Src>
template <typename... SrcArgs>
inline void FileReader<Src>::Reset(std::tuple<SrcArgs...> src_args,
                                   Options options) {
  FileReaderBase::Reset(options.buffer_size());
  src_.Reset(std::move(src_args));
  Initialize(src_.get(), options.env(), options.initial_pos());
}

template <typename Src>
inline void FileReader<Src>::Reset(absl::string_view filename,
                                   Options options) {
  FileReaderBase::Reset(options.buffer_size());
  src_.Reset();  // In case `OpenFile()` fails.
  Initialize(filename, options.env(), options.initial_pos());
}

template <typename Src>
inline void FileReader<Src>::Initialize(absl::string_view filename,
                                        ::tensorflow::Env* env,
                                        Position initial_pos) {
  if (ABSL_PREDICT_FALSE(!InitializeFilename(filename, env))) return;
  std::unique_ptr<::tensorflow::RandomAccessFile> src = OpenFile();
  if (ABSL_PREDICT_FALSE(src == nullptr)) return;
  src_.Reset(std::forward_as_tuple(src.release()));
  InitializePos(initial_pos);
}

template <typename Src>
inline FileReader<Src>::FileReader(FileReader&& that) noexcept
    : FileReaderBase(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      src_(std::move(that.src_)) {}

template <typename Src>
inline FileReader<Src>& FileReader<Src>::operator=(FileReader&& that) noexcept {
  FileReaderBase::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  src_ = std::move(that.src_);
  return *this;
}

template <typename Src>
void FileReader<Src>::Done() {
  FileReaderBase::Done();
  if (src_.is_owning()) {
    // The only way to close a `::tensorflow::RandomAccessFile` is to delete it.
    src_.Reset();
  }
}

}  // namespace tensorflow
}  // namespace riegeli

#endif  // RIEGELI_TENSORFLOW_IO_FILE_READER_H_
