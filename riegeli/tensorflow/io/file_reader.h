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
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/resetter.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"
#include "tensorflow/core/lib/core/status.h"
#include "tensorflow/core/platform/env.h"
#include "tensorflow/core/platform/file_system.h"

namespace riegeli {

namespace tensorflow {

// Template parameter invariant part of `FileReader`.
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

    // Tunes how much data is buffered after reading from the file.
    //
    // Default: 64K
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

   private:
    template <typename Src>
    friend class FileReader;

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

  bool SupportsRandomAccess() const override { return !filename_.empty(); }
  bool Size(Position* size) override;

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
  bool ReadSlow(char* dest, size_t length) override;
  bool ReadSlow(Chain* dest, size_t length) override;
  bool SeekSlow(Position new_pos) override;
  using Reader::CopyToSlow;
  bool CopyToSlow(Writer* dest, Position length) override;
  bool CopyToSlow(BackwardWriter* dest, size_t length) override;

 private:
  // Preferred size of the buffer to use.
  size_t BufferLength(size_t min_length = 0) const;

  // Minimum length for which it is better to append current contents of
  // `buffer_` and read the remaining data directly than to read the data
  // through `buffer_`.
  size_t LengthToReadDirectly() const;

  // Clears `buffer_`. Reads `length` bytes from `*src`, from the physical file
  // position which is `limit_pos_`, to `*dest`.
  //
  // Sets `*length_read` to the length read.
  //
  // Increments `limit_pos_` by the length read.
  bool ReadToDest(char* dest, size_t length,
                  ::tensorflow::RandomAccessFile* src, size_t* length_read);

  // Reads `flat_buffer.size()` bytes from `*src`, from the physical file
  // position which is `limit_pos_`, preferably to `flat_buffer.data()`. Newly
  // read data are adjacent to previously available data, if any.
  //
  // Increments `limit_` and `limit_pos_` by the length read.
  //
  // Preconditions:
  //   `start_ == buffer_.data()`
  //   `limit_ == flat_buffer.data()`
  bool ReadToBuffer(absl::Span<char> flat_buffer,
                    ::tensorflow::RandomAccessFile* src);

  // Discards buffer contents.
  void ClearBuffer();

  std::string filename_;
  // Invariant:
  //   if `healthy() && !filename_.empty()` then `file_system_ != nullptr`
  ::tensorflow::FileSystem* file_system_ = nullptr;
  // Invariant: if `healthy()` then `buffer_size_ > 0`
  size_t buffer_size_ = 0;
  // If `buffer_` is not empty, it contains buffered data, read directly before
  // the physical source position which is `limit_pos_`. Otherwise buffered data
  // are in memory managed by the `::tensorflow::RandomAccessFile`. In any case
  // `start_` points to them.
  ChainBlock buffer_;

  // Invariants if `!buffer_.empty()`:
  //   `start_ == buffer_.data()`
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

// Implementation details follow.

inline FileReaderBase::FileReaderBase(size_t buffer_size)
    : Reader(kInitiallyOpen), buffer_size_(buffer_size) {}

inline FileReaderBase::FileReaderBase(FileReaderBase&& that) noexcept
    : Reader(std::move(that)),
      filename_(std::move(that.filename_)),
      file_system_(that.file_system_),
      buffer_size_(that.buffer_size_),
      buffer_(std::move(that.buffer_)) {}

inline FileReaderBase& FileReaderBase::operator=(
    FileReaderBase&& that) noexcept {
  Reader::operator=(std::move(that));
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
    : FileReaderBase(options.buffer_size_), src_(src) {
  Initialize(src_.get(), options.env_, options.initial_pos_);
}

template <typename Src>
inline FileReader<Src>::FileReader(Src&& src, Options options)
    : FileReaderBase(options.buffer_size_), src_(std::move(src)) {
  Initialize(src_.get(), options.env_, options.initial_pos_);
}

template <typename Src>
template <typename... SrcArgs>
inline FileReader<Src>::FileReader(std::tuple<SrcArgs...> src_args,
                                   Options options)
    : FileReaderBase(options.buffer_size_), src_(std::move(src_args)) {
  Initialize(src_.get(), options.env_, options.initial_pos_);
}

template <typename Src>
inline FileReader<Src>::FileReader(absl::string_view filename, Options options)
    : FileReaderBase(options.buffer_size_) {
  Initialize(filename, options.env_, options.initial_pos_);
}

template <typename Src>
inline void FileReader<Src>::Reset() {
  FileReaderBase::Reset();
  src_.Reset();
}

template <typename Src>
inline void FileReader<Src>::Reset(const Src& src, Options options) {
  FileReaderBase::Reset(options.buffer_size_);
  src_.Reset(src);
  Initialize(src_.get(), options.env_, options.initial_pos_);
}

template <typename Src>
inline void FileReader<Src>::Reset(Src&& src, Options options) {
  FileReaderBase::Reset(options.buffer_size_);
  src_.Reset(std::move(src));
  Initialize(src_.get(), options.env_, options.initial_pos_);
}

template <typename Src>
template <typename... SrcArgs>
inline void FileReader<Src>::Reset(std::tuple<SrcArgs...> src_args,
                                   Options options) {
  FileReaderBase::Reset(options.buffer_size_);
  src_.Reset(std::move(src_args));
  Initialize(src_.get(), options.env_, options.initial_pos_);
}

template <typename Src>
inline void FileReader<Src>::Reset(absl::string_view filename,
                                   Options options) {
  FileReaderBase::Reset(options.buffer_size_);
  src_.Reset();  // In case `OpenFile()` fails.
  Initialize(filename, options.env_, options.initial_pos_);
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
    : FileReaderBase(std::move(that)), src_(std::move(that.src_)) {}

template <typename Src>
inline FileReader<Src>& FileReader<Src>::operator=(FileReader&& that) noexcept {
  FileReaderBase::operator=(std::move(that));
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

template <typename Src>
struct Resetter<tensorflow::FileReader<Src>>
    : ResetterByReset<tensorflow::FileReader<Src>> {};

}  // namespace riegeli

#endif  // RIEGELI_TENSORFLOW_IO_FILE_READER_H_
