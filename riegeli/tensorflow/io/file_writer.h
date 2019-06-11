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

#ifndef RIEGELI_TENSORFLOW_IO_FILE_WRITER_H_
#define RIEGELI_TENSORFLOW_IO_FILE_WRITER_H_

#include <stddef.h>

#include <memory>
#include <string>
#include <tuple>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/resetter.h"
#include "riegeli/bytes/writer.h"
#include "tensorflow/core/lib/core/status.h"
#include "tensorflow/core/platform/env.h"
#include "tensorflow/core/platform/file_system.h"

namespace riegeli {

namespace tensorflow {

// Template parameter invariant part of FileWriter.
class FileWriterBase : public Writer {
 public:
  class Options {
   public:
    Options() noexcept {}

    // Overrides the TensorFlow environment.
    //
    // nullptr is interpreted as Env::Default().
    //
    // Default: nullptr.
    Options& set_env(::tensorflow::Env* env) & {
      env_ = env;
      return *this;
    }
    Options&& set_env(::tensorflow::Env* env) && {
      return std::move(set_env(env));
    }

    // If false, the file will be truncated to empty if it exists.
    //
    // If true, the file will not be truncated if it exists, and writing will
    // continue at its end.
    //
    // This is applicable if FileWriter opens the file.
    //
    // Default: false.
    Options& set_append(bool append) & {
      append_ = append;
      return *this;
    }
    Options&& set_append(bool append) && {
      return std::move(set_append(append));
    }

    // Tunes how much data is buffered before writing to the file.
    //
    // Default: 64K
    Options& set_buffer_size(size_t buffer_size) & {
      RIEGELI_ASSERT_GT(buffer_size, 0u)
          << "Failed precondition of "
             "FileWriterBase::Options::set_buffer_size(): "
             "zero buffer size";
      buffer_size_ = buffer_size;
      return *this;
    }
    Options&& set_buffer_size(size_t buffer_size) && {
      return std::move(set_buffer_size(buffer_size));
    }

   private:
    template <typename Dest>
    friend class FileWriter;

    ::tensorflow::Env* env_ = nullptr;
    bool append_ = false;
    size_t buffer_size_ = kDefaultBufferSize;
  };

  // Returns the WritableFile being written to. Unchanged by Close().
  virtual ::tensorflow::WritableFile* dest_file() const = 0;

  // Returns the name of the WritableFile being written to. Unchanged by
  // Close().
  const std::string& filename() const { return filename_; }

  bool Flush(FlushType flush_type) override;

 protected:
  FileWriterBase() noexcept : Writer(kInitiallyClosed) {}

  explicit FileWriterBase(size_t buffer_size);

  FileWriterBase(FileWriterBase&& that) noexcept;
  FileWriterBase& operator=(FileWriterBase&& that) noexcept;

  void Reset();
  void Reset(size_t buffer_size);
  void Initialize(::tensorflow::WritableFile* dest);
  void InitializeFilename(::tensorflow::WritableFile* dest);
  std::unique_ptr<::tensorflow::WritableFile> OpenFile(
      ::tensorflow::Env* env, absl::string_view filename, bool append);
  void InitializePos(::tensorflow::WritableFile* dest);
  ABSL_ATTRIBUTE_COLD bool FailOperation(const ::tensorflow::Status& status,
                                         absl::string_view operation);

  bool PushSlow(size_t min_length, size_t recommended_length) override;

  // Writes buffered data to the destination, but unlike PushSlow(), does not
  // ensure that a buffer is allocated.
  bool PushInternal();

  using Writer::WriteSlow;
  bool WriteSlow(absl::string_view src) override;

  // Writes data to the destination.
  //
  // Increments start_pos_ by the length written.
  //
  // Preconditions:
  //   !src.empty()
  //   healthy()
  bool WriteInternal(absl::string_view src);

 private:
  // Preferred size of the buffer to use.
  size_t BufferLength(size_t min_length) const;

  // Minimum length for which it is better to push current contents of buffer_
  // and write the data directly than to write the data through buffer_.
  size_t LengthToWriteDirectly() const;

  std::string filename_;
  // Invariant: if healthy() then buffer_size_ > 0
  size_t buffer_size_ = 0;
  // Buffered data to be written.
  Buffer buffer_;
};

// A Writer which writes to a WritableFile.
//
// The Dest template parameter specifies the type of the object providing and
// possibly owning the File being written to. Dest must support
// Dependency<File*, Dest>, e.g. OwnedFile (owned, default), File* (not owned).
//
// The Dest template parameter specifies the type of the object providing and
// possibly owning the WritableFile being written to. Dest must support
// Dependency<WritableFile*, Src>, e.g.
// std::unique_ptr<WritableFile> (owned, default),
// WritableFile* (not owned).
//
// The WritableFile must not be closed until the FileWriter is closed
// or no longer used. Until then the WritableFile may be accessed, but not
// concurrently, Flush() is needed before switching to another writer to the
// same WritableFile, and pos() does not take other writers into account.
template <typename Dest = std::unique_ptr<::tensorflow::WritableFile>>
class FileWriter : public FileWriterBase {
 public:
  // Creates a closed FileWriter.
  FileWriter() noexcept {}

  // Will write to the WritableFile provided by dest.
  explicit FileWriter(const Dest& dest, Options options = Options());
  explicit FileWriter(Dest&& dest, Options options = Options());

  // Will write to the WritableFile provided by a Dest constructed from elements
  // of dest_args. This avoids constructing a temporary Dest and moving from it.
  template <typename... DestArgs>
  explicit FileWriter(std::tuple<DestArgs...> dest_args,
                      Options options = Options());

  // Opens a WritableFile for writing.
  explicit FileWriter(absl::string_view filename, Options options = Options());

  FileWriter(FileWriter&& that) noexcept;
  FileWriter& operator=(FileWriter&& that) noexcept;

  // Makes *this equivalent to a newly constructed FileWriter. This avoids
  // constructing a temporary FileWriter and moving from it.
  void Reset();
  void Reset(const Dest& dest, Options options = Options());
  void Reset(Dest&& dest, Options options = Options());
  template <typename... DestArgs>
  void Reset(std::tuple<DestArgs...> dest_args, Options options = Options());
  void Reset(absl::string_view filename, Options options = Options());

  // Returns the object providing and possibly owning the WritableFile being
  // written to. Unchanged by Close().
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  ::tensorflow::WritableFile* dest_file() const override { return dest_.get(); }

 protected:
  void Done() override;

 private:
  using FileWriterBase::Initialize;
  void Initialize(absl::string_view filename, ::tensorflow::Env* env,
                  bool append);

  // The object providing and possibly owning the WritableFile being written to.
  Dependency<::tensorflow::WritableFile*, Dest> dest_;
};

// Implementation details follow.

inline FileWriterBase::FileWriterBase(size_t buffer_size)
    : Writer(kInitiallyOpen), buffer_size_(buffer_size), buffer_(buffer_size) {}

inline FileWriterBase::FileWriterBase(FileWriterBase&& that) noexcept
    : Writer(std::move(that)),
      filename_(std::move(that.filename_)),
      buffer_size_(that.buffer_size_),
      buffer_(std::move(that.buffer_)) {}

inline FileWriterBase& FileWriterBase::operator=(
    FileWriterBase&& that) noexcept {
  Writer::operator=(std::move(that));
  filename_ = std::move(that.filename_);
  buffer_size_ = that.buffer_size_;
  buffer_ = std::move(that.buffer_);
  return *this;
}

inline void FileWriterBase::Reset() {
  Writer::Reset(kInitiallyClosed);
  filename_.clear();
  buffer_size_ = 0;
}

inline void FileWriterBase::Reset(size_t buffer_size) {
  Writer::Reset(kInitiallyOpen);
  filename_.clear();
  buffer_size_ = buffer_size;
  buffer_.Resize(buffer_size);
}

inline void FileWriterBase::Initialize(::tensorflow::WritableFile* dest) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of FileWriter: null WritableFile pointer";
  InitializeFilename(dest);
  InitializePos(dest);
}

template <typename Dest>
inline FileWriter<Dest>::FileWriter(const Dest& dest, Options options)
    : FileWriterBase(options.buffer_size_), dest_(dest) {
  Initialize(dest_.get());
}

template <typename Dest>
inline FileWriter<Dest>::FileWriter(Dest&& dest, Options options)
    : FileWriterBase(options.buffer_size_), dest_(std::move(dest)) {
  Initialize(dest_.get());
}

template <typename Dest>
template <typename... DestArgs>
inline FileWriter<Dest>::FileWriter(std::tuple<DestArgs...> dest_args,
                                    Options options)
    : FileWriterBase(options.buffer_size_), dest_(std::move(dest_args)) {
  Initialize(dest_.get());
}

template <typename Dest>
inline FileWriter<Dest>::FileWriter(absl::string_view filename, Options options)
    : FileWriterBase(options.buffer_size_) {
  Initialize(filename, options.env_, options.append_);
}

template <typename Dest>
inline FileWriter<Dest>::FileWriter(FileWriter&& that) noexcept
    : FileWriterBase(std::move(that)), dest_(std::move(that.dest_)) {}

template <typename Dest>
inline FileWriter<Dest>& FileWriter<Dest>::operator=(
    FileWriter&& that) noexcept {
  FileWriterBase::operator=(std::move(that));
  dest_ = std::move(that.dest_);
  return *this;
}

template <typename Dest>
inline void FileWriter<Dest>::Reset() {
  FileWriterBase::Reset();
  dest_.Reset();
  Initialize(dest_.get());
}

template <typename Dest>
inline void FileWriter<Dest>::Reset(const Dest& dest, Options options) {
  FileWriterBase::Reset(options.buffer_size_);
  dest_.Reset(dest);
  Initialize(dest_.get());
}

template <typename Dest>
inline void FileWriter<Dest>::Reset(Dest&& dest, Options options) {
  FileWriterBase::Reset(options.buffer_size_);
  dest_.Reset(std::move(dest));
  Initialize(dest_.get());
}

template <typename Dest>
template <typename... DestArgs>
inline void FileWriter<Dest>::Reset(std::tuple<DestArgs...> dest_args,
                                    Options options) {
  FileWriterBase::Reset(options.buffer_size_);
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get());
}

template <typename Dest>
inline void FileWriter<Dest>::Reset(absl::string_view filename,
                                    Options options) {
  FileWriterBase::Reset(options.buffer_size_);
  dest_.Reset();  // In case OpenFile() fails.
  Initialize(filename, options.env_, options.append_);
}

template <typename Dest>
inline void FileWriter<Dest>::Initialize(absl::string_view filename,
                                         ::tensorflow::Env* env, bool append) {
  std::unique_ptr<::tensorflow::WritableFile> dest =
      OpenFile(env, filename, append);
  if (ABSL_PREDICT_FALSE(dest == nullptr)) return;
  dest_.Reset(std::forward_as_tuple(dest.release()));
  InitializePos(dest_.get());
}

template <typename Dest>
void FileWriter<Dest>::Done() {
  PushInternal();
  FileWriterBase::Done();
  if (dest_.is_owning()) {
    const ::tensorflow::Status close_status = dest_->Close();
    if (ABSL_PREDICT_FALSE(!close_status.ok()) &&
        ABSL_PREDICT_TRUE(healthy())) {
      FailOperation(close_status, "WritableFile::Close()");
    }
  }
}

}  // namespace tensorflow

template <typename Dest>
struct Resetter<tensorflow::FileWriter<Dest>>
    : ResetterByReset<tensorflow::FileWriter<Dest>> {};

}  // namespace riegeli

#endif  // RIEGELI_TENSORFLOW_IO_FILE_WRITER_H_
