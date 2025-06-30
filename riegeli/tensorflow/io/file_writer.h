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
#include <optional>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/byte_fill.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/external_ref.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/object.h"
#include "riegeli/base/shared_buffer.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/path_ref.h"
#include "riegeli/bytes/writer.h"
#include "tensorflow/core/platform/env.h"
#include "tensorflow/core/platform/file_system.h"

namespace riegeli {

class Reader;

namespace tensorflow {

template <typename Src>
class FileReader;

// Template parameter independent part of `FileWriter`.
class FileWriterBase : public Writer {
 public:
  class Options : public BufferOptionsBase<Options> {
   public:
    Options() noexcept {}

    // `FileWriter` has a larger `kDefaultMaxBufferSize` (1M) because remote
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

    // If `false`, the file will be truncated to empty if it exists.
    //
    // If `true`, the file will not be truncated if it exists, and writing will
    // always happen at its end, or will continue at its end, depending on the
    // nature of the file.
    //
    // If `FileWriter` writes to an already open file, `append()` has no effect.
    //
    // Default: `false`.
    Options& set_append(bool append) & ABSL_ATTRIBUTE_LIFETIME_BOUND {
      append_ = append;
      return *this;
    }
    Options&& set_append(bool append) && ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_append(append));
    }
    bool append() const { return append_; }

   private:
    ::tensorflow::Env* env_ = nullptr;
    bool append_ = false;
  };

  // Returns the `::tensorflow::WritableFile` being written to. Unchanged by
  // `Close()`.
  virtual ::tensorflow::WritableFile* DestFile() const
      ABSL_ATTRIBUTE_LIFETIME_BOUND = 0;

  // Returns the name of the `::tensorflow::WritableFile` being written to.
  // Unchanged by `Close()`.
  absl::string_view filename() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return filename_;
  }

  bool SupportsReadMode() override { return file_system_ != nullptr; }

 protected:
  explicit FileWriterBase(Closed) noexcept : Writer(kClosed) {}

  explicit FileWriterBase(BufferOptions buffer_options, ::tensorflow::Env* env);

  FileWriterBase(FileWriterBase&& that) noexcept;
  FileWriterBase& operator=(FileWriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset(BufferOptions buffer_options, ::tensorflow::Env* env);
  void Initialize(::tensorflow::WritableFile* dest);
  bool InitializeFilename(::tensorflow::WritableFile* dest);
  bool InitializeFilename(PathInitializer filename);
  std::unique_ptr<::tensorflow::WritableFile> OpenFile(bool append);
  void InitializePos(::tensorflow::WritableFile* dest);
  ABSL_ATTRIBUTE_COLD bool FailOperation(const absl::Status& status,
                                         absl::string_view operation);

  void Done() override;
  absl::Status AnnotateStatusImpl(absl::Status status) override;
  void SetWriteSizeHintImpl(std::optional<Position> write_size_hint) override;
  bool PushSlow(size_t min_length, size_t recommended_length) override;
  using Writer::WriteSlow;
  bool WriteSlow(absl::string_view src) override;
  bool WriteSlow(ExternalRef src) override;
  bool WriteSlow(const Chain& src) override;
  bool WriteSlow(Chain&& src) override;
  bool WriteSlow(const absl::Cord& src) override;
  bool WriteSlow(ByteFill src) override;
  bool FlushImpl(FlushType flush_type) override;
  Reader* ReadModeImpl(Position initial_pos) override;

 private:
  // Writes `buffer_` to the file. Sets buffer pointers to `nullptr`.
  bool SyncBuffer();

  // Writes `src` to the destination.
  //
  // Does not use buffer pointers. Increments `start_pos()` by the length
  // written, which must be `src.size()` on success. Returns `true` on success.
  //
  // Preconditions:
  //   `!src.empty()`
  //   `ok()`
  bool WriteInternal(absl::string_view src);
  bool WriteInternal(const absl::Cord& src);

  std::string filename_{kDefaultFilename};
  // Invariant: if `is_open()` then `env_ != nullptr`
  ::tensorflow::Env* env_ = nullptr;
  ::tensorflow::FileSystem* file_system_ = nullptr;
  WriteBufferSizer buffer_sizer_;
  // Buffered data to be written.
  SharedBuffer buffer_;

  AssociatedReader<FileReader<std::unique_ptr<::tensorflow::RandomAccessFile>>>
      associated_reader_;
};

// A `Writer` which writes to a `::tensorflow::WritableFile`.
//
// It supports `ReadMode()` if the `::tensorflow::WritableFile` supports
// `::tensorflow::WritableFile::Name()` and the name is not empty.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the `::tensorflow::WritableFile` being written to. `Dest`
// must support `Dependency<::tensorflow::WritableFile*, Dest>`, e.g.
// `std::unique_ptr<::tensorflow::WritableFile>` (owned, default),
// `::tensorflow::WritableFile*` (not owned),
// `Any<::tensorflow::WritableFile*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as `TargetT` of the
// type of the first constructor argument.
//
// The `::tensorflow::WritableFile` must not be closed until the `FileWriter` is
// closed or no longer used. Until then the `::tensorflow::WritableFile` may be
// accessed, but not concurrently, `Flush()` is needed before switching to
// another writer to the same `::tensorflow::WritableFile`, and `pos()` does not
// take other writers into account.
template <typename Dest = std::unique_ptr<::tensorflow::WritableFile>>
class FileWriter : public FileWriterBase {
 public:
  // Creates a closed `FileWriter`.
  explicit FileWriter(Closed) noexcept : FileWriterBase(kClosed) {}

  // Will write to the `::tensorflow::WritableFile` provided by `dest`.
  explicit FileWriter(Initializer<Dest> dest, Options options = Options());

  // Opens a `::tensorflow::WritableFile` for writing.
  //
  // If opening the file fails, `FileWriter` will be failed and closed.
  explicit FileWriter(PathInitializer filename, Options options = Options());

  FileWriter(FileWriter&& that) = default;
  FileWriter& operator=(FileWriter&& that) = default;

  // Makes `*this` equivalent to a newly constructed `FileWriter`. This avoids
  // constructing a temporary `FileWriter` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Dest> dest,
                                          Options options = Options());
  ABSL_ATTRIBUTE_REINITIALIZES
  void Reset(PathInitializer filename, Options options = Options());

  // Returns the object providing and possibly owning the
  // `::tensorflow::WritableFile` being written to. Unchanged by `Close()`.
  Dest& dest() ABSL_ATTRIBUTE_LIFETIME_BOUND { return dest_.manager(); }
  const Dest& dest() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return dest_.manager();
  }
  ::tensorflow::WritableFile* DestFile() const
      ABSL_ATTRIBUTE_LIFETIME_BOUND override {
    return dest_.get();
  }

 protected:
  void Done() override;
  bool FlushImpl(FlushType flush_type) override;

 private:
  using FileWriterBase::Initialize;
  void Initialize(PathInitializer filename, Options&& options);

  // The object providing and possibly owning the `::tensorflow::WritableFile`
  // being written to.
  Dependency<::tensorflow::WritableFile*, Dest> dest_;
};

explicit FileWriter(Closed) -> FileWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit FileWriter(Dest&& dest,
                    FileWriterBase::Options options = FileWriterBase::Options())
    -> FileWriter<std::conditional_t<
        std::is_convertible_v<Dest&&, PathInitializer>,
        std::unique_ptr<::tensorflow::WritableFile>, TargetT<Dest>>>;

// Implementation details follow.

inline FileWriterBase::FileWriterBase(BufferOptions buffer_options,
                                      ::tensorflow::Env* env)
    : env_(env != nullptr ? env : ::tensorflow::Env::Default()),
      buffer_sizer_(buffer_options) {}

inline FileWriterBase::FileWriterBase(FileWriterBase&& that) noexcept
    : Writer(static_cast<Writer&&>(that)),
      filename_(std::exchange(that.filename_, std::string(kDefaultFilename))),
      env_(that.env_),
      file_system_(that.file_system_),
      buffer_sizer_(that.buffer_sizer_),
      buffer_(std::move(that.buffer_)),
      associated_reader_(std::move(that.associated_reader_)) {}

inline FileWriterBase& FileWriterBase::operator=(
    FileWriterBase&& that) noexcept {
  Writer::operator=(static_cast<Writer&&>(that));
  filename_ = std::exchange(that.filename_, std::string(kDefaultFilename));
  env_ = that.env_;
  file_system_ = that.file_system_;
  buffer_sizer_ = that.buffer_sizer_;
  buffer_ = std::move(that.buffer_);
  associated_reader_ = std::move(that.associated_reader_);
  return *this;
}

inline void FileWriterBase::Reset(Closed) {
  Writer::Reset(kClosed);
  filename_ = std::string(kDefaultFilename);
  env_ = nullptr;
  file_system_ = nullptr;
  buffer_sizer_.Reset();
  buffer_ = SharedBuffer();
  associated_reader_.Reset();
}

inline void FileWriterBase::Reset(BufferOptions buffer_options,
                                  ::tensorflow::Env* env) {
  Writer::Reset();
  // `filename_` will be set by `InitializeFilename()`.
  env_ = env != nullptr ? env : ::tensorflow::Env::Default();
  file_system_ = nullptr;
  buffer_sizer_.Reset(buffer_options);
  associated_reader_.Reset();
}

inline void FileWriterBase::Initialize(::tensorflow::WritableFile* dest) {
  RIEGELI_ASSERT_NE(dest, nullptr)
      << "Failed precondition of FileWriter: null WritableFile pointer";
  if (ABSL_PREDICT_FALSE(!InitializeFilename(dest))) return;
  InitializePos(dest);
}

template <typename Dest>
inline FileWriter<Dest>::FileWriter(Initializer<Dest> dest, Options options)
    : FileWriterBase(options.buffer_options(), options.env()),
      dest_(std::move(dest)) {
  Initialize(dest_.get());
}

template <typename Dest>
inline FileWriter<Dest>::FileWriter(PathInitializer filename, Options options)
    : FileWriterBase(options.buffer_options(), options.env()) {
  Initialize(std::move(filename), std::move(options));
}

template <typename Dest>
inline void FileWriter<Dest>::Reset(Closed) {
  FileWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void FileWriter<Dest>::Reset(Initializer<Dest> dest, Options options) {
  FileWriterBase::Reset(options.buffer_options(), options.env());
  dest_.Reset(std::move(dest));
  Initialize(dest_.get());
}

template <typename Dest>
inline void FileWriter<Dest>::Reset(PathInitializer filename, Options options) {
  FileWriterBase::Reset(options.buffer_options(), options.env());
  Initialize(std::move(filename), std::move(options));
}

template <typename Dest>
inline void FileWriter<Dest>::Initialize(PathInitializer filename,
                                         Options&& options) {
  if (ABSL_PREDICT_FALSE(!InitializeFilename(std::move(filename)))) return;
  std::unique_ptr<::tensorflow::WritableFile> dest = OpenFile(options.append());
  if (ABSL_PREDICT_FALSE(dest == nullptr)) return;
  dest_.Reset(riegeli::Maker(dest.release()));
  InitializePos(dest_.get());
}

template <typename Dest>
void FileWriter<Dest>::Done() {
  FileWriterBase::Done();
  if (dest_.IsOwning()) {
    if (const absl::Status status = dest_->Close();
        ABSL_PREDICT_FALSE(!status.ok()) && ABSL_PREDICT_TRUE(ok())) {
      FailOperation(status, "WritableFile::Close()");
    }
  }
}

template <typename Dest>
bool FileWriter<Dest>::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!FileWriterBase::FlushImpl(flush_type))) return false;
  switch (flush_type) {
    case FlushType::kFromObject:
      if (!dest_.IsOwning()) return true;
      ABSL_FALLTHROUGH_INTENDED;
    case FlushType::kFromProcess:
      if (const absl::Status status = dest_->Flush();
          ABSL_PREDICT_FALSE(!status.ok())) {
        return FailOperation(status, "WritableFile::Flush()");
      }
      return true;
    case FlushType::kFromMachine:
      if (const absl::Status status = dest_->Sync();
          ABSL_PREDICT_FALSE(!status.ok())) {
        return FailOperation(status, "WritableFile::Sync()");
      }
      return true;
  }
  RIEGELI_ASSUME_UNREACHABLE()
      << "Unknown flush type: " << static_cast<int>(flush_type);
}

}  // namespace tensorflow

}  // namespace riegeli

#endif  // RIEGELI_TENSORFLOW_IO_FILE_WRITER_H_
