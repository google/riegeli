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

#ifndef RIEGELI_BYTES_ISTREAM_READER_H_
#define RIEGELI_BYTES_ISTREAM_READER_H_

#include <stddef.h>

#include <cerrno>
#include <istream>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/stream_internal.h"

namespace riegeli {

// Template parameter independent part of `IStreamReader`.
class IStreamReaderBase : public BufferedReader {
 public:
  class Options {
   public:
    Options() noexcept {}

    // If `absl::nullopt`, the current position reported by `pos()` corresponds
    // to the current stream position if possible, otherwise 0 is assumed as the
    // initial position. Random access is supported if the stream supports
    // random access.
    //
    // If not `absl::nullopt`, this position is assumed initially, to be
    // reported by `pos()`. It does not need to correspond to the current stream
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

    // If `true`, supports reading up to the end of the stream, then retrying
    // when the stream has grown. This disables caching the stream size.
    //
    // Default: `true` (TODO: make it `false`).
    Options& set_growing_source(bool growing_source) & {
      growing_source_ = growing_source;
      return *this;
    }
    Options&& set_growing_source(bool growing_source) && {
      return std::move(set_growing_source(growing_source));
    }
    bool growing_source() const { return growing_source_; }

    // Tunes how much data is buffered after reading from the stream.
    //
    // Default: `kDefaultBufferSize` (64K).
    Options& set_buffer_size(size_t buffer_size) & {
      RIEGELI_ASSERT_GT(buffer_size, 0u)
          << "Failed precondition of "
             "IStreamReaderBase::Options::set_buffer_size(): "
             "zero buffer size";
      buffer_size_ = buffer_size;
      return *this;
    }
    Options&& set_buffer_size(size_t buffer_size) && {
      return std::move(set_buffer_size(buffer_size));
    }
    size_t buffer_size() const { return buffer_size_; }

   private:
    absl::optional<Position> assumed_pos_;
    bool growing_source_ = true;
    size_t buffer_size_ = kDefaultBufferSize;
  };

  // Returns the stream being read from. Unchanged by `Close()`.
  virtual std::istream* src_stream() = 0;
  virtual const std::istream* src_stream() const = 0;

  bool SupportsRandomAccess() override { return supports_random_access(); }

 protected:
  explicit IStreamReaderBase(Closed) noexcept : BufferedReader(kClosed) {}

  explicit IStreamReaderBase(bool growing_source, size_t buffer_size);

  IStreamReaderBase(IStreamReaderBase&& that) noexcept;
  IStreamReaderBase& operator=(IStreamReaderBase&& that) noexcept;

  void Reset(Closed);
  void Reset(bool growing_source, size_t buffer_size);
  void Initialize(std::istream* src, absl::optional<Position> assumed_pos);
  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation);
  bool supports_random_access();

  void Done() override;
  bool ReadInternal(size_t min_length, size_t max_length, char* dest) override;
  bool SeekBehindBuffer(Position new_pos) override;
  absl::optional<Position> SizeImpl() override;

 private:
  // Encodes a `bool` or a marker that the value is not fully resolved yet.
  enum class LazyBoolState { kFalse, kTrue, kUnknown };

  void FoundSize(Position size);

  // Invariant:
  //   if `is_open()` then `supports_random_access_ != LazyBoolState::kUnknown`
  LazyBoolState supports_random_access_ = LazyBoolState::kFalse;
  bool growing_source_ = false;
  absl::optional<Position> size_;

  // Invariant: `limit_pos() <= std::numeric_limits<std::streamoff>::max()`
};

// A `Reader` which reads from a `std::istream`.
//
// `IStreamReader` supports random access if
// `Options::assumed_pos() == absl::nullopt` and the stream supports random
// access (this is checked by calling `std::istream::tellg()` and
// `std::istream::seekg()` to the end and back).
//
// On Linux, some virtual file systems ("/proc", "/sys") contain files with
// contents generated on the fly when the files are read. The files appear as
// regular files, with an apparent size of 0 or 4096, and random access is only
// partially supported. `IStreamReader` does not properly detect lack of random
// access for these files. An explicit
// `IStreamReaderBase::Options().set_assumed_pos(0)` can be used to disable
// random access for such files.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the stream being read from. `Src` must support
// `Dependency<std::istream*, Src>`, e.g. `std::istream*` (not owned, default),
// `std::unique_ptr<std::istream>` (owned), `std::ifstream` (owned).
//
// By relying on CTAD the template argument can be deduced as the value type of
// the first constructor argument. This requires C++17.
//
// Warning: if random access is not supported and the stream is not owned,
// it will have an unpredictable amount of extra data consumed because of
// buffering.
//
// Until the `IStreamReader` is closed or no longer used, the stream must not be
// closed nor have its position changed.
template <typename Src = std::istream*>
class IStreamReader : public IStreamReaderBase {
 public:
  // Creates a closed `IStreamReader`.
  explicit IStreamReader(Closed) noexcept : IStreamReaderBase(kClosed) {}

  // Will read from the stream provided by `src`.
  explicit IStreamReader(const Src& src, Options options = Options());
  explicit IStreamReader(Src&& src, Options options = Options());

  // Will read from the stream provided by a `Src` constructed from elements of
  // `src_args`. This avoids constructing a temporary `Src` and moving from it.
  template <typename... SrcArgs>
  explicit IStreamReader(std::tuple<SrcArgs...> src_args,
                         Options options = Options());

  IStreamReader(IStreamReader&& that) noexcept;
  IStreamReader& operator=(IStreamReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `IStreamReader`. This
  // avoids constructing a temporary `IStreamReader` and moving from it.
  void Reset(Closed);
  void Reset(const Src& src, Options options = Options());
  void Reset(Src&& src, Options options = Options());
  template <typename... SrcArgs>
  void Reset(std::tuple<SrcArgs...> src_args, Options options = Options());

  // Returns the object providing and possibly owning the stream being read
  // from. Unchanged by `Close()`.
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  std::istream* src_stream() override { return src_.get(); }
  const std::istream* src_stream() const override { return src_.get(); }

 protected:
  void Done() override;
  bool SyncImpl(SyncType sync_type) override;

  // The object providing and possibly owning the stream being read from.
  Dependency<std::istream*, Src> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit IStreamReader(Closed)->IStreamReader<DeleteCtad<Closed>>;
template <typename Src>
explicit IStreamReader(const Src& src, IStreamReaderBase::Options options =
                                           IStreamReaderBase::Options())
    -> IStreamReader<std::decay_t<Src>>;
template <typename Src>
explicit IStreamReader(Src&& src, IStreamReaderBase::Options options =
                                      IStreamReaderBase::Options())
    -> IStreamReader<std::decay_t<Src>>;
template <typename... SrcArgs>
explicit IStreamReader(
    std::tuple<SrcArgs...> src_args,
    IStreamReaderBase::Options options = IStreamReaderBase::Options())
    -> IStreamReader<DeleteCtad<std::tuple<SrcArgs...>>>;
#endif

// Implementation details follow.

inline IStreamReaderBase::IStreamReaderBase(bool growing_source,
                                            size_t buffer_size)
    : BufferedReader(buffer_size), growing_source_(growing_source) {
  // Clear `errno` so that `Initialize()` can attribute failures to opening the
  // stream.
  errno = 0;
}

inline IStreamReaderBase::IStreamReaderBase(IStreamReaderBase&& that) noexcept
    : BufferedReader(static_cast<BufferedReader&&>(that)),
      supports_random_access_(that.supports_random_access_),
      growing_source_(that.growing_source_),
      size_(that.size_) {}

inline IStreamReaderBase& IStreamReaderBase::operator=(
    IStreamReaderBase&& that) noexcept {
  BufferedReader::operator=(static_cast<BufferedReader&&>(that));
  supports_random_access_ = that.supports_random_access_;
  growing_source_ = that.growing_source_;
  size_ = that.size_;
  return *this;
}

inline void IStreamReaderBase::Reset(Closed) {
  BufferedReader::Reset(kClosed);
  supports_random_access_ = LazyBoolState::kFalse;
  growing_source_ = false;
  size_ = absl::nullopt;
}

inline void IStreamReaderBase::Reset(bool growing_source, size_t buffer_size) {
  BufferedReader::Reset(buffer_size);
  supports_random_access_ = LazyBoolState::kFalse;
  growing_source_ = growing_source;
  size_ = absl::nullopt;
  // Clear `errno` so that `Initialize()` can attribute failures to opening the
  // stream.
  errno = 0;
}

template <typename Src>
inline IStreamReader<Src>::IStreamReader(const Src& src, Options options)
    : IStreamReaderBase(options.growing_source(), options.buffer_size()),
      src_(src) {
  Initialize(src_.get(), options.assumed_pos());
}

template <typename Src>
inline IStreamReader<Src>::IStreamReader(Src&& src, Options options)
    : IStreamReaderBase(options.growing_source(), options.buffer_size()),
      src_(std::move(src)) {
  Initialize(src_.get(), options.assumed_pos());
}

template <typename Src>
template <typename... SrcArgs>
inline IStreamReader<Src>::IStreamReader(std::tuple<SrcArgs...> src_args,
                                         Options options)
    : IStreamReaderBase(options.growing_source(), options.buffer_size()),
      src_(std::move(src_args)) {
  Initialize(src_.get(), options.assumed_pos());
}

template <typename Src>
inline IStreamReader<Src>::IStreamReader(IStreamReader&& that) noexcept
    : IStreamReaderBase(static_cast<IStreamReaderBase&&>(that)),
      src_(std::move(that.src_)) {}

template <typename Src>
inline IStreamReader<Src>& IStreamReader<Src>::operator=(
    IStreamReader&& that) noexcept {
  IStreamReaderBase::operator=(static_cast<IStreamReaderBase&&>(that));
  src_ = std::move(that.src_);
  return *this;
}

template <typename Src>
inline void IStreamReader<Src>::Reset(Closed) {
  IStreamReaderBase::Reset(kClosed);
  src_.Reset();
}

template <typename Src>
inline void IStreamReader<Src>::Reset(const Src& src, Options options) {
  IStreamReaderBase::Reset(options.growing_source(), options.buffer_size());
  src_.Reset(src);
  Initialize(src_.get(), options.assumed_pos());
}

template <typename Src>
inline void IStreamReader<Src>::Reset(Src&& src, Options options) {
  IStreamReaderBase::Reset(options.growing_source(), options.buffer_size());
  src_.Reset(std::move(src));
  Initialize(src_.get(), options.assumed_pos());
}

template <typename Src>
template <typename... SrcArgs>
inline void IStreamReader<Src>::Reset(std::tuple<SrcArgs...> src_args,
                                      Options options) {
  IStreamReaderBase::Reset(options.growing_source(), options.buffer_size());
  src_.Reset(std::move(src_args));
  Initialize(src_.get(), options.assumed_pos());
}

template <typename Src>
void IStreamReader<Src>::Done() {
  IStreamReaderBase::Done();
  if (src_.is_owning()) {
    errno = 0;
    stream_internal::Close(*src_);
    if (ABSL_PREDICT_FALSE(src_->fail()) && ABSL_PREDICT_TRUE(ok())) {
      FailOperation("istream::close()");
    }
  }
}

template <typename Src>
bool IStreamReader<Src>::SyncImpl(SyncType sync_type) {
  if (ABSL_PREDICT_FALSE(!IStreamReaderBase::SyncImpl(sync_type))) return false;
  if ((sync_type != SyncType::kFromObject || src_.is_owning()) &&
      supports_random_access()) {
    if (ABSL_PREDICT_FALSE(src_->sync() != 0)) {
      return FailOperation("istream::sync()");
    }
  }
  return true;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_ISTREAM_READER_H_
