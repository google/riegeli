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
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/stream_dependency.h"

namespace riegeli {

// Template parameter independent part of `IstreamReader`.
class IstreamReaderBase : public BufferedReader {
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

    // Tunes how much data is buffered after reading from the file.
    //
    // Default: `kDefaultBufferSize` (64K).
    Options& set_buffer_size(size_t buffer_size) & {
      RIEGELI_ASSERT_GT(buffer_size, 0u)
          << "Failed precondition of "
             "IstreamReaderBase::Options::set_buffer_size(): "
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
    size_t buffer_size_ = kDefaultBufferSize;
  };

  // Returns the stream being read from. Unchanged by `Close()`.
  virtual std::istream* src_stream() = 0;
  virtual const std::istream* src_stream() const = 0;

  bool SupportsRandomAccess() override { return supports_random_access(); }

 protected:
  // Encodes a `bool` or a marker that the value is not fully resolved yet.
  enum class LazyBoolState { kFalse, kTrue, kUnknown };

  IstreamReaderBase() noexcept {}

  explicit IstreamReaderBase(size_t buffer_size);

  IstreamReaderBase(IstreamReaderBase&& that) noexcept;
  IstreamReaderBase& operator=(IstreamReaderBase&& that) noexcept;

  void Reset();
  void Reset(size_t buffer_size);
  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation);
  void Initialize(std::istream* src, absl::optional<Position> assumed_pos);
  bool supports_random_access();
  bool SyncPos(std::istream& src);

  void Done() override;
  bool ReadInternal(size_t min_length, size_t max_length, char* dest) override;
  bool SeekSlow(Position new_pos) override;
  bool SyncImpl(SyncType sync_type) override;
  absl::optional<Position> SizeImpl() override;

  // Whether random access is supported, as detected by calling
  // `std::istream::tellg()` and `std::istream::seekg()` to the end and back.
  //
  // `std::istream::tellg()` is called during initialization;
  // `std::istream::seekg()` is called lazily.
  //
  // Invariant:
  //   if `supports_random_access_ == LazyBoolState::kUnknown` then `is_open()`
  LazyBoolState supports_random_access_ = LazyBoolState::kFalse;

  // Invariant: `limit_pos() <= std::numeric_limits<std::streamoff>::max()`
};

// A `Reader` which reads from a `std::istream`.
//
// `IstreamReader` supports random access if
// `Options::assumed_pos() == absl::nullopt` and the stream supports random
// access (this is checked by calling `std::istream::tellg()` and
// `std::istream::seekg()` to the end and back).
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
// Until the `IstreamReader` is closed or no longer used, the stream must not be
// closed nor have its position changed.
template <typename Src = std::istream*>
class IstreamReader : public IstreamReaderBase {
 public:
  // Creates a closed `IstreamReader`.
  IstreamReader() noexcept {}

  // Will read from the stream provided by `src`.
  explicit IstreamReader(const Src& src, Options options = Options());
  explicit IstreamReader(Src&& src, Options options = Options());

  // Will read from the stream provided by a `Src` constructed from elements of
  // `src_args`. This avoids constructing a temporary `Src` and moving from it.
  template <typename... SrcArgs>
  explicit IstreamReader(std::tuple<SrcArgs...> src_args,
                         Options options = Options());

  IstreamReader(IstreamReader&& that) noexcept;
  IstreamReader& operator=(IstreamReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `IstreamReader`. This
  // avoids constructing a temporary `IstreamReader` and moving from it.
  void Reset();
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
IstreamReader()->IstreamReader<DeleteCtad<>>;
template <typename Src>
explicit IstreamReader(const Src& src, IstreamReaderBase::Options options =
                                           IstreamReaderBase::Options())
    -> IstreamReader<std::decay_t<Src>>;
template <typename Src>
explicit IstreamReader(Src&& src, IstreamReaderBase::Options options =
                                      IstreamReaderBase::Options())
    -> IstreamReader<std::decay_t<Src>>;
template <typename... SrcArgs>
explicit IstreamReader(
    std::tuple<SrcArgs...> src_args,
    IstreamReaderBase::Options options = IstreamReaderBase::Options())
    -> IstreamReader<DeleteCtad<std::tuple<SrcArgs...>>>;
#endif

// Implementation details follow.

inline IstreamReaderBase::IstreamReaderBase(size_t buffer_size)
    : BufferedReader(buffer_size) {
  // Clear `errno` so that `Initialize()` can attribute failures to opening the
  // stream.
  errno = 0;
}

inline IstreamReaderBase::IstreamReaderBase(IstreamReaderBase&& that) noexcept
    : BufferedReader(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      supports_random_access_(that.supports_random_access_) {}

inline IstreamReaderBase& IstreamReaderBase::operator=(
    IstreamReaderBase&& that) noexcept {
  BufferedReader::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  supports_random_access_ = that.supports_random_access_;
  return *this;
}

inline void IstreamReaderBase::Reset() {
  BufferedReader::Reset();
  supports_random_access_ = LazyBoolState::kFalse;
}

inline void IstreamReaderBase::Reset(size_t buffer_size) {
  BufferedReader::Reset(buffer_size);
  supports_random_access_ = LazyBoolState::kFalse;
  // Clear `errno` so that `Initialize()` can attribute failures to opening the
  // stream.
  errno = 0;
}

template <typename Src>
inline IstreamReader<Src>::IstreamReader(const Src& src, Options options)
    : IstreamReaderBase(options.buffer_size()), src_(src) {
  Initialize(src_.get(), options.assumed_pos());
}

template <typename Src>
inline IstreamReader<Src>::IstreamReader(Src&& src, Options options)
    : IstreamReaderBase(options.buffer_size()), src_(std::move(src)) {
  Initialize(src_.get(), options.assumed_pos());
}

template <typename Src>
template <typename... SrcArgs>
inline IstreamReader<Src>::IstreamReader(std::tuple<SrcArgs...> src_args,
                                         Options options)
    : IstreamReaderBase(options.buffer_size()), src_(std::move(src_args)) {
  Initialize(src_.get(), options.assumed_pos());
}

template <typename Src>
inline IstreamReader<Src>::IstreamReader(IstreamReader&& that) noexcept
    : IstreamReaderBase(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      src_(std::move(that.src_)) {}

template <typename Src>
inline IstreamReader<Src>& IstreamReader<Src>::operator=(
    IstreamReader&& that) noexcept {
  IstreamReaderBase::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  src_ = std::move(that.src_);
  return *this;
}

template <typename Src>
inline void IstreamReader<Src>::Reset() {
  IstreamReaderBase::Reset();
  src_.Reset();
}

template <typename Src>
inline void IstreamReader<Src>::Reset(const Src& src, Options options) {
  IstreamReaderBase::Reset(options.buffer_size());
  src_.Reset(src);
  Initialize(src_.get(), options.assumed_pos());
}

template <typename Src>
inline void IstreamReader<Src>::Reset(Src&& src, Options options) {
  IstreamReaderBase::Reset(options.buffer_size());
  src_.Reset(std::move(src));
  Initialize(src_.get(), options.assumed_pos());
}

template <typename Src>
template <typename... SrcArgs>
inline void IstreamReader<Src>::Reset(std::tuple<SrcArgs...> src_args,
                                      Options options) {
  IstreamReaderBase::Reset(options.buffer_size());
  src_.Reset(std::move(src_args));
  Initialize(src_.get(), options.assumed_pos());
}

template <typename Src>
void IstreamReader<Src>::Done() {
  IstreamReaderBase::Done();
  if (src_.is_owning()) {
    errno = 0;
    internal::CloseStream(*src_);
    if (ABSL_PREDICT_FALSE(src_->fail()) && ABSL_PREDICT_TRUE(healthy())) {
      FailOperation("istream::close()");
    }
  }
}

template <typename Src>
bool IstreamReader<Src>::SyncImpl(SyncType sync_type) {
  if (ABSL_PREDICT_FALSE(!IstreamReaderBase::SyncImpl(sync_type))) return false;
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
