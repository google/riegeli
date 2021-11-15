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

#ifndef RIEGELI_BYTES_OSTREAM_WRITER_H_
#define RIEGELI_BYTES_OSTREAM_WRITER_H_

#include <stddef.h>

#include <cerrno>
#include <ostream>
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
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/stream_dependency.h"

namespace riegeli {

template <typename Src>
class IstreamReader;

// Template parameter independent part of `OstreamWriter`.
class OstreamWriterBase : public BufferedWriter {
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

    // Tunes how much data is buffered before writing to the file.
    //
    // Default: `kDefaultBufferSize` (64K).
    Options& set_buffer_size(size_t buffer_size) & {
      RIEGELI_ASSERT_GT(buffer_size, 0u)
          << "Failed precondition of "
             "OstreamWriterBase::Options::set_buffer_size(): "
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

  // Returns the stream being written to. Unchanged by `Close()`.
  virtual std::ostream* dest_stream() = 0;
  virtual const std::ostream* dest_stream() const = 0;

  bool SupportsRandomAccess() override { return supports_random_access(); }
  bool SupportsTruncate() override { return false; }
  bool SupportsReadMode() override { return src_stream() != nullptr; }

 protected:
  explicit OstreamWriterBase(Closed) noexcept : BufferedWriter(kClosed) {}

  explicit OstreamWriterBase(size_t buffer_size);

  OstreamWriterBase(OstreamWriterBase&& that) noexcept;
  OstreamWriterBase& operator=(OstreamWriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset(size_t buffer_size);
  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation);
  void Initialize(std::ostream* dest, absl::optional<Position> assumed_pos);

  // Returns the stream pointer as `std::istream*` if the static type of the
  // destination derives from `std::istream`, otherwise returns `nullptr`.
  virtual std::istream* src_stream() = 0;

  void Done() override;
  bool WriteInternal(absl::string_view src) override;
  bool FlushBehindBuffer(absl::string_view src, FlushType flush_type) override;
  bool SeekBehindBuffer(Position new_pos) override;
  absl::optional<Position> SizeBehindBuffer() override;
  Reader* ReadModeBehindBuffer(Position initial_pos) override;

 private:
  // Encodes a `bool` or a marker that the value is not fully resolved yet.
  enum class LazyBoolState { kFalse, kTrue, kUnknown };

  bool supports_random_access();
  bool WriteMode();

  // Invariant:
  //   if `is_open()` then `supports_random_access_ != LazyBoolState::kUnknown`
  LazyBoolState supports_random_access_ = LazyBoolState::kFalse;

  AssociatedReader<IstreamReader<std::istream*>> associated_reader_;
  bool read_mode_ = false;

  // Invariant: `start_pos() <= std::numeric_limits<std::streamoff>::max()`
};

// A `Writer` which writes to a `std::ostream`.
//
// `OstreamWriter` supports random access if
// `Options::assumed_pos() == absl::nullopt` and the stream supports random
// access (this is checked by calling `std::ostream::tellp()` and
// `std::ostream::seekp()` to the end and back).
//
// `OstreamWriter` supports `ReadMode()` if the static type of the stream
// derives also from `std::istream`.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the stream being written to. `Dest` must support
// `Dependency<std::ostream*, Dest>`, e.g. `std::ostream*` (not owned, default),
// `std::unique_ptr<std::ostream>` (owned), `std::ofstream` (owned).
//
// By relying on CTAD the template argument can be deduced as the value type of
// the first constructor argument. This requires C++17.
//
// Until the `OstreamWriter` is closed or no longer used, the `std::ostream`
// must not be closed nor have its position changed, except that if random
// access is not used, careful interleaving of multiple writers is possible:
// `Flush()` is needed before switching to another writer, and `pos()` does not
// take other writers into account.
template <typename Dest = std::ostream*>
class OstreamWriter : public OstreamWriterBase {
 public:
  // Creates a closed `OstreamWriter`.
  explicit OstreamWriter(Closed) noexcept : OstreamWriterBase(kClosed) {}

  // Will write to the stream provided by `dest`.
  explicit OstreamWriter(const Dest& dest, Options options = Options());
  explicit OstreamWriter(Dest&& dest, Options options = Options());

  // Will write to the stream provided by a `Dest` constructed from elements of
  // `dest_args`. This avoids constructing a temporary `Dest` and moving from
  // it.
  template <typename... DestArgs>
  explicit OstreamWriter(std::tuple<DestArgs...> dest_args,
                         Options options = Options());

  OstreamWriter(OstreamWriter&& that) noexcept;
  OstreamWriter& operator=(OstreamWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `OstreamWriter`. This
  // avoids constructing a temporary `OstreamWriter` and moving from it.
  void Reset(Closed);
  void Reset(const Dest& dest, Options options = Options());
  void Reset(Dest&& dest, Options options = Options());
  template <typename... DestArgs>
  void Reset(std::tuple<DestArgs...> dest_args, Options options = Options());

  // Returns the object providing and possibly owning the stream being written
  // to. Unchanged by `Close()`.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  std::ostream* dest_stream() override { return dest_.get(); }
  const std::ostream* dest_stream() const override { return dest_.get(); }

 protected:
  std::istream* src_stream() override;

  void Done() override;
  bool FlushImpl(FlushType flush_type) override;

 private:
  // The object providing and possibly owning the stream being written to.
  Dependency<std::ostream*, Dest> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit OstreamWriter(Closed)->OstreamWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit OstreamWriter(const Dest& dest, OstreamWriterBase::Options options =
                                             OstreamWriterBase::Options())
    -> OstreamWriter<std::decay_t<Dest>>;
template <typename Dest>
explicit OstreamWriter(Dest&& dest, OstreamWriterBase::Options options =
                                        OstreamWriterBase::Options())
    -> OstreamWriter<std::decay_t<Dest>>;
template <typename... DestArgs>
explicit OstreamWriter(
    std::tuple<DestArgs...> dest_args,
    OstreamWriterBase::Options options = OstreamWriterBase::Options())
    -> OstreamWriter<DeleteCtad<std::tuple<DestArgs...>>>;
#endif

// Implementation details follow.

inline OstreamWriterBase::OstreamWriterBase(size_t buffer_size)
    : BufferedWriter(buffer_size) {
  // Clear `errno` so that `Initialize()` can attribute failures to opening the
  // stream.
  errno = 0;
}

inline OstreamWriterBase::OstreamWriterBase(OstreamWriterBase&& that) noexcept
    : BufferedWriter(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      supports_random_access_(that.supports_random_access_),
      associated_reader_(std::move(that.associated_reader_)),
      read_mode_(that.read_mode_) {}

inline OstreamWriterBase& OstreamWriterBase::operator=(
    OstreamWriterBase&& that) noexcept {
  BufferedWriter::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  supports_random_access_ = that.supports_random_access_;
  associated_reader_ = std::move(that.associated_reader_);
  read_mode_ = that.read_mode_;
  return *this;
}

inline void OstreamWriterBase::Reset(Closed) {
  BufferedWriter::Reset(kClosed);
  supports_random_access_ = LazyBoolState::kFalse;
  associated_reader_.Reset();
  read_mode_ = false;
}

inline void OstreamWriterBase::Reset(size_t buffer_size) {
  BufferedWriter::Reset(buffer_size);
  supports_random_access_ = LazyBoolState::kFalse;
  associated_reader_.Reset();
  read_mode_ = false;
  // Clear `errno` so that `Initialize()` can attribute failures to opening the
  // stream.
  errno = 0;
}

template <typename Dest>
inline OstreamWriter<Dest>::OstreamWriter(const Dest& dest, Options options)
    : OstreamWriterBase(options.buffer_size()), dest_(dest) {
  Initialize(dest_.get(), options.assumed_pos());
}

template <typename Dest>
inline OstreamWriter<Dest>::OstreamWriter(Dest&& dest, Options options)
    : OstreamWriterBase(options.buffer_size()), dest_(std::move(dest)) {
  Initialize(dest_.get(), options.assumed_pos());
}

template <typename Dest>
template <typename... DestArgs>
inline OstreamWriter<Dest>::OstreamWriter(std::tuple<DestArgs...> dest_args,
                                          Options options)
    : OstreamWriterBase(options.buffer_size()), dest_(std::move(dest_args)) {
  Initialize(dest_.get(), options.assumed_pos());
}

template <typename Dest>
inline OstreamWriter<Dest>::OstreamWriter(OstreamWriter&& that) noexcept
    : OstreamWriterBase(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      dest_(std::move(that.dest_)) {}

template <typename Dest>
inline OstreamWriter<Dest>& OstreamWriter<Dest>::operator=(
    OstreamWriter&& that) noexcept {
  OstreamWriterBase::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  dest_ = std::move(that.dest_);
  return *this;
}

template <typename Dest>
inline void OstreamWriter<Dest>::Reset(Closed) {
  OstreamWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void OstreamWriter<Dest>::Reset(const Dest& dest, Options options) {
  OstreamWriterBase::Reset(options.buffer_size());
  dest_.Reset(dest);
  Initialize(dest_.get(), options.assumed_pos());
}

template <typename Dest>
inline void OstreamWriter<Dest>::Reset(Dest&& dest, Options options) {
  OstreamWriterBase::Reset(options.buffer_size());
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), options.assumed_pos());
}

template <typename Dest>
template <typename... DestArgs>
inline void OstreamWriter<Dest>::Reset(std::tuple<DestArgs...> dest_args,
                                       Options options) {
  OstreamWriterBase::Reset(options.buffer_size());
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get(), options.assumed_pos());
}

template <typename Dest>
inline std::istream* OstreamWriter<Dest>::src_stream() {
  return internal::DetectIstream(dest_.get());
}

template <typename Dest>
void OstreamWriter<Dest>::Done() {
  OstreamWriterBase::Done();
  if (dest_.is_owning()) {
    errno = 0;
    internal::CloseStream(*dest_);
    if (ABSL_PREDICT_FALSE(dest_->fail()) && ABSL_PREDICT_TRUE(healthy())) {
      FailOperation("ostream::close()");
    }
  }
}

template <typename Dest>
bool OstreamWriter<Dest>::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!OstreamWriterBase::FlushImpl(flush_type))) {
    return false;
  }
  switch (flush_type) {
    case FlushType::kFromObject:
      if (!dest_.is_owning()) return true;
      ABSL_FALLTHROUGH_INTENDED;
    case FlushType::kFromProcess:
    case FlushType::kFromMachine:
      errno = 0;
      dest_->flush();
      if (ABSL_PREDICT_FALSE(dest_->fail())) {
        return FailOperation("ostream::flush()");
      }
      return true;
  }
  RIEGELI_ASSERT_UNREACHABLE()
      << "Unknown flush type: " << static_cast<int>(flush_type);
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_OSTREAM_WRITER_H_
