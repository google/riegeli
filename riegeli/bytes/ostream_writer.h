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
#include <istream>
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
#include "riegeli/bytes/stream_internal.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

template <typename Src>
class IStreamReader;
class Reader;

// Template parameter independent part of `OStreamWriter`.
class OStreamWriterBase : public BufferedWriter {
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
             "OStreamWriterBase::Options::set_buffer_size(): "
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
  bool SupportsReadMode() override { return supports_read_mode(); }

 protected:
  explicit OStreamWriterBase(Closed) noexcept : BufferedWriter(kClosed) {}

  explicit OStreamWriterBase(size_t buffer_size);

  OStreamWriterBase(OStreamWriterBase&& that) noexcept;
  OStreamWriterBase& operator=(OStreamWriterBase&& that) noexcept;

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
  bool supports_read_mode();
  bool WriteMode();

  // Invariant:
  //   if `is_open()` then `supports_random_access_ != LazyBoolState::kUnknown`
  LazyBoolState supports_random_access_ = LazyBoolState::kFalse;
  // Invariant:
  //   if `is_open()` then `supports_read_mode_ != LazyBoolState::kUnknown`
  LazyBoolState supports_read_mode_ = LazyBoolState::kFalse;

  AssociatedReader<IStreamReader<std::istream*>> associated_reader_;
  bool read_mode_ = false;

  // Invariant: `start_pos() <= std::numeric_limits<std::streamoff>::max()`
};

// A `Writer` which writes to a `std::ostream`.
//
// `OStreamWriter` supports random access if
// `Options::assumed_pos() == absl::nullopt` and the stream supports random
// access (this is checked by calling `std::ostream::tellp()` and
// `std::ostream::seekp()` to the end and back).
//
// `OStreamWriter` supports `ReadMode()` if the static type of the stream
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
// Until the `OStreamWriter` is closed or no longer used, the `std::ostream`
// must not be closed nor have its position changed, except that if random
// access is not used, careful interleaving of multiple writers is possible:
// `Flush()` is needed before switching to another writer, and `pos()` does not
// take other writers into account.
template <typename Dest = std::ostream*>
class OStreamWriter : public OStreamWriterBase {
 public:
  // Creates a closed `OStreamWriter`.
  explicit OStreamWriter(Closed) noexcept : OStreamWriterBase(kClosed) {}

  // Will write to the stream provided by `dest`.
  explicit OStreamWriter(const Dest& dest, Options options = Options());
  explicit OStreamWriter(Dest&& dest, Options options = Options());

  // Will write to the stream provided by a `Dest` constructed from elements of
  // `dest_args`. This avoids constructing a temporary `Dest` and moving from
  // it.
  template <typename... DestArgs>
  explicit OStreamWriter(std::tuple<DestArgs...> dest_args,
                         Options options = Options());

  OStreamWriter(OStreamWriter&& that) noexcept;
  OStreamWriter& operator=(OStreamWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `OStreamWriter`. This
  // avoids constructing a temporary `OStreamWriter` and moving from it.
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
explicit OStreamWriter(Closed)->OStreamWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit OStreamWriter(const Dest& dest, OStreamWriterBase::Options options =
                                             OStreamWriterBase::Options())
    -> OStreamWriter<std::decay_t<Dest>>;
template <typename Dest>
explicit OStreamWriter(Dest&& dest, OStreamWriterBase::Options options =
                                        OStreamWriterBase::Options())
    -> OStreamWriter<std::decay_t<Dest>>;
template <typename... DestArgs>
explicit OStreamWriter(
    std::tuple<DestArgs...> dest_args,
    OStreamWriterBase::Options options = OStreamWriterBase::Options())
    -> OStreamWriter<DeleteCtad<std::tuple<DestArgs...>>>;
#endif

// Implementation details follow.

inline OStreamWriterBase::OStreamWriterBase(size_t buffer_size)
    : BufferedWriter(buffer_size) {
  // Clear `errno` so that `Initialize()` can attribute failures to opening the
  // stream.
  errno = 0;
}

inline OStreamWriterBase::OStreamWriterBase(OStreamWriterBase&& that) noexcept
    : BufferedWriter(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      supports_random_access_(that.supports_random_access_),
      supports_read_mode_(that.supports_read_mode_),
      associated_reader_(std::move(that.associated_reader_)),
      read_mode_(that.read_mode_) {}

inline OStreamWriterBase& OStreamWriterBase::operator=(
    OStreamWriterBase&& that) noexcept {
  BufferedWriter::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  supports_random_access_ = that.supports_random_access_;
  supports_read_mode_ = that.supports_read_mode_;
  associated_reader_ = std::move(that.associated_reader_);
  read_mode_ = that.read_mode_;
  return *this;
}

inline void OStreamWriterBase::Reset(Closed) {
  BufferedWriter::Reset(kClosed);
  supports_random_access_ = LazyBoolState::kFalse;
  supports_read_mode_ = LazyBoolState::kFalse;
  associated_reader_.Reset();
  read_mode_ = false;
}

inline void OStreamWriterBase::Reset(size_t buffer_size) {
  BufferedWriter::Reset(buffer_size);
  supports_random_access_ = LazyBoolState::kFalse;
  supports_read_mode_ = LazyBoolState::kFalse;
  associated_reader_.Reset();
  read_mode_ = false;
  // Clear `errno` so that `Initialize()` can attribute failures to opening the
  // stream.
  errno = 0;
}

template <typename Dest>
inline OStreamWriter<Dest>::OStreamWriter(const Dest& dest, Options options)
    : OStreamWriterBase(options.buffer_size()), dest_(dest) {
  Initialize(dest_.get(), options.assumed_pos());
}

template <typename Dest>
inline OStreamWriter<Dest>::OStreamWriter(Dest&& dest, Options options)
    : OStreamWriterBase(options.buffer_size()), dest_(std::move(dest)) {
  Initialize(dest_.get(), options.assumed_pos());
}

template <typename Dest>
template <typename... DestArgs>
inline OStreamWriter<Dest>::OStreamWriter(std::tuple<DestArgs...> dest_args,
                                          Options options)
    : OStreamWriterBase(options.buffer_size()), dest_(std::move(dest_args)) {
  Initialize(dest_.get(), options.assumed_pos());
}

template <typename Dest>
inline OStreamWriter<Dest>::OStreamWriter(OStreamWriter&& that) noexcept
    : OStreamWriterBase(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      dest_(std::move(that.dest_)) {}

template <typename Dest>
inline OStreamWriter<Dest>& OStreamWriter<Dest>::operator=(
    OStreamWriter&& that) noexcept {
  OStreamWriterBase::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  dest_ = std::move(that.dest_);
  return *this;
}

template <typename Dest>
inline void OStreamWriter<Dest>::Reset(Closed) {
  OStreamWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void OStreamWriter<Dest>::Reset(const Dest& dest, Options options) {
  OStreamWriterBase::Reset(options.buffer_size());
  dest_.Reset(dest);
  Initialize(dest_.get(), options.assumed_pos());
}

template <typename Dest>
inline void OStreamWriter<Dest>::Reset(Dest&& dest, Options options) {
  OStreamWriterBase::Reset(options.buffer_size());
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), options.assumed_pos());
}

template <typename Dest>
template <typename... DestArgs>
inline void OStreamWriter<Dest>::Reset(std::tuple<DestArgs...> dest_args,
                                       Options options) {
  OStreamWriterBase::Reset(options.buffer_size());
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get(), options.assumed_pos());
}

template <typename Dest>
inline std::istream* OStreamWriter<Dest>::src_stream() {
  return internal::DetectIStream(dest_.get());
}

template <typename Dest>
void OStreamWriter<Dest>::Done() {
  OStreamWriterBase::Done();
  if (dest_.is_owning()) {
    errno = 0;
    internal::CloseStream(*dest_);
    if (ABSL_PREDICT_FALSE(dest_->fail()) && ABSL_PREDICT_TRUE(healthy())) {
      FailOperation("ostream::close()");
    }
  }
}

template <typename Dest>
bool OStreamWriter<Dest>::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!OStreamWriterBase::FlushImpl(flush_type))) {
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
