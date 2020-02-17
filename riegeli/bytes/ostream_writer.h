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
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/resetter.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/stream_dependency.h"

namespace riegeli {

// Template parameter independent part of `OstreamWriter`.
class OstreamWriterBase : public BufferedWriter {
 public:
  class Options {
   public:
    Options() noexcept {}

    // If `absl::nullopt`, `OstreamWriter` will initially get the current stream
    // position, and will set the final stream position on `Close()`. The stream
    // must support random access and the `OstreamWriter` will support random
    // access.
    //
    // If not `absl::nullopt`, this stream position will be assumed initially.
    // The stream does not have to support random access and the `OstreamWriter`
    // will not support random access.
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
    // Default: 64K
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

  bool Flush(FlushType flush_type) override;
  bool SupportsRandomAccess() const override { return random_access_; }
  bool Size(Position* size) override;

 protected:
  OstreamWriterBase() noexcept {}

  explicit OstreamWriterBase(size_t buffer_size, bool random_access);

  OstreamWriterBase(OstreamWriterBase&& that) noexcept;
  OstreamWriterBase& operator=(OstreamWriterBase&& that) noexcept;

  void Reset();
  void Reset(size_t buffer_size, bool random_access);
  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation);
  void Initialize(std::ostream*, absl::optional<Position> assumed_pos);

  bool WriteInternal(absl::string_view src) override;
  bool SeekSlow(Position new_pos) override;

  bool random_access_ = false;

  // Invariant: `start_pos() <= std::numeric_limits<std::streamoff>::max()`
};

// A `Writer` which writes to a `std::ostream`. It supports random access and
// requires the stream to support random access unless
// `Options::set_assumed_pos(pos)`.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the stream being written to. `Dest` must support
// `Dependency<std::ostream*, Dest>`, e.g. `std::ostream*` (not owned, default),
// `std::unique_ptr<std::ostream>` (owned), `std::ofstream` (owned).
//
// The `std::ostream` must not be accessed until the `OstreamWriter` is closed
// or no longer used, except that it is allowed to read the destination of the
// `std::ostream` immediately after `Flush()`.
template <typename Dest = std::ostream*>
class OstreamWriter : public OstreamWriterBase {
 public:
  // Creates a closed `OstreamWriter`.
  OstreamWriter() noexcept {}

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
  void Reset();
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
  void Done() override;

 private:
  // The object providing and possibly owning the stream being written to.
  Dependency<std::ostream*, Dest> dest_;
};

// Implementation details follow.

inline OstreamWriterBase::OstreamWriterBase(size_t buffer_size,
                                            bool random_access)
    : BufferedWriter(buffer_size), random_access_(random_access) {
  // Clear `errno` so that `Initialize()` can attribute failures to opening the
  // stream.
  errno = 0;
}

inline OstreamWriterBase::OstreamWriterBase(OstreamWriterBase&& that) noexcept
    : BufferedWriter(std::move(that)), random_access_(that.random_access_) {}

inline OstreamWriterBase& OstreamWriterBase::operator=(
    OstreamWriterBase&& that) noexcept {
  BufferedWriter::operator=(std::move(that));
  random_access_ = that.random_access_;
  return *this;
}

inline void OstreamWriterBase::Reset() {
  BufferedWriter::Reset();
  random_access_ = false;
}

inline void OstreamWriterBase::Reset(size_t buffer_size, bool random_access) {
  BufferedWriter::Reset(buffer_size);
  random_access_ = random_access;
  // Clear `errno` so that `Initialize()` can attribute failures to opening the
  // stream.
  errno = 0;
}

template <typename Dest>
inline OstreamWriter<Dest>::OstreamWriter(const Dest& dest, Options options)
    : OstreamWriterBase(options.buffer_size(),
                        options.assumed_pos() == absl::nullopt),
      dest_(dest) {
  Initialize(dest_.get(), options.assumed_pos());
}

template <typename Dest>
inline OstreamWriter<Dest>::OstreamWriter(Dest&& dest, Options options)
    : OstreamWriterBase(options.buffer_size(),
                        options.assumed_pos() == absl::nullopt),
      dest_(std::move(dest)) {
  Initialize(dest_.get(), options.assumed_pos());
}

template <typename Dest>
template <typename... DestArgs>
inline OstreamWriter<Dest>::OstreamWriter(std::tuple<DestArgs...> dest_args,
                                          Options options)
    : OstreamWriterBase(options.buffer_size(),
                        options.assumed_pos() == absl::nullopt),
      dest_(std::move(dest_args)) {
  Initialize(dest_.get(), options.assumed_pos());
}

template <typename Dest>
inline OstreamWriter<Dest>::OstreamWriter(OstreamWriter&& that) noexcept
    : OstreamWriterBase(std::move(that)), dest_(std::move(that.dest_)) {}

template <typename Dest>
inline OstreamWriter<Dest>& OstreamWriter<Dest>::operator=(
    OstreamWriter&& that) noexcept {
  OstreamWriterBase::operator=(std::move(that));
  dest_ = std::move(that.dest_);
  return *this;
}

template <typename Dest>
inline void OstreamWriter<Dest>::Reset() {
  OstreamWriterBase::Reset();
  dest_.Reset();
}

template <typename Dest>
inline void OstreamWriter<Dest>::Reset(const Dest& dest, Options options) {
  OstreamWriterBase::Reset(options.buffer_size(),
                           options.assumed_pos() == absl::nullopt);
  dest_.Reset(dest);
  Initialize(dest_.get(), options.assumed_pos());
}

template <typename Dest>
inline void OstreamWriter<Dest>::Reset(Dest&& dest, Options options) {
  OstreamWriterBase::Reset(options.buffer_size(),
                           options.assumed_pos() == absl::nullopt);
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), options.assumed_pos());
}

template <typename Dest>
template <typename... DestArgs>
inline void OstreamWriter<Dest>::Reset(std::tuple<DestArgs...> dest_args,
                                       Options options) {
  OstreamWriterBase::Reset(options.buffer_size(),
                           options.assumed_pos() == absl::nullopt);
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get(), options.assumed_pos());
}

template <typename Dest>
void OstreamWriter<Dest>::Done() {
  PushInternal();
  OstreamWriterBase::Done();
  if (dest_.is_owning()) {
    errno = 0;
    internal::CloseStream(dest_.get());
    if (ABSL_PREDICT_FALSE(dest_->fail()) && ABSL_PREDICT_TRUE(healthy())) {
      FailOperation("ostream::close()");
    }
  }
}

template <typename Dest>
struct Resetter<OstreamWriter<Dest>> : ResetterByReset<OstreamWriter<Dest>> {};

}  // namespace riegeli

#endif  // RIEGELI_BYTES_OSTREAM_WRITER_H_
