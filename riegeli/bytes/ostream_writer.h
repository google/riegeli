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

#include <stdint.h>

#include <cerrno>
#include <istream>
#include <optional>
#include <ostream>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/iostream_internal.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

template <typename Src>
class IStreamReader;
class Reader;

// Template parameter independent part of `OStreamWriter`.
class OStreamWriterBase : public BufferedWriter {
 public:
  class Options : public BufferOptionsBase<Options> {
   public:
    Options() noexcept {}

    // If `std::nullopt`, the current position reported by `pos()` corresponds
    // to the current stream position if possible, otherwise 0 is assumed as the
    // initial position. Random access is supported if the stream supports
    // random access.
    //
    // If not `std::nullopt`, this position is assumed initially, to be reported
    // by `pos()`. It does not need to correspond to the current stream
    // position. Random access is not supported.
    //
    // Warning: On Windows this must not be `std::nullopt` if the stream is a
    // `std::ofstream` or `std::fstream` opened in text mode.
    //
    // Default: `std::nullopt`.
    Options& set_assumed_pos(std::optional<Position> assumed_pos) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      assumed_pos_ = assumed_pos;
      return *this;
    }
    Options&& set_assumed_pos(std::optional<Position> assumed_pos) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_assumed_pos(assumed_pos));
    }
    std::optional<Position> assumed_pos() const { return assumed_pos_; }

    // If `assumed_pos()` is not set, `assumed_append()` should be set to `true`
    // if the `std::ostream` refers a file open in append mode, i.e. if all
    // writes happen at the end. This lets `SupportsRandomAccess()` correctly
    // return `false`.
    //
    // Default: `false`.
    Options& set_assumed_append(bool assumed_append) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      assumed_append_ = assumed_append;
      return *this;
    }
    Options&& set_assumed_append(bool assumed_append) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_assumed_append(assumed_append));
    }
    bool assumed_append() const { return assumed_append_; }

   private:
    std::optional<Position> assumed_pos_;
    bool assumed_append_ = false;
  };

  // Returns the stream being written to. Unchanged by `Close()`.
  virtual std::ostream* DestStream() const ABSL_ATTRIBUTE_LIFETIME_BOUND = 0;

  bool SupportsRandomAccess() override;
  bool SupportsTruncate() override { return false; }
  bool SupportsReadMode() override;

 protected:
  explicit OStreamWriterBase(Closed) noexcept : BufferedWriter(kClosed) {}

  explicit OStreamWriterBase(BufferOptions buffer_options);

  OStreamWriterBase(OStreamWriterBase&& that) noexcept;
  OStreamWriterBase& operator=(OStreamWriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset(BufferOptions buffer_options);
  void Initialize(std::ostream* dest, std::optional<Position> assumed_pos,
                  bool assumed_append);
  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation);

  // Returns the stream pointer as `std::istream*` if the static type of the
  // destination derives from `std::istream`, otherwise returns `nullptr`.
  virtual std::istream* SrcStream() const = 0;

  void Done() override;
  bool WriteInternal(absl::string_view src) override;
  bool FlushBehindBuffer(absl::string_view src, FlushType flush_type) override;
  bool SeekBehindBuffer(Position new_pos) override;
  std::optional<Position> SizeBehindBuffer() override;
  Reader* ReadModeBehindBuffer(Position initial_pos) override;

 private:
  // Encodes a `bool` or a marker that the value is not resolved yet.
  enum class LazyBoolState : uint8_t { kUnknown, kTrue, kFalse };

  absl::Status FailedOperationStatus(absl::string_view operation);

  bool WriteMode();

  LazyBoolState supports_random_access_ = LazyBoolState::kUnknown;
  LazyBoolState supports_read_mode_ = LazyBoolState::kUnknown;
  absl::Status random_access_status_;
  absl::Status read_mode_status_;

  AssociatedReader<IStreamReader<std::istream*>> associated_reader_;
  bool read_mode_ = false;

  // Invariant: `start_pos() <= std::numeric_limits<std::streamoff>::max()`
};

// A `Writer` which writes to a `std::ostream`.
//
// `OStreamWriter` supports random access if
// `Options::assumed_pos() == std::nullopt` and the stream supports random
// access (this is checked by calling `std::ostream::tellp()` and
// `std::ostream::seekp()` to the end and back).
//
// `OStreamWriter` supports `ReadMode()` if the static type of the stream
// derives also from `std::istream`.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the stream being written to. `Dest` must support
// `Dependency<std::ostream*, Dest>`, e.g. `std::ostream*` (not owned, default),
// `std::ofstream` (owned), `std::unique_ptr<std::ostream>` (owned),
// `Any<std::ostream*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as `TargetT` of the
// type of the first constructor argument.
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
  explicit OStreamWriter(Initializer<Dest> dest, Options options = Options());

  OStreamWriter(OStreamWriter&& that) = default;
  OStreamWriter& operator=(OStreamWriter&& that) = default;

  // Makes `*this` equivalent to a newly constructed `OStreamWriter`. This
  // avoids constructing a temporary `OStreamWriter` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Dest> dest,
                                          Options options = Options());

  // Returns the object providing and possibly owning the stream being written
  // to. Unchanged by `Close()`.
  Dest& dest() ABSL_ATTRIBUTE_LIFETIME_BOUND { return dest_.manager(); }
  const Dest& dest() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return dest_.manager();
  }
  std::ostream* DestStream() const ABSL_ATTRIBUTE_LIFETIME_BOUND override {
    return dest_.get();
  }

 protected:
  std::istream* SrcStream() const override;

  void Done() override;
  bool FlushImpl(FlushType flush_type) override;

 private:
  // The object providing and possibly owning the stream being written to.
  Dependency<std::ostream*, Dest> dest_;
};

explicit OStreamWriter(Closed) -> OStreamWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit OStreamWriter(Dest&& dest, OStreamWriterBase::Options options =
                                        OStreamWriterBase::Options())
    -> OStreamWriter<TargetT<Dest>>;

// Implementation details follow.

inline OStreamWriterBase::OStreamWriterBase(BufferOptions buffer_options)
    : BufferedWriter(buffer_options) {
  // Clear `errno` so that `Initialize()` can attribute failures to opening the
  // stream.
  errno = 0;
}

inline OStreamWriterBase::OStreamWriterBase(OStreamWriterBase&& that) noexcept
    : BufferedWriter(static_cast<BufferedWriter&&>(that)),
      supports_random_access_(
          std::exchange(that.supports_random_access_, LazyBoolState::kUnknown)),
      supports_read_mode_(
          std::exchange(that.supports_read_mode_, LazyBoolState::kUnknown)),
      random_access_status_(std::move(that.random_access_status_)),
      read_mode_status_(std::move(that.read_mode_status_)),
      associated_reader_(std::move(that.associated_reader_)),
      read_mode_(that.read_mode_) {}

inline OStreamWriterBase& OStreamWriterBase::operator=(
    OStreamWriterBase&& that) noexcept {
  BufferedWriter::operator=(static_cast<BufferedWriter&&>(that));
  supports_random_access_ =
      std::exchange(that.supports_random_access_, LazyBoolState::kUnknown);
  supports_read_mode_ =
      std::exchange(that.supports_read_mode_, LazyBoolState::kUnknown);
  random_access_status_ = std::move(that.random_access_status_);
  read_mode_status_ = std::move(that.read_mode_status_);
  associated_reader_ = std::move(that.associated_reader_);
  read_mode_ = that.read_mode_;
  return *this;
}

inline void OStreamWriterBase::Reset(Closed) {
  BufferedWriter::Reset(kClosed);
  supports_random_access_ = LazyBoolState::kUnknown;
  supports_read_mode_ = LazyBoolState::kUnknown;
  random_access_status_ = absl::OkStatus();
  read_mode_status_ = absl::OkStatus();
  associated_reader_.Reset();
  read_mode_ = false;
}

inline void OStreamWriterBase::Reset(BufferOptions buffer_options) {
  BufferedWriter::Reset(buffer_options);
  supports_random_access_ = LazyBoolState::kUnknown;
  supports_read_mode_ = LazyBoolState::kUnknown;
  random_access_status_ = absl::OkStatus();
  read_mode_status_ = absl::OkStatus();
  associated_reader_.Reset();
  read_mode_ = false;
  // Clear `errno` so that `Initialize()` can attribute failures to opening the
  // stream.
  errno = 0;
}

template <typename Dest>
inline OStreamWriter<Dest>::OStreamWriter(Initializer<Dest> dest,
                                          Options options)
    : OStreamWriterBase(options.buffer_options()), dest_(std::move(dest)) {
  Initialize(dest_.get(), options.assumed_pos(), options.assumed_append());
}

template <typename Dest>
inline void OStreamWriter<Dest>::Reset(Closed) {
  OStreamWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void OStreamWriter<Dest>::Reset(Initializer<Dest> dest,
                                       Options options) {
  OStreamWriterBase::Reset(options.buffer_options());
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), options.assumed_pos(), options.assumed_append());
}

template <typename Dest>
inline std::istream* OStreamWriter<Dest>::SrcStream() const {
  return iostream_internal::DetectIStream(dest_.get());
}

template <typename Dest>
void OStreamWriter<Dest>::Done() {
  OStreamWriterBase::Done();
  if (dest_.IsOwning()) {
    errno = 0;
    iostream_internal::Close(*dest_);
    if (ABSL_PREDICT_FALSE(dest_->fail()) && ABSL_PREDICT_TRUE(ok())) {
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
      if (!dest_.IsOwning()) return true;
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
  RIEGELI_ASSUME_UNREACHABLE()
      << "Unknown flush type: " << static_cast<int>(flush_type);
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_OSTREAM_WRITER_H_
