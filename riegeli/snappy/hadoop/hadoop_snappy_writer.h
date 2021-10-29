// Copyright 2020 Google LLC
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

#ifndef RIEGELI_SNAPPY_HADOOP_HADOOP_SNAPPY_WRITER_H_
#define RIEGELI_SNAPPY_HADOOP_HADOOP_SNAPPY_WRITER_H_

#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/pushable_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Template parameter independent part of `HadoopSnappyWriter`.
class HadoopSnappyWriterBase : public PushableWriter {
 public:
  class Options {
   public:
    Options() noexcept {}

    // Expected uncompressed size, or `absl::nullopt` if unknown. This may
    // improve performance and memory usage.
    //
    // If the size hint turns out to not match reality, nothing breaks.
    //
    // Default: `absl::nullopt`.
    Options& set_size_hint(absl::optional<Position> size_hint) & {
      size_hint_ = size_hint;
      return *this;
    }
    Options&& set_size_hint(absl::optional<Position> size_hint) && {
      return std::move(set_size_hint(size_hint));
    }
    absl::optional<Position> size_hint() const { return size_hint_; }

   private:
    absl::optional<Position> size_hint_;
  };

  // Returns the compressed `Writer`. Unchanged by `Close()`.
  virtual Writer* dest_writer() = 0;
  virtual const Writer* dest_writer() const = 0;

 protected:
  explicit HadoopSnappyWriterBase(Closed) noexcept : PushableWriter(kClosed) {}

  explicit HadoopSnappyWriterBase(absl::optional<Position> size_hint);

  HadoopSnappyWriterBase(HadoopSnappyWriterBase&& that) noexcept;
  HadoopSnappyWriterBase& operator=(HadoopSnappyWriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset(absl::optional<Position> size_hint);
  void Initialize(Writer* dest);

  // `HadoopSnappyWriterBase` overrides `Writer::DefaultAnnotateStatus()` to
  // annotate the status with the current position, clarifying that this is the
  // uncompressed position. A status propagated from `*dest_writer()` might
  // carry annotation with the compressed position.
  ABSL_ATTRIBUTE_COLD void DefaultAnnotateStatus() override;
  bool PushBehindScratch() override;
  bool FlushBehindScratch(FlushType flush_type);

 private:
  // Compresses buffered data, but unlike `PushSlow()`, does not ensure that a
  // buffer is allocated.
  //
  // Precondition: `healthy()`
  //
  // Postcondition: `start_to_cursor() == 0`
  bool PushInternal(Writer& dest);

  Position size_hint_ = 0;
  // Buffered uncompressed data.
  Buffer uncompressed_;

  // Invariants if scratch is not used:
  //   `start() == nullptr` or `start() == uncompressed_.data()`
  //   `start_to_limit() <= snappy::kBlockSize`
};

// A `Writer` which compresses data with Hadoop Snappy format before passing it
// to another `Writer`:
// https://github.com/kubo/snzip#hadoop-snappy-format
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the compressed `Writer`. `Dest` must support
// `Dependency<Writer*, Dest>`, e.g. `Writer*` (not owned, default),
// `std::unique_ptr<Writer>` (owned), `ChainWriter<>` (owned).
//
// By relying on CTAD the template argument can be deduced as the value type of
// the first constructor argument. This requires C++17.
//
// The compressed `Writer` must not be accessed until the `HadoopSnappyWriter`
// is closed or no longer used.
template <typename Dest = Writer*>
class HadoopSnappyWriter : public HadoopSnappyWriterBase {
 public:
  // Creates a closed `HadoopSnappyWriter`.
  explicit HadoopSnappyWriter(Closed) noexcept
      : HadoopSnappyWriterBase(kClosed) {}

  // Will write to the compressed `Writer` provided by `dest`.
  explicit HadoopSnappyWriter(const Dest& dest, Options options = Options());
  explicit HadoopSnappyWriter(Dest&& dest, Options options = Options());

  // Will write to the compressed `Writer` provided by a `Dest` constructed from
  // elements of `dest_args`. This avoids constructing a temporary `Dest` and
  // moving from it.
  template <typename... DestArgs>
  explicit HadoopSnappyWriter(std::tuple<DestArgs...> dest_args,
                              Options options = Options());

  HadoopSnappyWriter(HadoopSnappyWriter&& that) noexcept;
  HadoopSnappyWriter& operator=(HadoopSnappyWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `HadoopSnappyWriter`. This
  // avoids constructing a temporary `HadoopSnappyWriter` and moving from it.
  void Reset(Closed);
  void Reset(const Dest& dest, Options options = Options());
  void Reset(Dest&& dest, Options options = Options());
  template <typename... DestArgs>
  void Reset(std::tuple<DestArgs...> dest_args, Options options = Options());

  // Returns the object providing and possibly owning the compressed `Writer`.
  // Unchanged by `Close()`.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  Writer* dest_writer() override { return dest_.get(); }
  const Writer* dest_writer() const override { return dest_.get(); }

 protected:
  void Done() override;
  bool FlushImpl(FlushType flush_type) override;

 private:
  // The object providing and possibly owning the compressed `Writer`.
  Dependency<Writer*, Dest> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit HadoopSnappyWriter(Closed)->HadoopSnappyWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit HadoopSnappyWriter(
    const Dest& dest,
    HadoopSnappyWriterBase::Options options = HadoopSnappyWriterBase::Options())
    -> HadoopSnappyWriter<std::decay_t<Dest>>;
template <typename Dest>
explicit HadoopSnappyWriter(
    Dest&& dest,
    HadoopSnappyWriterBase::Options options = HadoopSnappyWriterBase::Options())
    -> HadoopSnappyWriter<std::decay_t<Dest>>;
template <typename... DestArgs>
explicit HadoopSnappyWriter(
    std::tuple<DestArgs...> dest_args,
    HadoopSnappyWriterBase::Options options = HadoopSnappyWriterBase::Options())
    -> HadoopSnappyWriter<DeleteCtad<std::tuple<DestArgs...>>>;
#endif

// Implementation details follow.

inline HadoopSnappyWriterBase::HadoopSnappyWriterBase(
    absl::optional<Position> size_hint)
    : size_hint_(size_hint.value_or(0)) {}

inline HadoopSnappyWriterBase::HadoopSnappyWriterBase(
    HadoopSnappyWriterBase&& that) noexcept
    : PushableWriter(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      size_hint_(that.size_hint_),
      uncompressed_(std::move(that.uncompressed_)) {}

inline HadoopSnappyWriterBase& HadoopSnappyWriterBase::operator=(
    HadoopSnappyWriterBase&& that) noexcept {
  PushableWriter::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  size_hint_ = that.size_hint_;
  uncompressed_ = std::move(that.uncompressed_);
  return *this;
}

inline void HadoopSnappyWriterBase::Reset(Closed) {
  PushableWriter::Reset(kClosed);
  size_hint_ = 0;
}

inline void HadoopSnappyWriterBase::Reset(absl::optional<Position> size_hint) {
  PushableWriter::Reset();
  size_hint_ = size_hint.value_or(0);
}

template <typename Dest>
inline HadoopSnappyWriter<Dest>::HadoopSnappyWriter(const Dest& dest,
                                                    Options options)
    : HadoopSnappyWriterBase(options.size_hint()), dest_(dest) {
  Initialize(dest_.get());
}

template <typename Dest>
inline HadoopSnappyWriter<Dest>::HadoopSnappyWriter(Dest&& dest,
                                                    Options options)
    : HadoopSnappyWriterBase(options.size_hint()), dest_(std::move(dest)) {
  Initialize(dest_.get());
}

template <typename Dest>
template <typename... DestArgs>
inline HadoopSnappyWriter<Dest>::HadoopSnappyWriter(
    std::tuple<DestArgs...> dest_args, Options options)
    : HadoopSnappyWriterBase(options.size_hint()), dest_(std::move(dest_args)) {
  Initialize(dest_.get());
}

template <typename Dest>
inline HadoopSnappyWriter<Dest>::HadoopSnappyWriter(
    HadoopSnappyWriter&& that) noexcept
    : HadoopSnappyWriterBase(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      dest_(std::move(that.dest_)) {}

template <typename Dest>
inline HadoopSnappyWriter<Dest>& HadoopSnappyWriter<Dest>::operator=(
    HadoopSnappyWriter&& that) noexcept {
  HadoopSnappyWriterBase::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  dest_ = std::move(that.dest_);
  return *this;
}

template <typename Dest>
inline void HadoopSnappyWriter<Dest>::Reset(Closed) {
  HadoopSnappyWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void HadoopSnappyWriter<Dest>::Reset(const Dest& dest, Options options) {
  HadoopSnappyWriterBase::Reset(options.size_hint());
  dest_.Reset(dest);
  Initialize(dest_.get());
}

template <typename Dest>
inline void HadoopSnappyWriter<Dest>::Reset(Dest&& dest, Options options) {
  HadoopSnappyWriterBase::Reset(options.size_hint());
  dest_.Reset(std::move(dest));
  Initialize(dest_.get());
}

template <typename Dest>
template <typename... DestArgs>
inline void HadoopSnappyWriter<Dest>::Reset(std::tuple<DestArgs...> dest_args,
                                            Options options) {
  HadoopSnappyWriterBase::Reset(options.size_hint());
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get());
}

template <typename Dest>
void HadoopSnappyWriter<Dest>::Done() {
  HadoopSnappyWriterBase::Done();
  if (dest_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) Fail(*dest_);
  }
}

template <typename Dest>
bool HadoopSnappyWriter<Dest>::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!HadoopSnappyWriterBase::FlushImpl(flush_type))) {
    return false;
  }
  if (flush_type != FlushType::kFromObject || dest_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Flush(flush_type))) return Fail(*dest_);
  }
  return true;
}

}  // namespace riegeli

#endif  // RIEGELI_SNAPPY_HADOOP_HADOOP_SNAPPY_WRITER_H_
