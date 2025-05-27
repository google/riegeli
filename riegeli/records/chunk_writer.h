// Copyright 2017 Google LLC
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

#ifndef RIEGELI_RECORDS_CHUNK_WRITER_H_
#define RIEGELI_RECORDS_CHUNK_WRITER_H_

#include <limits>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/types/optional.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/records/block.h"

namespace riegeli {

class Reader;

// A `ChunkWriter` writes chunks of a Riegeli/records file (rather than
// individual records, as `RecordWriter` does) to a destination.
//
// A `ChunkWriter` object can manage a buffer of data to be pushed to the
// destination, which amortizes the overhead of pushing data over multiple
// writes.
class ChunkWriter : public Object {
 public:
  ~ChunkWriter() override;

  // Writes a chunk, pushing data to the destination as needed.
  //
  // Return values:
  //  * `true`  - success (`ok()`)
  //  * `false` - failure (`!ok()`)
  virtual bool WriteChunk(const Chunk& chunk) = 0;

  // Writes padding to reach a 64KB block boundary.
  //
  // Return values:
  //  * `true`  - success (`ok()`)
  //  * `false` - failure (`!ok()`)
  virtual bool PadToBlockBoundary() = 0;

  // Pushes buffered data to the destination.
  //
  // This makes data written so far visible, but in contrast to `Close()`,
  // keeps the possibility to write more data later. What exactly does it mean
  // for data to be visible depends on the destination.
  //
  // The scope of objects to flush and the intended data durability (without a
  // guarantee) are specified by `flush_type`:
  //  * `FlushType::kFromObject`  - Makes data written so far visible in other
  //                                objects, propagating flushing through owned
  //                                dependencies of the given writer.
  //  * `FlushType::kFromProcess` - Makes data written so far visible outside
  //                                the process, propagating flushing through
  //                                dependencies of the given writer.
  //                                This is the default.
  //  * `FlushType::kFromMachine` - Makes data written so far visible outside
  //                                the process and durable in case of operating
  //                                system crash, propagating flushing through
  //                                dependencies of the given writer.
  //
  // Return values:
  //  * `true`  - success (`ok()`)
  //  * `false` - failure (`!ok()`)
  bool Flush(FlushType flush_type = FlushType::kFromProcess);

  // Returns the current byte position. Unchanged by `Close()`.
  Position pos() const { return pos_; }

 protected:
  using Object::Object;

  ChunkWriter(ChunkWriter&& that) noexcept;
  ChunkWriter& operator=(ChunkWriter&& that) noexcept;

  void Reset(Closed);
  void Reset();
  void set_pos(Position pos) { pos_ = pos; }
  void move_pos(Position length) {
    RIEGELI_ASSERT_LE(length, std::numeric_limits<Position>::max() - pos_)
        << "Failed precondition of ChunkWriter::move_pos(): position overflow";
    pos_ += length;
  }
  virtual bool FlushImpl(FlushType flush_type) = 0;

  // Returns the expected position after `WriteChunk()` at the current position.
  Position PosAfterWriteChunk(const ChunkHeader& chunk_header) const;

  // Returns the expected position after `PadToBlockBoundary()` at the current
  // position.
  Position PosAfterPadToBlockBoundary() const;

 private:
  Position pos_ = 0;
};

// Template parameter independent part of `DefaultChunkWriter`.
class DefaultChunkWriterBase : public ChunkWriter {
 public:
  class Options {
   public:
    Options() noexcept {}

    // File position assumed initially.
    //
    // This can be used to prepare a file fragment which can be appended to the
    // target file at the given position.
    //
    // `absl::nullopt` means `DestWriter()->pos()`.
    //
    // Default: `absl::nullopt`.
    Options& set_assumed_pos(absl::optional<Position> assumed_pos) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      assumed_pos_ = assumed_pos;
      return *this;
    }
    Options&& set_assumed_pos(absl::optional<Position> assumed_pos) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_assumed_pos(assumed_pos));
    }
    absl::optional<Position> assumed_pos() const { return assumed_pos_; }

   private:
    absl::optional<Position> assumed_pos_;
  };

  // Returns the Riegeli/records file being written to. Unchanged by `Close()`.
  virtual Writer* DestWriter() const ABSL_ATTRIBUTE_LIFETIME_BOUND = 0;

  bool WriteChunk(const Chunk& chunk) override;
  bool PadToBlockBoundary() override;

 protected:
  using ChunkWriter::ChunkWriter;

  DefaultChunkWriterBase(DefaultChunkWriterBase&& that) noexcept;
  DefaultChunkWriterBase& operator=(DefaultChunkWriterBase&& that) noexcept;

  void Initialize(Writer* dest, Position pos);

  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;

 private:
  bool WriteSection(Reader& src, Position chunk_begin, Position chunk_end,
                    Writer& dest);
  bool WritePadding(Position chunk_begin, Position chunk_end, Writer& dest);
};

// The default `ChunkWriter`. Writes chunks to a byte `Writer`, interleaving
// them with block headers at multiples of the Riegeli/records block size.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the byte `Writer`. `Dest` must support
// `Dependency<Writer*, Dest>`, e.g. `Writer*` (not owned, default),
// `ChainWriter<>` (owned), `std::unique_ptr<Writer>` (owned),
// `Any<Writer*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as `TargetT` of the
// type of the first constructor argument.
//
// The byte `Writer` must not be accessed until the `DefaultChunkWriter` is
// closed or no longer used, except that it is allowed to read the destination
// of the byte `Writer` immediately after `Flush()`.
template <typename Dest = Writer*>
class DefaultChunkWriter : public DefaultChunkWriterBase {
 public:
  // Creates a closed `DefaultChunkWriter`.
  explicit DefaultChunkWriter(Closed) noexcept
      : DefaultChunkWriterBase(kClosed) {}

  // Will write to the byte `Writer` provided by `dest`.
  explicit DefaultChunkWriter(Initializer<Dest> dest,
                              Options options = Options());

  DefaultChunkWriter(DefaultChunkWriter&& that) = default;
  DefaultChunkWriter& operator=(DefaultChunkWriter&& that) = default;

  // Makes `*this` equivalent to a newly constructed `DefaultChunkWriter`. This
  // avoids constructing a temporary `DefaultChunkWriter` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Dest> dest,
                                          Options options = Options());

  // Returns the object providing and possibly owning the byte `Writer`.
  // Unchanged by `Close()`.
  Dest& dest() ABSL_ATTRIBUTE_LIFETIME_BOUND { return dest_.manager(); }
  const Dest& dest() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return dest_.manager();
  }
  Writer* DestWriter() const ABSL_ATTRIBUTE_LIFETIME_BOUND override {
    return dest_.get();
  }

 protected:
  void Done() override;
  bool FlushImpl(FlushType flush_type) override;

 private:
  // The object providing and possibly owning the Riegeli/records file being
  // written to.
  Dependency<Writer*, Dest> dest_;
};

explicit DefaultChunkWriter(Closed) -> DefaultChunkWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit DefaultChunkWriter(
    Dest&& dest,
    DefaultChunkWriterBase::Options options = DefaultChunkWriterBase::Options())
    -> DefaultChunkWriter<TargetT<Dest>>;

// Specialization of `DependencyImpl<ChunkWriter*, Manager>` adapted from
// `DependencyImpl<Writer*, Manager>` by wrapping `Manager` in
// `DefaultChunkWriter<Manager>`.
template <typename Manager>
class DependencyImpl<
    ChunkWriter*, Manager,
    std::enable_if_t<SupportsDependency<Writer*, Manager>::value>> {
 public:
  DependencyImpl() noexcept : chunk_writer_(kClosed) {}

  explicit DependencyImpl(Initializer<Manager> manager)
      : chunk_writer_(std::move(manager)) {}

  ABSL_ATTRIBUTE_REINITIALIZES void Reset() { chunk_writer_.Reset(kClosed); }

  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Manager> manager) {
    chunk_writer_.Reset(std::move(manager));
  }

  Manager& manager() ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return chunk_writer_.dest();
  }
  const Manager& manager() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return chunk_writer_.dest();
  }

  DefaultChunkWriter<Manager>* get() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return &chunk_writer_;
  }

  static constexpr bool kIsOwning = true;

  static constexpr bool kIsStable = false;

 protected:
  DependencyImpl(DependencyImpl&& that) = default;
  DependencyImpl& operator=(DependencyImpl&& that) = default;

  ~DependencyImpl() = default;

 private:
  mutable DefaultChunkWriter<Manager> chunk_writer_;
};

// Implementation details follow.

inline ChunkWriter::ChunkWriter(ChunkWriter&& that) noexcept
    : Object(static_cast<Object&&>(that)), pos_(that.pos_) {}

inline ChunkWriter& ChunkWriter::operator=(ChunkWriter&& that) noexcept {
  Object::operator=(static_cast<Object&&>(that));
  pos_ = that.pos_;
  return *this;
}

inline void ChunkWriter::Reset(Closed) {
  Object::Reset(kClosed);
  pos_ = 0;
}

inline void ChunkWriter::Reset() {
  Object::Reset();
  pos_ = 0;
}

inline bool ChunkWriter::Flush(FlushType flush_type) {
  return FlushImpl(flush_type);
}

inline Position ChunkWriter::PosAfterWriteChunk(
    const ChunkHeader& chunk_header) const {
  return records_internal::ChunkEnd(chunk_header, pos_);
}

inline Position ChunkWriter::PosAfterPadToBlockBoundary() const {
  Position length = records_internal::RemainingInBlock(pos_);
  if (length == 0) return pos_;
  if (length < ChunkHeader::size()) {
    // Not enough space for a padding chunk in this block. Write one more block.
    length += records_internal::kBlockSize;
  }
  return pos_ + length;
}

inline DefaultChunkWriterBase::DefaultChunkWriterBase(
    DefaultChunkWriterBase&& that) noexcept
    : ChunkWriter(static_cast<ChunkWriter&&>(that)) {}

inline DefaultChunkWriterBase& DefaultChunkWriterBase::operator=(
    DefaultChunkWriterBase&& that) noexcept {
  ChunkWriter::operator=(static_cast<ChunkWriter&&>(that));
  return *this;
}

template <typename Dest>
inline DefaultChunkWriter<Dest>::DefaultChunkWriter(Initializer<Dest> dest,
                                                    Options options)
    : dest_(std::move(dest)) {
  Initialize(dest_.get(), options.assumed_pos().value_or(dest_->pos()));
}

template <typename Dest>
inline void DefaultChunkWriter<Dest>::Reset(Closed) {
  DefaultChunkWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void DefaultChunkWriter<Dest>::Reset(Initializer<Dest> dest,
                                            Options options) {
  DefaultChunkWriterBase::Reset();
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), options.assumed_pos().value_or(dest_->pos()));
}

template <typename Dest>
void DefaultChunkWriter<Dest>::Done() {
  DefaultChunkWriterBase::Done();
  if (dest_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) {
      FailWithoutAnnotation(dest_->status());
    }
  }
}

template <typename Dest>
bool DefaultChunkWriter<Dest>::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (flush_type != FlushType::kFromObject || dest_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Flush(flush_type))) {
      return FailWithoutAnnotation(dest_->status());
    }
  }
  return true;
}

}  // namespace riegeli

#endif  // RIEGELI_RECORDS_CHUNK_WRITER_H_
