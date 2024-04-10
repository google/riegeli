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

#ifndef RIEGELI_RECORDS_CHUNK_READER_H_
#define RIEGELI_RECORDS_CHUNK_READER_H_

#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/types/optional.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/records/block.h"
#include "riegeli/records/skipped_region.h"

namespace riegeli {

// Template parameter independent part of `DefaultChunkReader`.
class DefaultChunkReaderBase : public Object {
 public:
  // Returns the Riegeli/records file being read from. Unchanged by `Close()`.
  virtual Reader* SrcReader() const = 0;

  // Ensures that the file looks like a valid Riegeli/Records file.
  //
  // Reading the file already checks whether it is valid. `CheckFileFormat()`
  // can verify this before (or instead of) performing other operations.
  //
  // Return values:
  //  * `true`                 - success
  //  * `false` (when `ok()`)  - source ends
  //  * `false` (when `!ok()`) - failure
  bool CheckFileFormat();

  // Reads the next chunk.
  //
  // Return values:
  //  * `true`                 - success (`chunk` is set)
  //  * `false` (when `ok()`)  - source ends
  //  * `false` (when `!ok()`) - failure
  bool ReadChunk(Chunk& chunk);

  // Reads the next chunk header, from same chunk which will be read by an
  // immediately following `ReadChunk()`.
  //
  // If `chunk_header != nullptr`, `*chunk_header` is set to the chunk header,
  // valid until the next non-const function of the `ChunkReader`.
  //
  // Return values:
  //  * `true`                 - success (`*chunk_header` is set)
  //  * `false` (when `ok()`)  - source ends
  //  * `false` (when `!ok()`) - failure
  bool PullChunkHeader(const ChunkHeader** chunk_header);

  // If `!ok()` and the failure was caused by invalid file contents, then
  // `Recover()` tries to recover from the failure and allow reading again by
  // skipping over the invalid region.
  //
  // If `Close()` failed and the failure was caused by truncated file contents,
  // then `Recover()` returns `true`. The `ChunkReader` remains closed.
  //
  // If `ok()`, or if `!ok()` but the failure was not caused by invalid file
  // contents, then `Recover()` returns false.
  //
  // If `skipped_region != nullptr`, `*skipped_region` is set to the position of
  // the skipped region on success.
  //
  // Return values:
  //  * `true`  - success
  //  * `false` - failure not caused by invalid file contents
  bool Recover(SkippedRegion* skipped_region = nullptr);

  // Returns the current position, which is a chunk boundary (except that if
  // the source ends in a skipped region, it can be greater than file size and
  // it can be a block boundary).
  //
  // `ReadChunk()` and `PullChunkHeader()` return a chunk which begins at
  // `pos()` if they succeed.
  //
  // `pos()` is unchanged by `Close()`.
  Position pos() const { return pos_; }

  // Returns `true` if this `ChunkReader` supports `Seek()`,
  // `SeekToChunkContaining()`, `SeekToChunkAfter()`, and `Size()`.
  bool SupportsRandomAccess();

  // Seeks to `new_pos`, which should be a chunk boundary.
  //
  // Return values:
  //  * `true`  - success
  //  * `false` - failure (`!ok()`)
  bool Seek(Position new_pos);

  // Seeks to the nearest chunk boundary before or at `new_pos` if the position
  // corresponds to some numeric record position in the following chunk (i.e. is
  // less than `num_records` bytes after chunk beginning), otherwise seeks to
  // the nearest chunk boundary at or after the given position.
  //
  // Return values:
  //  * `true`  - success
  //  * `false` - failure (`!ok()`)
  bool SeekToChunkContaining(Position new_pos);

  // Seeks to the nearest chunk boundary at or before `new_pos`.
  //
  // Return values:
  //  * `true`  - success
  //  * `false` - failure (`!ok()`)
  bool SeekToChunkBefore(Position new_pos);

  // Seeks to the nearest chunk boundary at or after `new_pos`.
  //
  // Return values:
  //  * `true`  - success
  //  * `false` - failure (`!ok()`)
  bool SeekToChunkAfter(Position new_pos);

  // Returns the size of the file, i.e. the position corresponding to its end.
  //
  // Returns `absl::nullopt` on failure (`!ok()`).
  absl::optional<Position> Size();

 protected:
  using Object::Object;

  DefaultChunkReaderBase(DefaultChunkReaderBase&& that) noexcept;
  DefaultChunkReaderBase& operator=(DefaultChunkReaderBase&& that) noexcept;

  void Reset(Closed);
  void Reset();
  void Initialize(Reader* src);

  void Done() override;
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;

 private:
  enum class Recoverable { kNo, kHaveChunk, kFindChunk };
  enum class WhichChunk { kContaining, kBefore, kAfter };

  // Interprets a `false` result from `src` reading or seeking function.
  //
  // End of file (i.e. if `ok()`) is propagated, setting `truncated_` if it was
  // in the middle of a chunk.
  //
  // Always returns `false`.
  bool FailReading(const Reader& src);

  // Interprets a `false` result from `src` reading or seeking function.
  //
  // End of file (i.e. if `ok()`) fails the `ChunkReader`.
  //
  // Always returns `false`.
  bool FailSeeking(const Reader& src, Position new_pos);

  // Reads or continues reading `chunk_.header`.
  bool ReadChunkHeader();

  // Reads or continues reading `block_header_`.
  //
  // Preconditions:
  //   `ok()`
  //   `records_internal::RemainingInBlockHeader(SrcReader()->pos()) > 0`
  bool ReadBlockHeader();

  // Shared implementation of `SeekToChunkContaining()`, `SeekToChunkBefore()`,
  // and `SeekToChunkAfter()`.
  //
  // This template is defined and used only in chunk_reader.cc.
  template <WhichChunk which_chunk>
  bool SeekToChunk(Position new_pos);

  // If `true`, the source is truncated (in the middle of a chunk) at the
  // current position. If the source does not grow, `Close()` will fail.
  //
  // Invariant: if `truncated_` then `SrcReader()->pos() > pos_`
  bool truncated_ = false;

  // Beginning of the current chunk.
  //
  // If `pos_ > SrcReader()->pos()`, the source ends in a skipped region. In
  // this case `pos_` can be a block boundary instead of a chunk boundary.
  Position pos_ = 0;

  // Chunk header and chunk data, filled to the point derived from `pos_` and
  // `SrcReader()->pos()`.
  Chunk chunk_;

  // Block header, filled to the point derived from `SrcReader()->pos()`.
  records_internal::BlockHeader block_header_;

  // Whether `Recover()` is applicable, and if so, how it should be performed:
  //
  //  * `Recoverable::kNo`        - `Recover()` is not applicable
  //  * `Recoverable::kHaveChunk` - `Recover()` assumes that a chunk starts
  //                                at `recoverable_pos_`
  //  * `Recoverable::kFindChunk` - `Recover()` finds a block after
  //                                `recoverable_pos_`, and a chunk after
  //                                 the block
  //
  // Invariants:
  //   if `ok()` then `recoverable_ == Recoverable::kNo`
  //   if `!is_open()` then `recoverable_ == Recoverable::kNo ||
  //                         recoverable_ == Recoverable::kHaveChunk`
  Recoverable recoverable_ = Recoverable::kNo;

  // If `recoverable_ != Recoverable::kNo`, the position to start recovery from.
  //
  // Invariant:
  //   if `recoverable_ != Recoverable::kNo` then `recoverable_pos_ >= pos_`
  Position recoverable_pos_ = 0;
};

// A `ChunkReader` reads chunks of a Riegeli/records file (rather than
// individual records, as `RecordReader` does).
//
// TODO: If the need arises, `ChunkReader` can be made more abstract
// than `DefaultChunkReaderBase`, similarly to `ChunkWriter`.
using ChunkReader = DefaultChunkReaderBase;

// The default `ChunkReader`. Reads chunks from a byte `Reader`, expecting them
// to be interleaved with block headers at multiples of the Riegeli/records
// block size.
//
// `DefaultChunkReader` can be used together with `DefaultChunkWriter` to
// rewrite Riegeli/records files without recompressing chunks, e.g. to
// concatenate files.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the byte `Reader`. `Src` must support
// `Dependency<Reader*, Src>`, e.g. `Reader*` (not owned, default),
// `ChainReader<>` (owned), `std::unique_ptr<Reader>` (owned),
// `AnyDependency<Reader*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as
// `InitializerTargetT` of the type of the first constructor argument.
// This requires C++17.
//
// The byte `Reader` must not be accessed until the `DefaultChunkReader` is
// closed or no longer used.
template <typename Src = Reader*>
class DefaultChunkReader : public DefaultChunkReaderBase {
 public:
  // Creates a closed `DefaultChunkReader`.
  explicit DefaultChunkReader(Closed) : DefaultChunkReaderBase(kClosed) {}

  // Will read from the byte `Reader` provided by `src`.
  explicit DefaultChunkReader(Initializer<Src> src);

  DefaultChunkReader(DefaultChunkReader&& that) noexcept;
  DefaultChunkReader& operator=(DefaultChunkReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `DefaultChunkReader`. This
  // avoids constructing a temporary `DefaultChunkReader` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Src> src);

  // Returns the object providing and possibly owning the byte `Reader`.
  // Unchanged by `Close()`.
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  Reader* SrcReader() const override { return src_.get(); }

 protected:
  void Done() override;

 private:
  // The object providing and possibly owning the Riegeli/records file being
  // read from.
  Dependency<Reader*, Src> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit DefaultChunkReader(Closed) -> DefaultChunkReader<DeleteCtad<Closed>>;
template <typename Src>
explicit DefaultChunkReader(Src&& src)
    -> DefaultChunkReader<InitializerTargetT<Src>>;
#endif

// Specialization of `DependencyImpl<ChunkReader*, Manager>` adapted from
// `DependencyImpl<Reader*, Manager>` by wrapping `Manager` in
// `DefaultChunkReader<Manager>`.
template <typename Manager>
class DependencyImpl<
    ChunkReader*, Manager,
    std::enable_if_t<IsValidDependency<Reader*, Manager>::value>> {
 public:
  DependencyImpl() noexcept : chunk_reader_(kClosed) {}

  explicit DependencyImpl(Initializer<Manager> manager)
      : chunk_reader_(std::move(manager)) {}

  ABSL_ATTRIBUTE_REINITIALIZES void Reset() { chunk_reader_.Reset(kClosed); }

  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Manager> manager) {
    chunk_reader_.Reset(std::move(manager));
  }

  Manager& manager() { return chunk_reader_.src(); }
  const Manager& manager() const { return chunk_reader_.src(); }

  DefaultChunkReader<Manager>* get() const { return &chunk_reader_; }

  static constexpr bool kIsOwning = true;

  static constexpr bool kIsStable = false;

 protected:
  DependencyImpl(DependencyImpl&& that) = default;
  DependencyImpl& operator=(DependencyImpl&& that) = default;

  ~DependencyImpl() = default;

 private:
  mutable DefaultChunkReader<Manager> chunk_reader_;
};

// Implementation details follow.

inline DefaultChunkReaderBase::DefaultChunkReaderBase(
    DefaultChunkReaderBase&& that) noexcept
    : Object(static_cast<Object&&>(that)),
      truncated_(that.truncated_),
      pos_(that.pos_),
      chunk_(std::move(that.chunk_)),
      block_header_(that.block_header_),
      recoverable_(std::exchange(that.recoverable_, Recoverable::kNo)),
      recoverable_pos_(that.recoverable_pos_) {}

inline DefaultChunkReaderBase& DefaultChunkReaderBase::operator=(
    DefaultChunkReaderBase&& that) noexcept {
  Object::operator=(static_cast<Object&&>(that));
  truncated_ = that.truncated_;
  pos_ = that.pos_;
  chunk_ = that.chunk_;
  block_header_ = that.block_header_;
  recoverable_ = std::exchange(that.recoverable_, Recoverable::kNo);
  recoverable_pos_ = that.recoverable_pos_;
  return *this;
}

inline void DefaultChunkReaderBase::Reset(Closed) {
  Object::Reset(kClosed);
  truncated_ = false;
  pos_ = 0;
  chunk_.Reset();
  recoverable_ = Recoverable::kNo;
  recoverable_pos_ = 0;
}

inline void DefaultChunkReaderBase::Reset() {
  Object::Reset();
  truncated_ = false;
  pos_ = 0;
  chunk_.Clear();
  recoverable_ = Recoverable::kNo;
  recoverable_pos_ = 0;
}

template <typename Src>
inline DefaultChunkReader<Src>::DefaultChunkReader(Initializer<Src> src)
    : src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
inline DefaultChunkReader<Src>::DefaultChunkReader(
    DefaultChunkReader&& that) noexcept
    : DefaultChunkReaderBase(static_cast<DefaultChunkReaderBase&&>(that)),
      src_(std::move(that.src_)) {}

template <typename Src>
inline DefaultChunkReader<Src>& DefaultChunkReader<Src>::operator=(
    DefaultChunkReader&& that) noexcept {
  DefaultChunkReaderBase::operator=(
      static_cast<DefaultChunkReaderBase&&>(that));
  src_ = std::move(that.src_);
  return *this;
}

template <typename Src>
inline void DefaultChunkReader<Src>::Reset(Closed) {
  DefaultChunkReaderBase::Reset(kClosed);
  src_.Reset();
}

template <typename Src>
inline void DefaultChunkReader<Src>::Reset(Initializer<Src> src) {
  DefaultChunkReaderBase::Reset();
  src_.Reset(std::move(src));
  Initialize(src_.get());
}

template <typename Src>
void DefaultChunkReader<Src>::Done() {
  DefaultChunkReaderBase::Done();
  if (src_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) {
      FailWithoutAnnotation(src_->status());
    }
  }
}

}  // namespace riegeli

#endif  // RIEGELI_RECORDS_CHUNK_READER_H_
