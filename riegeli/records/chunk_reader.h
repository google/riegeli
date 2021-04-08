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

#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/records/block.h"
#include "riegeli/records/skipped_region.h"

namespace riegeli {

// Template parameter independent part of `DefaultChunkReader`.
class DefaultChunkReaderBase : public Object {
 public:
  // Returns the Riegeli/records file being read from. Unchanged by `Close()`.
  virtual Reader* src_reader() = 0;
  virtual const Reader* src_reader() const = 0;

  // Ensures that the file looks like a valid Riegeli/Records file.
  //
  // Reading the file already checks whether it is valid. `CheckFileFormat()`
  // can verify this before (or instead of) performing other operations.
  //
  // Return values:
  //  * `true`                      - success
  //  * `false` (when `healthy()`)  - source ends
  //  * `false` (when `!healthy()`) - failure
  bool CheckFileFormat();

  // Reads the next chunk.
  //
  // Return values:
  //  * `true`                      - success (`chunk` is set)
  //  * `false` (when `healthy()`)  - source ends
  //  * `false` (when `!healthy()`) - failure
  bool ReadChunk(Chunk& chunk);

  // Reads the next chunk header, from same chunk which will be read by an
  // immediately following `ReadChunk()`.
  //
  // If `chunk_header != nullptr`, `*chunk_header` is set to the chunk header,
  // valid until the next non-const function of the `ChunkReader`.
  //
  // Return values:
  //  * `true`                      - success (`*chunk_header` is set)
  //  * `false` (when `healthy()`)  - source ends
  //  * `false` (when `!healthy()`) - failure
  bool PullChunkHeader(const ChunkHeader** chunk_header);

  // If `!healthy()` and the failure was caused by invalid file contents, then
  // `Recover()` tries to recover from the failure and allow reading again by
  // skipping over the invalid region.
  //
  // If `Close()` failed and the failure was caused by truncated file contents,
  // then `Recover()` returns `true`. The `ChunkReader` remains closed.
  //
  // If `healthy()`, or if `!healthy()` but the failure was not caused by
  // invalid file contents, then `Recover()` returns false.
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
  //  * `false` - failure (`!healthy()`)
  bool Seek(Position new_pos);

  // Seeks to the nearest chunk boundary before or at `new_pos` if the position
  // corresponds to some numeric record position in the following chunk (i.e. is
  // less than `num_records` bytes after chunk beginning), otherwise seeks to
  // the nearest chunk boundary at or after the given position.
  //
  // Return values:
  //  * `true`  - success
  //  * `false` - failure (`!healthy()`)
  bool SeekToChunkContaining(Position new_pos);

  // Seeks to the nearest chunk boundary at or before `new_pos`.
  //
  // Return values:
  //  * `true`  - success
  //  * `false` - failure (`!healthy()`)
  bool SeekToChunkBefore(Position new_pos);

  // Seeks to the nearest chunk boundary at or after `new_pos`.
  //
  // Return values:
  //  * `true`  - success
  //  * `false` - failure (`!healthy()`)
  bool SeekToChunkAfter(Position new_pos);

  // Returns the size of the file, i.e. the position corresponding to its end.
  //
  // Returns `absl::nullopt` on failure (`!healthy()`).
  absl::optional<Position> Size();

 protected:
  explicit DefaultChunkReaderBase(InitiallyClosed) : Object(kInitiallyClosed) {}
  explicit DefaultChunkReaderBase(InitiallyOpen) : Object(kInitiallyOpen) {}

  DefaultChunkReaderBase(DefaultChunkReaderBase&& that) noexcept;
  DefaultChunkReaderBase& operator=(DefaultChunkReaderBase&& that) noexcept;

  void Reset(InitiallyClosed);
  void Reset(InitiallyOpen);
  void Initialize(Reader* src);

  void Done() override;

 private:
  enum class Recoverable { kNo, kHaveChunk, kFindChunk };
  enum class WhichChunk { kContaining, kBefore, kAfter };

  // Interprets a `false` result from `src` reading or seeking function.
  //
  // End of file (i.e. if `healthy()`) is propagated, setting `truncated_` if it
  // was in the middle of a chunk.
  //
  // Always returns `false`.
  bool FailReading(const Reader& src);

  // Interprets a `false` result from `src` reading or seeking function.
  //
  // End of file (i.e. if `healthy()`) fails the `ChunkReader`.
  //
  // Always returns `false`.
  bool FailSeeking(const Reader& src, Position new_pos);

  // Reads or continues reading `chunk_.header`.
  bool ReadChunkHeader();

  // Reads or continues reading `block_header_`.
  //
  // Preconditions:
  //   `healthy()`
  //   `internal::RemainingInBlockHeader(src_reader()->pos()) > 0`
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
  // Invariant: if `truncated_` then `src_reader()->pos() > pos_`
  bool truncated_ = false;

  // Beginning of the current chunk.
  //
  // If `pos_ > src_reader()->pos()`, the source ends in a skipped region. In
  // this case `pos_` can be a block boundary instead of a chunk boundary.
  Position pos_ = 0;

  // Chunk header and chunk data, filled to the point derived from `pos_` and
  // `src_reader()->pos()`.
  Chunk chunk_;

  // Block header, filled to the point derived from `src_reader()->pos()`.
  internal::BlockHeader block_header_;

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
  //   if `healthy()` then `recoverable_ == Recoverable::kNo`
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
// `std::unique_ptr<Reader>` (owned), `ChainReader<>` (owned).
//
// By relying on CTAD the template argument can be deduced as the value type of
// the first constructor argument. This requires C++17.
//
// The byte `Reader` must not be accessed until the `DefaultChunkReader` is
// closed or no longer used.
template <typename Src = Reader*>
class DefaultChunkReader : public DefaultChunkReaderBase {
 public:
  // Creates a closed `DefaultChunkReader`.
  DefaultChunkReader() : DefaultChunkReaderBase(kInitiallyClosed) {}

  // Will read from the byte `Reader` provided by `src`.
  explicit DefaultChunkReader(const Src& src);
  explicit DefaultChunkReader(Src&& src);

  // Will read from the byte `Reader` provided by a `Src` constructed from
  // elements of `src_args`. This avoids constructing a temporary `Src` and
  // moving from it.
  template <typename... SrcArgs>
  explicit DefaultChunkReader(std::tuple<SrcArgs...> src_args);

  DefaultChunkReader(DefaultChunkReader&& that) noexcept;
  DefaultChunkReader& operator=(DefaultChunkReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `DefaultChunkReader`. This
  // avoids constructing a temporary `DefaultChunkReader` and moving from it.
  void Reset();
  void Reset(const Src& src);
  void Reset(Src&& src);
  template <typename... SrcArgs>
  void Reset(std::tuple<SrcArgs...> src_args);

  // Returns the object providing and possibly owning the byte `Reader`.
  // Unchanged by `Close()`.
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  Reader* src_reader() override { return src_.get(); }
  const Reader* src_reader() const override { return src_.get(); }

 protected:
  void Done() override;

 private:
  // The object providing and possibly owning the Riegeli/records file being
  // read from.
  Dependency<Reader*, Src> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
DefaultChunkReader()->DefaultChunkReader<DeleteCtad<>>;
template <typename Src>
explicit DefaultChunkReader(const Src& src)
    -> DefaultChunkReader<std::decay_t<Src>>;
template <typename Src>
explicit DefaultChunkReader(Src&& src) -> DefaultChunkReader<std::decay_t<Src>>;
template <typename... SrcArgs>
explicit DefaultChunkReader(std::tuple<SrcArgs...> src_args)
    -> DefaultChunkReader<DeleteCtad<std::tuple<SrcArgs...>>>;
#endif

// Implementation details follow.

inline DefaultChunkReaderBase::DefaultChunkReaderBase(
    DefaultChunkReaderBase&& that) noexcept
    : Object(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      truncated_(that.truncated_),
      pos_(that.pos_),
      chunk_(std::move(that.chunk_)),
      block_header_(that.block_header_),
      recoverable_(std::exchange(that.recoverable_, Recoverable::kNo)),
      recoverable_pos_(that.recoverable_pos_) {}

inline DefaultChunkReaderBase& DefaultChunkReaderBase::operator=(
    DefaultChunkReaderBase&& that) noexcept {
  Object::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  truncated_ = that.truncated_;
  pos_ = that.pos_;
  chunk_ = that.chunk_;
  block_header_ = that.block_header_;
  recoverable_ = std::exchange(that.recoverable_, Recoverable::kNo);
  recoverable_pos_ = that.recoverable_pos_;
  return *this;
}

inline void DefaultChunkReaderBase::Reset(InitiallyClosed) {
  Object::Reset(kInitiallyClosed);
  truncated_ = false;
  pos_ = 0;
  chunk_.Reset();
  recoverable_ = Recoverable::kNo;
  recoverable_pos_ = 0;
}

inline void DefaultChunkReaderBase::Reset(InitiallyOpen) {
  Object::Reset(kInitiallyOpen);
  truncated_ = false;
  pos_ = 0;
  chunk_.Reset();
  recoverable_ = Recoverable::kNo;
  recoverable_pos_ = 0;
}

template <typename Src>
inline DefaultChunkReader<Src>::DefaultChunkReader(const Src& src)
    : DefaultChunkReaderBase(kInitiallyOpen), src_(src) {
  Initialize(src_.get());
}

template <typename Src>
inline DefaultChunkReader<Src>::DefaultChunkReader(Src&& src)
    : DefaultChunkReaderBase(kInitiallyOpen), src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline DefaultChunkReader<Src>::DefaultChunkReader(
    std::tuple<SrcArgs...> src_args)
    : DefaultChunkReaderBase(kInitiallyOpen), src_(std::move(src_args)) {
  Initialize(src_.get());
}

template <typename Src>
inline DefaultChunkReader<Src>::DefaultChunkReader(
    DefaultChunkReader&& that) noexcept
    : DefaultChunkReaderBase(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      src_(std::move(that.src_)) {}

template <typename Src>
inline DefaultChunkReader<Src>& DefaultChunkReader<Src>::operator=(
    DefaultChunkReader&& that) noexcept {
  DefaultChunkReaderBase::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  src_ = std::move(that.src_);
  return *this;
}

template <typename Src>
inline void DefaultChunkReader<Src>::Reset() {
  DefaultChunkReaderBase::Reset(kInitiallyClosed);
  src_.Reset();
}

template <typename Src>
inline void DefaultChunkReader<Src>::Reset(const Src& src) {
  DefaultChunkReaderBase::Reset(kInitiallyOpen);
  src_.Reset(src);
  Initialize(src_.get());
}

template <typename Src>
inline void DefaultChunkReader<Src>::Reset(Src&& src) {
  DefaultChunkReaderBase::Reset(kInitiallyOpen);
  src_.Reset(std::move(src));
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline void DefaultChunkReader<Src>::Reset(std::tuple<SrcArgs...> src_args) {
  DefaultChunkReaderBase::Reset(kInitiallyOpen);
  src_.Reset(std::move(src_args));
  Initialize(src_.get());
}

template <typename Src>
void DefaultChunkReader<Src>::Done() {
  DefaultChunkReaderBase::Done();
  if (src_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) Fail(*src_);
  }
}

}  // namespace riegeli

#endif  // RIEGELI_RECORDS_CHUNK_READER_H_
