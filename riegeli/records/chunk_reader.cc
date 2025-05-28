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

#include "riegeli/records/chunk_reader.h"

#include <stddef.h>
#include <stdint.h>

#include <optional>
#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/chunk_encoding/constants.h"
#include "riegeli/chunk_encoding/hash.h"
#include "riegeli/records/block.h"
#include "riegeli/records/skipped_region.h"

namespace riegeli {

void DefaultChunkReaderBase::Initialize(Reader* src) {
  RIEGELI_ASSERT_NE(src, nullptr)
      << "Failed precondition of DefaultChunkReader: null Reader pointer";
  pos_ = src->pos();
  if (ABSL_PREDICT_FALSE(!src->ok()) && src->available() == 0) {
    FailWithoutAnnotation(src->status());
    return;
  }
  if (ABSL_PREDICT_FALSE(!records_internal::IsPossibleChunkBoundary(pos_))) {
    recoverable_ = Recoverable::kFindChunk;
    recoverable_pos_ = pos_;
    Fail(absl::InvalidArgumentError(
        absl::StrCat("Invalid chunk boundary: ", pos_)));
  }
}

void DefaultChunkReaderBase::Done() {
  recoverable_ = Recoverable::kNo;
  recoverable_pos_ = 0;
  if (ABSL_PREDICT_FALSE(truncated_)) {
    Reader& src = *SrcReader();
    RIEGELI_ASSERT_GT(src.pos(), pos_)
        << "Failed invariant of DefaultChunkReader: a chunk beginning must "
           "have been read for the chunk to be considered incomplete";
    recoverable_ = Recoverable::kHaveChunk;
    recoverable_pos_ = src.pos();
    const Position chunk_header_read =
        records_internal::DistanceWithoutOverhead(pos_, recoverable_pos_);
    Fail(absl::InvalidArgumentError(absl::StrCat(
        "Truncated Riegeli/records file, incomplete chunk at ", pos_,
        " with length ", recoverable_pos_ - pos_,
        chunk_header_read < chunk_.header.size()
            ? std::string()
            : absl::StrCat(
                  " < ",
                  records_internal::ChunkEnd(chunk_.header, pos_) - pos_))));
  }
  chunk_.Reset();
}

inline bool DefaultChunkReaderBase::FailReading(const Reader& src) {
  if (ABSL_PREDICT_FALSE(!src.ok())) return FailWithoutAnnotation(src.status());
  if (ABSL_PREDICT_FALSE(src.pos() > pos_)) truncated_ = true;
  return false;
}

inline bool DefaultChunkReaderBase::FailSeeking(const Reader& src,
                                                Position new_pos) {
  if (ABSL_PREDICT_FALSE(!src.ok())) return FailWithoutAnnotation(src.status());
  recoverable_ = Recoverable::kFindChunk;
  recoverable_pos_ = src.pos();
  return Fail(absl::InvalidArgumentError(absl::StrCat(
      "Position ", new_pos, " exceeds file size: ", recoverable_pos_)));
}

absl::Status DefaultChunkReaderBase::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) {
    Reader& src = *SrcReader();
    return src.AnnotateStatus(std::move(status));
  }
  return status;
}

bool DefaultChunkReaderBase::CheckFileFormat() {
  return PullChunkHeader(nullptr);
}

bool DefaultChunkReaderBase::ReadChunk(Chunk& chunk) {
  if (ABSL_PREDICT_FALSE(!PullChunkHeader(nullptr))) return false;
  Reader& src = *SrcReader();
  const Position chunk_end = records_internal::ChunkEnd(chunk_.header, pos_);
  src.ReadHint(SaturatingIntCast<size_t>(chunk_end - src.pos()),
               SaturatingIntCast<size_t>(records_internal::AddWithOverhead(
                                             chunk_end, ChunkHeader::size()) -
                                         src.pos()));

  while (chunk_.data.size() < chunk_.header.data_size()) {
    if (records_internal::RemainingInBlockHeader(src.pos()) > 0) {
      const Position block_begin =
          records_internal::RoundDownToBlockBoundary(src.pos());
      if (ABSL_PREDICT_FALSE(!ReadBlockHeader())) return false;
      if (ABSL_PREDICT_FALSE(block_header_.previous_chunk() !=
                             block_begin - pos_)) {
        if (block_header_.next_chunk() <= records_internal::kBlockSize) {
          // Trust the rest of the block header: skip to the next chunk.
          recoverable_ = Recoverable::kHaveChunk;
          recoverable_pos_ = block_begin + block_header_.next_chunk();
        } else {
          // Skip to the next block header.
          recoverable_ = Recoverable::kFindChunk;
          recoverable_pos_ = src.pos();
        }
        return Fail(absl::InvalidArgumentError(absl::StrCat(
            "Invalid Riegeli/records file: chunk boundary is ", pos_,
            " but block header at ", block_begin,
            " implies a different previous chunk boundary: ",
            block_begin >= block_header_.previous_chunk()
                ? absl::StrCat(block_begin - block_header_.previous_chunk())
                : absl::StrCat("-",
                               block_header_.previous_chunk() - block_begin))));
      }
      if (ABSL_PREDICT_FALSE(block_header_.next_chunk() !=
                             chunk_end - block_begin)) {
        recoverable_ = Recoverable::kFindChunk;
        recoverable_pos_ = src.pos();
        return Fail(absl::InvalidArgumentError(
            absl::StrCat("Invalid Riegeli/records file: chunk boundary is ",
                         chunk_end, " but block header at ", block_begin,
                         " implies a different next chunk boundary: ",
                         block_begin + block_header_.next_chunk())));
      }
    }
    if (ABSL_PREDICT_FALSE(!src.ReadAndAppend(
            IntCast<size_t>(
                UnsignedMin(chunk_.header.data_size() - chunk_.data.size(),
                            records_internal::RemainingInBlock(src.pos()))),
            chunk_.data))) {
      return FailReading(src);
    }
  }

  if (ABSL_PREDICT_FALSE(!src.Seek(chunk_end))) return FailReading(src);

  const uint64_t computed_data_hash =
      chunk_encoding_internal::Hash(chunk_.data);
  if (ABSL_PREDICT_FALSE(computed_data_hash != chunk_.header.data_hash())) {
    // `Recoverable::kHaveChunk`, not `Recoverable::kFindChunk`, because while
    // chunk data are invalid, chunk header has a correct hash, and thus the
    // next chunk is believed to be present after this chunk.
    recoverable_ = Recoverable::kHaveChunk;
    recoverable_pos_ = chunk_end;
    return Fail(absl::InvalidArgumentError(absl::StrCat(
        "Corrupted Riegeli/records file: chunk data hash mismatch (computed 0x",
        absl::Hex(computed_data_hash, absl::PadSpec::kZeroPad16), ", stored 0x",
        absl::Hex(chunk_.header.data_hash(), absl::PadSpec::kZeroPad16),
        "), chunk at ", pos_, " with length ", chunk_end - pos_)));
  }

  chunk = std::move(chunk_);
  pos_ = chunk_end;
  chunk_.Clear();
  return true;
}

bool DefaultChunkReaderBase::PullChunkHeader(const ChunkHeader** chunk_header) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader& src = *SrcReader();
  truncated_ = false;

  if (ABSL_PREDICT_FALSE(src.pos() < pos_)) {
    // Source ended in a skipped region.
    if (!src.Pull()) {
      // Source still ends at the same position.
      if (ABSL_PREDICT_FALSE(!src.ok())) {
        return FailWithoutAnnotation(src.status());
      }
      return false;
    }
    // Source has grown. Recovery can continue.
    recoverable_ = Recoverable::kHaveChunk;
    recoverable_pos_ = pos_;
    pos_ = src.pos();
    return Fail(absl::InvalidArgumentError(absl::StrCat(
        "Riegeli/records file ended at ", pos_,
        " but has grown and will be skipped until ", recoverable_pos_)));
  }

  const Position chunk_header_read =
      records_internal::DistanceWithoutOverhead(pos_, src.pos());
  if (chunk_header_read < chunk_.header.size()) {
    if (ABSL_PREDICT_FALSE(!ReadChunkHeader())) return false;
  }
  if (chunk_header != nullptr) *chunk_header = &chunk_.header;
  return true;
}

inline bool DefaultChunkReaderBase::ReadChunkHeader() {
  RIEGELI_ASSERT_OK(*this)
      << "Failed precondition of DefaultChunkReaderBase::ReadChunkHeader()";
  Reader& src = *SrcReader();
  RIEGELI_ASSERT_LT(records_internal::DistanceWithoutOverhead(pos_, src.pos()),
                    chunk_.header.size())
      << "Failed precondition of DefaultChunkReaderBase::ReadChunkHeader(): "
         "chunk header already read";
  size_t remaining_length;
  size_t length_to_read;
  do {
    if (records_internal::RemainingInBlockHeader(src.pos()) > 0) {
      const Position block_begin =
          records_internal::RoundDownToBlockBoundary(src.pos());
      if (ABSL_PREDICT_FALSE(!ReadBlockHeader())) return false;
      if (ABSL_PREDICT_FALSE(block_header_.previous_chunk() !=
                             block_begin - pos_)) {
        if (block_header_.next_chunk() <= records_internal::kBlockSize) {
          // Trust the rest of the block header: skip to the next chunk.
          recoverable_ = Recoverable::kHaveChunk;
          recoverable_pos_ = block_begin + block_header_.next_chunk();
        } else {
          // Skip to the next block header.
          recoverable_ = Recoverable::kFindChunk;
          recoverable_pos_ = src.pos();
        }
        return Fail(absl::InvalidArgumentError(absl::StrCat(
            "Invalid Riegeli/records file: chunk boundary is ", pos_,
            " but block header at ", block_begin,
            " implies a different previous chunk boundary: ",
            block_begin >= block_header_.previous_chunk()
                ? absl::StrCat(block_begin - block_header_.previous_chunk())
                : absl::StrCat("-",
                               block_header_.previous_chunk() - block_begin))));
      }
    }
    const size_t chunk_header_read = IntCast<size_t>(
        records_internal::DistanceWithoutOverhead(pos_, src.pos()));
    remaining_length = chunk_.header.size() - chunk_header_read;
    length_to_read = UnsignedMin(remaining_length,
                                 records_internal::RemainingInBlock(src.pos()));
    if (ABSL_PREDICT_FALSE(!src.Read(
            length_to_read, chunk_.header.bytes() + chunk_header_read))) {
      return FailReading(src);
    }
  } while (length_to_read < remaining_length);

  const uint64_t computed_header_hash = chunk_.header.computed_header_hash();
  if (ABSL_PREDICT_FALSE(computed_header_hash !=
                         chunk_.header.stored_header_hash())) {
    recoverable_ = Recoverable::kFindChunk;
    recoverable_pos_ = src.pos();
    return Fail(absl::InvalidArgumentError(absl::StrCat(
        "Corrupted Riegeli/records file: chunk header hash mismatch "
        "(computed 0x",
        absl::Hex(computed_header_hash, absl::PadSpec::kZeroPad16),
        ", stored 0x",
        absl::Hex(chunk_.header.stored_header_hash(),
                  absl::PadSpec::kZeroPad16),
        "), chunk at ", pos_)));
  }
  if (records_internal::RemainingInBlock(pos_) < chunk_.header.size()) {
    // The chunk header was interrupted by a block header. Both headers have
    // been read so verify that they agree.
    const Position block_begin =
        pos_ + records_internal::RemainingInBlock(pos_);
    const Position chunk_end = records_internal::ChunkEnd(chunk_.header, pos_);
    if (ABSL_PREDICT_FALSE(block_header_.next_chunk() !=
                           chunk_end - block_begin)) {
      recoverable_ = Recoverable::kFindChunk;
      recoverable_pos_ = src.pos();
      return Fail(absl::InvalidArgumentError(
          absl::StrCat("Invalid Riegeli/records file: chunk boundary is ",
                       chunk_end, " but block header at ", block_begin,
                       " implies a different next chunk boundary: ",
                       block_begin + block_header_.next_chunk())));
    }
  }
  if (pos_ == 0) {
    // Verify file signature.
    if (ABSL_PREDICT_FALSE(chunk_.header.data_size() != 0 ||
                           chunk_.header.chunk_type() !=
                               ChunkType::kFileSignature ||
                           chunk_.header.num_records() != 0 ||
                           chunk_.header.decoded_data_size() != 0)) {
      recoverable_ = Recoverable::kFindChunk;
      recoverable_pos_ = src.pos();
      return Fail(absl::InvalidArgumentError(
          "Invalid Riegeli/records file: missing file signature"));
    }
  }
  return true;
}

inline bool DefaultChunkReaderBase::ReadBlockHeader() {
  RIEGELI_ASSERT_OK(*this)
      << "Failed precondition of DefaultChunkReaderBase::ReadBlockHeader()";
  Reader& src = *SrcReader();
  const size_t remaining_length =
      records_internal::RemainingInBlockHeader(src.pos());
  RIEGELI_ASSERT_GT(remaining_length, 0u)
      << "Failed precondition of DefaultChunkReaderBase::ReadBlockHeader(): "
         "not before nor inside a block header";
  if (ABSL_PREDICT_FALSE(!src.Read(
          remaining_length,
          block_header_.bytes() + block_header_.size() - remaining_length))) {
    return FailReading(src);
  }
  const uint64_t computed_header_hash = block_header_.computed_header_hash();
  if (ABSL_PREDICT_FALSE(computed_header_hash !=
                         block_header_.stored_header_hash())) {
    recoverable_ = Recoverable::kFindChunk;
    recoverable_pos_ = src.pos();
    return Fail(absl::InvalidArgumentError(absl::StrCat(
        "Corrupted Riegeli/records file: block header hash mismatch "
        "(computed 0x",
        absl::Hex(computed_header_hash, absl::PadSpec::kZeroPad16),
        ", stored 0x",
        absl::Hex(block_header_.stored_header_hash(),
                  absl::PadSpec::kZeroPad16),
        "), block at ",
        records_internal::RoundDownToBlockBoundary(recoverable_pos_))));
  }
  return true;
}

bool DefaultChunkReaderBase::Recover(SkippedRegion* skipped_region) {
  if (recoverable_ == Recoverable::kNo) return false;
  Reader& src = *SrcReader();
  const Position region_begin = pos_;
again:
  RIEGELI_ASSERT(!ok()) << "Failed invariant of DefaultChunkReader: "
                           "recovery applicable but DefaultChunkReader OK";
  const Recoverable recoverable = recoverable_;
  recoverable_ = Recoverable::kNo;
  Position recoverable_pos = recoverable_pos_;
  recoverable_pos_ = 0;
  std::string saved_message(status().message());
  MarkNotFailed();
  chunk_.Clear();
  if (recoverable == Recoverable::kHaveChunk) {
    pos_ = recoverable_pos;
    if (ok()) {
      if (ABSL_PREDICT_FALSE(!src.Seek(pos_))) {
        if (ABSL_PREDICT_FALSE(!src.ok())) {
          return FailWithoutAnnotation(src.status());
        }
        if (skipped_region != nullptr) {
          *skipped_region =
              SkippedRegion(region_begin, src.pos(), std::move(saved_message));
        }
        return true;
      }
      if (ABSL_PREDICT_FALSE(
              !records_internal::IsPossibleChunkBoundary(pos_))) {
        recoverable_ = Recoverable::kFindChunk;
        recoverable_pos_ = pos_;
        goto again;
      }
    }
    if (skipped_region != nullptr) {
      *skipped_region =
          SkippedRegion(region_begin, pos_, std::move(saved_message));
    }
    return true;
  }
  RIEGELI_ASSERT_OK(*this)
      << "Failed invariant of DefaultChunkReader: "
         "chunk boundary not reached yet but DefaultChunkReader is closed";
  pos_ = recoverable_pos;

find_chunk:
  pos_ += records_internal::RemainingInBlock(pos_);
  if (ABSL_PREDICT_FALSE(!src.Seek(pos_))) {
    if (ABSL_PREDICT_FALSE(!src.ok())) {
      return FailWithoutAnnotation(src.status());
    }
    if (skipped_region != nullptr) {
      *skipped_region =
          SkippedRegion(region_begin, src.pos(), std::move(saved_message));
    }
    return true;
  }
  if (ABSL_PREDICT_FALSE(!ReadBlockHeader())) {
    if (recoverable_ != Recoverable::kNo) goto again;
    if (ABSL_PREDICT_FALSE(!src.ok())) {
      return FailWithoutAnnotation(src.status());
    }
  } else if (block_header_.previous_chunk() == 0) {
    // A chunk boundary coincides with block boundary. Recovery is done.
  } else {
    pos_ += block_header_.next_chunk();
    if (ABSL_PREDICT_FALSE(!records_internal::IsPossibleChunkBoundary(pos_))) {
      goto find_chunk;
    }
    if (ABSL_PREDICT_FALSE(!src.Seek(pos_))) {
      if (ABSL_PREDICT_FALSE(!src.ok())) {
        return FailWithoutAnnotation(src.status());
      }
      if (skipped_region != nullptr) {
        *skipped_region =
            SkippedRegion(region_begin, src.pos(), std::move(saved_message));
      }
      return true;
    }
  }
  if (skipped_region != nullptr) {
    *skipped_region =
        SkippedRegion(region_begin, pos_, std::move(saved_message));
  }
  return true;
}

bool DefaultChunkReaderBase::SupportsRandomAccess() {
  Reader* const src = SrcReader();
  return src != nullptr && src->SupportsRandomAccess();
}

bool DefaultChunkReaderBase::Seek(Position new_pos) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (pos_ == new_pos) return true;
  Reader& src = *SrcReader();
  truncated_ = false;
  pos_ = new_pos;
  chunk_.Clear();
  if (ABSL_PREDICT_FALSE(!src.Seek(pos_))) return FailSeeking(src, pos_);
  if (ABSL_PREDICT_FALSE(!records_internal::IsPossibleChunkBoundary(pos_))) {
    recoverable_ = Recoverable::kFindChunk;
    recoverable_pos_ = pos_;
    return Fail(absl::InvalidArgumentError(
        absl::StrCat("Invalid chunk boundary: ", pos_)));
  }
  return true;
}

bool DefaultChunkReaderBase::SeekToChunkContaining(Position new_pos) {
  return SeekToChunk<WhichChunk::kContaining>(new_pos);
}

bool DefaultChunkReaderBase::SeekToChunkBefore(Position new_pos) {
  return SeekToChunk<WhichChunk::kBefore>(new_pos);
}

bool DefaultChunkReaderBase::SeekToChunkAfter(Position new_pos) {
  return SeekToChunk<WhichChunk::kAfter>(new_pos);
}

template <DefaultChunkReaderBase::WhichChunk which_chunk>
bool DefaultChunkReaderBase::SeekToChunk(Position new_pos) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (pos_ == new_pos) return true;
  Reader& src = *SrcReader();
  truncated_ = false;
  const Position block_begin =
      records_internal::RoundDownToBlockBoundary(new_pos);
  Position chunk_begin;
  if (pos_ < new_pos) {
    // The current chunk begins before `new_pos`. If it also ends at or after
    // `block_begin`, it is better to start searching from the current position
    // than to seek back to `block_begin`.
    if (ABSL_PREDICT_FALSE(!PullChunkHeader(nullptr))) {
      if (ABSL_PREDICT_FALSE(!ok())) return false;
      truncated_ = false;
      return FailSeeking(src, new_pos);
    }
    if (which_chunk == WhichChunk::kContaining &&
        pos_ + chunk_.header.num_records() > new_pos) {
      return true;
    }
    const Position chunk_end = records_internal::ChunkEnd(chunk_.header, pos_);
    if (which_chunk == WhichChunk::kBefore && chunk_end > new_pos) return true;
    if (chunk_end < block_begin) {
      // The current chunk ends too early. Skip to `block_begin`.
      goto read_block_header;
    }
    chunk_begin = chunk_end;
    chunk_.Clear();
  } else {
  read_block_header:
    pos_ = block_begin;
    chunk_.Clear();
    if (ABSL_PREDICT_FALSE(!src.Seek(pos_))) return FailSeeking(src, new_pos);
    if (ABSL_PREDICT_FALSE(!ReadBlockHeader())) {
      if (ABSL_PREDICT_FALSE(!ok())) return false;
      if (ABSL_PREDICT_TRUE(!truncated_)) {
        // File ends at this block boundary, so a chunk ends here too.
        if (ABSL_PREDICT_TRUE(pos_ >= new_pos)) return true;
      }
      truncated_ = false;
      return FailSeeking(src, new_pos);
    }
    if (block_header_.previous_chunk() == 0) {
      // A chunk boundary coincides with block boundary. The current position is
      // already before the chunk header; start searching from this chunk,
      // skipping seeking back and reading the block header again.
      goto check_current_chunk;
    }
    chunk_begin = block_begin + block_header_.next_chunk();
    if (which_chunk != WhichChunk::kAfter && chunk_begin > new_pos) {
      // `new_pos` is inside the chunk which contains this block boundary, so
      // start the search from this chunk instead of the next chunk.
      if (ABSL_PREDICT_FALSE(block_header_.previous_chunk() > block_begin)) {
        recoverable_ = Recoverable::kFindChunk;
        recoverable_pos_ = src.pos();
        return Fail(absl::InvalidArgumentError(absl::StrCat(
            "Invalid Riegeli/records file: block header at ", block_begin,
            " implies a negative previous chunk boundary: -",
            block_header_.previous_chunk() - block_begin)));
      }
      chunk_begin = block_begin - block_header_.previous_chunk();
    }
    if (ABSL_PREDICT_FALSE(
            !records_internal::IsPossibleChunkBoundary(chunk_begin))) {
      recoverable_ = Recoverable::kFindChunk;
      recoverable_pos_ = src.pos();
      return Fail(absl::InvalidArgumentError(absl::StrCat(
          "Invalid Riegeli/records file: block header at ", block_begin,
          " implies an invalid chunk boundary: ", chunk_begin)));
    }
  }

  for (;;) {
    pos_ = chunk_begin;
    if (ABSL_PREDICT_FALSE(!src.Seek(pos_))) return FailSeeking(src, new_pos);
  check_current_chunk:
    if (pos_ >= new_pos) return true;
    if (ABSL_PREDICT_FALSE(!ReadChunkHeader())) {
      if (ABSL_PREDICT_FALSE(!ok())) return false;
      truncated_ = false;
      return FailSeeking(src, new_pos);
    }
    if (which_chunk == WhichChunk::kContaining &&
        pos_ + chunk_.header.num_records() > new_pos) {
      return true;
    }
    const Position chunk_end = records_internal::ChunkEnd(chunk_.header, pos_);
    if (which_chunk == WhichChunk::kBefore && chunk_end > new_pos) return true;
    chunk_begin = chunk_end;
  }
}

std::optional<Position> DefaultChunkReaderBase::Size() {
  if (ABSL_PREDICT_FALSE(!ok())) return std::nullopt;
  Reader& src = *SrcReader();
  const std::optional<Position> size = src.Size();
  if (ABSL_PREDICT_FALSE(size == std::nullopt)) {
    FailWithoutAnnotation(src.status());
  }
  return size;
}

}  // namespace riegeli
