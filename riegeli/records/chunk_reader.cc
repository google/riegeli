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
#include <memory>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/str_cat.h"
#include "riegeli/base/base.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/chunk_encoding/hash.h"
#include "riegeli/records/block.h"

namespace riegeli {

ChunkReader::ChunkReader(std::unique_ptr<Reader> byte_reader)
    : ChunkReader(byte_reader.get()) {
  owned_byte_reader_ = std::move(byte_reader);
}

ChunkReader::ChunkReader(Reader* byte_reader)
    : Object(State::kOpen),
      byte_reader_(RIEGELI_ASSERT_NOTNULL(byte_reader)),
      pos_(byte_reader_->pos()) {
  if (ABSL_PREDICT_FALSE(!internal::IsPossibleChunkBoundary(pos_))) {
    recoverable_ = Recoverable::kFindChunk;
    recoverable_pos_ = pos_;
    Fail(absl::StrCat("Invalid chunk boundary: ", pos_));
  }
}

void ChunkReader::Done() {
  recoverable_ = Recoverable::kNo;
  recoverable_pos_ = 0;
  if (ABSL_PREDICT_FALSE(truncated_)) {
    RIEGELI_ASSERT_GT(byte_reader_->pos(), pos_)
        << "Failed invariant of ChunkReader: a chunk beginning must have been "
           "read for the chunk to be considered incomplete";
    recoverable_ = Recoverable::kHaveChunk;
    recoverable_pos_ = byte_reader_->pos();
    Fail(absl::StrCat("Truncated Riegeli/records file, incomplete chunk at ",
                      pos_, " with length ", byte_reader_->pos() - pos_));
  }
  if (owned_byte_reader_ != nullptr) {
    if (ABSL_PREDICT_FALSE(!owned_byte_reader_->Close())) {
      Fail(*owned_byte_reader_);
    }
  }
  chunk_.Close();
}

inline bool ChunkReader::ReadingFailed() {
  if (ABSL_PREDICT_FALSE(!byte_reader_->healthy())) return Fail(*byte_reader_);
  if (ABSL_PREDICT_FALSE(byte_reader_->pos() > pos_)) truncated_ = true;
  return false;
}

inline bool ChunkReader::SeekingFailed(Position new_pos) {
  if (ABSL_PREDICT_FALSE(!byte_reader_->healthy())) return Fail(*byte_reader_);
  recoverable_ = Recoverable::kFindChunk;
  recoverable_pos_ = byte_reader_->pos();
  return Fail(absl::StrCat("Position ", new_pos,
                           " exceeds file size: ", recoverable_pos_));
}

bool ChunkReader::CheckFileFormat() { return PullChunkHeader(nullptr); }

bool ChunkReader::ReadChunk(Chunk* chunk) {
  if (ABSL_PREDICT_FALSE(!PullChunkHeader(nullptr))) return false;
  const Position chunk_end = internal::ChunkEnd(chunk_.header, pos_);

  while (chunk_.data.size() < chunk_.header.data_size()) {
    if (internal::RemainingInBlockHeader(byte_reader_->pos()) > 0) {
      const Position block_begin =
          internal::RoundDownToBlockBoundary(byte_reader_->pos());
      if (ABSL_PREDICT_FALSE(!ReadBlockHeader())) return false;
      if (ABSL_PREDICT_FALSE(block_header_.previous_chunk() !=
                             block_begin - pos_)) {
        if (block_header_.next_chunk() <= internal::kBlockSize()) {
          // Trust the rest of the block header: skip to the next chunk.
          recoverable_ = Recoverable::kHaveChunk;
          recoverable_pos_ = block_begin + block_header_.next_chunk();
        } else {
          // Skip to the next block header.
          recoverable_ = Recoverable::kFindChunk;
          recoverable_pos_ = byte_reader_->pos();
        }
        return Fail(absl::StrCat(
            "Invalid Riegeli/records file: chunk boundary is ", pos_,
            " but block header at ", block_begin,
            " implies a different previous chunk boundary: ",
            block_begin >= block_header_.previous_chunk()
                ? absl::StrCat(block_begin - block_header_.previous_chunk())
                : absl::StrCat("-",
                               block_header_.previous_chunk() - block_begin)));
      }
      if (ABSL_PREDICT_FALSE(block_header_.next_chunk() !=
                             chunk_end - block_begin)) {
        recoverable_ = Recoverable::kFindChunk;
        recoverable_pos_ = byte_reader_->pos();
        return Fail(
            absl::StrCat("Invalid Riegeli/records file: chunk boundary is ",
                         chunk_end, " but block header at ", block_begin,
                         " implies a different next chunk boundary: ",
                         block_begin + block_header_.next_chunk()));
      }
    }
    if (ABSL_PREDICT_FALSE(!byte_reader_->Read(
            &chunk_.data,
            IntCast<size_t>(UnsignedMin(
                chunk_.header.data_size() - chunk_.data.size(),
                internal::RemainingInBlock(byte_reader_->pos())))))) {
      return ReadingFailed();
    }
  }

  if (ABSL_PREDICT_FALSE(!byte_reader_->Seek(chunk_end))) {
    return ReadingFailed();
  }

  const uint64_t computed_data_hash = internal::Hash(chunk_.data);
  if (ABSL_PREDICT_FALSE(computed_data_hash != chunk_.header.data_hash())) {
    // Recoverable::kHaveChunk, not Recoverable::kFindChunk, because while chunk
    // data are invalid, chunk header has a correct hash, and thus the next
    // chunk is believed to be present after this chunk.
    recoverable_ = Recoverable::kHaveChunk;
    recoverable_pos_ = chunk_end;
    return Fail(absl::StrCat(
        "Corrupted Riegeli/records file: chunk data hash mismatch (computed 0x",
        absl::Hex(computed_data_hash, absl::PadSpec::kZeroPad16), ", stored 0x",
        absl::Hex(chunk_.header.data_hash(), absl::PadSpec::kZeroPad16),
        "), chunk at ", pos_, " with length ", chunk_end - pos_));
  }

  *chunk = std::move(chunk_);
  pos_ = chunk_end;
  chunk_.Reset();
  return true;
}

bool ChunkReader::PullChunkHeader(const ChunkHeader** chunk_header) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  truncated_ = false;

  if (ABSL_PREDICT_FALSE(byte_reader_->pos() < pos_)) {
    // Source ended in a skipped region.
    if (!byte_reader_->Pull()) {
      // Source still ends at the same position.
      if (ABSL_PREDICT_FALSE(!byte_reader_->healthy())) {
        return Fail(*byte_reader_);
      }
      return false;
    }
    // Source has grown. Recovery can continue.
    recoverable_ = Recoverable::kHaveChunk;
    recoverable_pos_ = pos_;
    pos_ = byte_reader_->pos();
    return Fail(absl::StrCat("Riegeli/records file ended at ", pos_,
                             " but has grown and will be skipped until ",
                             recoverable_pos_));
  }

  const Position chunk_header_read =
      internal::DistanceWithoutOverhead(pos_, byte_reader_->pos());
  if (chunk_header_read < chunk_.header.size()) {
    if (ABSL_PREDICT_FALSE(!ReadChunkHeader())) return false;
  }
  if (chunk_header != nullptr) *chunk_header = &chunk_.header;
  return true;
}

inline bool ChunkReader::ReadChunkHeader() {
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of ChunkReader::ReadChunkReader(): " << message();
  RIEGELI_ASSERT_LT(
      internal::DistanceWithoutOverhead(pos_, byte_reader_->pos()),
      chunk_.header.size())
      << "Failed precondition of ChunkReader::ReadChunkHeader(): "
         "chunk header already read";
  size_t remaining_length;
  size_t length_to_read;
  do {
    if (internal::RemainingInBlockHeader(byte_reader_->pos()) > 0) {
      const Position block_begin =
          internal::RoundDownToBlockBoundary(byte_reader_->pos());
      if (ABSL_PREDICT_FALSE(!ReadBlockHeader())) return false;
      if (ABSL_PREDICT_FALSE(block_header_.previous_chunk() !=
                             block_begin - pos_)) {
        if (block_header_.next_chunk() <= internal::kBlockSize()) {
          // Trust the rest of the block header: skip to the next chunk.
          recoverable_ = Recoverable::kHaveChunk;
          recoverable_pos_ = block_begin + block_header_.next_chunk();
        } else {
          // Skip to the next block header.
          recoverable_ = Recoverable::kFindChunk;
          recoverable_pos_ = byte_reader_->pos();
        }
        return Fail(absl::StrCat(
            "Invalid Riegeli/records file: chunk boundary is ", pos_,
            " but block header at ", block_begin,
            " implies a different previous chunk boundary: ",
            block_begin >= block_header_.previous_chunk()
                ? absl::StrCat(block_begin - block_header_.previous_chunk())
                : absl::StrCat("-",
                               block_header_.previous_chunk() - block_begin)));
      }
    }
    const size_t chunk_header_read = IntCast<size_t>(
        internal::DistanceWithoutOverhead(pos_, byte_reader_->pos()));
    remaining_length = chunk_.header.size() - chunk_header_read;
    length_to_read = UnsignedMin(
        remaining_length, internal::RemainingInBlock(byte_reader_->pos()));
    if (ABSL_PREDICT_FALSE(!byte_reader_->Read(
            chunk_.header.bytes() + chunk_header_read, length_to_read))) {
      return ReadingFailed();
    }
  } while (length_to_read < remaining_length);

  const uint64_t computed_header_hash = chunk_.header.computed_header_hash();
  if (ABSL_PREDICT_FALSE(computed_header_hash !=
                         chunk_.header.stored_header_hash())) {
    recoverable_ = Recoverable::kFindChunk;
    recoverable_pos_ = byte_reader_->pos();
    return Fail(absl::StrCat(
        "Corrupted Riegeli/records file: chunk header hash mismatch "
        "(computed 0x",
        absl::Hex(computed_header_hash, absl::PadSpec::kZeroPad16),
        ", stored 0x",
        absl::Hex(chunk_.header.stored_header_hash(),
                  absl::PadSpec::kZeroPad16),
        "), chunk at ", pos_));
  }
  if (internal::RemainingInBlock(pos_) < chunk_.header.size()) {
    // The chunk header was interrupted by a block header. Both headers have
    // been read so verify that they agree.
    const Position block_begin = pos_ + internal::RemainingInBlock(pos_);
    const Position chunk_end = internal::ChunkEnd(chunk_.header, pos_);
    if (ABSL_PREDICT_FALSE(block_header_.next_chunk() !=
                           chunk_end - block_begin)) {
      recoverable_ = Recoverable::kFindChunk;
      recoverable_pos_ = byte_reader_->pos();
      return Fail(
          absl::StrCat("Invalid Riegeli/records file: chunk boundary is ",
                       chunk_end, " but block header at ", block_begin,
                       " implies a different next chunk boundary: ",
                       block_begin + block_header_.next_chunk()));
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
      recoverable_pos_ = byte_reader_->pos();
      return Fail("Invalid Riegeli/records file: missing file signature");
    }
  }
  return true;
}

inline bool ChunkReader::ReadBlockHeader() {
  const size_t remaining_length =
      internal::RemainingInBlockHeader(byte_reader_->pos());
  RIEGELI_ASSERT_GT(remaining_length, 0u)
      << "Failed precondition of ChunkReader::ReadBlockHeader(): "
         "not before nor inside a block header";
  if (ABSL_PREDICT_FALSE(!byte_reader_->Read(
          block_header_.bytes() + block_header_.size() - remaining_length,
          remaining_length))) {
    return ReadingFailed();
  }
  const uint64_t computed_header_hash = block_header_.computed_header_hash();
  if (ABSL_PREDICT_FALSE(computed_header_hash !=
                         block_header_.stored_header_hash())) {
    recoverable_ = Recoverable::kFindChunk;
    recoverable_pos_ = byte_reader_->pos();
    return Fail(absl::StrCat(
        "Corrupted Riegeli/records file: block header hash mismatch "
        "(computed 0x",
        absl::Hex(computed_header_hash, absl::PadSpec::kZeroPad16),
        ", stored 0x",
        absl::Hex(block_header_.stored_header_hash(),
                  absl::PadSpec::kZeroPad16),
        "), block at ", internal::RoundDownToBlockBoundary(recoverable_pos_)));
  }
  return true;
}

bool ChunkReader::Recover(SkippedRegion* skipped_region) {
  if (recoverable_ == Recoverable::kNo) return false;
  const Position region_begin = pos_;
again:
  RIEGELI_ASSERT(!healthy()) << "Failed invariant of ChunkReader: "
                                "recovery applicable but ChunkReader healthy";
  const Recoverable recoverable = recoverable_;
  recoverable_ = Recoverable::kNo;
  Position recoverable_pos = recoverable_pos_;
  recoverable_pos_ = 0;
  MarkNotFailed();
  chunk_.Reset();
  if (recoverable == Recoverable::kHaveChunk) {
    if (healthy()) {
      pos_ = recoverable_pos;
      if (ABSL_PREDICT_FALSE(!byte_reader_->Seek(pos_))) {
        if (ABSL_PREDICT_FALSE(!byte_reader_->healthy())) {
          return Fail(*byte_reader_);
        }
      }
      if (ABSL_PREDICT_FALSE(!internal::IsPossibleChunkBoundary(pos_))) {
        recoverable_ = Recoverable::kFindChunk;
        recoverable_pos_ = pos_;
        goto again;
      }
    }
    if (skipped_region != nullptr) {
      *skipped_region = SkippedRegion(region_begin, byte_reader_->pos());
    }
    return true;
  }
  RIEGELI_ASSERT(healthy())
      << "Failed invariant of ChunkReader: "
         "chunk boundary not reached yet but ChunkReader is closed";
  pos_ = recoverable_pos;

find_chunk:
  pos_ += internal::RemainingInBlock(pos_);
  if (ABSL_PREDICT_FALSE(!byte_reader_->Seek(pos_))) {
    if (ABSL_PREDICT_FALSE(!byte_reader_->healthy())) {
      return Fail(*byte_reader_);
    }
  } else if (ABSL_PREDICT_FALSE(!ReadBlockHeader())) {
    if (recoverable_ != Recoverable::kNo) goto again;
    if (ABSL_PREDICT_FALSE(!byte_reader_->healthy())) {
      return Fail(*byte_reader_);
    }
    if (skipped_region != nullptr) {
      *skipped_region = SkippedRegion(region_begin, pos_);
    }
    return true;
  } else if (block_header_.previous_chunk() == 0) {
    // A chunk boundary coincides with block boundary. Recovery is done.
  } else {
    pos_ += block_header_.next_chunk();
    if (ABSL_PREDICT_FALSE(!internal::IsPossibleChunkBoundary(pos_))) {
      goto find_chunk;
    }
    if (ABSL_PREDICT_FALSE(!byte_reader_->Seek(pos_))) {
      if (ABSL_PREDICT_FALSE(!byte_reader_->healthy())) {
        return Fail(*byte_reader_);
      }
    }
  }
  if (skipped_region != nullptr) {
    *skipped_region = SkippedRegion(region_begin, byte_reader_->pos());
  }
  return true;
}

bool ChunkReader::Seek(Position new_pos) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  truncated_ = false;
  pos_ = new_pos;
  chunk_.Reset();
  if (ABSL_PREDICT_FALSE(!byte_reader_->Seek(pos_))) return SeekingFailed(pos_);
  if (ABSL_PREDICT_FALSE(!internal::IsPossibleChunkBoundary(pos_))) {
    recoverable_ = Recoverable::kFindChunk;
    recoverable_pos_ = pos_;
    return Fail(absl::StrCat("Invalid chunk boundary: ", pos_));
  }
  return true;
}

bool ChunkReader::SeekToChunkContaining(Position new_pos) {
  return SeekToChunk(new_pos, true);
}

bool ChunkReader::SeekToChunkAfter(Position new_pos) {
  return SeekToChunk(new_pos, false);
}

inline bool ChunkReader::SeekToChunk(Position new_pos, bool containing) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  truncated_ = false;
  chunk_.Reset();
  const Position block_begin = internal::RoundDownToBlockBoundary(new_pos);
  Position chunk_begin;
  if (pos_ <= new_pos) {
    // The current chunk begins at or before new_pos. If it also ends at or
    // after block_begin, it is better to start searching from the current
    // position than to seek back to block_begin.
    if (pos_ == new_pos) return true;
    if (ABSL_PREDICT_FALSE(!PullChunkHeader(nullptr))) {
      truncated_ = false;
      return SeekingFailed(new_pos);
    }
    const Position chunk_end = internal::ChunkEnd(chunk_.header, pos_);
    if (chunk_end < block_begin) {
      // The current chunk ends too early. Skip to block_begin.
      goto read_block_header;
    }
    if (containing && pos_ + chunk_.header.num_records() > new_pos) {
      return true;
    }
    chunk_begin = chunk_end;
  } else {
  read_block_header:
    pos_ = block_begin;
    if (ABSL_PREDICT_FALSE(!byte_reader_->Seek(pos_))) {
      return SeekingFailed(new_pos);
    }
    if (ABSL_PREDICT_FALSE(!ReadBlockHeader())) {
      truncated_ = false;
      return SeekingFailed(new_pos);
    }
    if (block_header_.previous_chunk() == 0) {
      // A chunk boundary coincides with block boundary. The current position is
      // already before the chunk header; start searching from this chunk,
      // skipping seeking back and reading the block header again.
      goto check_current_chunk;
    }
    chunk_begin = block_begin + block_header_.next_chunk();
    if (containing && chunk_begin > new_pos) {
      // new_pos is inside the chunk which contains this block boundary, so
      // start the search from this chunk instead of the next chunk.
      if (ABSL_PREDICT_FALSE(block_header_.previous_chunk() > block_begin)) {
        recoverable_ = Recoverable::kFindChunk;
        recoverable_pos_ = byte_reader_->pos();
        return Fail(absl::StrCat(
            "Invalid Riegeli/records file: block header at ", block_begin,
            " implies a negative previous chunk boundary: -",
            block_header_.previous_chunk() - block_begin));
      }
      chunk_begin = block_begin - block_header_.previous_chunk();
    }
    if (ABSL_PREDICT_FALSE(!internal::IsPossibleChunkBoundary(chunk_begin))) {
      recoverable_ = Recoverable::kFindChunk;
      recoverable_pos_ = byte_reader_->pos();
      return Fail(absl::StrCat(
          "Invalid Riegeli/records file: block header at ", block_begin,
          " implies an invalid chunk boundary: ", chunk_begin));
    }
  }

  for (;;) {
    if (ABSL_PREDICT_FALSE(!byte_reader_->Seek(chunk_begin))) {
      return SeekingFailed(new_pos);
    }
    pos_ = chunk_begin;
  check_current_chunk:
    if (pos_ >= new_pos) return true;
    if (ABSL_PREDICT_FALSE(!ReadChunkHeader())) {
      truncated_ = false;
      return SeekingFailed(new_pos);
    }
    if (containing && pos_ + chunk_.header.num_records() > new_pos) {
      return true;
    }
    chunk_begin = internal::ChunkEnd(chunk_.header, pos_);
  }
}

}  // namespace riegeli
