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

#include <stddef.h>
#include <memory>
#include <utility>

#include "riegeli/base/base.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/records/block.h"

namespace riegeli {

// A ChunkReader reads chunks of a Riegeli/records file (rather than individual
// records, as RecordReader does).
//
// ChunkReader can be used together with ChunkWriter to rewrite Riegeli/records
// files without recompressing chunks, e.g. to concatenate files.
//
// TODO: If use cases arise, this could be made an abstract class,
// together with a default implementation, analogously to ChunkWriter.
class ChunkReader final : public Object {
 public:
  class Options {
   public:
    // Not defaulted because of a C++ defect:
    // https://stackoverflow.com/questions/17430377
    constexpr Options() noexcept {}

    // If true, corrupted regions will be skipped. if false, corrupted regions
    // will cause reading to fail.
    //
    // Default: false
    Options& set_skip_corruption(bool skip_corruption) & {
      skip_corruption_ = std::move(skip_corruption);
      return *this;
    }
    Options&& set_skip_corruption(bool skip_corruption) && {
      return std::move(set_skip_corruption(skip_corruption));
    }

   private:
    friend class ChunkReader;

    bool skip_corruption_ = false;
  };

  // Will read chunks from the byte Reader which is owned by this ChunkReader
  // and will be closed and deleted when the ChunkReader is closed.
  explicit ChunkReader(std::unique_ptr<Reader> byte_reader,
                       Options options = Options());

  // Will read chunks from the byte Reader which is not owned by this
  // ChunkReader and must be kept alive but not accessed until closing the
  // ChunkReader.
  explicit ChunkReader(Reader* byte_reader, Options options = Options());

  ChunkReader(const ChunkReader&) = delete;
  ChunkReader& operator=(const ChunkReader&) = delete;

  ~ChunkReader();

  // Ensures that the file looks like a valid Riegeli/Records file.
  //
  // options.set_skip_corruption(true) should not be used, otherwise the whole
  // file will be scanned for a valid chunk.
  //
  // Return values:
  //  * true                    - success
  //  * false (when healthy())  - source ends
  //  * false (when !healthy()) - failure
  bool CheckFileFormat();

  // Reads the next chunk.
  //
  // If chunk_begin != nullptr, *chunk_begin is set to the chunk beginning
  // position on success.
  //
  // Return values:
  //  * true                    - success (*chunk is set)
  //  * false (when healthy())  - source ends
  //  * false (when !healthy()) - failure
  bool ReadChunk(Chunk* chunk, Position* chunk_begin = nullptr);

  // Returns true if reading from the current position might succeed, possibly
  // after some data is appended to the source. Returns false if reading from
  // the current position will always return false.
  bool HopeForMore() const { return healthy() && byte_reader_->HopeForMore(); }

  // Returns the current position, which is a chunk boundary (or a block
  // boundary which should be interpreted as the nearest chunk boundary at or
  // after the block boundary).
  Position pos() const { return pos_; }

  // Seeks to the given position, which should be a chunk boundary (or a block
  // boundary which is interpreted as the nearest chunk boundary at or after the
  // block boundary).
  //
  // Return values:
  //  * true                    - success (position is set to pos)
  //  * false (when healthy())  - source ends before new_pos (position is set to
  //                              the end) or seeking backwards is not supported
  //                              (position is unchanged)
  //  * false (when !healthy()) - failure
  bool Seek(Position new_pos);

  // Seeks to the nearest chunk boundary before or at the given position if the
  // position corresponds to some numeric record position in the following chunk
  // (i.e. is less than num_records bytes after chunk beginning), otherwise
  // seeks to the nearest chunk boundary at or after the given position.
  //
  // Return values:
  //  * true                    - success (position is set to pos)
  //  * false (when healthy())  - source ends before new_pos (position is set to
  //                              the end) or seeking backwards is not supported
  //                              (position is unchanged)
  //  * false (when !healthy()) - failure
  bool SeekToChunkContaining(Position new_pos);

  // Seeks to the nearest chunk boundary at or after the given position.
  //
  // Return values:
  //  * true                    - success (position is set to pos)
  //  * false (when healthy())  - source ends before new_pos (position is set to
  //                              the end) or seeking backwards is not supported
  //                              (position is unchanged)
  //  * false (when !healthy()) - failure
  bool SeekToChunkAfter(Position new_pos);

  // Returns the size of the file, i.e. the position corresponding to its end.
  //
  // Return values:
  //  * true  - success (*size is set, healthy())
  //  * false - failure (healthy() is unchanged)
  bool Size(Position* size) const;

 protected:
  void Done() override;

 private:
  struct Reading {
    void Reset();

    // Chunk header, filled up to chunk_header_read, and chunk data read so far.
    Chunk chunk;
    // Length of chunk.header read so far.
    size_t chunk_header_read = 0;
  };

  struct Recovering {
    void Reset();

    // If true, pos_ has been adjusted to a block boundary and skip_corruption_
    // has been checked.
    bool in_progress = false;
    // If true, chunk_begin is known since the block header has been read.
    bool chunk_begin_known = false;
    // Target position after recovery is complete.
    Position chunk_begin = 0;
  };

  // Interprets a false result from a byte_reader_ reading function. Always
  // returns false.
  bool ReadingFailed();

  // Reads or continues reading a chunk header into state_.
  //
  // Precondition: !is_recovering_
  bool ReadChunkHeader();

  // Reads or continues reading block_header_ if the current position is
  // immediately before or inside a block header, otherwise does nothing.
  bool ReadBlockHeader();

  // Prepares for reading a new chunk.
  void PrepareForReading();

  // Prepares for recovery (locating a chunk using block headers) even if
  // skip_corruption_ is false. pos_ must be a block boundary.
  void PrepareForFindingChunk();

  // Prepares for recovery (locating a chunk using block headers) if
  // skip_corruption_ is true, otherwise fails.
  void PrepareForRecovering();

  // Begins or continues recovery by reading block headers and looking for a
  // chunk beginning. Scans forwards only so that it does not require the Reader
  // to support random access.
  bool Recover();

  // Shared implementation of SeekToChunkContaining() (containing = true) and
  // SeekToChunkAfter() (containing = false).
  bool SeekToChunk(Position new_pos, bool containing);

  std::unique_ptr<Reader> owned_byte_reader_;
  // Invariant: if healthy() then byte_reader_ != nullptr
  Reader* byte_reader_;
  bool skip_corruption_;

  // Current position, excluding data buffered in reading_ or implied by
  // recovering_.
  //
  // If !is_recovering_, this is a chunk boundary.
  //
  // If is_recovering_, this is a block boundary.
  Position pos_;

  // If true, the source is truncated, but truncation was not reported yet
  // because byte_reader_->HopeForMore().
  bool is_truncated_ = false;

  // If true, recovery is needed (a chunk must be located using block headers),
  // either because corruption was detected and skip_corruption_ is true, or
  // after seeking to a block boundary.
  //
  // If false, the next chunk can be read from byte_reader_, or a truncated
  // chunk can resume reading.
  bool is_recovering_;

  union {
    Reading reading_;        // State if !is_recovering_.
    Recovering recovering_;  // State if is_recovering_.
  };

  // The block header, partially filled to the point derived from the current
  // position, if a block header is being read (which is determined by state_).
  internal::BlockHeader block_header_;
};

// Implementation details follow.

inline bool ChunkReader::Size(Position* size) const {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  return byte_reader_->Size(size);
}

}  // namespace riegeli

#endif  // RIEGELI_RECORDS_CHUNK_READER_H_
