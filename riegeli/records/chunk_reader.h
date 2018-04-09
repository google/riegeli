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

#include "absl/base/optimization.h"
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

    // If true, corrupted regions will be skipped. If false, they will cause
    // reading to fail.
    //
    // Default: false
    Options& set_skip_errors(bool skip_errors) & {
      skip_errors_ = skip_errors;
      return *this;
    }
    Options&& set_skip_errors(bool skip_errors) && {
      return std::move(set_skip_errors(skip_errors));
    }

   private:
    friend class ChunkReader;

    bool skip_errors_ = false;
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
  // options.set_skip_errors(true) should not be used, otherwise the whole
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

  // Seeks to new_pos, which should be a chunk boundary (or a block boundary
  // which is interpreted as the nearest chunk boundary at or after the block
  // boundary).
  //
  // Return values:
  //  * true                    - success (position is set to new_pos)
  //  * false (when healthy())  - source ends before new_pos (position is set to
  //                              the end) or seeking backwards is not supported
  //                              (position is unchanged)
  //  * false (when !healthy()) - failure
  bool Seek(Position new_pos);

  // Seeks to the nearest chunk boundary before or at new_pos if the position
  // corresponds to some numeric record position in the following chunk (i.e. is
  // less than num_records bytes after chunk beginning), otherwise seeks to the
  // nearest chunk boundary at or after the given position.
  //
  // Return values:
  //  * true                    - success (position is set to new_pos)
  //  * false (when healthy())  - source ends before new_pos (position is set to
  //                              the end) or seeking backwards is not supported
  //                              (position is unchanged)
  //  * false (when !healthy()) - failure
  bool SeekToChunkContaining(Position new_pos);

  // Seeks to the nearest chunk boundary at or after new_pos.
  //
  // Return values:
  //  * true                    - success (position is set to new_pos)
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

  // Returns the number of bytes skipped because of corrupted regions.
  Position skipped_bytes() const { return skipped_bytes_; }

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

    // If true, recovery is caused by corruption.
    bool corrupted = false;
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
  //
  // If the block header is invalid, this is treated as corruption and pos_ is
  // changed to begin a new recovery past this block header.
  bool ReadBlockHeader();

  // Prepares for reading a new chunk.
  void PrepareForReading();

  // Prepares for recovery (locating a chunk using block headers).
  void PrepareForRecovering();

  // Increases pos_ over a region which should be counted as corrupted if
  // recovering because of corruption.
  //
  // Preconditions:
  //   is_recovering_
  //   new_pos >= pos_
  void SeekOverCorruption(Position new_pos);

  // Begins or continues recovery by reading block headers and looking for a
  // chunk beginning. Scans forwards only so that it does not require the Reader
  // to support random access.
  //
  // Precondition: is_recovering_
  bool Recover();

  // Reports an invalid chunk boundary. Returns true if recovery should be
  // attempted.
  bool InvalidChunkBoundary();

  // Shared implementation of SeekToChunkContaining() (containing = true) and
  // SeekToChunkAfter() (containing = false).
  bool SeekToChunk(Position new_pos, bool containing);

  std::unique_ptr<Reader> owned_byte_reader_;
  // Invariant: if healthy() then byte_reader_ != nullptr
  Reader* byte_reader_;
  bool skip_errors_;

  // Current position, excluding data buffered in reading_ or implied by
  // recovering_.
  //
  // If !is_recovering_, this is a chunk boundary.
  //
  // If is_recovering_, this is a block boundary or end of file.
  Position pos_;

  // If true, the current chunk is incomplete, but this was not reported yet as
  // a truncated file because HopeForMore() is true.
  bool current_chunk_is_incomplete_ = false;

  // The number of bytes skipped because of corrupted regions.
  Position skipped_bytes_ = 0;

  // If true, recovery is needed (a chunk must be located using block headers),
  // either because corruption was detected and skip_errors_ is true, or
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
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  return byte_reader_->Size(size);
}

}  // namespace riegeli

#endif  // RIEGELI_RECORDS_CHUNK_READER_H_
