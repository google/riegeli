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

#include <memory>

#include "riegeli/base/base.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/chunk_encoding/chunk.h"

namespace riegeli {

class Reader;

// A ChunkWriter writes chunks of a Riegeli/records file (rather than individual
// records, as RecordWriter does) to a destination. The nature of the
// destination depends on the particular class derived from ChunkWriter.
//
// Specifying a ChunkWriter instead of a byte Writer as the destination of a
// RecordWriter allows to customize how chunks are stored, e.g. by forwarding
// them to a DefaultChunkWriter running elsewhere.
//
// A ChunkWriter object can manage a buffer of data to be pushed to the
// destination, which amortizes the overhead of pushing data over multiple
// writes.
class ChunkWriter : public Object {
 public:
  explicit ChunkWriter(State state) noexcept : Object(state) {}

  ChunkWriter(const ChunkWriter&) = delete;
  ChunkWriter& operator=(const ChunkWriter&) = delete;

  ~ChunkWriter() override;

  // Writes a chunk, pushing data to the destination as needed.
  //
  // Return values:
  //  * true  - success (healthy())
  //  * false - failure (!healthy())
  virtual bool WriteChunk(const Chunk& chunk) = 0;

  // Pushes buffered data to the destination.
  //
  // Additionally, attempts to ensure the following, depending on flush_type
  // (without a guarantee though):
  //  * FlushType::kFromObject  - nothing
  //  * FlushType::kFromProcess - data survives process crash
  //  * FlushType::kFromMachine - data survives operating system crash
  //
  // The precise meaning of Flush() depends on the particular ChunkWriter. The
  // intent is to make data written so far visible, but in contrast to Close(),
  // keeping the possibility to write more data later.
  //
  // Return values:
  //  * true                    - success (pushed and synced, healthy())
  //  * false (when healthy())  - failure to sync
  //  * false (when !healthy()) - failure to push
  virtual bool Flush(FlushType flush_type) = 0;

  // Returns the current byte position. Unchanged by Close().
  Position pos() const { return pos_; }

 protected:
  Position pos_ = 0;
};

// The default ChunkWriter. Writes chunks to a byte Writer, interleaving them
// with block headers at multiples of the Riegeli/records block size.
class DefaultChunkWriter final : public ChunkWriter {
 public:
  class Options {
   public:
    Options() noexcept {}

    // Sets the file position assumed initially.
    //
    // This can be used to prepare a file fragment which can be appended to the
    // target file at the given position.
    //
    // Default: byte_writer->pos()
    Options& set_assumed_pos(Position assumed_pos) & {
      has_assumed_pos_ = true;
      assumed_pos_ = assumed_pos;
      return *this;
    }
    Options&& set_assumed_pos(Position assumed_pos) && {
      return std::move(set_assumed_pos(assumed_pos));
    }

   private:
    friend class DefaultChunkWriter;

    bool has_assumed_pos_ = false;
    Position assumed_pos_ = 0;
  };

  // Will write chunks to the byte Writer which is owned by this ChunkWriter and
  // will be closed and deleted when the ChunkWriter is closed.
  explicit DefaultChunkWriter(std::unique_ptr<Writer> byte_writer,
                              Options options = Options());

  // Will write chunks to the byte Writer which is not owned by this ChunkWriter
  // and must be kept alive but not accessed until closing the ChunkWriter.
  explicit DefaultChunkWriter(Writer* byte_writer, Options options = Options());

  bool WriteChunk(const Chunk& chunk) override;
  bool Flush(FlushType flush_type) override;

 protected:
  void Done() override;

 private:
  bool WriteSection(Reader* src, Position chunk_begin, Position chunk_end);
  bool WritePadding(Position chunk_begin, Position chunk_end);

  std::unique_ptr<Writer> owned_byte_writer_;
  // Invariant: if healthy() then byte_writer_ != nullptr
  Writer* byte_writer_;
};

}  // namespace riegeli

#endif  // RIEGELI_RECORDS_CHUNK_WRITER_H_
