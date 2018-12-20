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
#include <utility>

#include "absl/base/optimization.h"
#include "absl/types/optional.h"
#include "absl/utility/utility.h"
#include "riegeli/base/base.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/chunk_encoding/chunk.h"

namespace riegeli {

class Reader;

// A ChunkWriter writes chunks of a Riegeli/records file (rather than individual
// records, as RecordWriter does) to a destination.
//
// A ChunkWriter object can manage a buffer of data to be pushed to the
// destination, which amortizes the overhead of pushing data over multiple
// writes.
class ChunkWriter : public Object {
 public:
  explicit ChunkWriter(State state) noexcept : Object(state) {}

  ChunkWriter(ChunkWriter&& that) noexcept;
  ChunkWriter& operator=(ChunkWriter&& that) noexcept;

  ~ChunkWriter() override;

  // Writes a chunk, pushing data to the destination as needed.
  //
  // Return values:
  //  * true  - success (healthy())
  //  * false - failure (!healthy())
  virtual bool WriteChunk(const Chunk& chunk) = 0;

  // Writes padding to reach a 64KB block boundary.
  //
  // Return values:
  //  * true  - success (healthy())
  //  * false - failure (!healthy())
  virtual bool PadToBlockBoundary() = 0;

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
  //  * true  - success (healthy())
  //  * false - failure (!healthy())
  virtual bool Flush(FlushType flush_type) = 0;

  // Returns the current byte position. Unchanged by Close().
  Position pos() const { return pos_; }

 protected:
  void Initialize(Position pos) { pos_ = pos; }

  Position pos_ = 0;
};

// Template parameter invariant part of DefaultChunkWriter.
class DefaultChunkWriterBase : public ChunkWriter {
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
    Options& set_assumed_pos(absl::optional<Position> assumed_pos) & {
      assumed_pos_ = assumed_pos;
      return *this;
    }
    Options&& set_assumed_pos(absl::optional<Position> assumed_pos) && {
      return std::move(set_assumed_pos(assumed_pos));
    }

   private:
    template <typename Dest>
    friend class DefaultChunkWriter;

    absl::optional<Position> assumed_pos_;
  };

  // Returns the Riegeli/records file being written to. Unchanged by Close().
  virtual Writer* dest_writer() = 0;
  virtual const Writer* dest_writer() const = 0;

  bool WriteChunk(const Chunk& chunk) override;
  bool PadToBlockBoundary() override;
  bool Flush(FlushType flush_type) override;

 protected:
  explicit DefaultChunkWriterBase(State state) noexcept : ChunkWriter(state) {}

  DefaultChunkWriterBase(DefaultChunkWriterBase&& that) noexcept;
  DefaultChunkWriterBase& operator=(DefaultChunkWriterBase&& that) noexcept;

  void Initialize(Writer* dest, Position pos);

 private:
  bool WriteSection(Reader* src, Position chunk_begin, Position chunk_end,
                    Writer* dest);
  bool WritePadding(Position chunk_begin, Position chunk_end, Writer* dest);
};

// The default ChunkWriter. Writes chunks to a byte Writer, interleaving them
// with block headers at multiples of the Riegeli/records block size.
//
// The Dest template parameter specifies the type of the object providing and
// possibly owning the byte Writer. Dest must support Dependency<Writer*, Dest>,
// e.g. Writer* (not owned, default), unique_ptr<Writer> (owned),
// ChainWriter<> (owned).
//
// The byte Writer must not be accessed until the DefaultChunkWriter is closed
// or no longer used, except that it is allowed to read the destination of the
// byte Writer immediately after Flush().
template <typename Dest = Writer*>
class DefaultChunkWriter : public DefaultChunkWriterBase {
 public:
  DefaultChunkWriter() noexcept : DefaultChunkWriterBase(State::kClosed) {}

  // Will write to the byte Writer provided by dest.
  explicit DefaultChunkWriter(Dest dest, Options options = Options());

  DefaultChunkWriter(DefaultChunkWriter&& that) noexcept;
  DefaultChunkWriter& operator=(DefaultChunkWriter&& that) noexcept;

  // Returns the object providing and possibly owning the byte Writer. Unchanged
  // by Close().
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  Writer* dest_writer() override { return dest_.ptr(); }
  const Writer* dest_writer() const override { return dest_.ptr(); }

 protected:
  void Done() override;

 private:
  // The object providing and possibly owning the Riegeli/records file being
  // written to.
  Dependency<Writer*, Dest> dest_;
};

// Implementation details follow.

inline ChunkWriter::ChunkWriter(ChunkWriter&& that) noexcept
    : Object(std::move(that)), pos_(absl::exchange(that.pos_, 0)) {}

inline ChunkWriter& ChunkWriter::operator=(ChunkWriter&& that) noexcept {
  Object::operator=(std::move(that));
  pos_ = absl::exchange(that.pos_, 0);
  return *this;
}

inline DefaultChunkWriterBase::DefaultChunkWriterBase(
    DefaultChunkWriterBase&& that) noexcept
    : ChunkWriter(std::move(that)) {}

inline DefaultChunkWriterBase& DefaultChunkWriterBase::operator=(
    DefaultChunkWriterBase&& that) noexcept {
  ChunkWriter::operator=(std::move(that));
  return *this;
}

template <typename Dest>
DefaultChunkWriter<Dest>::DefaultChunkWriter(Dest dest, Options options)
    : DefaultChunkWriterBase(State::kOpen), dest_(std::move(dest)) {
  Initialize(dest_.ptr(), options.assumed_pos_.value_or(dest_->pos()));
}

template <typename Dest>
inline DefaultChunkWriter<Dest>::DefaultChunkWriter(
    DefaultChunkWriter&& that) noexcept
    : DefaultChunkWriterBase(std::move(that)), dest_(std::move(that.dest_)) {}

template <typename Dest>
inline DefaultChunkWriter<Dest>& DefaultChunkWriter<Dest>::operator=(
    DefaultChunkWriter&& that) noexcept {
  DefaultChunkWriterBase::operator=(std::move(that));
  dest_ = std::move(that.dest_);
  return *this;
}

template <typename Dest>
void DefaultChunkWriter<Dest>::Done() {
  DefaultChunkWriterBase::Done();
  if (dest_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) Fail(*dest_);
  }
}

extern template class DefaultChunkWriter<Writer*>;
extern template class DefaultChunkWriter<std::unique_ptr<Writer>>;

}  // namespace riegeli

#endif  // RIEGELI_RECORDS_CHUNK_WRITER_H_
