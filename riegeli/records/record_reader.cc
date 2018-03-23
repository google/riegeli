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

#include "riegeli/records/record_reader.h"

#include <memory>
#include <string>
#include <utility>

#include "google/protobuf/message_lite.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/memory.h"
#include "riegeli/base/object.h"
#include "riegeli/base/string_view.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/chunk_encoding/chunk_decoder.h"
#include "riegeli/records/chunk_reader.h"
#include "riegeli/records/record_position.h"

namespace riegeli {

RecordReader::RecordReader() noexcept : Object(State::kClosed) {}

RecordReader::RecordReader(std::unique_ptr<Reader> byte_reader, Options options)
    : RecordReader(riegeli::make_unique<ChunkReader>(
                       std::move(byte_reader),
                       ChunkReader::Options().set_skip_corruption(
                           options.skip_corruption_)),
                   std::move(options)) {}

RecordReader::RecordReader(Reader* byte_reader, Options options)
    : RecordReader(riegeli::make_unique<ChunkReader>(
                       byte_reader, ChunkReader::Options().set_skip_corruption(
                                        options.skip_corruption_)),
                   std::move(options)) {}

inline RecordReader::RecordReader(std::unique_ptr<ChunkReader> chunk_reader,
                                  Options options)
    : Object(State::kOpen),
      chunk_reader_(std::move(chunk_reader)),
      skip_corruption_(options.skip_corruption_),
      chunk_begin_(chunk_reader_->pos()),
      chunk_decoder_(ChunkDecoder::Options()
                         .set_skip_corruption(options.skip_corruption_)
                         .set_field_filter(std::move(options.field_filter_))) {
  if (chunk_begin_ == 0 && !skip_corruption_) {
    // Verify file signature before any records are read, done proactively here
    // in case the caller calls Seek() before ReadRecord(). This is not done if
    // skip_corruption_ is true because in this case invalid file beginning
    // would cause scanning the file until a valid chunk is found, which might
    // not be intended.
    ReadChunk();
  }
}

RecordReader::RecordReader(RecordReader&& src) noexcept
    : Object(std::move(src)),
      chunk_reader_(std::move(src.chunk_reader_)),
      skip_corruption_(riegeli::exchange(src.skip_corruption_, false)),
      chunk_begin_(riegeli::exchange(src.chunk_begin_, 0)),
      chunk_decoder_(std::move(src.chunk_decoder_)),
      corrupted_bytes_skipped_(
          riegeli::exchange(src.corrupted_bytes_skipped_, 0)) {}

RecordReader& RecordReader::operator=(RecordReader&& src) noexcept {
  Object::operator=(std::move(src));
  chunk_reader_ = std::move(src.chunk_reader_);
  skip_corruption_ = riegeli::exchange(src.skip_corruption_, false);
  chunk_begin_ = riegeli::exchange(src.chunk_begin_, 0);
  chunk_decoder_ = std::move(src.chunk_decoder_);
  corrupted_bytes_skipped_ = riegeli::exchange(src.corrupted_bytes_skipped_, 0);
  return *this;
}

void RecordReader::Done() {
  if (chunk_reader_ != nullptr) {
    if (RIEGELI_UNLIKELY(!chunk_reader_->Close())) Fail(*chunk_reader_);
    // Do not reset chunk_reader_ so that corrupted_bytes_skipped() remains
    // available.
  }
  skip_corruption_ = false;
  chunk_begin_ = 0;
  chunk_decoder_ = ChunkDecoder();
}

template <typename Record>
bool RecordReader::ReadRecordSlow(Record* record, RecordPosition* key) {
  RIEGELI_ASSERT_EQ(chunk_decoder_.index(), chunk_decoder_.num_records())
      << "Failed precondition of RecordReader::ReadRecordSlow(): "
         "records available, use ReadRecord() instead";
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  RIEGELI_ASSERT(chunk_decoder_.healthy())
      << "Failed invariant of RecordReader: "
         "RecordReader healthy but ChunkDecoder unhealthy";
  for (;;) {
    if (RIEGELI_UNLIKELY(!ReadChunk())) return false;
    uint64_t index;
    if (RIEGELI_LIKELY(chunk_decoder_.ReadRecord(record, &index))) {
      if (key != nullptr) *key = RecordPosition(chunk_begin_, index);
      return true;
    }
    if (RIEGELI_UNLIKELY(!chunk_decoder_.healthy())) {
      RIEGELI_ASSERT(!skip_corruption_)
          << "ChunkDecoder failed but skip_corruption is true";
      return Fail(chunk_decoder_);
    }
  }
}

template bool RecordReader::ReadRecordSlow(google::protobuf::MessageLite* record,
                                           RecordPosition* key);
template bool RecordReader::ReadRecordSlow(string_view* record,
                                           RecordPosition* key);
template bool RecordReader::ReadRecordSlow(std::string* record, RecordPosition* key);
template bool RecordReader::ReadRecordSlow(Chain* record, RecordPosition* key);

bool RecordReader::Seek(RecordPosition new_pos) {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  if (new_pos.chunk_begin() == chunk_begin_) {
    if (new_pos.record_index() == 0 || chunk_reader_->pos() > chunk_begin_) {
      // Seeking to the beginning of a chunk does not need reading the chunk nor
      // checking its size, which is important because it may be non-existent at
      // end of file, corrupted, or empty.
      //
      // If chunk_reader_->pos() > chunk_begin_, the chunk is already read.
      goto skip_reading_chunk;
    }
  } else {
    if (RIEGELI_UNLIKELY(!chunk_reader_->Seek(new_pos.chunk_begin()))) {
      chunk_begin_ = chunk_reader_->pos();
      chunk_decoder_.Reset();
      if (RIEGELI_LIKELY(chunk_reader_->healthy())) return false;
      return Fail(*chunk_reader_);
    }
    if (new_pos.record_index() == 0 ||
        RIEGELI_UNLIKELY(chunk_reader_->pos() > new_pos.chunk_begin())) {
      // Seeking to the beginning of a chunk does not need reading the chunk nor
      // checking its size, which is important because it may be non-existent at
      // end of file, corrupted, or empty.
      //
      // If chunk_reader_->pos() > new_pos.chunk_begin(), corruption was
      // skipped.
      chunk_begin_ = chunk_reader_->pos();
      chunk_decoder_.Reset();
      return true;
    }
  }
  if (RIEGELI_UNLIKELY(!ReadChunk())) return false;
  if (RIEGELI_UNLIKELY(chunk_begin_ > new_pos.chunk_begin())) {
    // Corruption was skipped. Leave the position after the corruption.
    return true;
  }
skip_reading_chunk:
  chunk_decoder_.SetIndex(new_pos.record_index());
  return true;
}

bool RecordReader::Seek(Position new_pos) {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  if (new_pos >= chunk_begin_ && new_pos <= chunk_reader_->pos()) {
    // Seeking inside or just after the current chunk which has been read,
    // or to the beginning of the current chunk which has been located,
    // or to the end of file which has been reached.
  } else {
    if (RIEGELI_UNLIKELY(!chunk_reader_->SeekToChunkContaining(new_pos))) {
      chunk_begin_ = chunk_reader_->pos();
      chunk_decoder_.Reset();
      if (RIEGELI_LIKELY(chunk_reader_->healthy())) return false;
      return Fail(*chunk_reader_);
    }
    if (chunk_reader_->pos() >= new_pos) {
      // If chunk_reader_->pos() == new_pos, seeking to the beginning of a
      // chunk. This does not need reading the chunk, which is important because
      // it may be non-existent at end of file or corrupted.
      //
      // If chunk_reader_->pos() > new_pos, corruption was skipped.
      chunk_begin_ = chunk_reader_->pos();
      chunk_decoder_.Reset();
      return true;
    }
    if (RIEGELI_UNLIKELY(!ReadChunk())) return false;
    if (RIEGELI_UNLIKELY(chunk_begin_ > new_pos)) {
      // Corruption was skipped. Leave the position after the corruption.
      return true;
    }
  }
  chunk_decoder_.SetIndex(IntCast<uint64_t>(new_pos - chunk_begin_));
  return true;
}

inline bool RecordReader::ReadChunk() {
  Chunk chunk;
  for (;;) {
    if (RIEGELI_UNLIKELY(!chunk_reader_->ReadChunk(&chunk, &chunk_begin_))) {
      chunk_begin_ = chunk_reader_->pos();
      chunk_decoder_.Reset();
      if (RIEGELI_LIKELY(chunk_reader_->healthy())) return false;
      return Fail(*chunk_reader_);
    }
    if (chunk_begin_ == 0) {
      // Verify file signature.
      if (RIEGELI_UNLIKELY(chunk.header.data_size() != 0 ||
                           chunk.header.num_records() != 0 ||
                           chunk.header.decoded_data_size() != 0)) {
        chunk_decoder_.Reset();
        return Fail("Invalid Riegeli/records file: missing file signature");
      }
      // Decoding this chunk will yield no records and ReadChunk() will be
      // called again if needed.
    }
    if (RIEGELI_LIKELY(chunk_decoder_.Reset(chunk))) return true;
    if (!skip_corruption_) return Fail(chunk_decoder_);
    chunk_decoder_.Reset();
    corrupted_bytes_skipped_ = SaturatingAdd(
        corrupted_bytes_skipped_, chunk_reader_->pos() - chunk_begin_);
  }
}

}  // namespace riegeli
