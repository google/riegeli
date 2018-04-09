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
#include "absl/base/optimization.h"
#include "absl/memory/memory.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/chunk_encoding/chunk_decoder.h"
#include "riegeli/records/chunk_reader.h"
#include "riegeli/records/record_position.h"

namespace riegeli {

RecordReader::RecordReader() noexcept : Object(State::kClosed) {}

RecordReader::RecordReader(std::unique_ptr<Reader> byte_reader, Options options)
    : RecordReader(
          absl::make_unique<ChunkReader>(
              std::move(byte_reader),
              ChunkReader::Options().set_skip_errors(options.skip_errors_)),
          std::move(options)) {}

RecordReader::RecordReader(Reader* byte_reader, Options options)
    : RecordReader(absl::make_unique<ChunkReader>(
                       byte_reader, ChunkReader::Options().set_skip_errors(
                                        options.skip_errors_)),
                   std::move(options)) {}

inline RecordReader::RecordReader(std::unique_ptr<ChunkReader> chunk_reader,
                                  Options options)
    : Object(State::kOpen),
      chunk_reader_(std::move(chunk_reader)),
      skip_errors_(options.skip_errors_),
      chunk_begin_(chunk_reader_->pos()),
      chunk_decoder_(ChunkDecoder::Options()
                         .set_skip_errors(options.skip_errors_)
                         .set_field_filter(std::move(options.field_filter_))) {
  if (chunk_begin_ == 0 && !skip_errors_) {
    // Verify file signature before any records are read, done proactively here
    // in case the caller calls Seek() before ReadRecord(). This is not done if
    // skip_errors_ is true because in this case invalid file beginning would
    // cause scanning the file until a valid chunk is found, which might not be
    // intended.
    ReadChunk();
  }
}

RecordReader::RecordReader(RecordReader&& src) noexcept
    : Object(std::move(src)),
      chunk_reader_(std::move(src.chunk_reader_)),
      skip_errors_(riegeli::exchange(src.skip_errors_, false)),
      chunk_begin_(riegeli::exchange(src.chunk_begin_, 0)),
      chunk_decoder_(std::move(src.chunk_decoder_)),
      skipped_bytes_(riegeli::exchange(src.skipped_bytes_, 0)) {}

RecordReader& RecordReader::operator=(RecordReader&& src) noexcept {
  Object::operator=(std::move(src));
  chunk_reader_ = std::move(src.chunk_reader_);
  skip_errors_ = riegeli::exchange(src.skip_errors_, false);
  chunk_begin_ = riegeli::exchange(src.chunk_begin_, 0);
  chunk_decoder_ = std::move(src.chunk_decoder_);
  skipped_bytes_ = riegeli::exchange(src.skipped_bytes_, 0);
  return *this;
}

void RecordReader::Done() {
  if (chunk_reader_ != nullptr) {
    if (ABSL_PREDICT_FALSE(!chunk_reader_->Close())) Fail(*chunk_reader_);
    // Do not reset chunk_reader_ so that skipped_bytes() remains
    // available.
  }
  skip_errors_ = false;
  chunk_begin_ = 0;
  chunk_decoder_ = ChunkDecoder();
}

bool RecordReader::ReadRecordSlow(google::protobuf::MessageLite* record,
                                  RecordPosition* key, uint64_t index_before) {
  RIEGELI_ASSERT_EQ(chunk_decoder_.index(), chunk_decoder_.num_records())
      << "Failed precondition of RecordReader::ReadRecordSlow(): "
         "records available, use ReadRecord() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  for (;;) {
    if (ABSL_PREDICT_FALSE(!chunk_decoder_.healthy())) {
      RIEGELI_ASSERT(!skip_errors_)
          << "ChunkDecoder::ReadRecord() made ChunkDecoder unhealthy "
             "but skip_errors is true";
      return Fail(chunk_decoder_);
    }
    if (skip_errors_) {
      RIEGELI_ASSERT_GE(chunk_decoder_.index(), index_before)
          << "ChunkDecoder::ReadRecord() decremented record index";
      if (ABSL_PREDICT_FALSE(chunk_decoder_.index() > index_before)) {
        // Last records of the chunk were skipped. In skipped_bytes_, account
        // for them as the rest of the chunk size.
        RIEGELI_ASSERT_GE(chunk_reader_->pos(), chunk_begin_)
            << "Failed invariant of RecordReader: negative chunk size";
        const Position chunk_size = chunk_reader_->pos() - chunk_begin_;
        RIEGELI_ASSERT_LE(chunk_decoder_.index(), chunk_size)
            << "Failed invariant of RecordReader: "
               "number of records greater than chunk size";
        skipped_bytes_ =
            SaturatingAdd(skipped_bytes_, chunk_size - index_before);
      }
    }
    if (ABSL_PREDICT_FALSE(!ReadChunk())) return false;
    index_before = chunk_decoder_.index();
    if (ABSL_PREDICT_TRUE(chunk_decoder_.ReadRecord(record))) {
      RIEGELI_ASSERT_GT(chunk_decoder_.index(), index_before)
          << "ChunkDecoder::ReadRecord() did not increment record index";
      if (key != nullptr) {
        *key = RecordPosition(chunk_begin_, chunk_decoder_.index() - 1);
      }
      const uint64_t skipped_records =
          chunk_decoder_.index() - index_before - 1;
      if (ABSL_PREDICT_FALSE(skipped_records > 0)) {
        // Records other than last records of the chunk were skipped.
        // In skipped_bytes_, account for them as one byte each.
        skipped_bytes_ = SaturatingAdd(skipped_bytes_, skipped_records);
      }
      return true;
    }
  }
}

template <typename Record>
bool RecordReader::ReadRecordSlow(Record* record, RecordPosition* key) {
  RIEGELI_ASSERT_EQ(chunk_decoder_.index(), chunk_decoder_.num_records())
      << "Failed precondition of RecordReader::ReadRecordSlow(): "
         "records available, use ReadRecord() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  for (;;) {
    RIEGELI_ASSERT(chunk_decoder_.healthy())
        << "ChunkDecoder::ReadRecord() made ChunkDecoder unhealthy "
           "but record was not being parsed to a proto message";
    if (ABSL_PREDICT_FALSE(!ReadChunk())) return false;
    if (ABSL_PREDICT_TRUE(chunk_decoder_.ReadRecord(record))) {
      if (key != nullptr) {
        RIEGELI_ASSERT_GT(chunk_decoder_.index(), 0u)
            << "ChunkDecoder::ReadRecord() left record index at 0";
        *key = RecordPosition(chunk_begin_, chunk_decoder_.index() - 1);
      }
      return true;
    }
  }
}

template bool RecordReader::ReadRecordSlow(absl::string_view* record,
                                           RecordPosition* key);
template bool RecordReader::ReadRecordSlow(std::string* record, RecordPosition* key);
template bool RecordReader::ReadRecordSlow(Chain* record, RecordPosition* key);

bool RecordReader::Seek(RecordPosition new_pos) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
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
    if (ABSL_PREDICT_FALSE(!chunk_reader_->Seek(new_pos.chunk_begin()))) {
      chunk_begin_ = chunk_reader_->pos();
      chunk_decoder_.Reset();
      if (ABSL_PREDICT_TRUE(chunk_reader_->healthy())) return false;
      return Fail(*chunk_reader_);
    }
    if (new_pos.record_index() == 0 ||
        ABSL_PREDICT_FALSE(chunk_reader_->pos() > new_pos.chunk_begin())) {
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
  if (ABSL_PREDICT_FALSE(!ReadChunk())) return false;
  if (ABSL_PREDICT_FALSE(chunk_begin_ > new_pos.chunk_begin())) {
    // Corruption was skipped. Leave the position after the corruption.
    return true;
  }
skip_reading_chunk:
  chunk_decoder_.SetIndex(new_pos.record_index());
  return true;
}

bool RecordReader::Seek(Position new_pos) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (new_pos >= chunk_begin_ && new_pos <= chunk_reader_->pos()) {
    // Seeking inside or just after the current chunk which has been read,
    // or to the beginning of the current chunk which has been located,
    // or to the end of file which has been reached.
  } else {
    if (ABSL_PREDICT_FALSE(!chunk_reader_->SeekToChunkContaining(new_pos))) {
      chunk_begin_ = chunk_reader_->pos();
      chunk_decoder_.Reset();
      if (ABSL_PREDICT_TRUE(chunk_reader_->healthy())) return false;
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
    if (ABSL_PREDICT_FALSE(!ReadChunk())) return false;
    if (ABSL_PREDICT_FALSE(chunk_begin_ > new_pos)) {
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
    if (ABSL_PREDICT_FALSE(!chunk_reader_->ReadChunk(&chunk, &chunk_begin_))) {
      chunk_begin_ = chunk_reader_->pos();
      chunk_decoder_.Reset();
      if (ABSL_PREDICT_TRUE(chunk_reader_->healthy())) return false;
      return Fail(*chunk_reader_);
    }
    if (chunk_begin_ == 0) {
      // Verify file signature.
      if (ABSL_PREDICT_FALSE(chunk.header.data_size() != 0 ||
                             chunk.header.num_records() != 0 ||
                             chunk.header.decoded_data_size() != 0)) {
        chunk_decoder_.Reset();
        return Fail("Invalid Riegeli/records file: missing file signature");
      }
      // Decoding this chunk will yield no records and ReadChunk() will be
      // called again if needed.
    }
    if (ABSL_PREDICT_TRUE(chunk_decoder_.Reset(chunk))) return true;
    if (!skip_errors_) return Fail(chunk_decoder_);
    chunk_decoder_.Reset();
    skipped_bytes_ =
        SaturatingAdd(skipped_bytes_, chunk_reader_->pos() - chunk_begin_);
  }
}

}  // namespace riegeli
