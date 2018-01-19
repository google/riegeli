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
#include "riegeli/base/assert.h"
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

RecordReader::RecordReader() : skip_corruption_(false), chunk_begin_(0) {
  MarkClosed();
}

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
    : chunk_reader_(std::move(chunk_reader)),
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
      chunk_decoder_(std::move(src.chunk_decoder_)) {}

RecordReader& RecordReader::operator=(RecordReader&& src) noexcept {
  if (&src != this) {
    Object::operator=(std::move(src));
    chunk_reader_ = std::move(src.chunk_reader_);
    skip_corruption_ = riegeli::exchange(src.skip_corruption_, false);
    chunk_begin_ = riegeli::exchange(src.chunk_begin_, 0);
    chunk_decoder_ = std::move(src.chunk_decoder_);
  }
  return *this;
}

RecordReader::~RecordReader() = default;

void RecordReader::Done() {
  if (RIEGELI_LIKELY(healthy())) {
    if (RIEGELI_UNLIKELY(!chunk_reader_->Close())) {
      Fail(chunk_reader_->Message());
    }
  }
  chunk_reader_.reset();
  skip_corruption_ = false;
  chunk_begin_ = 0;
  chunk_decoder_.Clear();
}

bool RecordReader::ReadRecord(google::protobuf::MessageLite* record,
                              RecordPosition* key) {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  for (;;) {
    uint64_t index;
    if (RIEGELI_LIKELY(chunk_decoder_.ReadRecord(record, &index))) {
      if (key != nullptr) *key = RecordPosition(chunk_begin_, index);
      return true;
    }
    if (RIEGELI_UNLIKELY(!chunk_decoder_.healthy())) {
      return Fail(chunk_decoder_.Message());
    }
    if (RIEGELI_UNLIKELY(!ReadChunk())) return false;
  }
}

template <typename String>
bool RecordReader::ReadRecordSlow(String* record, RecordPosition* key) {
  RIEGELI_ASSERT_GE(chunk_decoder_.index(), chunk_decoder_.num_records());
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  RIEGELI_ASSERT(chunk_decoder_.healthy());
  for (;;) {
    if (RIEGELI_UNLIKELY(!ReadChunk())) return false;
    uint64_t index;
    if (RIEGELI_LIKELY(chunk_decoder_.ReadRecord(record, &index))) {
      if (key != nullptr) *key = RecordPosition(chunk_begin_, index);
      return true;
    }
    RIEGELI_ASSERT(chunk_decoder_.healthy());
  }
}

template bool RecordReader::ReadRecordSlow(string_view* record,
                                           RecordPosition* key);
template bool RecordReader::ReadRecordSlow(std::string* record, RecordPosition* key);
template bool RecordReader::ReadRecordSlow(Chain* record, RecordPosition* key);

bool RecordReader::Seek(RecordPosition new_pos) {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  if (new_pos.chunk_begin() == chunk_begin_) {
    if (new_pos.record_index() == 0) {
      // Seeking to the beginning of a chunk does not need pulling the chunk nor
      // checking its size, which is important because it may be non-existent at
      // end of file, corrupted, or empty.
      chunk_decoder_.SetIndex(0);
      return true;
    }
  } else {
    if (RIEGELI_UNLIKELY(!chunk_reader_->Seek(new_pos.chunk_begin()))) {
      chunk_begin_ = chunk_reader_->pos();
      chunk_decoder_.Clear();
      if (chunk_reader_->healthy()) return false;
      return Fail(chunk_reader_->Message());
    }
    if (new_pos.record_index() == 0) {
      // Seeking to the beginning of a chunk does not need pulling the chunk nor
      // checking its size, which is important because it may be non-existent at
      // end of file, corrupted, or empty.
      chunk_begin_ = chunk_reader_->pos();
      chunk_decoder_.Clear();
      return true;
    }
    if (RIEGELI_UNLIKELY(!ReadChunk())) return false;
  }
  RIEGELI_ASSERT_EQ(new_pos.chunk_begin(), chunk_begin_);
  chunk_decoder_.SetIndex(new_pos.record_index());
  return true;
}

bool RecordReader::Seek(Position new_pos) {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  if (new_pos >= chunk_begin_ && new_pos <= chunk_reader_->pos()) {
    // Seeking inside or just after the current chunk which has been pulled,
    // or to the beginning of the current chunk which has been located,
    // or to the end of file which has been reached.
  } else {
    if (RIEGELI_UNLIKELY(!chunk_reader_->SeekToChunkContaining(new_pos))) {
      chunk_begin_ = chunk_reader_->pos();
      chunk_decoder_.Clear();
      if (chunk_reader_->healthy()) return false;
      return Fail(chunk_reader_->Message());
    }
    if (chunk_reader_->pos() >= new_pos) {
      // Seeking to the beginning of a chunk does not need pulling the chunk,
      // which is important because it may be non-existent at end of file or
      // corrupted.
      chunk_begin_ = chunk_reader_->pos();
      chunk_decoder_.Clear();
      return true;
    }
    if (RIEGELI_UNLIKELY(!ReadChunk())) return false;
  }
  RIEGELI_ASSERT_GE(new_pos, chunk_begin_);
  chunk_decoder_.SetIndex(new_pos - chunk_begin_);
  return true;
}

inline bool RecordReader::ReadChunk() {
again:
  Chunk chunk;
  if (RIEGELI_UNLIKELY(!chunk_reader_->ReadChunk(&chunk, &chunk_begin_))) {
    chunk_begin_ = chunk_reader_->pos();
    chunk_decoder_.Clear();
    if (chunk_reader_->healthy()) return false;
    return Fail(chunk_reader_->Message());
  }
  if (chunk_begin_ == 0) {
    // Verify file signature.
    if (RIEGELI_UNLIKELY(chunk.header.data_size() != 0 ||
                         chunk.header.num_records() != 0 ||
                         chunk.header.decoded_data_size() != 0)) {
      chunk_decoder_.Clear();
      return Fail("Invalid Riegeli/records file: missing file signature");
    }
    // Decoding this chunk will yield no records and ReadChunk() will be called
    // again if needed.
  }
  if (RIEGELI_UNLIKELY(!chunk_decoder_.Reset(chunk))) {
    if (skip_corruption_) {
      chunk_decoder_.Clear();
      goto again;
    }
    const std::string message = chunk_decoder_.Message();
    chunk_decoder_.Clear();
    return Fail(message);
  }
  return true;
}

}  // namespace riegeli
