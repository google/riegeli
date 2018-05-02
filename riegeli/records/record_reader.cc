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

#include <stdint.h>
#include <memory>
#include <string>
#include <utility>

#include "google/protobuf/message_lite.h"
#include "absl/base/optimization.h"
#include "absl/memory/memory.h"
#include "absl/strings/str_cat.h"
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
    : RecordReader(absl::make_unique<ChunkReader>(std::move(byte_reader)),
                   std::move(options)) {}

RecordReader::RecordReader(Reader* byte_reader, Options options)
    : RecordReader(absl::make_unique<ChunkReader>(byte_reader),
                   std::move(options)) {}

inline RecordReader::RecordReader(std::unique_ptr<ChunkReader> chunk_reader,
                                  Options options)
    : Object(State::kOpen),
      chunk_reader_(std::move(chunk_reader)),
      chunk_begin_(chunk_reader_->pos()),
      chunk_decoder_(ChunkDecoder::Options().set_field_filter(
          std::move(options.field_filter_))) {}

RecordReader::RecordReader(RecordReader&& src) noexcept
    : Object(std::move(src)),
      chunk_reader_(std::move(src.chunk_reader_)),
      chunk_begin_(riegeli::exchange(src.chunk_begin_, 0)),
      chunk_decoder_(std::move(src.chunk_decoder_)),
      recoverable_(riegeli::exchange(src.recoverable_, Recoverable::kNo)),
      recoverable_length_(riegeli::exchange(src.recoverable_length_, 0)) {}

RecordReader& RecordReader::operator=(RecordReader&& src) noexcept {
  Object::operator=(std::move(src));
  chunk_reader_ = std::move(src.chunk_reader_);
  chunk_begin_ = riegeli::exchange(src.chunk_begin_, 0);
  chunk_decoder_ = std::move(src.chunk_decoder_);
  recoverable_ = riegeli::exchange(src.recoverable_, Recoverable::kNo);
  recoverable_length_ = riegeli::exchange(src.recoverable_length_, 0);
  return *this;
}

void RecordReader::Done() {
  recoverable_ = Recoverable::kNo;
  recoverable_length_ = 0;
  if (ABSL_PREDICT_TRUE(healthy())) {
    if (ABSL_PREDICT_FALSE(!chunk_reader_->Close())) {
      Fail(*chunk_reader_);
      if (chunk_reader_->Recover(&recoverable_length_)) {
        recoverable_ = Recoverable::kReportSkippedBytes;
      }
    }
  }
  chunk_reader_.reset();
  chunk_begin_ = 0;
  chunk_decoder_ = ChunkDecoder();
}

bool RecordReader::CheckFileFormat() {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (chunk_decoder_.index() < chunk_decoder_.num_records()) return true;
  if (ABSL_PREDICT_FALSE(!chunk_reader_->CheckFileFormat())) {
    chunk_decoder_.Reset();
    if (ABSL_PREDICT_TRUE(chunk_reader_->healthy())) return false;
    recoverable_ = Recoverable::kRecoverChunkReader;
    return Fail(*chunk_reader_);
  }
  return true;
}

template <typename Record>
bool RecordReader::ReadRecordSlow(Record* record, RecordPosition* key) {
  if (chunk_decoder_.healthy()) {
    RIEGELI_ASSERT_EQ(chunk_decoder_.index(), chunk_decoder_.num_records())
        << "Failed precondition of RecordReader::ReadRecordSlow(): "
           "records available, use ReadRecord() instead";
  }
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  for (;;) {
    if (ABSL_PREDICT_FALSE(!chunk_decoder_.healthy())) {
      recoverable_ = Recoverable::kRecoverChunkDecoder;
      return Fail(chunk_decoder_);
    }
    if (ABSL_PREDICT_FALSE(!ReadChunk())) return false;
    if (ABSL_PREDICT_TRUE(chunk_decoder_.ReadRecord(record))) {
      RIEGELI_ASSERT_GT(chunk_decoder_.index(), 0u)
          << "ChunkDecoder::ReadRecord() left record index at 0";
      if (key != nullptr) {
        *key = RecordPosition(chunk_begin_, chunk_decoder_.index() - 1);
      }
      return true;
    }
  }
}

template bool RecordReader::ReadRecordSlow(google::protobuf::MessageLite* record,
                                           RecordPosition* key);
template bool RecordReader::ReadRecordSlow(absl::string_view* record,
                                           RecordPosition* key);
template bool RecordReader::ReadRecordSlow(std::string* record, RecordPosition* key);
template bool RecordReader::ReadRecordSlow(Chain* record, RecordPosition* key);

bool RecordReader::Recover(Position* skipped_bytes,
                           Position* skipped_messages) {
  if (recoverable_ == Recoverable::kNo) return false;
  RIEGELI_ASSERT(!healthy()) << "Failed invariant of RecordReader: "
                                "recovery applicable but RecordReader healthy";
  const Recoverable recoverable = recoverable_;
  recoverable_ = Recoverable::kNo;
  if (recoverable != Recoverable::kReportSkippedBytes) {
    RIEGELI_ASSERT(!closed()) << "Failed invariant of RecordReader: "
                                 "recovery does not only report skipped bytes "
                                 "but RecordReader is closed";
  }
  MarkNotFailed();
  switch (recoverable) {
    case Recoverable::kNo:
      RIEGELI_ASSERT_UNREACHABLE() << "kNo handled above";
    case Recoverable::kRecoverChunkReader:
      if (ABSL_PREDICT_FALSE(!chunk_reader_->Recover(skipped_bytes))) {
        return Fail(*chunk_reader_);
      }
      return true;
    case Recoverable::kRecoverChunkDecoder:
      if (ABSL_PREDICT_FALSE(!chunk_decoder_.Recover())) {
        return Fail(chunk_decoder_);
      }
      if (skipped_messages != nullptr) {
        *skipped_messages = SaturatingAdd(*skipped_messages, Position{1});
      }
      return true;
    case Recoverable::kReportSkippedBytes:
      if (skipped_bytes != nullptr) {
        *skipped_bytes = SaturatingAdd(*skipped_bytes, recoverable_length_);
      }
      recoverable_length_ = 0;
      return true;
  }
  RIEGELI_ASSERT_UNREACHABLE()
      << "Unknown recoverable method: " << static_cast<int>(recoverable);
}

bool RecordReader::Seek(RecordPosition new_pos) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (new_pos.chunk_begin() == chunk_begin_) {
    if (new_pos.record_index() == 0 || chunk_reader_->pos() > chunk_begin_) {
      // Seeking to the beginning of a chunk does not need reading the chunk,
      // which is important because it may be non-existent at end of file.
      //
      // If chunk_reader_->pos() > chunk_begin_, the chunk is already read.
      goto skip_reading_chunk;
    }
  } else {
    if (ABSL_PREDICT_FALSE(!chunk_reader_->Seek(new_pos.chunk_begin()))) {
      chunk_begin_ = chunk_reader_->pos();
      chunk_decoder_.Reset();
      if (ABSL_PREDICT_TRUE(chunk_reader_->healthy())) return false;
      recoverable_ = Recoverable::kRecoverChunkReader;
      return Fail(*chunk_reader_);
    }
    if (new_pos.record_index() == 0) {
      // Seeking to the beginning of a chunk does not need reading the chunk,
      // which is important because it may be non-existent at end of file.
      chunk_begin_ = chunk_reader_->pos();
      chunk_decoder_.Reset();
      return true;
    }
  }
  if (ABSL_PREDICT_FALSE(!ReadChunk())) return false;
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
      recoverable_ = Recoverable::kRecoverChunkReader;
      return Fail(*chunk_reader_);
    }
    if (chunk_reader_->pos() == new_pos) {
      // Seeking to the beginning of a chunk does not need reading the chunk,
      // which is important because it may be non-existent at end of file.
      chunk_begin_ = chunk_reader_->pos();
      chunk_decoder_.Reset();
      return true;
    }
    if (ABSL_PREDICT_FALSE(!ReadChunk())) return false;
  }
  chunk_decoder_.SetIndex(IntCast<uint64_t>(new_pos - chunk_begin_));
  return true;
}

inline bool RecordReader::ReadChunk() {
  Chunk chunk;
  chunk_begin_ = chunk_reader_->pos();
  if (ABSL_PREDICT_FALSE(!chunk_reader_->ReadChunk(&chunk))) {
    chunk_decoder_.Reset();
    if (ABSL_PREDICT_TRUE(chunk_reader_->healthy())) return false;
    recoverable_ = Recoverable::kRecoverChunkReader;
    return Fail(*chunk_reader_);
  }
  if (ABSL_PREDICT_FALSE(!ParseChunkIfMeta(chunk))) {
    chunk_decoder_.Reset();
    recoverable_ = Recoverable::kReportSkippedBytes;
    recoverable_length_ = chunk_reader_->pos() - chunk_begin_;
    return false;
  }
  if (chunk.header.num_records() == 0) {
    if (ABSL_PREDICT_FALSE(chunk.header.decoded_data_size() > 0)) {
      chunk_decoder_.Reset();
      recoverable_ = Recoverable::kReportSkippedBytes;
      recoverable_length_ = chunk_reader_->pos() - chunk_begin_;
      return Fail(
          absl::StrCat("Invalid chunk of type ",
                       static_cast<uint64_t>(chunk.header.chunk_type()),
                       ": no records but decoded data size is not zero: ",
                       chunk.header.decoded_data_size()));
    }
    // Ignore chunks with no records, even if the type is unknown.
    return true;
  }
  if (ABSL_PREDICT_FALSE(!chunk_decoder_.Reset(chunk))) {
    Fail(chunk_decoder_);
    chunk_decoder_.Reset();
    recoverable_ = Recoverable::kReportSkippedBytes;
    recoverable_length_ = chunk_reader_->pos() - chunk_begin_;
    return false;
  }
  return true;
}

inline bool RecordReader::ParseChunkIfMeta(const Chunk& chunk) {
  switch (chunk.header.chunk_type()) {
    case ChunkType::kFileSignature:
      if (ABSL_PREDICT_FALSE(chunk.header.data_size() > 0)) {
        return Fail(absl::StrCat(
            "Invalid file signature chunk: data size is not zero: ",
            chunk.header.data_size()));
      }
      if (ABSL_PREDICT_FALSE(chunk.header.num_records() > 0)) {
        return Fail(absl::StrCat(
            "Invalid file signature chunk: number of records is not zero: ",
            chunk.header.num_records()));
      }
      if (ABSL_PREDICT_FALSE(chunk.header.decoded_data_size() > 0)) {
        return Fail(absl::StrCat(
            "Invalid file signature chunk: decoded data size is not zero: ",
            chunk.header.decoded_data_size()));
      }
      return true;
    case ChunkType::kPadding:
      if (ABSL_PREDICT_FALSE(chunk.header.num_records() > 0)) {
        return Fail(absl::StrCat(
            "Invalid padding chunk: number of records is not zero: ",
            chunk.header.num_records()));
      }
      if (ABSL_PREDICT_FALSE(chunk.header.decoded_data_size() > 0)) {
        return Fail(absl::StrCat(
            "Invalid padding chunk: decoded data size is not zero: ",
            chunk.header.decoded_data_size()));
      }
      return true;
    default:
      return true;
  }
}

}  // namespace riegeli
