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

#include "absl/base/optimization.h"
#include "absl/memory/memory.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/chain_backward_writer.h"
#include "riegeli/bytes/message_parse.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/chunk_encoding/chunk_decoder.h"
#include "riegeli/chunk_encoding/transpose_decoder.h"
#include "riegeli/records/chunk_reader.h"
#include "riegeli/records/record_position.h"
#include "riegeli/records/records_metadata.pb.h"
#include "riegeli/records/skipped_region.h"

namespace riegeli {

class RecordsMetadataDescriptors::ErrorCollector
    : public google::protobuf::DescriptorPool::ErrorCollector {
 public:
  void AddError(const std::string& filename, const std::string& element_name,
                const google::protobuf::Message* descriptor,
                ErrorLocation location, const std::string& message) override {
    descriptors_->Fail(absl::StrCat("Error in file ", filename, ", element ",
                                    element_name, ": ", message));
  }

  void AddWarning(const std::string& filename, const std::string& element_name,
                  const google::protobuf::Message* descriptor,
                  ErrorLocation location, const std::string& message) override {
  }

 private:
  friend class RecordsMetadataDescriptors;

  explicit ErrorCollector(RecordsMetadataDescriptors* descriptors)
      : descriptors_(descriptors) {}

  RecordsMetadataDescriptors* descriptors_;
};

RecordsMetadataDescriptors::RecordsMetadataDescriptors(
    const RecordsMetadata& metadata)
    : Object(State::kOpen), record_type_name_(metadata.record_type_name()) {
  if (record_type_name_.empty() || metadata.file_descriptor().empty()) return;
  pool_ = absl::make_unique<google::protobuf::DescriptorPool>();
  ErrorCollector error_collector(this);
  for (const google::protobuf::FileDescriptorProto& file_descriptor :
       metadata.file_descriptor()) {
    if (ABSL_PREDICT_FALSE(pool_->BuildFileCollectingErrors(
                               file_descriptor, &error_collector) == nullptr)) {
      return;
    }
  }
}

void RecordsMetadataDescriptors::Done() {
  record_type_name_ = std::string();
  pool_.reset();
}

const google::protobuf::Descriptor* RecordsMetadataDescriptors::descriptor()
    const {
  if (pool_ == nullptr) return nullptr;
  return pool_->FindMessageTypeByName(record_type_name_);
}

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
      recoverable_(riegeli::exchange(src.recoverable_, Recoverable::kNo)) {}

RecordReader& RecordReader::operator=(RecordReader&& src) noexcept {
  Object::operator=(std::move(src));
  chunk_reader_ = std::move(src.chunk_reader_);
  chunk_begin_ = riegeli::exchange(src.chunk_begin_, 0);
  chunk_decoder_ = std::move(src.chunk_decoder_);
  recoverable_ = riegeli::exchange(src.recoverable_, Recoverable::kNo);
  return *this;
}

void RecordReader::Done() {
  recoverable_ = Recoverable::kNo;
  if (ABSL_PREDICT_FALSE(!chunk_reader_->Close())) {
    recoverable_ = Recoverable::kRecoverChunkReader;
    Fail(*chunk_reader_);
  }
  if (ABSL_PREDICT_FALSE(!chunk_decoder_.Close())) Fail(chunk_decoder_);
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

bool RecordReader::ReadMetadata(RecordsMetadata* metadata) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(chunk_reader_->pos() != 0)) {
    return Fail(
        "RecordReader::ReadMetadata() must be called while the RecordReader is "
        "at the beginning of the file");
  }

  chunk_begin_ = chunk_reader_->pos();
  Chunk chunk;
  if (ABSL_PREDICT_FALSE(!chunk_reader_->ReadChunk(&chunk))) {
    if (ABSL_PREDICT_TRUE(chunk_reader_->healthy())) return false;
    recoverable_ = Recoverable::kRecoverChunkReader;
    return Fail(*chunk_reader_);
  }
  RIEGELI_ASSERT(chunk.header.chunk_type() == ChunkType::kFileSignature);

  chunk_begin_ = chunk_reader_->pos();
  const ChunkHeader* chunk_header;
  if (ABSL_PREDICT_FALSE(!chunk_reader_->PullChunkHeader(&chunk_header))) {
    if (ABSL_PREDICT_TRUE(chunk_reader_->healthy())) return false;
    recoverable_ = Recoverable::kRecoverChunkReader;
    return Fail(*chunk_reader_);
  }
  if (chunk_header->chunk_type() != ChunkType::kFileMetadata) {
    // Missing file metadata chunk, assume empty RecordMetadata.
    metadata->Clear();
    return true;
  }
  if (ABSL_PREDICT_FALSE(!chunk_reader_->ReadChunk(&chunk))) {
    if (ABSL_PREDICT_TRUE(chunk_reader_->healthy())) return false;
    recoverable_ = Recoverable::kRecoverChunkReader;
    return Fail(*chunk_reader_);
  }
  if (ABSL_PREDICT_FALSE(!ParseMetadata(chunk, metadata))) {
    metadata->Clear();
    recoverable_ = Recoverable::kRecoverChunkDecoder;
    return false;
  }
  return true;
}

inline bool RecordReader::ParseMetadata(const Chunk& chunk,
                                        RecordsMetadata* metadata) {
  RIEGELI_ASSERT(chunk.header.chunk_type() == ChunkType::kFileMetadata)
      << "Failed precondition of RecordReader::ParseMetadata(): "
         "wrong chunk type";
  if (ABSL_PREDICT_FALSE(chunk.header.num_records() != 0)) {
    return Fail(absl::StrCat(
        "Invalid file metadata chunk: number of records is not zero: ",
        chunk.header.num_records()));
  }
  ChainReader data_reader(&chunk.data);
  TransposeDecoder transpose_decoder;
  Chain serialized_metadata;
  ChainBackwardWriter serialized_metadata_writer(&serialized_metadata);
  std::vector<size_t> limits;
  const bool ok = transpose_decoder.Reset(
      &data_reader, 1, chunk.header.decoded_data_size(), FieldFilter::All(),
      &serialized_metadata_writer, &limits);
  if (ABSL_PREDICT_FALSE(!serialized_metadata_writer.Close())) {
    return Fail(serialized_metadata_writer);
  }
  if (ABSL_PREDICT_FALSE(!ok)) {
    return Fail("Invalid metadata chunk", transpose_decoder);
  }
  if (ABSL_PREDICT_FALSE(!data_reader.VerifyEndAndClose())) {
    return Fail("Invalid metadata chunk", data_reader);
  }
  RIEGELI_ASSERT_EQ(limits.size(), 1u)
      << "Metadata chunk has unexpected record limits";
  RIEGELI_ASSERT_EQ(limits.back(), serialized_metadata.size())
      << "Metadata chunk has unexpected record limits";
  if (ABSL_PREDICT_FALSE(
          !ParsePartialFromChain(metadata, serialized_metadata))) {
    return Fail("Failed to parse message of type riegeli.RecordsMetadata");
  }
  if (ABSL_PREDICT_FALSE(!metadata->IsInitialized())) {
    return Fail(
        absl::StrCat("Failed to parse message of type riegeli.RecordsMetadata"
                     " because it is missing required fields: ",
                     metadata->InitializationErrorString()));
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

template bool RecordReader::ReadRecordSlow(
    google::protobuf::MessageLite* record, RecordPosition* key);
template bool RecordReader::ReadRecordSlow(absl::string_view* record,
                                           RecordPosition* key);
template bool RecordReader::ReadRecordSlow(std::string* record,
                                           RecordPosition* key);
template bool RecordReader::ReadRecordSlow(Chain* record, RecordPosition* key);

bool RecordReader::Recover(SkippedRegion* skipped_region) {
  if (recoverable_ == Recoverable::kNo) return false;
  RIEGELI_ASSERT(!healthy()) << "Failed invariant of RecordReader: "
                                "recovery applicable but RecordReader healthy";
  const Recoverable recoverable = recoverable_;
  recoverable_ = Recoverable::kNo;
  if (recoverable != Recoverable::kRecoverChunkReader) {
    RIEGELI_ASSERT(!closed()) << "Failed invariant of RecordReader: "
                                 "recovery does not apply to chunk reader "
                                 "but RecordReader is closed";
  }
  MarkNotFailed();
  switch (recoverable) {
    case Recoverable::kNo:
      RIEGELI_ASSERT_UNREACHABLE() << "kNo handled above";
    case Recoverable::kRecoverChunkReader:
      if (ABSL_PREDICT_FALSE(!chunk_reader_->Recover(skipped_region))) {
        return Fail(*chunk_reader_);
      }
      return true;
    case Recoverable::kRecoverChunkDecoder: {
      const uint64_t index_before = chunk_decoder_.index();
      if (ABSL_PREDICT_FALSE(!chunk_decoder_.Recover())) {
        chunk_decoder_.Reset();
      }
      if (skipped_region != nullptr) {
        const Position region_begin = chunk_begin_ + index_before;
        const Position region_end = pos().numeric();
        *skipped_region = SkippedRegion(region_begin, region_end);
      }
      return true;
    }
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
      recoverable_ = Recoverable::kRecoverChunkReader;
      return Fail(*chunk_reader_);
    }
    if (chunk_reader_->pos() >= new_pos) {
      // Seeking to the beginning of a chunk does not need reading the chunk,
      // which is important because it may be non-existent at end of file.
      //
      // It is possible that the chunk position is greater than new_pos if
      // new_pos falls after all records of the previous chunk. This also seeks
      // to the beginning of the chunk.
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
  chunk_begin_ = chunk_reader_->pos();
  Chunk chunk;
  if (ABSL_PREDICT_FALSE(!chunk_reader_->ReadChunk(&chunk))) {
    chunk_decoder_.Reset();
    if (ABSL_PREDICT_TRUE(chunk_reader_->healthy())) return false;
    recoverable_ = Recoverable::kRecoverChunkReader;
    return Fail(*chunk_reader_);
  }
  if (ABSL_PREDICT_FALSE(!chunk_decoder_.Reset(chunk))) {
    recoverable_ = Recoverable::kRecoverChunkDecoder;
    return Fail(chunk_decoder_);
  }
  return true;
}

}  // namespace riegeli
