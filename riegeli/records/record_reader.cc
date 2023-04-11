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

#include <stddef.h>
#include <stdint.h>

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/compare.h"
#include "absl/types/optional.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/descriptor.pb.h"
#include "google/protobuf/message.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/binary_search.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/chain_backward_writer.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/chunk_encoding/chunk_decoder.h"
#include "riegeli/chunk_encoding/constants.h"
#include "riegeli/chunk_encoding/field_projection.h"
#include "riegeli/chunk_encoding/transpose_decoder.h"
#include "riegeli/messages/message_parse.h"
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
    descriptors_->Fail(absl::InvalidArgumentError(
        absl::StrCat("Error in file ", filename, ", element ", element_name,
                     ": ", message)));
  }

 private:
  friend class RecordsMetadataDescriptors;

  explicit ErrorCollector(RecordsMetadataDescriptors* descriptors)
      : descriptors_(descriptors) {}

  RecordsMetadataDescriptors* descriptors_;
};

RecordsMetadataDescriptors::RecordsMetadataDescriptors(
    const RecordsMetadata& metadata)
    : record_type_name_(metadata.record_type_name()) {
  if (record_type_name_.empty() || metadata.file_descriptor().empty()) return;
  pool_ = std::make_unique<google::protobuf::DescriptorPool>();
  ErrorCollector error_collector(this);
  for (const google::protobuf::FileDescriptorProto& file_descriptor :
       metadata.file_descriptor()) {
    if (ABSL_PREDICT_FALSE(pool_->BuildFileCollectingErrors(
                               file_descriptor, &error_collector) == nullptr)) {
      return;
    }
  }
}

const google::protobuf::Descriptor* RecordsMetadataDescriptors::descriptor()
    const {
  if (pool_ == nullptr) return nullptr;
  return pool_->FindMessageTypeByName(record_type_name_);
}

RecordReaderBase::RecordReaderBase(Closed) noexcept : Object(kClosed) {}

RecordReaderBase::RecordReaderBase() noexcept {}

RecordReaderBase::RecordReaderBase(RecordReaderBase&& that) noexcept
    : Object(static_cast<Object&&>(that)),
      chunk_begin_(that.chunk_begin_),
      chunk_decoder_(std::move(that.chunk_decoder_)),
      last_record_is_valid_(std::exchange(that.last_record_is_valid_, false)),
      recoverable_(std::exchange(that.recoverable_, Recoverable::kNo)),
      recovery_(std::move(that.recovery_)) {}

RecordReaderBase& RecordReaderBase::operator=(
    RecordReaderBase&& that) noexcept {
  Object::operator=(static_cast<Object&&>(that));
  chunk_begin_ = that.chunk_begin_;
  chunk_decoder_ = std::move(that.chunk_decoder_);
  last_record_is_valid_ = std::exchange(that.last_record_is_valid_, false);
  recoverable_ = std::exchange(that.recoverable_, Recoverable::kNo);
  recovery_ = std::move(that.recovery_);
  return *this;
}

void RecordReaderBase::Reset(Closed) {
  Object::Reset(kClosed);
  chunk_begin_ = 0;
  chunk_decoder_.Reset();
  last_record_is_valid_ = false;
  recoverable_ = Recoverable::kNo;
  recovery_ = nullptr;
}

void RecordReaderBase::Reset() {
  Object::Reset();
  chunk_begin_ = 0;
  chunk_decoder_.Clear();
  last_record_is_valid_ = false;
  recoverable_ = Recoverable::kNo;
  recovery_ = nullptr;
}

void RecordReaderBase::Initialize(ChunkReader* src, Options&& options) {
  RIEGELI_ASSERT(src != nullptr)
      << "Failed precondition of RecordReader: null ChunkReader pointer";
  if (ABSL_PREDICT_FALSE(!src->ok())) {
    FailWithoutAnnotation(src->status());
    return;
  }
  chunk_begin_ = src->pos();
  chunk_decoder_.Reset(ChunkDecoder::Options().set_field_projection(
      std::move(options.field_projection())));
  recovery_ = std::move(options.recovery());
}

void RecordReaderBase::Done() {
  last_record_is_valid_ = false;
  recoverable_ = Recoverable::kNo;
  if (ABSL_PREDICT_FALSE(!chunk_decoder_.Close())) {
    Fail(chunk_decoder_.status());
  }
}

inline bool RecordReaderBase::FailReading(const ChunkReader& src) {
  recoverable_ = Recoverable::kRecoverChunkReader;
  FailWithoutAnnotation(AnnotateOverSrc(src.status()));
  return TryRecovery();
}

inline bool RecordReaderBase::FailSeeking(const ChunkReader& src) {
  chunk_begin_ = src.pos();
  chunk_decoder_.Clear();
  recoverable_ = Recoverable::kRecoverChunkReader;
  FailWithoutAnnotation(AnnotateOverSrc(src.status()));
  return TryRecovery();
}

absl::Status RecordReaderBase::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) {
    ChunkReader& src = *SrcChunkReader();
    status = src.AnnotateStatus(std::move(status));
  }
  return AnnotateOverSrc(std::move(status));
}

absl::Status RecordReaderBase::AnnotateOverSrc(absl::Status status) {
  return Annotate(status, absl::StrCat("at record ", pos().ToString()));
}

bool RecordReaderBase::CheckFileFormat() {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (chunk_decoder_.num_records() > 0) return true;
  ChunkReader& src = *SrcChunkReader();
  if (ABSL_PREDICT_FALSE(!src.CheckFileFormat())) {
    chunk_decoder_.Clear();
    if (ABSL_PREDICT_FALSE(!src.ok())) {
      recoverable_ = Recoverable::kRecoverChunkReader;
      return FailWithoutAnnotation(AnnotateOverSrc(src.status()));
    }
    return false;
  }
  return true;
}

bool RecordReaderBase::ReadMetadata(RecordsMetadata& metadata) {
  Chain serialized_metadata;
  if (ABSL_PREDICT_FALSE(!ReadSerializedMetadata(serialized_metadata))) {
    metadata.Clear();
    return false;
  }
  {
    absl::Status status = ParseFromChain(serialized_metadata, metadata);
    if (ABSL_PREDICT_FALSE(!status.ok())) {
      metadata.Clear();
      recoverable_ = Recoverable::kRecoverMetadata;
      Fail(std::move(status));
      if (!TryRecovery()) return false;
      // Recovered metadata parsing, assume empty `RecordsMetadata`.
    }
  }
  return true;
}

bool RecordReaderBase::ReadSerializedMetadata(Chain& metadata) {
  metadata.Clear();
  last_record_is_valid_ = false;
  if (ABSL_PREDICT_FALSE(!ok())) {
    if (!TryRecovery()) return false;
    last_record_is_valid_ = true;
    return ok();
  }
  ChunkReader& src = *SrcChunkReader();
  if (ABSL_PREDICT_FALSE(src.pos() != 0)) {
    return Fail(absl::FailedPreconditionError(
        "RecordReaderBase::ReadMetadata() must be called "
        "while the RecordReader is at the beginning of the file"));
  }

  chunk_begin_ = src.pos();
  Chunk chunk;
  if (ABSL_PREDICT_FALSE(!src.ReadChunk(chunk))) {
    if (ABSL_PREDICT_FALSE(!src.ok())) return FailReading(src);
    return false;
  }
  RIEGELI_ASSERT(chunk.header.chunk_type() == ChunkType::kFileSignature)
      << "Unexpected type of the first chunk: "
      << static_cast<unsigned>(chunk.header.chunk_type());

  chunk_begin_ = src.pos();
  const ChunkHeader* chunk_header;
  if (ABSL_PREDICT_FALSE(!src.PullChunkHeader(&chunk_header))) {
    if (ABSL_PREDICT_FALSE(!src.ok())) return FailReading(src);
    return false;
  }
  if (chunk_header->chunk_type() != ChunkType::kFileMetadata) {
    // Missing file metadata chunk, assume empty `RecordsMetadata`.
    return true;
  }
  if (ABSL_PREDICT_FALSE(!src.ReadChunk(chunk))) {
    if (ABSL_PREDICT_FALSE(!src.ok())) return FailReading(src);
    return false;
  }
  if (ABSL_PREDICT_FALSE(!ParseMetadata(chunk, metadata))) {
    metadata.Clear();
    recoverable_ = Recoverable::kRecoverChunkDecoder;
    if (!TryRecovery()) return false;
    // Recovered metadata decoding, assume empty `RecordsMetadata`.
  }
  last_record_is_valid_ = true;
  return true;
}

inline bool RecordReaderBase::ParseMetadata(const Chunk& chunk,
                                            Chain& metadata) {
  RIEGELI_ASSERT(chunk.header.chunk_type() == ChunkType::kFileMetadata)
      << "Failed precondition of RecordReaderBase::ParseMetadata(): "
         "wrong chunk type";
  if (ABSL_PREDICT_FALSE(chunk.header.num_records() != 0)) {
    return Fail(absl::InvalidArgumentError(absl::StrCat(
        "Invalid file metadata chunk: number of records is not zero: ",
        chunk.header.num_records())));
  }
  ChainReader<> data_reader(&chunk.data);
  TransposeDecoder transpose_decoder;
  ChainBackwardWriter<> serialized_metadata_writer(&metadata);
  serialized_metadata_writer.SetWriteSizeHint(chunk.header.decoded_data_size());
  std::vector<size_t> limits;
  const bool decode_ok = transpose_decoder.Decode(
      1, chunk.header.decoded_data_size(), FieldProjection::All(), data_reader,
      serialized_metadata_writer, limits);
  if (ABSL_PREDICT_FALSE(!serialized_metadata_writer.Close())) {
    return Fail(serialized_metadata_writer.status());
  }
  if (ABSL_PREDICT_FALSE(!decode_ok)) return Fail(transpose_decoder.status());
  if (ABSL_PREDICT_FALSE(!data_reader.VerifyEndAndClose())) {
    return Fail(data_reader.status());
  }
  RIEGELI_ASSERT_EQ(limits.size(), 1u)
      << "Metadata chunk has unexpected record limits";
  RIEGELI_ASSERT_EQ(limits.back(), metadata.size())
      << "Metadata chunk has unexpected record limits";
  return true;
}

bool RecordReaderBase::ReadRecord(google::protobuf::MessageLite& record) {
  return ReadRecordImpl(record);
}

bool RecordReaderBase::ReadRecord(absl::string_view& record) {
  return ReadRecordImpl(record);
}

bool RecordReaderBase::ReadRecord(std::string& record) {
  return ReadRecordImpl(record);
}

bool RecordReaderBase::ReadRecord(Chain& record) {
  return ReadRecordImpl(record);
}

bool RecordReaderBase::ReadRecord(absl::Cord& record) {
  return ReadRecordImpl(record);
}

template <typename Record>
inline bool RecordReaderBase::ReadRecordImpl(Record& record) {
  last_record_is_valid_ = false;
  for (;;) {
    if (ABSL_PREDICT_TRUE(chunk_decoder_.ReadRecord(record))) {
      RIEGELI_ASSERT_GT(chunk_decoder_.index(), 0u)
          << "ChunkDecoder::ReadRecord() left record index at 0";
      last_record_is_valid_ = true;
      return true;
    }
    if (ABSL_PREDICT_FALSE(!ok())) {
      if (!TryRecovery()) return false;
      continue;
    }
    if (ABSL_PREDICT_FALSE(!chunk_decoder_.ok())) {
      recoverable_ = Recoverable::kRecoverChunkDecoder;
      Fail(chunk_decoder_.status());
      if (!TryRecovery()) return false;
      continue;
    }
    if (ABSL_PREDICT_FALSE(!ReadChunk())) {
      if (!TryRecovery()) return false;
    }
  }
}

bool RecordReaderBase::SetFieldProjection(FieldProjection field_projection) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  ChunkReader& src = *SrcChunkReader();
  const uint64_t record_index = chunk_decoder_.index();
  chunk_decoder_.Reset(ChunkDecoder::Options().set_field_projection(
      std::move(field_projection)));
  if (ABSL_PREDICT_FALSE(!src.Seek(chunk_begin_))) return FailSeeking(src);
  if (record_index > 0) {
    if (ABSL_PREDICT_FALSE(!ReadChunk())) return TryRecovery();
    chunk_decoder_.SetIndex(record_index);
  }
  return true;
}

bool RecordReaderBase::Recover(SkippedRegion* skipped_region) {
  if (recoverable_ == Recoverable::kNo) return false;
  ChunkReader& src = *SrcChunkReader();
  RIEGELI_ASSERT(!ok()) << "Failed invariant of RecordReader: "
                           "recovery applicable but RecordReader OK";
  const Recoverable recoverable = recoverable_;
  recoverable_ = Recoverable::kNo;
  if (recoverable != Recoverable::kRecoverChunkReader) {
    RIEGELI_ASSERT(is_open()) << "Failed invariant of RecordReader: "
                                 "recovery does not apply to chunk reader "
                                 "but RecordReader is closed";
  }
  std::string saved_message(status().message());
  MarkNotFailed();
  switch (recoverable) {
    case Recoverable::kNo:
      RIEGELI_ASSERT_UNREACHABLE() << "kNo handled above";
    case Recoverable::kRecoverChunkReader:
      if (ABSL_PREDICT_FALSE(!src.Recover(skipped_region))) {
        return FailWithoutAnnotation(AnnotateOverSrc(src.status()));
      }
      return true;
    case Recoverable::kRecoverChunkDecoder: {
      const uint64_t index_before = chunk_decoder_.index();
      if (ABSL_PREDICT_FALSE(!chunk_decoder_.Recover())) chunk_decoder_.Clear();
      if (skipped_region != nullptr) {
        *skipped_region =
            SkippedRegion(chunk_begin_ + index_before, pos().numeric(),
                          std::move(saved_message));
      }
      return true;
    }
    case Recoverable::kRecoverMetadata:
      if (skipped_region != nullptr) {
        *skipped_region = SkippedRegion(chunk_begin_, pos().numeric(),
                                        std::move(saved_message));
      }
      return true;
  }
  RIEGELI_ASSERT_UNREACHABLE()
      << "Unknown recoverable method: " << static_cast<int>(recoverable);
}

bool RecordReaderBase::SupportsRandomAccess() {
  ChunkReader* const src = SrcChunkReader();
  return src != nullptr && src->SupportsRandomAccess();
}

bool RecordReaderBase::Seek(RecordPosition new_pos) {
  last_record_is_valid_ = false;
  if (ABSL_PREDICT_FALSE(!ok())) return TryRecovery();
  ChunkReader& src = *SrcChunkReader();
  if (new_pos.chunk_begin() == chunk_begin_) {
    if (new_pos.record_index() == 0 || src.pos() > chunk_begin_) {
      // Seeking to the beginning of a chunk does not need reading the chunk,
      // which is important because it may be non-existent at end of file.
      //
      // If `src.pos() > chunk_begin_`, the chunk is already read.
      goto skip_reading_chunk;
    }
  } else {
    if (ABSL_PREDICT_FALSE(!src.Seek(new_pos.chunk_begin()))) {
      return FailSeeking(src);
    }
    if (new_pos.record_index() == 0) {
      // Seeking to the beginning of a chunk does not need reading the chunk,
      // which is important because it may be non-existent at end of file.
      chunk_begin_ = src.pos();
      chunk_decoder_.Clear();
      return true;
    }
  }
  if (ABSL_PREDICT_FALSE(!ReadChunk())) return TryRecovery();
skip_reading_chunk:
  chunk_decoder_.SetIndex(new_pos.record_index());
  return true;
}

bool RecordReaderBase::Seek(Position new_pos) {
  last_record_is_valid_ = false;
  if (ABSL_PREDICT_FALSE(!ok())) return TryRecovery();
  ChunkReader& src = *SrcChunkReader();
  if (new_pos >= chunk_begin_ && new_pos <= src.pos()) {
    // Seeking inside or just after the current chunk which has been read,
    // or to the beginning of the current chunk which has been located,
    // or to the end of file which has been reached.
  } else {
    if (ABSL_PREDICT_FALSE(!src.SeekToChunkContaining(new_pos))) {
      return FailSeeking(src);
    }
    if (src.pos() >= new_pos) {
      // Seeking to the beginning of a chunk does not need reading the chunk,
      // which is important because it may be non-existent at end of file.
      //
      // It is possible that the chunk position is greater than `new_pos` if
      // `new_pos` falls after all records of the previous chunk. This also
      // seeks to the beginning of the chunk.
      chunk_begin_ = src.pos();
      chunk_decoder_.Clear();
      return true;
    }
    if (ABSL_PREDICT_FALSE(!ReadChunk())) return TryRecovery();
  }
  chunk_decoder_.SetIndex(IntCast<uint64_t>(new_pos - chunk_begin_));
  return true;
}

bool RecordReaderBase::SeekBack() {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  last_record_is_valid_ = false;
  if (ABSL_PREDICT_TRUE(chunk_decoder_.index() > 0)) {
    chunk_decoder_.SetIndex(chunk_decoder_.index() - 1);
    return true;
  }
  ChunkReader& src = *SrcChunkReader();
  Position chunk_pos = chunk_begin_;
  while (chunk_pos > 0) {
    if (ABSL_PREDICT_FALSE(!src.SeekToChunkBefore(chunk_pos - 1))) {
      // If recovery succeeds, continue searching back from the beginning of the
      // skipped region.
      chunk_pos = src.pos();
      if (!FailSeeking(src)) return false;
      continue;
    }
    chunk_pos = chunk_begin_;
    if (ABSL_PREDICT_FALSE(!ReadChunk())) {
      // If recovery succeeds, continue searching back from the beginning of the
      // skipped region.
      if (!TryRecovery()) return false;
      continue;
    }
    if (ABSL_PREDICT_TRUE(chunk_decoder_.num_records() > 0)) {
      chunk_decoder_.SetIndex(chunk_decoder_.num_records() - 1);
      return true;
    }
    // The chunk has no records. Continue searching back from the beginning of
    // the chunk.
  }
  return false;
}

absl::optional<Position> RecordReaderBase::Size() {
  if (ABSL_PREDICT_FALSE(!ok())) return absl::nullopt;
  ChunkReader& src = *SrcChunkReader();
  const absl::optional<Position> size = src.Size();
  if (ABSL_PREDICT_FALSE(size == absl::nullopt)) {
    FailWithoutAnnotation(AnnotateOverSrc(src.status()));
  }
  return size;
}

// Traits for `BinarySearch()`: searching for a chunk.
class RecordReaderBase::ChunkSearchTraits {
 public:
  explicit ChunkSearchTraits(RecordReaderBase* self)
      : self_(RIEGELI_ASSERT_NOTNULL(self)) {}

  using Pos = Position;

  bool Empty(Position low, Position high) const { return low >= high; }

  absl::optional<Position> Middle(Position low, Position high) const {
    if (low >= high) return absl::nullopt;
    ChunkReader& src = *self_->SrcChunkReader();
    if (ABSL_PREDICT_FALSE(!src.SeekToChunkBefore(low + (high - low) / 2))) {
      if (!self_->FailSeeking(src)) {
        // There was a failure or unexpected end of file. Cancel the search.
        return absl::nullopt;
      }
      if (src.pos() >= high) {
        // Skipped region after the middle ends after `high`. Find the next
        // chunk after `low` instead.
        if (ABSL_PREDICT_FALSE(!src.Seek(low))) {
          if (!self_->FailSeeking(src) || src.pos() >= high) {
            // There was a failure or unexpected end of file, or the whole range
            // is skipped. Cancel the search.
            return absl::nullopt;
          }
        }
      }
    }
    return src.pos();
  }

 private:
  RecordReaderBase* self_;
};

absl::optional<absl::partial_ordering> RecordReaderBase::Search(
    absl::FunctionRef<
        absl::optional<absl::partial_ordering>(RecordReaderBase& reader)>
        test) {
  if (ABSL_PREDICT_FALSE(!ok())) return absl::nullopt;
  last_record_is_valid_ = false;
  ChunkReader& src = *SrcChunkReader();
  const absl::optional<Position> size = src.Size();
  if (ABSL_PREDICT_FALSE(size == absl::nullopt)) {
    FailWithoutAnnotation(AnnotateOverSrc(src.status()));
    return absl::nullopt;
  }

  struct ChunkSuffix {
    Position chunk_begin;
    uint64_t record_index;
    uint64_t num_records;
  };
  absl::optional<ChunkSuffix> less_found;
  uint64_t greater_record_index = 0;
  absl::optional<SearchResult<Position>> greater_chunk_begin = BinarySearch(
      0, *size,
      [&](Position chunk_begin) -> absl::optional<SearchGuide<Position>> {
        if (ABSL_PREDICT_FALSE(!src.Seek(chunk_begin))) {
          if (!FailSeeking(src)) return absl::nullopt;
          // Declare the skipped region unordered.
          return SearchGuide<Position>{absl::partial_ordering::unordered,
                                       src.pos()};
        }
        if (ABSL_PREDICT_FALSE(!ReadChunk())) {
          if (!TryRecovery()) {
            if (!ok()) return absl::nullopt;
            // The chunk is truncated. Continue the search before the chunk.
            greater_record_index = 0;
            return SearchGuide<Position>{absl::partial_ordering::greater,
                                         chunk_begin};
          }
          // Declare the skipped region unordered.
          return SearchGuide<Position>{absl::partial_ordering::unordered,
                                       src.pos()};
        }
        // `src.pos()` points to the next chunk. Adjust `chunk_begin` in case
        // recovery moved it forwards.
        chunk_begin = chunk_begin_;
        const uint64_t num_records = chunk_decoder_.num_records();
        // Judge the chunk by its earliest record which is not unordered.
        for (uint64_t record_index = 0; record_index < num_records;
             ++record_index) {
          if (ABSL_PREDICT_FALSE(
                  !Seek(RecordPosition(chunk_begin, record_index)))) {
            return absl::nullopt;
          }
          std::function<bool(const SkippedRegion&, RecordReaderBase&)>
              recovery = std::exchange(recovery_, nullptr);
          const absl::optional<absl::partial_ordering> ordering = test(*this);
          recovery_ = std::move(recovery);
          if (ABSL_PREDICT_FALSE(!ok())) {
            // Reading the record made the `RecordReader` not OK, probably
            // because a message could not be parsed (or `test()` did something
            // unusual).
            if (!TryRecovery()) return absl::nullopt;
            // Declare the skipped record unordered.
            continue;
          }
          if (ABSL_PREDICT_FALSE(ordering == absl::nullopt)) {
            return absl::nullopt;
          }
          if (*ordering < 0) {
            less_found =
                ChunkSuffix{chunk_begin, record_index + 1, num_records};
            return SearchGuide<Position>{absl::partial_ordering::less,
                                         src.pos()};
          }
          if (*ordering >= 0) {
            greater_record_index = record_index;
            return SearchGuide<Position>{*ordering, chunk_begin};
          }
        }
        // All records are unordered.
        return SearchGuide<Position>{absl::partial_ordering::unordered,
                                     src.pos()};
      },
      ChunkSearchTraits(this));

  if (ABSL_PREDICT_FALSE(greater_chunk_begin == absl::nullopt)) {
    return absl::nullopt;
  }
  if (greater_chunk_begin->ordering != 0 && less_found != absl::nullopt) {
    const absl::optional<SearchResult<uint64_t>> less_record_index =
        BinarySearch(
            less_found->record_index, less_found->num_records,
            [&](uint64_t record_index)
                -> absl::optional<absl::partial_ordering> {
              if (ABSL_PREDICT_FALSE(!Seek(
                      RecordPosition(less_found->chunk_begin, record_index)))) {
                return absl::nullopt;
              }
              std::function<bool(const SkippedRegion&, RecordReaderBase&)>
                  recovery = std::exchange(recovery_, nullptr);
              const absl::optional<absl::partial_ordering> ordering =
                  test(*this);
              recovery_ = std::move(recovery);
              if (ABSL_PREDICT_FALSE(!ok())) {
                // Reading the record made the `RecordReader` not OK, probably
                // because a message could not be parsed (or `test()` did
                // something unusual).
                if (!TryRecovery()) return absl::nullopt;
                // Declare the skipped record unordered.
                return absl::partial_ordering::unordered;
              }
              return ordering;
            });
    if (ABSL_PREDICT_FALSE(less_record_index == absl::nullopt)) {
      return absl::nullopt;
    }
    if (less_record_index->ordering >= 0) {
      greater_chunk_begin->ordering = less_record_index->ordering;
      greater_chunk_begin->found = less_found->chunk_begin;
      greater_record_index = less_record_index->found;
    }
  }
  if (ABSL_PREDICT_FALSE(!Seek(
          RecordPosition(greater_chunk_begin->found, greater_record_index)))) {
    Fail(absl::DataLossError("Riegeli/records file got truncated"));
    return absl::nullopt;
  }
  return greater_chunk_begin->ordering;
}

inline bool RecordReaderBase::ReadChunk() {
  RIEGELI_ASSERT(ok())
      << "Failed precondition of RecordReaderBase::ReadChunk(): " << status();
  ChunkReader& src = *SrcChunkReader();
  chunk_begin_ = src.pos();
  Chunk chunk;
  if (ABSL_PREDICT_FALSE(!src.ReadChunk(chunk))) {
    chunk_decoder_.Clear();
    if (ABSL_PREDICT_FALSE(!src.ok())) {
      recoverable_ = Recoverable::kRecoverChunkReader;
      return FailWithoutAnnotation(AnnotateOverSrc(src.status()));
    }
    return false;
  }
  if (ABSL_PREDICT_FALSE(!chunk_decoder_.Decode(chunk))) {
    recoverable_ = Recoverable::kRecoverChunkDecoder;
    return Fail(chunk_decoder_.status());
  }
  return true;
}

}  // namespace riegeli
