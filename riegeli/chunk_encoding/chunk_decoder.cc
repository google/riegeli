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

#include "riegeli/chunk_encoding/chunk_decoder.h"

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>

#include "google/protobuf/message_lite.h"
#include "absl/base/optimization.h"
#include "absl/strings/str_cat.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/chain_backward_writer.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/limiting_reader.h"
#include "riegeli/bytes/message_parse.h"
#include "riegeli/bytes/reader_utils.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/chunk_encoding/simple_decoder.h"
#include "riegeli/chunk_encoding/transpose_decoder.h"
#include "riegeli/chunk_encoding/types.h"

namespace riegeli {

ChunkDecoder::ChunkDecoder(Options options)
    : Object(State::kOpen),
      skip_corruption_(options.skip_corruption_),
      field_filter_(std::move(options.field_filter_)),
      values_reader_(Chain()),
      index_(0) {}

ChunkDecoder::ChunkDecoder(ChunkDecoder&& src) noexcept
    : Object(std::move(src)),
      skip_corruption_(src.skip_corruption_),
      field_filter_(std::move(src.field_filter_)),
      limits_(std::move(src.limits_)),
      values_reader_(
          riegeli::exchange(src.values_reader_, ChainReader(Chain()))),
      index_(riegeli::exchange(src.index_, 0)),
      record_scratch_(riegeli::exchange(src.record_scratch_, std::string())) {}

ChunkDecoder& ChunkDecoder::operator=(ChunkDecoder&& src) noexcept {
  Object::operator=(std::move(src));
  skip_corruption_ = src.skip_corruption_;
  field_filter_ = std::move(src.field_filter_);
  limits_ = std::move(src.limits_);
  values_reader_ = riegeli::exchange(src.values_reader_, ChainReader(Chain()));
  index_ = riegeli::exchange(src.index_, 0);
  record_scratch_ = riegeli::exchange(src.record_scratch_, std::string());
  return *this;
}

void ChunkDecoder::Done() {
  limits_ = std::vector<size_t>();
  values_reader_ = ChainReader();
  index_ = 0;
  record_scratch_ = std::string();
}

void ChunkDecoder::Reset() {
  limits_.clear();
  values_reader_ = ChainReader(Chain());
  index_ = 0;
  MarkHealthy();
}

bool ChunkDecoder::Reset(const Chunk& chunk) {
  Reset();
  ChainReader data_reader(&chunk.data);
  uint8_t chunk_type_byte;
  const ChunkType chunk_type = ReadByte(&data_reader, &chunk_type_byte)
                                   ? static_cast<ChunkType>(chunk_type_byte)
                                   : ChunkType::kPadding;
  if (ABSL_PREDICT_FALSE(chunk.header.num_records() > limits_.max_size())) {
    return Fail("Too many records");
  }
  if (ABSL_PREDICT_FALSE(chunk.header.decoded_data_size() >
                         record_scratch_.max_size())) {
    return Fail("Too large chunk");
  }
  limits_.reserve(IntCast<size_t>(chunk.header.num_records()));
  Chain values;
  if (ABSL_PREDICT_FALSE(
          !Parse(chunk_type, chunk.header, &data_reader, &values))) {
    limits_.clear();  // Ensure that index() == num_records().
    return false;
  }
  RIEGELI_ASSERT_EQ(limits_.size(), chunk.header.num_records())
      << "Wrong number of record end positions";
  RIEGELI_ASSERT_EQ(limits_.empty() ? 0u : limits_.back(), values.size())
      << "Wrong last record end position";
  if (field_filter_.include_all()) {
    RIEGELI_ASSERT_EQ(values.size(), chunk.header.decoded_data_size())
        << "Wrong decoded data size";
  } else {
    RIEGELI_ASSERT_LE(values.size(), chunk.header.decoded_data_size())
        << "Wrong decoded data size";
  }
  values_reader_ = ChainReader(std::move(values));
  return true;
}

bool ChunkDecoder::Parse(ChunkType chunk_type, const ChunkHeader& header,
                         ChainReader* src, Chain* dest) {
  switch (chunk_type) {
    case ChunkType::kPadding:
      return true;
    case ChunkType::kSimple: {
      SimpleDecoder simple_decoder;
      if (ABSL_PREDICT_FALSE(!simple_decoder.Reset(src, header.num_records(),
                                                   header.decoded_data_size(),
                                                   &limits_))) {
        return Fail("Invalid simple chunk", simple_decoder);
      }
      dest->Clear();
      if (ABSL_PREDICT_FALSE(!simple_decoder.reader()->Read(
              dest, IntCast<size_t>(header.decoded_data_size())))) {
        return Fail("Reading record values failed", *simple_decoder.reader());
      }
      if (ABSL_PREDICT_FALSE(!simple_decoder.VerifyEndAndClose())) {
        return Fail(simple_decoder);
      }
      if (ABSL_PREDICT_FALSE(!src->VerifyEndAndClose())) {
        return Fail("Invalid simple chunk", *src);
      }
      return true;
    }
    case ChunkType::kTransposed: {
      TransposeDecoder transpose_decoder;
      dest->Clear();
      ChainBackwardWriter dest_writer(
          dest,
          ChainBackwardWriter::Options().set_size_hint(
              field_filter_.include_all() ? header.decoded_data_size() : 0u));
      const bool ok = transpose_decoder.Reset(
          src, header.num_records(), header.decoded_data_size(), field_filter_,
          &dest_writer, &limits_);
      if (ABSL_PREDICT_FALSE(!dest_writer.Close())) return Fail(dest_writer);
      if (ABSL_PREDICT_FALSE(!ok)) {
        return Fail("Invalid transposed chunk", transpose_decoder);
      }
      if (ABSL_PREDICT_FALSE(!src->VerifyEndAndClose())) {
        return Fail("Invalid transposed chunk", *src);
      }
      return true;
    }
  }
  return Fail(
      absl::StrCat("Unknown chunk type: ", static_cast<unsigned>(chunk_type)));
}

bool ChunkDecoder::ReadRecord(google::protobuf::MessageLite* record, uint64_t* key) {
  for (;;) {
    if (ABSL_PREDICT_FALSE(index_ == num_records())) return false;
    if (key != nullptr) *key = index_;
    const size_t start = IntCast<size_t>(values_reader_.pos());
    const size_t limit = limits_[IntCast<size_t>(index_++)];
    RIEGELI_ASSERT_LE(start, limit)
        << "Failed invariant of ChunkDecoder: record end positions not sorted";
    LimitingReader message_reader(&values_reader_, limit);
    if (ABSL_PREDICT_TRUE(ParsePartialFromReader(record, &message_reader))) {
      RIEGELI_ASSERT_EQ(message_reader.pos(), limit)
          << "Record was not read up to its end";
      if (!message_reader.Close()) {
        RIEGELI_ASSERT_UNREACHABLE()
            << "Closing message reader failed: " << message_reader.Message();
      }
      if (ABSL_PREDICT_TRUE(record->IsInitialized())) return true;
      if (!skip_corruption_) {
        index_ = num_records();
        return Fail(absl::StrCat("Failed to parse message of type ",
                                 record->GetTypeName(),
                                 " because it is missing required fields: ",
                                 record->InitializationErrorString()));
      }
    } else {
      message_reader.Close();
      if (!values_reader_.Seek(limit)) {
        RIEGELI_ASSERT_UNREACHABLE()
            << "Seeking record values failed: " << values_reader_.Message();
      }
      if (!skip_corruption_) {
        index_ = num_records();
        return Fail(absl::StrCat("Failed to parse message of type ",
                                 record->GetTypeName()));
      }
    }
    // TODO: Corruption is being skipped here without accounting for
    // this in RecordReader::corrupted_bytes_skipped_. But that would be tricky.
    // Ordinarily corrupted_bytes_skipped_ measures file bytes (compressed),
    // which is a meaningless concept when skipping a subset of records after
    // decoding.
    //
    // We might account for record skipping in the same way as record positions:
    // each record counts for 1 byte, except the last record in a chunk which
    // counts for the remaining chunk size. This would maintain the property
    // that when the whole file is skipped, corrupted_bytes_skipped_ is the file
    // size.
  }
}

}  // namespace riegeli
