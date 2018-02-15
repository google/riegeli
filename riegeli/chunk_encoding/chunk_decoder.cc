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
#include <limits>
#include <string>
#include <utility>

#include "google/protobuf/message_lite.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/base/str_cat.h"
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
      boundaries_{0},
      values_reader_(Chain()),
      num_records_(0),
      index_(0) {}

ChunkDecoder::ChunkDecoder(ChunkDecoder&& src) noexcept
    : Object(std::move(src)),
      skip_corruption_(src.skip_corruption_),
      field_filter_(std::move(src.field_filter_)),
      boundaries_(riegeli::exchange(src.boundaries_, std::vector<size_t>{0})),
      values_reader_(
          riegeli::exchange(src.values_reader_, ChainReader(Chain()))),
      num_records_(riegeli::exchange(src.num_records_, 0)),
      index_(riegeli::exchange(src.index_, 0)),
      record_scratch_(riegeli::exchange(src.record_scratch_, std::string())) {}

ChunkDecoder& ChunkDecoder::operator=(ChunkDecoder&& src) noexcept {
  Object::operator=(std::move(src));
  skip_corruption_ = src.skip_corruption_;
  field_filter_ = std::move(src.field_filter_);
  boundaries_ = riegeli::exchange(src.boundaries_, std::vector<size_t>{0});
  values_reader_ = riegeli::exchange(src.values_reader_, ChainReader(Chain()));
  num_records_ = riegeli::exchange(src.num_records_, 0);
  index_ = riegeli::exchange(src.index_, 0);
  record_scratch_ = riegeli::exchange(src.record_scratch_, std::string());
  return *this;
}

void ChunkDecoder::Done() {
  boundaries_ = std::vector<size_t>();
  values_reader_ = ChainReader();
  num_records_ = 0;
  index_ = 0;
  record_scratch_ = std::string();
}

void ChunkDecoder::Reset() {
  boundaries_ = {0};
  values_reader_ = ChainReader(Chain());
  num_records_ = 0;
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
  if (RIEGELI_UNLIKELY(chunk.header.num_records() >
                       boundaries_.max_size() - 1)) {
    return Fail("Too many records");
  }
  if (RIEGELI_UNLIKELY(chunk.header.decoded_data_size() >
                       record_scratch_.max_size())) {
    return Fail("Too large chunk");
  }
  boundaries_.reserve(IntCast<size_t>(chunk.header.num_records()) + 1);
  Chain values;
  if (RIEGELI_UNLIKELY(
          !Parse(chunk_type, chunk.header, &data_reader, &values))) {
    return false;
  }
  RIEGELI_ASSERT_EQ(boundaries_.size(), chunk.header.num_records() + 1)
      << "Wrong number of boundaries";
  RIEGELI_ASSERT_EQ(boundaries_.front(), 0u) << "Non-zero first boundary";
  RIEGELI_ASSERT_EQ(boundaries_.back(), values.size()) << "Wrong last boundary";
  if (field_filter_.include_all()) {
    RIEGELI_ASSERT_EQ(boundaries_.back(), chunk.header.decoded_data_size())
        << "Wrong decoded data size";
  } else {
    RIEGELI_ASSERT_LE(boundaries_.back(), chunk.header.decoded_data_size())
        << "Wrong decoded data size";
  }
  values_reader_ = ChainReader(std::move(values));
  num_records_ = IntCast<uint64_t>(boundaries_.size() - 1);
  return true;
}

bool ChunkDecoder::Parse(ChunkType chunk_type, const ChunkHeader& header,
                         ChainReader* src, Chain* dest) {
  switch (chunk_type) {
    case ChunkType::kPadding:
      return true;
    case ChunkType::kSimple: {
      SimpleDecoder simple_decoder;
      if (RIEGELI_UNLIKELY(!simple_decoder.Reset(src, header.num_records(),
                                                 header.decoded_data_size(),
                                                 &boundaries_))) {
        return Fail("Invalid simple chunk", simple_decoder);
      }
      dest->Clear();
      if (RIEGELI_UNLIKELY(!simple_decoder.reader()->Read(
              dest, IntCast<size_t>(header.decoded_data_size())))) {
        return Fail("Reading record values failed", *simple_decoder.reader());
      }
      if (RIEGELI_UNLIKELY(!simple_decoder.VerifyEndAndClose())) {
        return Fail(simple_decoder);
      }
      if (RIEGELI_UNLIKELY(!src->VerifyEndAndClose())) {
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
          &dest_writer, &boundaries_);
      if (RIEGELI_UNLIKELY(!dest_writer.Close())) return Fail(dest_writer);
      if (RIEGELI_UNLIKELY(!ok)) {
        return Fail("Invalid transposed chunk", transpose_decoder);
      }
      if (RIEGELI_UNLIKELY(!src->VerifyEndAndClose())) {
        return Fail("Invalid transposed chunk", *src);
      }
      return true;
    }
  }
  return Fail(
      StrCat("Unknown chunk type: ", static_cast<unsigned>(chunk_type)));
}

bool ChunkDecoder::ReadRecord(google::protobuf::MessageLite* record, uint64_t* key) {
again:
  if (RIEGELI_UNLIKELY(index_ == num_records_)) return false;
  if (key != nullptr) *key = index_;
  ++index_;
  const size_t record_end = boundaries_[IntCast<size_t>(index_)];
  LimitingReader message_reader(&values_reader_, record_end);
  if (RIEGELI_UNLIKELY(!ParsePartialFromReader(record, &message_reader))) {
    message_reader.Close();
    if (!values_reader_.Seek(record_end)) {
      RIEGELI_ASSERT_UNREACHABLE()
          << "Seeking record values failed: " << values_reader_.Message();
    }
    if (skip_corruption_) goto again;
    index_ = num_records_;
    return Fail(
        StrCat("Failed to parse message of type ", record->GetTypeName()));
  }
  if (!message_reader.Close()) {
    RIEGELI_ASSERT_UNREACHABLE()
        << "Closing message reader failed: " << message_reader.Message();
  }
  if (RIEGELI_UNLIKELY(!record->IsInitialized())) {
    if (skip_corruption_) goto again;
    index_ = num_records_;
    return Fail(StrCat("Failed to parse message of type ",
                       record->GetTypeName(),
                       " because it is missing required fields: ",
                       record->InitializationErrorString()));
  }
  return true;
}

}  // namespace riegeli
