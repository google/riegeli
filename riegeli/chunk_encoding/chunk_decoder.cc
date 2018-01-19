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
#include <memory>
#include <string>
#include <utility>

#include "google/protobuf/message_lite.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/memory.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/brotli_reader.h"
#include "riegeli/bytes/chain_backward_writer.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/limiting_reader.h"
#include "riegeli/bytes/message_parse.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/reader_utils.h"
#include "riegeli/bytes/zstd_reader.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/chunk_encoding/internal_types.h"
#include "riegeli/chunk_encoding/transpose_decoder.h"

namespace riegeli {

namespace {

class Decompressor {
 public:
  bool Initialize(ChainReader* src, internal::CompressionType compression_type,
                  std::string* message);

  Reader* reader() const { return reader_; }

  bool VerifyEndAndClose();

 private:
  ChainReader* src_;
  std::unique_ptr<Reader> owned_reader_;
  Reader* reader_;
};

bool Decompressor::Initialize(ChainReader* src,
                              internal::CompressionType compression_type,
                              std::string* message) {
  src_ = src;
  switch (compression_type) {
    case internal::CompressionType::kNone:
      reader_ = src;
      return true;
    case internal::CompressionType::kBrotli:
      owned_reader_ = riegeli::make_unique<BrotliReader>(src);
      reader_ = owned_reader_.get();
      return true;
    case internal::CompressionType::kZstd:
      owned_reader_ = riegeli::make_unique<ZstdReader>(src);
      reader_ = owned_reader_.get();
      return true;
  }
  *message = "Unknown compression type: " +
             std::to_string(static_cast<int>(compression_type));
  return false;
}

bool Decompressor::VerifyEndAndClose() {
  if (RIEGELI_UNLIKELY(!reader_->VerifyEndAndClose())) return false;
  return src_ == reader_ || src_->VerifyEndAndClose();
}

}  // namespace

ChunkDecoder::ChunkDecoder(Options options)
    : skip_corruption_(options.skip_corruption_),
      field_filter_(std::move(options.field_filter_)) {
  Clear();
}

ChunkDecoder::ChunkDecoder(ChunkDecoder&& src) noexcept
    : Object(std::move(src)),
      skip_corruption_(src.skip_corruption_),
      field_filter_(std::move(src.field_filter_)),
      boundaries_(std::move(src.boundaries_)),
      values_reader_(std::move(src.values_reader_)),
      num_records_(src.num_records_),
      index_(src.index_) {
  src.Clear();
}

ChunkDecoder& ChunkDecoder::operator=(ChunkDecoder&& src) noexcept {
  if (this != &src) {
    Object::operator=(std::move(src));
    skip_corruption_ = src.skip_corruption_;
    field_filter_ = std::move(src.field_filter_);
    boundaries_ = std::move(src.boundaries_);
    values_reader_ = std::move(src.values_reader_);
    num_records_ = src.num_records_;
    index_ = src.index_;
    src.Clear();
  }
  return *this;
}

ChunkDecoder::~ChunkDecoder() = default;

void ChunkDecoder::Clear() {
  MarkHealthy();
  boundaries_.clear();
  boundaries_.push_back(0);
  values_reader_ = ChainReader(Chain());
  num_records_ = 0;
  index_ = 0;
}

bool ChunkDecoder::Reset(const Chunk& chunk) {
  Clear();
  ChainReader data_reader(&chunk.data);
  uint8_t chunk_type;
  if (!ReadByte(&data_reader, &chunk_type)) {
    chunk_type = static_cast<uint8_t>(internal::ChunkType::kPadding);
  }
  boundaries_.reserve(chunk.header.num_records() + 1);
  Chain values;
  if (RIEGELI_UNLIKELY(
          !Initialize(chunk_type, chunk.header, &data_reader, &values))) {
    return false;
  }
  if (RIEGELI_UNLIKELY(boundaries_.size() != chunk.header.num_records() + 1)) {
    return Fail("Invalid chunk (number of records)");
  }
  if (field_filter_.include_all() &&
      RIEGELI_UNLIKELY(values.size() != chunk.header.decoded_data_size())) {
    return Fail("Invalid chunk (total size)");
  }
  RIEGELI_ASSERT(!boundaries_.empty());
  RIEGELI_ASSERT_EQ(boundaries_.front(), 0u);
  RIEGELI_ASSERT_EQ(boundaries_.back(), values.size());
  values_reader_ = ChainReader(std::move(values));
  num_records_ = boundaries_.size() - 1;
  return true;
}

bool ChunkDecoder::Initialize(uint8_t chunk_type, const ChunkHeader& header,
                              ChainReader* data_reader, Chain* values) {
  switch (static_cast<internal::ChunkType>(chunk_type)) {
    case internal::ChunkType::kPadding:
      return true;
    case internal::ChunkType::kSimple:
      return InitializeSimple(header, data_reader, values);
    case internal::ChunkType::kTransposed:
      return InitializeTransposed(header, data_reader, values);
  }
  return Fail("Unknown chunk type: " + std::to_string(chunk_type));
}

inline bool ChunkDecoder::InitializeSimple(const ChunkHeader& header,
                                           ChainReader* data_reader,
                                           Chain* values) {
  uint8_t compression_type_byte;
  if (RIEGELI_UNLIKELY(!ReadByte(data_reader, &compression_type_byte))) {
    return Fail("Invalid simple chunk (compression type)");
  }
  const internal::CompressionType compression_type =
      static_cast<internal::CompressionType>(compression_type_byte);

  uint64_t sizes_size;
  if (RIEGELI_UNLIKELY(!ReadVarint64(data_reader, &sizes_size))) {
    return Fail("Invalid simple chunk (sizes size)");
  }

  Chain compressed_sizes;
  if (RIEGELI_UNLIKELY(!data_reader->Read(&compressed_sizes, sizes_size))) {
    return Fail("Invalid simple chunk (compressed sizes)");
  }

  ChainReader compressed_sizes_reader(&compressed_sizes);
  Decompressor sizes_decompressor;
  std::string message;
  if (RIEGELI_UNLIKELY(!sizes_decompressor.Initialize(
          &compressed_sizes_reader, compression_type, &message))) {
    return Fail(message);
  }

  Decompressor values_decompressor;
  if (RIEGELI_UNLIKELY(!values_decompressor.Initialize(
          data_reader, compression_type, &message))) {
    return Fail(message);
  }

  const uint64_t decoded_data_size = header.decoded_data_size();
  if (RIEGELI_UNLIKELY(
          !values_decompressor.reader()->Read(values, decoded_data_size))) {
    return Fail("Invalid simple chunk (values)");
  }
  if (RIEGELI_UNLIKELY(!values_decompressor.VerifyEndAndClose())) {
    return Fail("Invalid simple chunk (closing values)");
  }

  const size_t num_boundaries = header.num_records() + 1;
  uint64_t boundary = 0;
  RIEGELI_ASSERT_EQ(boundaries_.size(), 1u);
  boundaries_[0] = boundary;
  while (boundaries_.size() < num_boundaries) {
    uint64_t size;
    if (RIEGELI_UNLIKELY(!ReadVarint64(sizes_decompressor.reader(), &size))) {
      return Fail("Invalid simple chunk (record size)");
    }
    if (RIEGELI_UNLIKELY(size > decoded_data_size - boundary)) {
      return Fail("Invalid simple chunk (overflow)");
    }
    boundary += size;
    boundaries_.push_back(boundary);
  }
  if (RIEGELI_UNLIKELY(!sizes_decompressor.VerifyEndAndClose())) {
    return Fail("Invalid simple chunk (closing sizes)");
  }
  return true;
}

inline bool ChunkDecoder::InitializeTransposed(const ChunkHeader& header,
                                               ChainReader* data_reader,
                                               Chain* values) {
  TransposeDecoder transpose_decoder;
  if (RIEGELI_UNLIKELY(
          !transpose_decoder.Initialize(data_reader, field_filter_))) {
    return Fail("Invalid transposed chunk");
  }

  ChainBackwardWriter values_writer(
      values,
      ChainBackwardWriter::Options().set_size_hint(header.decoded_data_size()));
  if (RIEGELI_UNLIKELY(
          !transpose_decoder.Decode(&values_writer, &boundaries_))) {
    return Fail("Invalid transposed chunk");
  }
  if (!values_writer.Close()) RIEGELI_UNREACHABLE();
  return data_reader->VerifyEndAndClose();
}

bool ChunkDecoder::ReadRecord(google::protobuf::MessageLite* record, uint64_t* key) {
again:
  if (RIEGELI_UNLIKELY(index_ == num_records())) return false;
  if (key != nullptr) *key = index_;
  ++index_;
  LimitingReader message_reader(&values_reader_,
                                boundaries_[index_] - boundaries_[0]);
  if (RIEGELI_UNLIKELY(!ParseFromReader(record, &message_reader))) {
    if (!values_reader_.Seek(boundaries_[index_] - boundaries_[0])) {
      RIEGELI_UNREACHABLE();
    }
    if (skip_corruption_) goto again;
    index_ = num_records();
    return Fail("Failed to parse message as type " + record->GetTypeName());
  }
  if (RIEGELI_UNLIKELY(!message_reader.Close())) RIEGELI_UNREACHABLE();
  return true;
}

}  // namespace riegeli
