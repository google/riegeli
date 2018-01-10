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

#include "riegeli/chunk_encoding/chunk_encoder.h"

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>

#include "google/protobuf/message_lite.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/string_view.h"
#include "riegeli/bytes/brotli_writer.h"
#include "riegeli/bytes/chain_writer.h"
#include "riegeli/bytes/message_serialize.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/bytes/writer_utils.h"
#include "riegeli/bytes/zstd_writer.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/chunk_encoding/internal_types.h"

namespace riegeli {

inline void SimpleChunkEncoder::Compressor::Reset() {
  writer_.Cancel();
  data_.Clear();
  writer_ = ChainWriter(&data_);
}

inline bool SimpleChunkEncoder::Compressor::Encode(
    ChainWriter* dest, internal::CompressionType compression_type,
    int compression_level) {
  if (!writer_.Close()) RIEGELI_UNREACHABLE() << "ChainWriter::Close() failed";
  switch (compression_type) {
    case internal::CompressionType::kNone:
      return dest->Write(std::move(data_));
    case internal::CompressionType::kBrotli:
      return Compress<BrotliWriter>(dest, compression_level);
    case internal::CompressionType::kZstd:
      return Compress<ZstdWriter>(dest, compression_level);
  }
  RIEGELI_UNREACHABLE() << "Unknown compression type: "
                        << static_cast<int>(compression_type);
}

inline const Chain& SimpleChunkEncoder::Compressor::EncodeUncompressed() {
  if (!writer_.Close()) RIEGELI_UNREACHABLE() << "ChainWriter::Close() failed";
  return data_;
}

template <typename Engine>
bool SimpleChunkEncoder::Compressor::Compress(ChainWriter* dest,
                                              int compression_level) {
  Engine compressor(dest, typename Engine::Options()
                              .set_compression_level(compression_level)
                              .set_size_hint(data_.size()));
  compressor.Write(std::move(data_));
  return compressor.Close();
}

ChunkEncoder::~ChunkEncoder() = default;

SimpleChunkEncoder::SimpleChunkEncoder(
    internal::CompressionType compression_type, int compression_level)
    : compression_type_(compression_type),
      compression_level_(compression_level) {}

void SimpleChunkEncoder::Reset() {
  num_records_ = 0;
  sizes_compressor_.Reset();
  values_compressor_.Reset();
}

void SimpleChunkEncoder::AddRecord(const google::protobuf::MessageLite& record) {
  ++num_records_;
  WriteVarint64(sizes_compressor_.writer(), record.ByteSizeLong());
  SerializeToWriter(record, values_compressor_.writer());
}

void SimpleChunkEncoder::AddRecord(string_view record) {
  ++num_records_;
  WriteVarint64(sizes_compressor_.writer(), record.size());
  values_compressor_.writer()->Write(record);
}

void SimpleChunkEncoder::AddRecord(std::string&& record) {
  ++num_records_;
  WriteVarint64(sizes_compressor_.writer(), record.size());
  values_compressor_.writer()->Write(std::move(record));
}

void SimpleChunkEncoder::AddRecord(const Chain& record) {
  ++num_records_;
  WriteVarint64(sizes_compressor_.writer(), record.size());
  values_compressor_.writer()->Write(record);
}

void SimpleChunkEncoder::AddRecord(Chain&& record) {
  ++num_records_;
  WriteVarint64(sizes_compressor_.writer(), record.size());
  values_compressor_.writer()->Write(std::move(record));
}

bool SimpleChunkEncoder::Encode(Chunk* chunk) {
  chunk->data.Clear();
  ChainWriter data_writer(&chunk->data);
  WriteByte(&data_writer, static_cast<uint8_t>(internal::ChunkType::kSimple));
  WriteByte(&data_writer, static_cast<uint8_t>(compression_type_));

  Chain compressed_sizes;
  if (compression_type_ == internal::CompressionType::kNone) {
    // sizes_compressor_ already holds the data as a Chain, there is no need to
    // copy a Chain to a Chain using ChainWriter.
    compressed_sizes = sizes_compressor_.EncodeUncompressed();
  } else {
    ChainWriter compressed_sizes_writer(&compressed_sizes);
    if (RIEGELI_UNLIKELY(!sizes_compressor_.Encode(
            &compressed_sizes_writer, compression_type_, compression_level_))) {
      return false;
    }
    if (!compressed_sizes_writer.Close()) {
      RIEGELI_UNREACHABLE() << "ChainWriter::Close() failed";
    }
  }
  WriteVarint64(&data_writer, compressed_sizes.size());
  data_writer.Write(std::move(compressed_sizes));

  const Position decoded_data_size = values_compressor_.writer()->pos();
  if (RIEGELI_UNLIKELY(!values_compressor_.Encode(
          &data_writer, compression_type_, compression_level_))) {
    return false;
  }
  if (!data_writer.Close()) {
    RIEGELI_UNREACHABLE() << "ChainWriter::Close() failed";
  }
  chunk->header = ChunkHeader(chunk->data, num_records_, decoded_data_size);
  return true;
}

EagerTransposedChunkEncoder::EagerTransposedChunkEncoder(
    internal::CompressionType compression_type, int compression_level,
    size_t desired_bucket_size) {
  SetCompression(compression_type, compression_level);
  transpose_encoder_.SetDesiredBucketSize(desired_bucket_size);
}

inline void EagerTransposedChunkEncoder::SetCompression(
    internal::CompressionType compression_type, int compression_level) {
  switch (compression_type) {
    case internal::CompressionType::kNone:
      return;
    case internal::CompressionType::kBrotli:
      transpose_encoder_.EnableBrotliCompression(compression_level);
      return;
    case internal::CompressionType::kZstd:
      transpose_encoder_.EnableZstdCompression(compression_level);
      return;
  }
  RIEGELI_UNREACHABLE() << "Unknown compression type: "
                        << static_cast<int>(compression_type);
}

void EagerTransposedChunkEncoder::Reset() {
  num_records_ = 0;
  decoded_data_size_ = 0;
  transpose_encoder_.Reset();
}

void EagerTransposedChunkEncoder::AddRecord(const google::protobuf::MessageLite& record) {
  ++num_records_;
  decoded_data_size_ += record.ByteSizeLong();
  transpose_encoder_.AddMessage(SerializeAsChain(record));
}

void EagerTransposedChunkEncoder::AddRecord(string_view record) {
  ++num_records_;
  decoded_data_size_ += record.size();
  transpose_encoder_.AddMessage(record);
}

void EagerTransposedChunkEncoder::AddRecord(std::string&& record) {
  ++num_records_;
  decoded_data_size_ += record.size();
  transpose_encoder_.AddMessage(std::move(record));
}

void EagerTransposedChunkEncoder::AddRecord(const Chain& record) {
  ++num_records_;
  decoded_data_size_ += record.size();
  transpose_encoder_.AddMessage(record);
}

void EagerTransposedChunkEncoder::AddRecord(Chain&& record) {
  ++num_records_;
  decoded_data_size_ += record.size();
  // No std::move(): TransposeEncoder::AddMessage() does not have a Chain&&
  // overload.
  transpose_encoder_.AddMessage(record);
}

bool EagerTransposedChunkEncoder::Encode(Chunk* chunk) {
  chunk->data.Clear();
  ChainWriter data_writer(&chunk->data);
  WriteByte(&data_writer,
            static_cast<uint8_t>(internal::ChunkType::kTransposed));
  if (!transpose_encoder_.Encode(&data_writer)) return false;
  if (!data_writer.Close()) return false;
  chunk->header = ChunkHeader(chunk->data, num_records_, decoded_data_size_);
  return true;
}

DeferredTransposedChunkEncoder::DeferredTransposedChunkEncoder(
    internal::CompressionType compression_type, int compression_level,
    size_t desired_bucket_size)
    : compression_type_(compression_type),
      compression_level_(compression_level),
      desired_bucket_size_(desired_bucket_size) {}

void DeferredTransposedChunkEncoder::Reset() { records_.clear(); }

void DeferredTransposedChunkEncoder::AddRecord(
    const google::protobuf::MessageLite& record) {
  records_.emplace_back();
  AppendToChain(record, &records_.back());
}

void DeferredTransposedChunkEncoder::AddRecord(string_view record) {
  records_.emplace_back(record);
}

void DeferredTransposedChunkEncoder::AddRecord(std::string&& record) {
  records_.emplace_back(std::move(record));
}

void DeferredTransposedChunkEncoder::AddRecord(const Chain& record) {
  records_.push_back(record);
}

void DeferredTransposedChunkEncoder::AddRecord(Chain&& record) {
  records_.push_back(std::move(record));
}

bool DeferredTransposedChunkEncoder::Encode(Chunk* chunk) {
  EagerTransposedChunkEncoder eager_chunk_encoder(
      compression_type_, compression_level_, desired_bucket_size_);
  for (const auto& record : records_) {
    eager_chunk_encoder.AddRecord(record);
  }
  return eager_chunk_encoder.Encode(chunk);
}

}  // namespace riegeli
