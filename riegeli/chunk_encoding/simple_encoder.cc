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

#include "riegeli/chunk_encoding/simple_encoder.h"

#include <stdint.h>
#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "google/protobuf/message_lite.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/memory.h"
#include "riegeli/base/object.h"
#include "riegeli/base/str_cat.h"
#include "riegeli/base/string_view.h"
#include "riegeli/bytes/brotli_writer.h"
#include "riegeli/bytes/chain_writer.h"
#include "riegeli/bytes/message_serialize.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/bytes/writer_utils.h"
#include "riegeli/bytes/zstd_writer.h"
#include "riegeli/chunk_encoding/types.h"

namespace riegeli {

SimpleEncoder::Compressor::Compressor(CompressionType compression_type,
                                      int compression_level)
    : Object(State::kOpen) {
  Reset(compression_type, compression_level);
}

void SimpleEncoder::Compressor::Done() {
  data_ = Chain();
  writer_.reset();
}

inline void SimpleEncoder::Compressor::Reset(CompressionType compression_type,
                                             int compression_level) {
  MarkHealthy();
  data_.Clear();
  std::unique_ptr<Writer> data_writer =
      riegeli::make_unique<ChainWriter>(&data_);
  switch (compression_type) {
    case CompressionType::kNone:
      writer_ = std::move(data_writer);
      return;
    case CompressionType::kBrotli:
      writer_ = riegeli::make_unique<BrotliWriter>(
          std::move(data_writer),
          BrotliWriter::Options().set_compression_level(compression_level));
      return;
    case CompressionType::kZstd:
      writer_ = riegeli::make_unique<ZstdWriter>(
          std::move(data_writer),
          ZstdWriter::Options().set_compression_level(compression_level));
      return;
  }
  RIEGELI_ASSERT_UNREACHABLE()
      << "Unknown compression type: " << static_cast<int>(compression_type);
}

Chain* SimpleEncoder::Compressor::Encode() {
  if (RIEGELI_UNLIKELY(!writer_->Close())) return nullptr;
  return &data_;
}

SimpleEncoder::SimpleEncoder(CompressionType compression_type,
                             int compression_level)
    : compression_type_(compression_type),
      compression_level_(compression_level),
      sizes_compressor_(compression_type, compression_level),
      values_compressor_(compression_type, compression_level) {}

void SimpleEncoder::Done() {
  num_records_ = 0;
  sizes_compressor_.Close();
  values_compressor_.Close();
}

void SimpleEncoder::Reset() {
  MarkHealthy();
  num_records_ = 0;
  sizes_compressor_.Reset(compression_type_, compression_level_);
  values_compressor_.Reset(compression_type_, compression_level_);
}

bool SimpleEncoder::AddRecord(const google::protobuf::MessageLite& record) {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  if (RIEGELI_UNLIKELY(!record.IsInitialized())) {
    return Fail(StrCat("Failed to serialize message of type ",
                       record.GetTypeName(),
                       " because it is missing required fields: ",
                       record.InitializationErrorString()));
  }
  const size_t size = record.ByteSizeLong();
  if (RIEGELI_UNLIKELY(size > size_t{std::numeric_limits<int>::max()})) {
    return Fail(
        StrCat("Failed to serialize message of type ", record.GetTypeName(),
               " because it exceeds maximum protobuf size of 2GB: ", size));
  }
  if (RIEGELI_UNLIKELY(num_records_ == std::numeric_limits<uint64_t>::max())) {
    return Fail("Too many records");
  }
  ++num_records_;
  if (RIEGELI_UNLIKELY(!WriteVarint64(sizes_compressor_.writer(), size))) {
    return Fail(*sizes_compressor_.writer());
  }
  if (RIEGELI_UNLIKELY(
          !SerializePartialToWriter(record, values_compressor_.writer()))) {
    return Fail(*values_compressor_.writer());
  }
  return true;
}

bool SimpleEncoder::AddRecord(string_view record) {
  return AddRecordImpl(record);
}

bool SimpleEncoder::AddRecord(std::string&& record) {
  return AddRecordImpl(std::move(record));
}

bool SimpleEncoder::AddRecord(const Chain& record) {
  return AddRecordImpl(record);
}

bool SimpleEncoder::AddRecord(Chain&& record) {
  return AddRecordImpl(std::move(record));
}

template <typename Record>
bool SimpleEncoder::AddRecordImpl(Record&& record) {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  if (RIEGELI_UNLIKELY(num_records_ == std::numeric_limits<uint64_t>::max())) {
    return Fail("Too many records");
  }
  ++num_records_;
  if (RIEGELI_UNLIKELY(
          !WriteVarint64(sizes_compressor_.writer(), record.size()))) {
    return Fail(*sizes_compressor_.writer());
  }
  if (RIEGELI_UNLIKELY(
          !values_compressor_.writer()->Write(std::forward<Record>(record)))) {
    return Fail(*values_compressor_.writer());
  }
  return true;
}

bool SimpleEncoder::Encode(Writer* dest, uint64_t* num_records,
                           uint64_t* decoded_data_size) {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  if (RIEGELI_UNLIKELY(values_compressor_.writer()->pos() >
                       std::numeric_limits<uint64_t>::max())) {
    return Fail("Decoded data size too large");
  }
  *num_records = num_records_;
  *decoded_data_size = values_compressor_.writer()->pos();

  WriteByte(dest, static_cast<uint8_t>(compression_type_));

  Chain* compressed_sizes = sizes_compressor_.Encode();
  if (RIEGELI_UNLIKELY(compressed_sizes == nullptr)) {
    return Fail(sizes_compressor_);
  }
  WriteVarint64(dest, compressed_sizes->size());
  dest->Write(std::move(*compressed_sizes));

  Chain* compressed_values = values_compressor_.Encode();
  if (RIEGELI_UNLIKELY(compressed_values == nullptr)) {
    return Fail(values_compressor_);
  }
  if (RIEGELI_UNLIKELY(!dest->Write(std::move(*compressed_values)))) {
    return Fail(*dest);
  }
  return true;
}

}  // namespace riegeli
