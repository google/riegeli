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

#include <stddef.h>
#include <stdint.h>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/chain_writer.h"
#include "riegeli/bytes/message_serialize.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/bytes/writer_utils.h"
#include "riegeli/chunk_encoding/compressor_options.h"
#include "riegeli/chunk_encoding/constants.h"

namespace riegeli {

SimpleEncoder::SimpleEncoder(CompressorOptions options, uint64_t size_hint)
    : compression_type_(options.compression_type()),
      sizes_compressor_(options),
      values_compressor_(options, size_hint) {}

void SimpleEncoder::Reset() {
  ChunkEncoder::Reset();
  sizes_compressor_.Reset();
  values_compressor_.Reset();
}

bool SimpleEncoder::AddRecord(const google::protobuf::MessageLite& record) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  const size_t size = record.ByteSizeLong();
  if (ABSL_PREDICT_FALSE(num_records_ == kMaxNumRecords)) {
    return Fail("Too many records");
  }
  if (ABSL_PREDICT_FALSE(size > std::numeric_limits<uint64_t>::max() -
                                    decoded_data_size_)) {
    return Fail("Decoded data size too large");
  }
  ++num_records_;
  decoded_data_size_ += IntCast<uint64_t>(size);
  if (ABSL_PREDICT_FALSE(!WriteVarint64(sizes_compressor_.writer(),
                                        IntCast<uint64_t>(size)))) {
    return Fail(*sizes_compressor_.writer());
  }
  std::string error_message;
  if (ABSL_PREDICT_FALSE(!SerializeToWriter(record, values_compressor_.writer(),
                                            &error_message))) {
    return Fail(error_message);
  }
  return true;
}

bool SimpleEncoder::AddRecord(absl::string_view record) {
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
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(num_records_ == kMaxNumRecords)) {
    return Fail("Too many records");
  }
  if (ABSL_PREDICT_FALSE(record.size() > std::numeric_limits<uint64_t>::max() -
                                             decoded_data_size_)) {
    return Fail("Decoded data size too large");
  }
  ++num_records_;
  decoded_data_size_ += IntCast<uint64_t>(record.size());
  if (ABSL_PREDICT_FALSE(!WriteVarint64(sizes_compressor_.writer(),
                                        IntCast<uint64_t>(record.size())))) {
    return Fail(*sizes_compressor_.writer());
  }
  if (ABSL_PREDICT_FALSE(
          !values_compressor_.writer()->Write(std::forward<Record>(record)))) {
    return Fail(*values_compressor_.writer());
  }
  return true;
}

bool SimpleEncoder::AddRecords(Chain records, std::vector<size_t> limits) {
  RIEGELI_ASSERT_EQ(limits.empty() ? size_t{0} : limits.back(), records.size())
      << "Failed precondition of ChunkEncoder::AddRecords(): "
         "record end positions do not match concatenated record values";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(limits.size() > kMaxNumRecords - num_records_)) {
    return Fail("Too many records");
  }
  if (ABSL_PREDICT_FALSE(records.size() > std::numeric_limits<uint64_t>::max() -
                                              decoded_data_size_)) {
    return Fail("Decoded data size too large");
  }
  num_records_ += IntCast<uint64_t>(limits.size());
  decoded_data_size_ += IntCast<uint64_t>(records.size());
  size_t start = 0;
  for (const size_t limit : limits) {
    RIEGELI_ASSERT_GE(limit, start)
        << "Failed precondition of ChunkEncoder::AddRecords(): "
           "record end positions not sorted";
    RIEGELI_ASSERT_LE(limit, records.size())
        << "Failed precondition of ChunkEncoder::AddRecords(): "
           "record end positions do not match concatenated record values";
    if (ABSL_PREDICT_FALSE(!WriteVarint64(sizes_compressor_.writer(),
                                          IntCast<uint64_t>(limit - start)))) {
      return Fail(*sizes_compressor_.writer());
    }
    start = limit;
  }
  if (ABSL_PREDICT_FALSE(
          !values_compressor_.writer()->Write(std::move(records)))) {
    return Fail(*values_compressor_.writer());
  }
  return true;
}

bool SimpleEncoder::EncodeAndClose(Writer* dest, ChunkType* chunk_type,
                                   uint64_t* num_records,
                                   uint64_t* decoded_data_size) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  *chunk_type = ChunkType::kSimple;
  *num_records = num_records_;
  *decoded_data_size = decoded_data_size_;

  if (ABSL_PREDICT_FALSE(
          !WriteByte(dest, static_cast<uint8_t>(compression_type_)))) {
    return Fail(*dest);
  }

  ChainWriter<Chain> compressed_sizes_writer((Chain()));
  if (ABSL_PREDICT_FALSE(
          !sizes_compressor_.EncodeAndClose(&compressed_sizes_writer))) {
    return Fail(sizes_compressor_);
  }
  if (ABSL_PREDICT_FALSE(!compressed_sizes_writer.Close())) {
    return Fail(compressed_sizes_writer);
  }
  if (ABSL_PREDICT_FALSE(!WriteVarint64(
          dest, IntCast<uint64_t>(compressed_sizes_writer.dest().size()))) ||
      ABSL_PREDICT_FALSE(
          !dest->Write(std::move(compressed_sizes_writer.dest())))) {
    return Fail(*dest);
  }

  if (ABSL_PREDICT_FALSE(!values_compressor_.EncodeAndClose(dest))) {
    return Fail(values_compressor_);
  }
  return Close();
}

}  // namespace riegeli
