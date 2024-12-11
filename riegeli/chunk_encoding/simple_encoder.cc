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
#include <utility>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/external_ref.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/chunk_encoding/chunk_encoder.h"
#include "riegeli/chunk_encoding/compressor.h"
#include "riegeli/chunk_encoding/compressor_options.h"
#include "riegeli/chunk_encoding/constants.h"
#include "riegeli/messages/serialize_message.h"
#include "riegeli/varint/varint_writing.h"

namespace riegeli {

SimpleEncoder::SimpleEncoder(CompressorOptions compressor_options,
                             TuningOptions tuning_options)
    : compression_type_(compressor_options.compression_type()),
      sizes_compressor_(compressor_options,
                        chunk_encoding_internal::Compressor::TuningOptions()
                            .set_recycling_pool_options(
                                tuning_options.recycling_pool_options())),
      values_compressor_(compressor_options,
                         chunk_encoding_internal::Compressor::TuningOptions()
                             .set_size_hint(tuning_options.size_hint())
                             .set_recycling_pool_options(
                                 tuning_options.recycling_pool_options())) {}

void SimpleEncoder::Clear() {
  ChunkEncoder::Clear();
  sizes_compressor_.Clear();
  values_compressor_.Clear();
}

bool SimpleEncoder::AddRecord(const google::protobuf::MessageLite& record,
                              SerializeOptions serialize_options) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  const size_t size = serialize_options.GetByteSize(record);
  if (ABSL_PREDICT_FALSE(num_records_ == kMaxNumRecords)) {
    return Fail(absl::ResourceExhaustedError("Too many records"));
  }
  if (ABSL_PREDICT_FALSE(size > std::numeric_limits<uint64_t>::max() -
                                    decoded_data_size_)) {
    return Fail(absl::ResourceExhaustedError("Decoded data size too large"));
  }
  ++num_records_;
  decoded_data_size_ += IntCast<uint64_t>(size);
  if (ABSL_PREDICT_FALSE(!WriteVarint64(IntCast<uint64_t>(size),
                                        sizes_compressor_.writer()))) {
    return Fail(sizes_compressor_.writer().status());
  }
  {
    absl::Status status = SerializeMessage(record, values_compressor_.writer(),
                                           serialize_options);
    if (ABSL_PREDICT_FALSE(!status.ok())) {
      return Fail(std::move(status));
    }
  }
  return true;
}

bool SimpleEncoder::AddRecord(absl::string_view record) {
  return AddRecordImpl(record);
}

bool SimpleEncoder::AddRecord(const Chain& record) {
  return AddRecordImpl(record);
}

bool SimpleEncoder::AddRecord(Chain&& record) {
  return AddRecordImpl(std::move(record));
}

bool SimpleEncoder::AddRecord(const absl::Cord& record) {
  return AddRecordImpl(record);
}

bool SimpleEncoder::AddRecord(absl::Cord&& record) {
  return AddRecordImpl(std::move(record));
}

bool SimpleEncoder::AddRecord(ExternalRef record) {
  return AddRecordImpl(std::move(record));
}

template <typename Record>
bool SimpleEncoder::AddRecordImpl(Record&& record) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (ABSL_PREDICT_FALSE(num_records_ == kMaxNumRecords)) {
    return Fail(absl::ResourceExhaustedError("Too many records"));
  }
  if (ABSL_PREDICT_FALSE(record.size() > std::numeric_limits<uint64_t>::max() -
                                             decoded_data_size_)) {
    return Fail(absl::ResourceExhaustedError("Decoded data size too large"));
  }
  ++num_records_;
  decoded_data_size_ += IntCast<uint64_t>(record.size());
  if (ABSL_PREDICT_FALSE(!WriteVarint64(IntCast<uint64_t>(record.size()),
                                        sizes_compressor_.writer()))) {
    return Fail(sizes_compressor_.writer().status());
  }
  if (ABSL_PREDICT_FALSE(
          !values_compressor_.writer().Write(std::forward<Record>(record)))) {
    return Fail(values_compressor_.writer().status());
  }
  return true;
}

bool SimpleEncoder::AddRecords(Chain records, std::vector<size_t> limits) {
  RIEGELI_ASSERT_EQ(limits.empty() ? size_t{0} : limits.back(), records.size())
      << "Failed precondition of ChunkEncoder::AddRecords(): "
         "record end positions do not match concatenated record values";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (ABSL_PREDICT_FALSE(limits.size() > kMaxNumRecords - num_records_)) {
    return Fail(absl::ResourceExhaustedError("Too many records"));
  }
  if (ABSL_PREDICT_FALSE(records.size() > std::numeric_limits<uint64_t>::max() -
                                              decoded_data_size_)) {
    return Fail(absl::ResourceExhaustedError("Decoded data size too large"));
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
    if (ABSL_PREDICT_FALSE(!WriteVarint64(IntCast<uint64_t>(limit - start),
                                          sizes_compressor_.writer()))) {
      return Fail(sizes_compressor_.writer().status());
    }
    start = limit;
  }
  if (ABSL_PREDICT_FALSE(
          !values_compressor_.writer().Write(std::move(records)))) {
    return Fail(values_compressor_.writer().status());
  }
  return true;
}

bool SimpleEncoder::EncodeAndClose(Writer& dest, ChunkType& chunk_type,
                                   uint64_t& num_records,
                                   uint64_t& decoded_data_size) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  chunk_type = ChunkType::kSimple;
  num_records = num_records_;
  decoded_data_size = decoded_data_size_;

  if (ABSL_PREDICT_FALSE(
          !dest.WriteByte(static_cast<uint8_t>(compression_type_)))) {
    return Fail(dest.status());
  }

  if (ABSL_PREDICT_FALSE(
          !sizes_compressor_.LengthPrefixedEncodeAndClose(dest))) {
    return Fail(sizes_compressor_.status());
  }

  if (ABSL_PREDICT_FALSE(!values_compressor_.EncodeAndClose(dest))) {
    return Fail(values_compressor_.status());
  }
  return Close();
}

}  // namespace riegeli
