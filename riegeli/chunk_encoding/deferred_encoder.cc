// Copyright 2018 Google LLC
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

#include "riegeli/chunk_encoding/deferred_encoder.h"

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
#include "riegeli/base/canonical_errors.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/chain_writer.h"
#include "riegeli/bytes/message_serialize.h"
#include "riegeli/chunk_encoding/constants.h"

namespace riegeli {

void DeferredEncoder::Reset() {
  ChunkEncoder::Reset();
  base_encoder_->Reset();
  records_writer_ = ChainWriter<Chain>(Chain());
  limits_.clear();
}

bool DeferredEncoder::AddRecord(const google::protobuf::MessageLite& record) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  const size_t size = record.ByteSizeLong();
  if (ABSL_PREDICT_FALSE(num_records_ ==
                         UnsignedMin(limits_.max_size(), kMaxNumRecords))) {
    return Fail(ResourceExhaustedError("Too many records"));
  }
  if (ABSL_PREDICT_FALSE(size > std::numeric_limits<uint64_t>::max() -
                                    decoded_data_size_)) {
    return Fail(ResourceExhaustedError("Decoded data size too large"));
  }
  ++num_records_;
  decoded_data_size_ += IntCast<uint64_t>(size);
  Status serialize_status = SerializeToWriter<>(record, &records_writer_);
  if (ABSL_PREDICT_FALSE(!serialize_status.ok())) {
    return Fail(std::move(serialize_status));
  }
  limits_.push_back(IntCast<size_t>(records_writer_.pos()));
  return true;
}

bool DeferredEncoder::AddRecord(absl::string_view record) {
  return AddRecordImpl(record);
}

bool DeferredEncoder::AddRecord(std::string&& record) {
  return AddRecordImpl(std::move(record));
}

bool DeferredEncoder::AddRecord(const Chain& record) {
  return AddRecordImpl(record);
}

bool DeferredEncoder::AddRecord(Chain&& record) {
  return AddRecordImpl(std::move(record));
}

template <typename Record>
bool DeferredEncoder::AddRecordImpl(Record&& record) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(num_records_ ==
                         UnsignedMin(limits_.max_size(), kMaxNumRecords))) {
    return Fail(ResourceExhaustedError("Too many records"));
  }
  if (ABSL_PREDICT_FALSE(record.size() > std::numeric_limits<uint64_t>::max() -
                                             decoded_data_size_)) {
    return Fail(ResourceExhaustedError("Decoded data size too large"));
  }
  ++num_records_;
  decoded_data_size_ += IntCast<uint64_t>(record.size());
  if (ABSL_PREDICT_FALSE(
          !records_writer_.Write(std::forward<Record>(record)))) {
    return Fail(records_writer_);
  }
  limits_.push_back(IntCast<size_t>(records_writer_.pos()));
  return true;
}

bool DeferredEncoder::AddRecords(Chain records, std::vector<size_t> limits) {
  RIEGELI_ASSERT_EQ(limits.empty() ? 0u : limits.back(), records.size())
      << "Failed precondition of ChunkEncoder::AddRecords(): "
         "record end positions do not match concatenated record values";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(limits.size() >
                         UnsignedMin(limits_.max_size(), kMaxNumRecords) -
                             num_records_)) {
    return Fail(ResourceExhaustedError("Too many records"));
  }
  num_records_ += IntCast<uint64_t>(limits.size());
  decoded_data_size_ += IntCast<uint64_t>(records.size());
  if (ABSL_PREDICT_FALSE(!records_writer_.Write(std::move(records)))) {
    return Fail(records_writer_);
  }
  if (limits_.empty()) {
    limits_ = std::move(limits);
  } else {
    const size_t base = limits_.back();
    for (size_t& limit : limits) limit += base;
    limits_.insert(limits_.cend(), limits.begin(), limits.end());
  }
  return true;
}

bool DeferredEncoder::EncodeAndClose(Writer* dest, ChunkType* chunk_type,
                                     uint64_t* num_records,
                                     uint64_t* decoded_data_size) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(!records_writer_.Close())) {
    return Fail(records_writer_);
  }
  if (ABSL_PREDICT_FALSE(!base_encoder_->AddRecords(
          std::move(records_writer_.dest()), std::move(limits_))) ||
      ABSL_PREDICT_FALSE(!base_encoder_->EncodeAndClose(
          dest, chunk_type, num_records, decoded_data_size))) {
    Fail(*base_encoder_);
  }
  return Close();
}

}  // namespace riegeli
