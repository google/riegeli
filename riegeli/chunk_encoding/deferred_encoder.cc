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

#include "google/protobuf/message_lite.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/chain_writer.h"
#include "riegeli/bytes/message_serialize.h"
#include "riegeli/chunk_encoding/types.h"

namespace riegeli {

void DeferredEncoder::Done() {
  base_encoder_.reset();
  records_ = Chain();
  records_writer_ = ChainWriter();
  limits_ = std::vector<size_t>();
}

void DeferredEncoder::Reset() {
  base_encoder_->Reset();
  records_.Clear();
  records_writer_ = ChainWriter(&records_);
  limits_.clear();
}

bool DeferredEncoder::AddRecord(const google::protobuf::MessageLite& record) {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  if (RIEGELI_UNLIKELY(!record.IsInitialized())) {
    return Fail(absl::StrCat("Failed to serialize message of type ",
                             record.GetTypeName(),
                             " because it is missing required fields: ",
                             record.InitializationErrorString()));
  }
  const size_t size = record.ByteSizeLong();
  if (RIEGELI_UNLIKELY(size > size_t{std::numeric_limits<int>::max()})) {
    return Fail(absl::StrCat(
        "Failed to serialize message of type ", record.GetTypeName(),
        " because it exceeds maximum protobuf size of 2GB: ", size));
  }
  if (RIEGELI_UNLIKELY(limits_.size() ==
                       UnsignedMin(limits_.max_size(),
                                   std::numeric_limits<uint64_t>::max()))) {
    return Fail("Too many records");
  }
  if (RIEGELI_UNLIKELY(!SerializePartialToWriter(record, &records_writer_))) {
    return Fail(records_writer_);
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
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  if (RIEGELI_UNLIKELY(limits_.size() ==
                       UnsignedMin(limits_.max_size(),
                                   std::numeric_limits<uint64_t>::max()))) {
    return Fail("Too many records");
  }
  if (RIEGELI_UNLIKELY(!records_writer_.Write(std::forward<Record>(record)))) {
    return Fail(records_writer_);
  }
  limits_.push_back(IntCast<size_t>(records_writer_.pos()));
  return true;
}

bool DeferredEncoder::AddRecords(Chain records, std::vector<size_t> limits) {
  RIEGELI_ASSERT_EQ(limits.empty() ? 0u : limits.back(), records.size())
      << "Failed precondition of ChunkEncoder::AddRecords(): "
         "record end positions do not match concatenated record values";
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  if (RIEGELI_UNLIKELY(limits.size() >
                       UnsignedMin(limits_.max_size(),
                                   std::numeric_limits<uint64_t>::max()) -
                           limits_.size())) {
    return Fail("Too many records");
  }
  if (RIEGELI_UNLIKELY(!records_writer_.Write(std::move(records)))) {
    return Fail(records_writer_);
  }
  if (limits_.empty()) {
    limits_ = std::move(limits);
  } else {
    const size_t base = limits_.back();
    for (auto& limit : limits) limit += base;
    limits_.insert(limits_.cend(), limits.begin(), limits.end());
  }
  return true;
}

bool DeferredEncoder::EncodeAndClose(Writer* dest, uint64_t* num_records,
                                     uint64_t* decoded_data_size) {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  if (RIEGELI_UNLIKELY(!records_writer_.Close())) return Fail(records_writer_);
  if (RIEGELI_UNLIKELY(!base_encoder_->AddRecords(std::move(records_),
                                                  std::move(limits_))) ||
      RIEGELI_UNLIKELY(!base_encoder_->EncodeAndClose(dest, num_records,
                                                      decoded_data_size))) {
    Fail(*base_encoder_);
  }
  return Close();
}

ChunkType DeferredEncoder::GetChunkType() const {
  return base_encoder_->GetChunkType();
}

}  // namespace riegeli
