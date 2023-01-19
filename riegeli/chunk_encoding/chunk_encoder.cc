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

#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/chain.h"
#include "riegeli/messages/message_serialize.h"

namespace riegeli {

void ChunkEncoder::Done() {
  num_records_ = 0;
  decoded_data_size_ = 0;
}

bool ChunkEncoder::AddRecord(const google::protobuf::MessageLite& record,
                             SerializeOptions serialize_options) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Chain serialized;
  if (absl::Status status =
          SerializeToChain(record, serialized, std::move(serialize_options));
      ABSL_PREDICT_FALSE(!status.ok())) {
    return Fail(std::move(status));
  }
  return AddRecord(std::move(serialized));
}

bool ChunkEncoder::AddRecord(Chain&& record) {
  // Not `std::move(record)`: forward to `AddRecord(const Chain&)`.
  return AddRecord(record);
}

bool ChunkEncoder::AddRecord(absl::Cord&& record) {
  // Not `std::move(record)`: forward to `AddRecord(const absl::Cord&)`.
  return AddRecord(record);
}

}  // namespace riegeli
