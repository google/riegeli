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
#include <limits>

#include "google/protobuf/message_lite.h"
#include "absl/base/optimization.h"
#include "absl/strings/str_cat.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/chain_writer.h"
#include "riegeli/bytes/message_serialize.h"
#include "riegeli/bytes/writer_utils.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/chunk_encoding/types.h"

namespace riegeli {

bool ChunkEncoder::AddRecord(const google::protobuf::MessageLite& record) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(!record.IsInitialized())) {
    return Fail(absl::StrCat("Failed to serialize message of type ",
                             record.GetTypeName(),
                             " because it is missing required fields: ",
                             record.InitializationErrorString()));
  }
  const size_t size = record.ByteSizeLong();
  if (ABSL_PREDICT_FALSE(size > size_t{std::numeric_limits<int>::max()})) {
    return Fail(absl::StrCat(
        "Failed to serialize message of type ", record.GetTypeName(),
        " because it exceeds maximum protobuf size of 2GB: ", size));
  }
  return AddRecord(SerializePartialAsChain(record));
}

bool ChunkEncoder::AddRecord(Chain&& record) {
  // Not std::move(record): forward to AddRecord(const Chain&).
  return AddRecord(record);
}

bool ChunkEncoder::EncodeAndClose(Chunk* chunk) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  ChunkType chunk_type;
  uint64_t num_records;
  uint64_t decoded_data_size;
  chunk->data.Clear();
  ChainWriter data_writer(&chunk->data);
  if (ABSL_PREDICT_FALSE(!EncodeAndClose(&data_writer, &chunk_type,
                                         &num_records, &decoded_data_size))) {
    return false;
  }
  if (ABSL_PREDICT_FALSE(!data_writer.Close())) return Fail(data_writer);
  chunk->header = ChunkHeader(chunk->data, chunk_type, num_records,
                              IntCast<uint64_t>(decoded_data_size));
  return true;
}

}  // namespace riegeli
