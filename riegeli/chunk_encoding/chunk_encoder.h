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

#ifndef RIEGELI_CHUNK_ENCODING_CHUNK_ENCODER_H_
#define RIEGELI_CHUNK_ENCODING_CHUNK_ENCODER_H_

#include <stdint.h>
#include <string>

#include "google/protobuf/message_lite.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/base/string_view.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/chunk_encoding/types.h"

namespace riegeli {

class ChunkEncoder : public Object {
 public:
  ChunkEncoder() noexcept : Object(State::kOpen) {}

  ChunkEncoder(const ChunkEncoder&) = delete;
  ChunkEncoder& operator=(const ChunkEncoder&) = delete;

  virtual void Reset() = 0;

  virtual bool AddRecord(const google::protobuf::MessageLite& record);
  virtual bool AddRecord(string_view record) = 0;
  virtual bool AddRecord(std::string&& record) = 0;
  bool AddRecord(const char* record) { return AddRecord(string_view(record)); }
  virtual bool AddRecord(const Chain& record) = 0;
  virtual bool AddRecord(Chain&& record);

  virtual bool Encode(Writer* dest, uint64_t* num_records,
                      uint64_t* decoded_data_size) = 0;

  bool Encode(Chunk* chunk);

 protected:
  virtual ChunkType GetChunkType() const = 0;
};

}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_CHUNK_ENCODER_H_
