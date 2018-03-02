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

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

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
  // Creates an empty ChunkEncoder.
  ChunkEncoder() noexcept : Object(State::kOpen) {}

  ChunkEncoder(const ChunkEncoder&) = delete;
  ChunkEncoder& operator=(const ChunkEncoder&) = delete;

  // Resets the ChunkEncoder back to empty.
  virtual void Reset() = 0;

  // Adds the next record.
  //
  // AddRecord(MessageLite) serializes a proto message to raw bytes beforehand.
  // The remaining overloads accept raw bytes.
  //
  // Return values:
  //  * true  - success (healthy())
  //  * false - failure (!healthy())
  virtual bool AddRecord(const google::protobuf::MessageLite& record);
  virtual bool AddRecord(string_view record) = 0;
  virtual bool AddRecord(std::string&& record) = 0;
  bool AddRecord(const char* record) { return AddRecord(string_view(record)); }
  virtual bool AddRecord(const Chain& record) = 0;
  virtual bool AddRecord(Chain&& record);

  // Add multiple records, expressed as concatenated record values and sorted
  // record end positions.
  //
  // Preconditions:
  //   limits are sorted
  //   (limits.empty() ? 0 : limits.back()) == records.size()
  //
  // Return values:
  //  * true  - success (healthy())
  //  * false - failure (!healthy())
  virtual bool AddRecords(Chain records, std::vector<size_t> limits) = 0;

  // Encodes the chunk to *dest, setting *num_records and *decoded_data_size.
  // Closes the ChunkEncoder.
  //
  // Return values:
  //  * true  - success (healthy())
  //  * false - failure (!healthy());
  //            if !dest->healthy() then the problem was at dest
  virtual bool EncodeAndClose(Writer* dest, uint64_t* num_records,
                              uint64_t* decoded_data_size) = 0;

  // Encodes the chunk to *chunk. Closes the ChunkEncoder.
  //
  // Return values:
  //  * true  - success (healthy())
  //  * false - failure (!healthy());
  bool EncodeAndClose(Chunk* chunk);

  // Returns the chunk type to write in a chunk header.
  virtual ChunkType GetChunkType() const = 0;
};

}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_CHUNK_ENCODER_H_
