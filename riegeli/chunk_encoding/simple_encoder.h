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

#ifndef RIEGELI_CHUNK_ENCODING_SIMPLE_ENCODER_H_
#define RIEGELI_CHUNK_ENCODING_SIMPLE_ENCODER_H_

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/chunk_encoding/chunk_encoder.h"
#include "riegeli/chunk_encoding/compressor.h"
#include "riegeli/chunk_encoding/compressor_options.h"

namespace riegeli {

// Format:
//  - Compression type
//  - Size of record sizes (compressed if applicable)
//  - Record sizes (possibly compressed):
//    - Array of "num_records" varints: sizes of records
//  - Record values (possibly compressed):
//    - Concatenated record data (bytes)
//
// If compression is used, a compressed block is prefixed by its varint-encoded
// uncompressed size.
class SimpleEncoder final : public ChunkEncoder {
 public:
  // Creates an empty SimpleEncoder.
  SimpleEncoder(CompressorOptions options, uint64_t size_hint);

  void Reset() override;

  using ChunkEncoder::AddRecord;
  bool AddRecord(const google::protobuf::MessageLite& record) override;
  bool AddRecord(absl::string_view record) override;
  bool AddRecord(std::string&& record) override;
  bool AddRecord(const Chain& record) override;
  bool AddRecord(Chain&& record) override;

  bool AddRecords(Chain records, std::vector<size_t> limits) override;

  bool EncodeAndClose(Writer* dest, ChunkType* chunk_type,
                      uint64_t* num_records,
                      uint64_t* decoded_data_size) override;

 protected:
  void Done() override;

 private:
  template <typename Record>
  bool AddRecordImpl(Record&& record);

  CompressionType compression_type_;
  internal::Compressor sizes_compressor_;
  internal::Compressor values_compressor_;
};

}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_SIMPLE_ENCODER_H_
