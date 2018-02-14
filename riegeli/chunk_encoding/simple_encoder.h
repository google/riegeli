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

#include <stdint.h>
#include <memory>
#include <string>

#include "google/protobuf/message_lite.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/base/string_view.h"
#include "riegeli/bytes/chain_writer.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/chunk_encoding/chunk_encoder.h"
#include "riegeli/chunk_encoding/types.h"

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
  SimpleEncoder(CompressionType compression_type, int compression_level);

  void Reset() override;

  bool AddRecord(const google::protobuf::MessageLite& record) override;
  bool AddRecord(string_view record) override;
  bool AddRecord(std::string&& record) override;
  bool AddRecord(const Chain& record) override;
  bool AddRecord(Chain&& record) override;

  bool Encode(Writer* dest, uint64_t* num_records,
              uint64_t* decoded_data_size) override;

 protected:
  void Done() override;

  ChunkType GetChunkType() const override { return ChunkType::kSimple; }

 private:
  class Compressor final : public Object {
   public:
    Compressor(CompressionType compression_type, int compression_level);

    Compressor(const Compressor&) = delete;
    Compressor& operator=(const Compressor&) = delete;

    void Reset(CompressionType compression_type, int compression_level);
    Writer* writer() const { return writer_.get(); }
    Chain* Encode();

   protected:
    void Done() override;

   private:
    Chain data_;
    std::unique_ptr<Writer> writer_;
  };

  template <typename Record>
  bool AddRecordImpl(Record&& record);

  CompressionType compression_type_;
  int compression_level_;
  uint64_t num_records_ = 0;
  Compressor sizes_compressor_;
  Compressor values_compressor_;
};

}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_SIMPLE_ENCODER_H_
