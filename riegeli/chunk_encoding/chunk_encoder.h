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
#include <memory>
#include <string>
#include <vector>

#include "google/protobuf/message_lite.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/string_view.h"
#include "riegeli/bytes/chain_writer.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/chunk_encoding/internal_types.h"
#include "riegeli/chunk_encoding/transpose_encoder.h"

namespace riegeli {

class ChunkEncoder {
 public:
  ChunkEncoder() = default;

  ChunkEncoder(const ChunkEncoder&) = delete;
  ChunkEncoder& operator=(const ChunkEncoder&) = delete;

  virtual ~ChunkEncoder();

  virtual void Reset() = 0;
  virtual void AddRecord(const google::protobuf::MessageLite& record) = 0;
  virtual void AddRecord(string_view record) = 0;
  virtual void AddRecord(std::string&& record) = 0;
  void AddRecord(const char* record) { AddRecord(string_view(record)); }
  virtual void AddRecord(const Chain& record) = 0;
  virtual void AddRecord(Chain&& record) = 0;
  virtual bool Encode(Chunk* chunk) = 0;
};

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
class SimpleChunkEncoder final : public ChunkEncoder {
 public:
  SimpleChunkEncoder(internal::CompressionType compression_type,
                     int compression_level);

  void Reset() override;
  void AddRecord(const google::protobuf::MessageLite& record) override;
  void AddRecord(string_view record) override;
  void AddRecord(std::string&& record) override;
  void AddRecord(const Chain& record) override;
  void AddRecord(Chain&& record) override;
  bool Encode(Chunk* data) override;

 private:
  class Compressor {
   public:
    Compressor(internal::CompressionType compression_type,
               int compression_level);

    void Reset(internal::CompressionType compression_type,
               int compression_level);
    Writer* writer() const { return writer_.get(); }
    Chain* Encode();

   private:
    Chain data_;
    std::unique_ptr<Writer> writer_;
  };

  internal::CompressionType compression_type_;
  int compression_level_;
  size_t num_records_ = 0;
  Compressor sizes_compressor_;
  Compressor values_compressor_;
};

// This implementation of a transposed chunk encoder performs a part of the
// encoding work in AddRecord() and the rest in Encode(). It does less memory
// copying than DeferredTransposedChunkEncoder.
class EagerTransposedChunkEncoder final : public ChunkEncoder {
 public:
  EagerTransposedChunkEncoder(internal::CompressionType compression_type,
                              int compression_level,
                              size_t desired_bucket_size);

  void Reset() override;
  void AddRecord(const google::protobuf::MessageLite& record) override;
  void AddRecord(string_view record) override;
  void AddRecord(std::string&& record) override;
  void AddRecord(const Chain& record) override;
  void AddRecord(Chain&& record) override;
  bool Encode(Chunk* data) override;

 private:
  void SetCompression(internal::CompressionType compression_type,
                      int compression_level);

  size_t num_records_ = 0;
  size_t decoded_data_size_ = 0;
  TransposeEncoder transpose_encoder_;
};

// This implementation of a transposed chunk encoder performs a minimal amount
// of the encoding work in AddRecord(), deferring as much as possible to
// Encode(). It does more memory copying than EagerTransposedChunkEncoder.
class DeferredTransposedChunkEncoder final : public ChunkEncoder {
 public:
  DeferredTransposedChunkEncoder(internal::CompressionType compression_type,
                                 int compression_level,
                                 size_t desired_bucket_size);

  void Reset() override;
  void AddRecord(const google::protobuf::MessageLite& record) override;
  void AddRecord(string_view record) override;
  void AddRecord(std::string&& record) override;
  void AddRecord(const Chain& record) override;
  void AddRecord(Chain&& record) override;
  bool Encode(Chunk* data) override;

 private:
  internal::CompressionType compression_type_;
  int compression_level_;
  size_t desired_bucket_size_;
  std::vector<Chain> records_;
};

}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_CHUNK_ENCODER_H_
