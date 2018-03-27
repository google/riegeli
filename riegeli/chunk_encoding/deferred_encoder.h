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

#ifndef RIEGELI_CHUNK_ENCODING_DEFERRED_ENCODER_H_
#define RIEGELI_CHUNK_ENCODING_DEFERRED_ENCODER_H_

#include <stddef.h>
#include <stdint.h>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "google/protobuf/message_lite.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/chain_writer.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/chunk_encoding/chunk_encoder.h"
#include "riegeli/chunk_encoding/types.h"

namespace riegeli {

// DeferredEncoder performs a minimal amount of the encoding work in
// AddRecord(), deferring as much as possible to EncodeAndClose().
// It does more memory copying than the base encoder though.
class DeferredEncoder : public ChunkEncoder {
 public:
  explicit DeferredEncoder(std::unique_ptr<ChunkEncoder> base_encoder);

  void Reset() override;

  using ChunkEncoder::AddRecord;
  bool AddRecord(const google::protobuf::MessageLite& record) override;
  bool AddRecord(absl::string_view record) override;
  bool AddRecord(std::string&& record) override;
  bool AddRecord(const Chain& record) override;
  bool AddRecord(Chain&& record) override;

  bool AddRecords(Chain records, std::vector<size_t> limits) override;

  bool EncodeAndClose(Writer* dest, uint64_t* num_records,
                      uint64_t* decoded_data_size) override;

  ChunkType GetChunkType() const override;

 protected:
  void Done() override;

 private:
  template <typename Record>
  bool AddRecordImpl(Record&& record);

  std::unique_ptr<ChunkEncoder> base_encoder_;
  // Concatenated record values.
  Chain records_;
  ChainWriter records_writer_;
  // Sorted record end positions.
  //
  // Invariant: limits_.size() == num_records_
  std::vector<size_t> limits_;

  // Invariant: records_writer_.pos() == (limits_.empty() ? 0 : limits_.back())
};

// Implementation details follow.

inline DeferredEncoder::DeferredEncoder(
    std::unique_ptr<ChunkEncoder> base_encoder)
    : base_encoder_(std::move(base_encoder)), records_writer_(&records_) {}

}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_DEFERRED_ENCODER_H_
