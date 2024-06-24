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

#include <utility>
#include <vector>

#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/external_ref.h"
#include "riegeli/base/recycling_pool.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/chunk_encoding/chunk_encoder.h"
#include "riegeli/chunk_encoding/compressor.h"
#include "riegeli/chunk_encoding/compressor_options.h"
#include "riegeli/chunk_encoding/constants.h"
#include "riegeli/messages/message_serialize.h"

namespace riegeli {

// Format:
//  - Compression type
//  - Size of record sizes (compressed if applicable)
//  - Record sizes (possibly compressed):
//    - Array of `num_records` varints: sizes of records
//  - Record values (possibly compressed):
//    - Concatenated record data (bytes)
//
// If compression is used, a compressed block is prefixed by its varint-encoded
// uncompressed size.
class SimpleEncoder : public ChunkEncoder {
 public:
  class TuningOptions {
   public:
    TuningOptions() noexcept {}

    // Expected uncompressed size of concatenated values, or `absl::nullopt` if
    // unknown. This may improve compression density and performance.
    //
    // If the size hint turns out to not match reality, nothing breaks.
    //
    // Default: `absl::nullopt`.
    TuningOptions& set_size_hint(absl::optional<Position> size_hint) & {
      size_hint_ = size_hint;
      return *this;
    }
    TuningOptions&& set_size_hint(absl::optional<Position> size_hint) && {
      return std::move(set_size_hint(size_hint));
    }
    absl::optional<Position> size_hint() const { return size_hint_; }

    // Options for a global `RecyclingPool` of compression contexts.
    //
    // They tune the amount of memory which is kept to speed up creation of new
    // compression sessions, and usage of a background thread to clean it.
    //
    // Default: `RecyclingPoolOptions()`.
    TuningOptions& set_recycling_pool_options(
        const RecyclingPoolOptions& recycling_pool_options) & {
      recycling_pool_options_ = recycling_pool_options;
      return *this;
    }
    TuningOptions&& set_recycling_pool_options(
        const RecyclingPoolOptions& recycling_pool_options) && {
      return std::move(set_recycling_pool_options(recycling_pool_options));
    }
    const RecyclingPoolOptions& recycling_pool_options() const {
      return recycling_pool_options_;
    }

   private:
    absl::optional<Position> size_hint_;
    RecyclingPoolOptions recycling_pool_options_;
  };

  // Creates an empty `SimpleEncoder`.
  explicit SimpleEncoder(CompressorOptions compressor_options,
                         TuningOptions tuning_options = TuningOptions());

  void Clear() override;

  using ChunkEncoder::AddRecord;
  bool AddRecord(const google::protobuf::MessageLite& record,
                 SerializeOptions serialize_options) override;
  bool AddRecord(absl::string_view record) override;
  bool AddRecord(const Chain& record) override;
  bool AddRecord(Chain&& record) override;
  bool AddRecord(const absl::Cord& record) override;
  bool AddRecord(absl::Cord&& record) override;
  bool AddRecord(ExternalRef record) override;

  bool AddRecords(Chain records, std::vector<size_t> limits) override;

  bool EncodeAndClose(Writer& dest, ChunkType& chunk_type,
                      uint64_t& num_records,
                      uint64_t& decoded_data_size) override;

 private:
  // This template is defined and used only in simple_encoder.cc.
  template <typename Record>
  bool AddRecordImpl(Record&& record);

  CompressionType compression_type_;
  chunk_encoding_internal::Compressor sizes_compressor_;
  chunk_encoding_internal::Compressor values_compressor_;
};

}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_SIMPLE_ENCODER_H_
