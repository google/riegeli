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

#include "riegeli/chunk_encoding/compressor.h"

#include <stdint.h>

#include <memory>
#include <tuple>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/brotli/brotli_writer.h"
#include "riegeli/bytes/chain_writer.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/chunk_encoding/compressor_options.h"
#include "riegeli/chunk_encoding/constants.h"
#include "riegeli/snappy/snappy_writer.h"
#include "riegeli/varint/varint_writing.h"
#include "riegeli/zstd/zstd_writer.h"

namespace riegeli {
namespace chunk_encoding_internal {

Compressor::Compressor(CompressorOptions compressor_options,
                       TuningOptions tuning_options)
    : compressor_options_(std::move(compressor_options)),
      tuning_options_(std::move(tuning_options)) {
  Initialize();
  SetWriteSizeHint();
}

void Compressor::Clear(TuningOptions tuning_options) {
  tuning_options_ = std::move(tuning_options);
  Clear();
}

void Compressor::Clear() {
  Object::Reset();
  Initialize();
  SetWriteSizeHint();
}

inline void Compressor::Initialize() {
  switch (compressor_options_.compression_type()) {
    case CompressionType::kNone:
      writer_ = std::make_unique<ChainWriter<>>(&compressed_);
      return;
    case CompressionType::kBrotli:
      writer_ = std::make_unique<BrotliWriter<ChainWriter<>>>(
          std::forward_as_tuple(&compressed_),
          BrotliWriterBase::Options()
              .set_compression_level(compressor_options_.compression_level())
              .set_window_log(compressor_options_.brotli_window_log()));
      return;
    case CompressionType::kZstd:
      writer_ = std::make_unique<ZstdWriter<ChainWriter<>>>(
          std::forward_as_tuple(&compressed_),
          ZstdWriterBase::Options()
              .set_compression_level(compressor_options_.compression_level())
              .set_window_log(compressor_options_.zstd_window_log())
              .set_pledged_size(tuning_options_.pledged_size()));
      return;
    case CompressionType::kSnappy:
      writer_ = std::make_unique<SnappyWriter<ChainWriter<>>>(
          std::forward_as_tuple(&compressed_));
      return;
  }
  RIEGELI_ASSERT_UNREACHABLE()
      << "Unknown compression type: "
      << static_cast<unsigned>(compressor_options_.compression_type());
}

inline void Compressor::SetWriteSizeHint() {
  writer_->SetWriteSizeHint(tuning_options_.pledged_size() != absl::nullopt
                                ? tuning_options_.pledged_size()
                                : tuning_options_.size_hint());
}

bool Compressor::EncodeAndClose(Writer& dest) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  const Position uncompressed_size = writer().pos();
  if (ABSL_PREDICT_FALSE(!writer().Close())) return Fail(writer().status());
  if (compressor_options_.compression_type() != CompressionType::kNone) {
    if (ABSL_PREDICT_FALSE(
            !WriteVarint64(IntCast<uint64_t>(uncompressed_size), dest))) {
      return Fail(dest.status());
    }
  }
  if (ABSL_PREDICT_FALSE(!dest.Write(std::move(compressed_)))) {
    return Fail(dest.status());
  }
  return Close();
}

bool Compressor::LengthPrefixedEncodeAndClose(Writer& dest) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  const Position uncompressed_size = writer().pos();
  if (ABSL_PREDICT_FALSE(!writer().Close())) return Fail(writer().status());
  uint64_t compressed_size = compressed_.size();
  if (compressor_options_.compression_type() != CompressionType::kNone) {
    compressed_size += LengthVarint64(IntCast<uint64_t>(uncompressed_size));
  }
  if (ABSL_PREDICT_FALSE(!WriteVarint64(compressed_size, dest))) {
    return Fail(dest.status());
  }
  if (compressor_options_.compression_type() != CompressionType::kNone) {
    if (ABSL_PREDICT_FALSE(
            !WriteVarint64(IntCast<uint64_t>(uncompressed_size), dest))) {
      return Fail(dest.status());
    }
  }
  if (ABSL_PREDICT_FALSE(!dest.Write(std::move(compressed_)))) {
    return Fail(dest.status());
  }
  return Close();
}

}  // namespace chunk_encoding_internal
}  // namespace riegeli
