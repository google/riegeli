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

#include <tuple>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/types/optional.h"
#include "absl/types/variant.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/brotli/brotli_writer.h"
#include "riegeli/bytes/chain_writer.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/chunk_encoding/compressor_options.h"
#include "riegeli/chunk_encoding/constants.h"
#include "riegeli/snappy/snappy_writer.h"
#include "riegeli/varint/varint_writing.h"
#include "riegeli/zstd/zstd_writer.h"

namespace riegeli {
namespace internal {

Compressor::Compressor(CompressorOptions compressor_options,
                       TuningOptions tuning_options)
    : Object(kInitiallyOpen),
      compressor_options_(std::move(compressor_options)),
      tuning_options_(std::move(tuning_options)) {
  Initialize();
}

void Compressor::Clear(TuningOptions tuning_options) {
  tuning_options_ = std::move(tuning_options);
  Clear();
}

void Compressor::Clear() {
  Object::Reset(kInitiallyOpen);
  Initialize();
}

void Compressor::Initialize() {
  switch (compressor_options_.compression_type()) {
    case CompressionType::kNone:
      writer_.emplace<ChainWriter<>>(
          &compressed_, ChainWriterBase::Options().set_size_hint(
                            tuning_options_.pledged_size() != absl::nullopt
                                ? tuning_options_.pledged_size()
                                : tuning_options_.size_hint()));
      return;
    case CompressionType::kBrotli:
      writer_.emplace<BrotliWriter<ChainWriter<>>>(
          std::forward_as_tuple(&compressed_),
          BrotliWriterBase::Options()
              .set_compression_level(compressor_options_.compression_level())
              .set_window_log(compressor_options_.brotli_window_log())
              .set_size_hint(tuning_options_.pledged_size() != absl::nullopt
                                 ? tuning_options_.pledged_size()
                                 : tuning_options_.size_hint()));
      return;
    case CompressionType::kZstd:
      writer_.emplace<ZstdWriter<ChainWriter<>>>(
          std::forward_as_tuple(&compressed_),
          ZstdWriterBase::Options()
              .set_compression_level(compressor_options_.compression_level())
              .set_window_log(compressor_options_.zstd_window_log())
              .set_pledged_size(tuning_options_.pledged_size())
              .set_size_hint(tuning_options_.size_hint()));
      return;
    case CompressionType::kSnappy:
      writer_.emplace<SnappyWriter<ChainWriter<>>>(
          std::forward_as_tuple(&compressed_),
          SnappyWriterBase::Options().set_size_hint(
              tuning_options_.size_hint()));
      return;
  }
  RIEGELI_ASSERT_UNREACHABLE()
      << "Unknown compression type: "
      << static_cast<unsigned>(compressor_options_.compression_type());
}

bool Compressor::EncodeAndClose(Writer& dest) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  const Position uncompressed_size = writer().pos();
  if (ABSL_PREDICT_FALSE(!writer().Close())) return Fail(writer());
  if (compressor_options_.compression_type() != CompressionType::kNone) {
    if (ABSL_PREDICT_FALSE(
            !WriteVarint64(IntCast<uint64_t>(uncompressed_size), dest))) {
      return Fail(dest);
    }
  }
  if (ABSL_PREDICT_FALSE(!dest.Write(std::move(compressed_)))) {
    return Fail(dest);
  }
  return Close();
}

}  // namespace internal
}  // namespace riegeli
