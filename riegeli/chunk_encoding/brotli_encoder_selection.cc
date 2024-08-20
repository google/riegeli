// Copyright 2024 Google LLC
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

#include "riegeli/chunk_encoding/brotli_encoder_selection.h"

#include <memory>

#include "absl/base/attributes.h"
#include "absl/status/status.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/recycling_pool.h"
#include "riegeli/brotli/brotli_writer.h"
#include "riegeli/bytes/chain_writer.h"
#include "riegeli/bytes/null_writer.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/chunk_encoding/compressor_options.h"

namespace riegeli {
namespace chunk_encoding_internal {

ABSL_ATTRIBUTE_WEAK std::unique_ptr<Writer> NewBrotliWriter(
    Chain* compressed, const CompressorOptions& compressor_options,
    ABSL_ATTRIBUTE_UNUSED const RecyclingPoolOptions& recycling_pool_options) {
  switch (compressor_options.brotli_encoder()) {
    case BrotliEncoder::kRBrotliOrCBrotli:
    case BrotliEncoder::kCBrotli:
      return NewCBrotliWriter(compressed, compressor_options);
    case BrotliEncoder::kRBrotli: {
      std::unique_ptr<Writer> writer = std::make_unique<riegeli::NullWriter>();
      writer->Fail(absl::UnimplementedError("Rust Brotli not available"));
      return writer;
    }
  }
  RIEGELI_ASSERT_UNREACHABLE()
      << "Unknown Brotli encoder: "
      << static_cast<int>(compressor_options.brotli_encoder());
}

std::unique_ptr<Writer> NewCBrotliWriter(
    Chain* compressed, const CompressorOptions& compressor_options) {
  return std::make_unique<BrotliWriter<ChainWriter<>>>(
      riegeli::Maker(compressed),
      BrotliWriterBase::Options()
          .set_compression_level(compressor_options.compression_level())
          .set_window_log(compressor_options.brotli_window_log()));
}

}  // namespace chunk_encoding_internal
}  // namespace riegeli
