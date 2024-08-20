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

#ifndef RIEGELI_CHUNK_ENCODING_BROTLI_ENCODER_SELECTION_H_
#define RIEGELI_CHUNK_ENCODING_BROTLI_ENCODER_SELECTION_H_

#include <memory>

#include "riegeli/base/chain.h"
#include "riegeli/base/recycling_pool.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/chunk_encoding/compressor_options.h"

namespace riegeli {
namespace chunk_encoding_internal {

// Creates a `Writer` which compresses data with Brotli and writes them to
// `compressed`.
//
// The encoder implementation is determined by
// `compressor_options.brotli_encoder()`.
//
// This is a weak function. Its default definition supports only C Brotli.
// It can be overridden to support also Rust Brotli.
std::unique_ptr<Writer> NewBrotliWriter(
    Chain* compressed, const CompressorOptions& compressor_options,
    const RecyclingPoolOptions& recycling_pool_options);

// Support for `NewBrotliWriter()`: uses C Brotli, ignores
// `compressor_options.brotli_encoder()`.
std::unique_ptr<Writer> NewCBrotliWriter(
    Chain* compressed, const CompressorOptions& compressor_options);

}  // namespace chunk_encoding_internal
}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_BROTLI_ENCODER_SELECTION_H_
