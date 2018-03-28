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
#include <new>
#include <utility>

#include "absl/base/optimization.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/memory.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/brotli_writer.h"
#include "riegeli/bytes/chain_writer.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/bytes/writer_utils.h"
#include "riegeli/bytes/zstd_writer.h"
#include "riegeli/chunk_encoding/compressor_options.h"
#include "riegeli/chunk_encoding/types.h"

namespace riegeli {
namespace internal {

Compressor::Compressor(CompressorOptions options, uint64_t size_hint)
    : Object(State::kOpen),
      options_(std::move(options)),
      size_hint_(size_hint),
      compressed_writer_(&compressed_, GetChainWriterOptions()) {
  switch (options_.compression_type()) {
    case CompressionType::kNone:
      writer_ = &compressed_writer_;
      return;
    case CompressionType::kBrotli:
      new (&brotli_writer_)
          BrotliWriter(&compressed_writer_, GetBrotliWriterOptions());
      writer_ = &brotli_writer_;
      return;
    case CompressionType::kZstd:
      new (&zstd_writer_)
          ZstdWriter(&compressed_writer_, GetZstdWriterOptions());
      writer_ = &zstd_writer_;
      return;
  }
  RIEGELI_ASSERT_UNREACHABLE()
      << "Unknown compression type: "
      << static_cast<unsigned>(options_.compression_type());
}

Compressor::~Compressor() {
  switch (options_.compression_type()) {
    case CompressionType::kNone:
      return;
    case CompressionType::kBrotli:
      brotli_writer_.~BrotliWriter();
      return;
    case CompressionType::kZstd:
      zstd_writer_.~ZstdWriter();
      return;
  }
  RIEGELI_ASSERT_UNREACHABLE()
      << "Unknown compression type: "
      << static_cast<unsigned>(options_.compression_type());
}

void Compressor::Reset() {
  MarkHealthy();
  compressed_.Clear();
  compressed_writer_ = ChainWriter(&compressed_, GetChainWriterOptions());
  switch (options_.compression_type()) {
    case CompressionType::kNone:
      return;
    case CompressionType::kBrotli:
      brotli_writer_ =
          BrotliWriter(&compressed_writer_, GetBrotliWriterOptions());
      return;
    case CompressionType::kZstd:
      zstd_writer_ = ZstdWriter(&compressed_writer_, GetZstdWriterOptions());
      return;
  }
  RIEGELI_ASSERT_UNREACHABLE()
      << "Unknown compression type: "
      << static_cast<unsigned>(options_.compression_type());
}

inline ChainWriter::Options Compressor::GetChainWriterOptions() const {
  return ChainWriter::Options().set_size_hint(
      options_.compression_type() == CompressionType::kNone ? size_hint_ : 0u);
}

inline BrotliWriter::Options Compressor::GetBrotliWriterOptions() const {
  return BrotliWriter::Options()
      .set_compression_level(options_.compression_level())
      .set_window_log(options_.window_log())
      .set_size_hint(size_hint_);
}

inline ZstdWriter::Options Compressor::GetZstdWriterOptions() const {
  return ZstdWriter::Options()
      .set_compression_level(options_.compression_level())
      .set_window_log(options_.window_log())
      .set_size_hint(size_hint_);
}

void Compressor::Done() {
  CloseCompressor();
  if (ABSL_PREDICT_FALSE(!compressed_writer_.Close())) Fail(compressed_writer_);
  compressed_ = Chain();
}

inline void Compressor::CloseCompressor() {
  switch (options_.compression_type()) {
    case CompressionType::kNone:
      return;
    case CompressionType::kBrotli:
      if (ABSL_PREDICT_FALSE(!brotli_writer_.Close())) Fail(brotli_writer_);
      return;
    case CompressionType::kZstd:
      if (ABSL_PREDICT_FALSE(!zstd_writer_.Close())) Fail(zstd_writer_);
      return;
  }
  RIEGELI_ASSERT_UNREACHABLE()
      << "Unknown compression type: "
      << static_cast<unsigned>(options_.compression_type());
}

bool Compressor::EncodeAndClose(Writer* dest) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  const Position uncompressed_size = writer_->pos();
  if (ABSL_PREDICT_FALSE(!writer_->Close())) return Fail(*writer_);
  if (options_.compression_type() != CompressionType::kNone) {
    if (ABSL_PREDICT_FALSE(!compressed_writer_.Close())) {
      return Fail(compressed_writer_);
    }
    if (ABSL_PREDICT_FALSE(
            !WriteVarint64(dest, IntCast<uint64_t>(uncompressed_size)))) {
      return Fail(*dest);
    }
  }
  if (ABSL_PREDICT_FALSE(!dest->Write(std::move(compressed_)))) {
    return Fail(*dest);
  }
  return Close();
}

}  // namespace internal
}  // namespace riegeli
