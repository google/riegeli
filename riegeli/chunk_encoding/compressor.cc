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

#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/memory.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/brotli_writer.h"
#include "riegeli/bytes/chain_writer.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/bytes/writer_utils.h"
#include "riegeli/bytes/zstd_writer.h"
#include "riegeli/chunk_encoding/types.h"

namespace riegeli {
namespace internal {

Compressor::Compressor(CompressionType compression_type, int compression_level,
                       int window_log, uint64_t size_hint)
    : Object(State::kOpen),
      compression_type_(compression_type),
      compression_level_(compression_level),
      window_log_(window_log),
      size_hint_(size_hint),
      compressed_writer_(
          &compressed_,
          ChainWriter::Options().set_size_hint(
              compression_type_ == CompressionType::kNone ? size_hint_ : 0u)) {
  switch (compression_type_) {
    case CompressionType::kNone:
      writer_ = &compressed_writer_;
      return;
    case CompressionType::kBrotli:
      new (&brotli_writer_) BrotliWriter(
          &compressed_writer_, BrotliWriter::Options()
                                   .set_compression_level(compression_level_)
                                   .set_window_log(window_log_)
                                   .set_size_hint(size_hint_));
      writer_ = &brotli_writer_;
      return;
    case CompressionType::kZstd:
      new (&zstd_writer_) ZstdWriter(
          &compressed_writer_, ZstdWriter::Options()
                                   .set_compression_level(compression_level_)
                                   .set_window_log(window_log_)
                                   .set_size_hint(size_hint_));
      writer_ = &zstd_writer_;
      return;
  }
  RIEGELI_ASSERT_UNREACHABLE() << "Unknown compression type: "
                               << static_cast<unsigned>(compression_type_);
}

Compressor::~Compressor() {
  switch (compression_type_) {
    case CompressionType::kNone:
      return;
    case CompressionType::kBrotli:
      brotli_writer_.~BrotliWriter();
      return;
    case CompressionType::kZstd:
      zstd_writer_.~ZstdWriter();
      return;
  }
  RIEGELI_ASSERT_UNREACHABLE() << "Unknown compression type: "
                               << static_cast<unsigned>(compression_type_);
}

void Compressor::Reset() {
  MarkHealthy();
  compressed_.Clear();
  compressed_writer_ = ChainWriter(
      &compressed_,
      ChainWriter::Options().set_size_hint(
          compression_type_ == CompressionType::kNone ? size_hint_ : 0u));
  switch (compression_type_) {
    case CompressionType::kNone:
      return;
    case CompressionType::kBrotli:
      brotli_writer_ = BrotliWriter(
          &compressed_writer_, BrotliWriter::Options()
                                   .set_compression_level(compression_level_)
                                   .set_window_log(window_log_)
                                   .set_size_hint(size_hint_));
      return;
    case CompressionType::kZstd:
      zstd_writer_ = ZstdWriter(&compressed_writer_,
                                ZstdWriter::Options()
                                    .set_compression_level(compression_level_)
                                    .set_window_log(window_log_)
                                    .set_size_hint(size_hint_));
      return;
  }
  RIEGELI_ASSERT_UNREACHABLE() << "Unknown compression type: "
                               << static_cast<unsigned>(compression_type_);
}

void Compressor::Done() {
  CloseCompressor();
  if (RIEGELI_UNLIKELY(!compressed_writer_.Close())) Fail(compressed_writer_);
  compressed_ = Chain();
}

inline void Compressor::CloseCompressor() {
  switch (compression_type_) {
    case CompressionType::kNone:
      return;
    case CompressionType::kBrotli:
      if (RIEGELI_UNLIKELY(!brotli_writer_.Close())) Fail(brotli_writer_);
      return;
    case CompressionType::kZstd:
      if (RIEGELI_UNLIKELY(!zstd_writer_.Close())) Fail(zstd_writer_);
      return;
  }
  RIEGELI_ASSERT_UNREACHABLE() << "Unknown compression type: "
                               << static_cast<unsigned>(compression_type_);
}

bool Compressor::EncodeAndClose(Writer* dest) {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  const Position uncompressed_size = writer_->pos();
  if (RIEGELI_UNLIKELY(!writer_->Close())) return Fail(*writer_);
  if (compression_type_ != CompressionType::kNone) {
    if (RIEGELI_UNLIKELY(!compressed_writer_.Close())) {
      return Fail(compressed_writer_);
    }
    if (RIEGELI_UNLIKELY(
            !WriteVarint64(dest, IntCast<uint64_t>(uncompressed_size)))) {
      return Fail(*dest);
    }
  }
  if (RIEGELI_UNLIKELY(!dest->Write(std::move(compressed_)))) {
    return Fail(*dest);
  }
  return Close();
}

}  // namespace internal
}  // namespace riegeli
