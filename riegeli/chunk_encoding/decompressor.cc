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

#include "riegeli/chunk_encoding/decompressor.h"

#include <stdint.h>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/memory/memory.h"
#include "absl/strings/str_cat.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/brotli_reader.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/reader_utils.h"
#include "riegeli/bytes/zstd_reader.h"
#include "riegeli/chunk_encoding/chunk.h"

namespace riegeli {
namespace internal {

bool Decompressor::UncompressedSize(const Chain& compressed_data,
                                    CompressionType compression_type,
                                    uint64_t* uncompressed_size) {
  if (compression_type == CompressionType::kNone) {
    *uncompressed_size = compressed_data.size();
    return true;
  }
  ChainReader compressed_data_reader(&compressed_data);
  return ReadVarint64(&compressed_data_reader, uncompressed_size);
}

Decompressor::Decompressor(std::unique_ptr<Reader> src,
                           CompressionType compression_type)
    : Decompressor(src.get(), compression_type) {
  owned_src_ = std::move(src);
}

Decompressor::Decompressor(Reader* src, CompressionType compression_type)
    : Object(State::kOpen) {
  RIEGELI_ASSERT_NOTNULL(src);
  if (compression_type == CompressionType::kNone) {
    reader_ = src;
    return;
  }
  uint64_t decompressed_size;
  if (ABSL_PREDICT_FALSE(!ReadVarint64(src, &decompressed_size))) {
    Fail("Reading decompressed size failed");
    return;
  }
  switch (compression_type) {
    case CompressionType::kNone:
      RIEGELI_ASSERT_UNREACHABLE() << "kNone handled above";
    case CompressionType::kBrotli:
      owned_reader_ = absl::make_unique<BrotliReader>(src);
      reader_ = owned_reader_.get();
      return;
    case CompressionType::kZstd:
      owned_reader_ = absl::make_unique<ZstdReader>(src);
      reader_ = owned_reader_.get();
      return;
  }
  Fail(absl::StrCat("Unknown compression type: ",
                    static_cast<unsigned>(compression_type)));
}

void Decompressor::Done() {
  if (owned_reader_ != nullptr) {
    if (ABSL_PREDICT_TRUE(healthy())) {
      if (ABSL_PREDICT_FALSE(!owned_reader_->Close())) Fail(*owned_reader_);
    }
    owned_reader_.reset();
  }
  if (owned_src_ != nullptr) {
    if (ABSL_PREDICT_TRUE(healthy())) {
      if (ABSL_PREDICT_FALSE(!owned_src_->Close())) Fail(*owned_src_);
    }
    owned_src_.reset();
  }
  reader_ = nullptr;
}

bool Decompressor::VerifyEndAndClose() {
  if (owned_reader_ != nullptr) {
    if (ABSL_PREDICT_FALSE(!owned_reader_->VerifyEndAndClose())) {
      return Fail(*owned_reader_);
    }
  }
  if (owned_src_ != nullptr) {
    if (ABSL_PREDICT_FALSE(!owned_src_->VerifyEndAndClose())) {
      return Fail(*owned_src_);
    }
  }
  return Close();
}

}  // namespace internal
}  // namespace riegeli
