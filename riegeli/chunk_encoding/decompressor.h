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

#ifndef RIEGELI_CHUNK_ENCODING_DECOMPRESSOR_H_
#define RIEGELI_CHUNK_ENCODING_DECOMPRESSOR_H_

#include <stdint.h>

#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/types/optional.h"
#include "riegeli/base/any_dependency.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/object.h"
#include "riegeli/base/recycling_pool.h"
#include "riegeli/brotli/brotli_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/chunk_encoding/constants.h"
#include "riegeli/snappy/snappy_reader.h"
#include "riegeli/varint/varint_reading.h"
#include "riegeli/zstd/zstd_reader.h"

namespace riegeli {
namespace chunk_encoding_internal {

// Returns uncompressed size of `compressed_data`.
//
// If `compression_type` is `kNone`, uncompressed size is the same as compressed
// size, otherwise reads uncompressed size as a varint from the beginning of
// compressed_data.
//
// Returns `absl::nullopt` on failure.
absl::optional<uint64_t> UncompressedSize(const Chain& compressed_data,
                                          CompressionType compression_type);

// Options for a `Decompressor`.
class DecompressorOptions {
 public:
  DecompressorOptions() noexcept {}

  // Options for a global `RecyclingPool` of decompression contexts.
  //
  // They tune the amount of memory which is kept to speed up creation of new
  // decompression sessions, and usage of a background thread to clean it.
  //
  // Default: `RecyclingPoolOptions()`.
  DecompressorOptions& set_recycling_pool_options(
      const RecyclingPoolOptions& recycling_pool_options) & {
    recycling_pool_options_ = recycling_pool_options;
    return *this;
  }
  DecompressorOptions&& set_recycling_pool_options(
      const RecyclingPoolOptions& recycling_pool_options) && {
    return std::move(set_recycling_pool_options(recycling_pool_options));
  }
  const RecyclingPoolOptions& recycling_pool_options() const {
    return recycling_pool_options_;
  }

 private:
  RecyclingPoolOptions recycling_pool_options_;
};

// Decompresses a compressed stream.
//
// If `compression_type` is not `kNone`, reads uncompressed size as a varint
// from the beginning of compressed data.
template <typename Src = Reader*>
class Decompressor : public Object {
 public:
  using Options = DecompressorOptions;

  // Creates a closed `Decompressor`.
  explicit Decompressor(Closed) noexcept : Object(kClosed) {}

  // Will read from the compressed stream provided by `src`.
  explicit Decompressor(Initializer<Src> src, CompressionType compression_type,
                        DecompressorOptions options = DecompressorOptions());

  Decompressor(Decompressor&& that) = default;
  Decompressor& operator=(Decompressor&& that) = default;

  // Makes `*this` equivalent to a newly constructed `Decompressor`. This avoids
  // constructing a temporary `Decompressor` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      Initializer<Src> src, CompressionType compression_type,
      DecompressorOptions options = DecompressorOptions());

  // Returns the `Reader` from which uncompressed data should be read.
  //
  // Precondition: `ok()`
  Reader& reader();

  // Verifies that the source ends at the current position (i.e. has no more
  // compressed data and has no data after the compressed stream), failing the
  // `Decompressor` if not. Closes the `Decompressor`.
  //
  // Return values:
  //  * `true`  - success (the source ends at the former current position)
  //  * `false` - failure (the source does not end at the former current
  //                       position or the `Decompressor` was not OK before
  //                       closing)
  bool VerifyEndAndClose();

  // Verifies that the source ends at the current position (i.e. has no more
  // compressed data and has no data after the compressed stream), failing the
  // `Decompressor` if not.
  void VerifyEnd();

 protected:
  void Done() override;

 private:
  void Initialize(Initializer<Src> src, CompressionType compression_type,
                  const RecyclingPoolOptions& recycling_pool_options);

  AnyDependency<Reader*>::Inlining<Src, BrotliReader<Src>, ZstdReader<Src>,
                                   SnappyReader<Src>>
      decompressed_;
};

// Implementation details follow.

template <typename Src>
inline Decompressor<Src>::Decompressor(Initializer<Src> src,
                                       CompressionType compression_type,
                                       DecompressorOptions options) {
  Initialize(std::move(src), compression_type,
             options.recycling_pool_options());
}

template <typename Src>
inline void Decompressor<Src>::Reset(Closed) {
  Object::Reset(kClosed);
  decompressed_.Reset();
}

template <typename Src>
inline void Decompressor<Src>::Reset(Initializer<Src> src,
                                     CompressionType compression_type,
                                     DecompressorOptions options) {
  Object::Reset();
  Initialize(std::move(src), compression_type,
             options.recycling_pool_options());
}

template <typename Src>
inline void Decompressor<Src>::Initialize(
    Initializer<Src> src, CompressionType compression_type,
    const RecyclingPoolOptions& recycling_pool_options) {
  if (compression_type == CompressionType::kNone) {
    decompressed_ = std::move(src);
    return;
  }
  Dependency<Reader*, Src> compressed_reader(std::move(src));
  uint64_t uncompressed_size;
  if (ABSL_PREDICT_FALSE(
          !ReadVarint64(*compressed_reader, uncompressed_size))) {
    Fail(compressed_reader->StatusOrAnnotate(
        absl::InvalidArgumentError("Reading uncompressed size failed")));
    return;
  }
  switch (compression_type) {
    case CompressionType::kNone:
      RIEGELI_ASSERT_UNREACHABLE() << "kNone handled above";
    case CompressionType::kBrotli:
      decompressed_ = riegeli::Maker<BrotliReader<Src>>(
          std::move(compressed_reader.manager()));
      return;
    case CompressionType::kZstd:
      decompressed_ = riegeli::Maker<ZstdReader<Src>>(
          std::move(compressed_reader.manager()),
          ZstdReaderBase::Options().set_recycling_pool_options(
              recycling_pool_options));
      return;
    case CompressionType::kSnappy:
      decompressed_ = riegeli::Maker<SnappyReader<Src>>(
          std::move(compressed_reader.manager()));
      return;
  }
  Fail(absl::UnimplementedError(absl::StrCat(
      "Unknown compression type: ", static_cast<unsigned>(compression_type))));
}

template <typename Src>
inline Reader& Decompressor<Src>::reader() {
  RIEGELI_ASSERT(ok()) << "Failed precondition of Decompressor::reader(): "
                       << status();
  return *decompressed_;
}

template <typename Src>
void Decompressor<Src>::Done() {
  if (ABSL_PREDICT_FALSE(!decompressed_->Close())) {
    Fail(decompressed_->status());
  }
}

template <typename Src>
inline bool Decompressor<Src>::VerifyEndAndClose() {
  VerifyEnd();
  return Close();
}

template <typename Src>
inline void Decompressor<Src>::VerifyEnd() {
  if (ABSL_PREDICT_TRUE(ok())) decompressed_->VerifyEnd();
}

}  // namespace chunk_encoding_internal
}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_DECOMPRESSOR_H_
