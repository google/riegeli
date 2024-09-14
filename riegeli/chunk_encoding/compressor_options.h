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

#ifndef RIEGELI_CHUNK_ENCODING_COMPRESSOR_OPTIONS_H_
#define RIEGELI_CHUNK_ENCODING_COMPRESSOR_OPTIONS_H_

#include <utility>

#include "absl/base/attributes.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/brotli/brotli_writer.h"
#include "riegeli/chunk_encoding/constants.h"
#include "riegeli/snappy/snappy_writer.h"
#include "riegeli/zstd/zstd_writer.h"

namespace riegeli {

// The implementation of the Brotli encoder to use. Experimental, meant for
// evaluation. Prefer to keep the default.
//
// Rust Brotli is currently not available in open sourced Riegeli.
enum class BrotliEncoder {
  kRBrotliOrCBrotli,  // Rust Brotli if available, C Brotli otherwise. Default.
  kCBrotli,           // C Brotli.
  kRBrotli,           // Rust Brotli if available, fail otherwise.
};

class CompressorOptions {
 public:
  CompressorOptions() noexcept {}

  // Parses options from text:
  // ```
  //   options ::= option? ("," option?)*
  //   option ::=
  //     "uncompressed" |
  //     "brotli" (":" brotli_level)? |
  //     "zstd" (":" zstd_level)? |
  //     "snappy" (":" snappy_level)? |
  //     "window_log" ":" window_log |
  //     "brotli_encoder" ":" ("rbrotli_or_cbrotli" | "cbrotli" | "rbrotli")
  //   brotli_level ::= integer in the range [0..11] (default 6)
  //   zstd_level ::= integer in the range [-131072..22] (default 3)
  //   snappy_level ::= integer in the range [1..2] (default 1)
  //   window_log ::= "auto" or integer in the range [10..31]
  // ```
  //
  // Returns status:
  //  * `status.ok()`  - success
  //  * `!status.ok()` - failure
  absl::Status FromString(absl::string_view text);

  // Changes compression algorithm to Uncompressed (turns compression off).
  CompressorOptions& set_uncompressed() & ABSL_ATTRIBUTE_LIFETIME_BOUND {
    compression_type_ = CompressionType::kNone;
    compression_level_ = 0;
    return *this;
  }
  CompressorOptions&& set_uncompressed() && ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return std::move(set_uncompressed());
  }

  // Changes compression algorithm to Brotli. Sets compression level which
  // tunes the tradeoff between compression density and compression speed
  // (higher = better density but slower).
  //
  // `compression_level` must be between `kMinBrotli` (0) and `kMaxBrotli` (11).
  // Default: `kDefaultBrotli` (6).
  //
  // This is the default compression algorithm.
  static constexpr int kMinBrotli =
      BrotliWriterBase::Options::kMinCompressionLevel;
  static constexpr int kMaxBrotli =
      BrotliWriterBase::Options::kMaxCompressionLevel;
  static constexpr int kDefaultBrotli =
      BrotliWriterBase::Options::kDefaultCompressionLevel;
  CompressorOptions& set_brotli(int compression_level = kDefaultBrotli) &
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT_GE(compression_level, kMinBrotli)
        << "Failed precondition of CompressorOptions::set_brotli(): "
           "compression level out of range";
    RIEGELI_ASSERT_LE(compression_level, kMaxBrotli)
        << "Failed precondition of CompressorOptions::set_brotli(): "
           "compression level out of range";
    compression_type_ = CompressionType::kBrotli;
    compression_level_ = compression_level;
    return *this;
  }
  CompressorOptions&& set_brotli(int compression_level = kDefaultBrotli) &&
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return std::move(set_brotli(compression_level));
  }

  // Changes compression algorithm to Zstd. Sets compression level which tunes
  // the tradeoff between compression density and compression speed (higher =
  // better density but slower).
  //
  // `compression_level` must be between `kMinZstd` (-131072) and
  // `kMaxZstd` (22). Level 0 is currently equivalent to 3.
  // Default: `kDefaultZstd` (3).
  static constexpr int kMinZstd = ZstdWriterBase::Options::kMinCompressionLevel;
  static constexpr int kMaxZstd = ZstdWriterBase::Options::kMaxCompressionLevel;
  static constexpr int kDefaultZstd =
      ZstdWriterBase::Options::kDefaultCompressionLevel;
  CompressorOptions& set_zstd(int compression_level = kDefaultZstd) &
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT_GE(compression_level, kMinZstd)
        << "Failed precondition of CompressorOptions::set_zstd(): "
           "compression level out of range";
    RIEGELI_ASSERT_LE(compression_level, kMaxZstd)
        << "Failed precondition of CompressorOptions::set_zstd(): "
           "compression level out of range";
    compression_type_ = CompressionType::kZstd;
    compression_level_ = compression_level;
    return *this;
  }
  CompressorOptions&& set_zstd(int compression_level = kDefaultZstd) &&
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return std::move(set_zstd(compression_level));
  }

  // Changes compression algorithm to Snappy.  Sets compression level which
  // tunes the tradeoff between compression density and compression speed
  // (higher = better density but slower).
  static constexpr int kMinSnappy =
      SnappyWriterBase::Options::kMinCompressionLevel;
  static constexpr int kMaxSnappy =
      SnappyWriterBase::Options::kMaxCompressionLevel;
  static constexpr int kDefaultSnappy =
      SnappyWriterBase::Options::kDefaultCompressionLevel;
  CompressorOptions& set_snappy(int compression_level = kDefaultSnappy) &
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    compression_type_ = CompressionType::kSnappy;
    compression_level_ = compression_level;
    return *this;
  }
  CompressorOptions&& set_snappy(int compression_level = kDefaultSnappy) &&
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return std::move(set_snappy(compression_level));
  }

  CompressionType compression_type() const { return compression_type_; }

  int compression_level() const { return compression_level_; }

  // Logarithm of the LZ77 sliding window size. This tunes the tradeoff
  // between compression density and memory usage (higher = better density but
  // more memory).
  //
  // Special value `absl::nullopt` means to keep the default (Brotli: 22,
  // Zstd: derived from compression level and chunk size).
  //
  // For Uncompressed and Snappy, `window_log` must be `absl::nullopt`.
  //
  // For Brotli, `window_log` must be `absl::nullopt` or between
  // `BrotliWriterBase::Options::kMinWindowLog` (10) and
  // `BrotliWriterBase::Options::kMaxWindowLog` (30).
  //
  // For Zstd, `window_log` must be `absl::nullopt` or between
  // `ZstdWriterBase::Options::kMinWindowLog` (10) and
  // `ZstdWriterBase::Options::kMaxWindowLog` (30 in 32-bit build,
  // 31 in 64-bit build).
  //
  // Default: `absl::nullopt`.
  static constexpr int kMinWindowLog =
      SignedMin(BrotliWriterBase::Options::kMinWindowLog,
                ZstdWriterBase::Options::kMinWindowLog);
  static constexpr int kMaxWindowLog =
      SignedMax(BrotliWriterBase::Options::kMaxWindowLog,
                ZstdWriterBase::Options::kMaxWindowLog);
  CompressorOptions& set_window_log(absl::optional<int> window_log) &
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    if (window_log != absl::nullopt) {
      RIEGELI_ASSERT_GE(*window_log, kMinWindowLog)
          << "Failed precondition of CompressorOptions::set_window_log(): "
             "window log out of range";
      RIEGELI_ASSERT_LE(*window_log, kMaxWindowLog)
          << "Failed precondition of CompressorOptions::set_window_log(): "
             "window log out of range";
    }
    window_log_ = window_log;
    return *this;
  }
  CompressorOptions&& set_window_log(absl::optional<int> window_log) &&
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return std::move(set_window_log(window_log));
  }
  absl::optional<int> window_log() const { return window_log_; }

  // Returns `window_log()` translated for `BrotliWriter`.
  //
  // Precondition: `compression_type() == CompressionType::kBrotli`
  int brotli_window_log() const;

  // Returns `window_log()` translated for `ZstdWriter`.
  //
  // Precondition: `compression_type() == CompressionType::kZstd`
  absl::optional<int> zstd_window_log() const;

  // The implementation of the Brotli encoder to use. Experimental, meant for
  // evaluation. Prefer to keep the default.
  //
  // This is ignored if `compression_type() != CompressionType::kBrotli`.
  //
  // If Rust Brotli is used, the interpretation of compression levels is
  // slightly different (in particular compression levels smaller than 3 are
  // equivalent to 3, and compression levels larger than 7 are equivalent to 7),
  // and `window_log()` is ignored.
  //
  // Default: `BrotliEncoder::kRBrotliOrCBrotli`.
  CompressorOptions& set_brotli_encoder(BrotliEncoder brotli_encoder) &
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    brotli_encoder_ = brotli_encoder;
    return *this;
  }
  CompressorOptions&& set_brotli_encoder(BrotliEncoder brotli_encoder) &&
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return std::move(set_brotli_encoder(brotli_encoder));
  }
  BrotliEncoder brotli_encoder() const { return brotli_encoder_; }

 private:
  CompressionType compression_type_ = CompressionType::kBrotli;
  int compression_level_ = kDefaultBrotli;
  absl::optional<int> window_log_;
  BrotliEncoder brotli_encoder_ = BrotliEncoder::kRBrotliOrCBrotli;
};

}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_COMPRESSOR_OPTIONS_H_
