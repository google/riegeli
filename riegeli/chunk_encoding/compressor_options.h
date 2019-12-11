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

#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/brotli_writer.h"
#include "riegeli/bytes/zstd_writer.h"
#include "riegeli/chunk_encoding/constants.h"

namespace riegeli {

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
  //     "snappy" |
  //     "window_log" ":" window_log
  //   brotli_level ::= integer 0..11 (default 9)
  //   zstd_level ::= integer -131072..22 (default 9)
  //   window_log ::= "auto" or integer 10..31
  // ```
  //
  // Returns status:
  //  * `status.ok()`  - success
  //  * `!status.ok()` - failure
  Status FromString(absl::string_view text);

  // Changes compression algorithm to none.
  CompressorOptions& set_uncompressed() & {
    compression_type_ = CompressionType::kNone;
    compression_level_ = 0;
    return *this;
  }
  CompressorOptions&& set_uncompressed() && {
    return std::move(set_uncompressed());
  }

  // Changes compression algorithm to Brotli. Sets compression level which
  // tunes the tradeoff between compression density and compression speed
  // (higher = better density but slower).
  //
  // `compression_level` must be between `kMinBrotli` (0) and `kMaxBrotli` (11).
  // Default: `kDefaultBrotli` (9).
  //
  // This is the default compression algorithm.
  static constexpr int kMinBrotli =
      BrotliWriterBase::Options::kMinCompressionLevel;
  static constexpr int kMaxBrotli =
      BrotliWriterBase::Options::kMaxCompressionLevel;
  static constexpr int kDefaultBrotli =
      BrotliWriterBase::Options::kDefaultCompressionLevel;
  CompressorOptions& set_brotli(int compression_level = kDefaultBrotli) & {
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
  CompressorOptions&& set_brotli(int compression_level = kDefaultBrotli) && {
    return std::move(set_brotli(compression_level));
  }

  // Changes compression algorithm to Zstd. Sets compression level which tunes
  // the tradeoff between compression density and compression speed (higher =
  // better density but slower).
  //
  // `compression_level` must be between `kMinZstd` (-131072) and
  // `kMaxZstd` (22). Level 0 is currently equivalent to 3.
  // Default: `kDefaultZstd` (9).
  static constexpr int kMinZstd = ZstdWriterBase::Options::kMinCompressionLevel;
  static constexpr int kMaxZstd = ZstdWriterBase::Options::kMaxCompressionLevel;
  static constexpr int kDefaultZstd =
      ZstdWriterBase::Options::kDefaultCompressionLevel;
  CompressorOptions& set_zstd(int compression_level = kDefaultZstd) & {
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
  CompressorOptions&& set_zstd(int compression_level = kDefaultZstd) && {
    return std::move(set_zstd(compression_level));
  }

  // Changes compression algorithm to Snappy.
  //
  // There are no Snappy compression levels to tune.
  CompressorOptions& set_snappy() & {
    compression_type_ = CompressionType::kSnappy;
    compression_level_ = 0;
    return *this;
  }
  CompressorOptions&& set_snappy() && { return std::move(set_snappy()); }

  CompressionType compression_type() const { return compression_type_; }

  int compression_level() const { return compression_level_; }

  // Logarithm of the LZ77 sliding window size. This tunes the tradeoff
  // between compression density and memory usage (higher = better density but
  // more memory).
  //
  // Special value `kDefaultWindowLog` (-1) means to keep the default
  // (brotli: 22, zstd: derived from compression level and chunk size).
  //
  // For uncompressed and snappy, `window_log` must be `kDefaultWindowLog` (-1).
  //
  // For brotli, `window_log` must be `kDefaultWindowLog` (-1) or between
  // `BrotliWriterBase::Options::kMinWindowLog` (10) and
  // `BrotliWriterBase::Options::kMaxWindowLog` (30).
  //
  // For zstd, `window_log` must be `kDefaultWindowLog` (-1) or between
  // `ZstdWriterBase::Options::kMinWindowLog` (10) and
  // `ZstdWriterBase::Options::kMaxWindowLog` (30 in 32-bit build,
  // 31 in 64-bit build).
  //
  // Default: `kDefaultWindowLog` (-1).
  static constexpr int kMinWindowLog =
      SignedMin(BrotliWriterBase::Options::kMinWindowLog,
                ZstdWriterBase::Options::kMinWindowLog);
  static constexpr int kMaxWindowLog =
      SignedMax(BrotliWriterBase::Options::kMaxWindowLog,
                ZstdWriterBase::Options::kMaxWindowLog);
  static constexpr int kDefaultWindowLog = -1;
  CompressorOptions& set_window_log(int window_log) & {
    if (window_log != kDefaultWindowLog) {
      RIEGELI_ASSERT_GE(window_log, kMinWindowLog)
          << "Failed precondition of CompressorOptions::set_window_log(): "
             "window log out of range";
      RIEGELI_ASSERT_LE(window_log, kMaxWindowLog)
          << "Failed precondition of CompressorOptions::set_window_log(): "
             "window log out of range";
    }
    window_log_ = window_log;
    return *this;
  }
  CompressorOptions&& set_window_log(int window_log) && {
    return std::move(set_window_log(window_log));
  }

  // Returns `window_log` translated for `BrotliWriter` or `ZstdWriter`.
  //
  // Precondition: `compression_type() != CompressionType::kNone`
  int window_log() const;

 private:
  CompressionType compression_type_ = CompressionType::kBrotli;
  int compression_level_ = kDefaultBrotli;
  int window_log_ = kDefaultWindowLog;
};

}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_COMPRESSOR_OPTIONS_H_
