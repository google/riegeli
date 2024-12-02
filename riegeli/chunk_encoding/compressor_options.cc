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

#include "riegeli/chunk_encoding/compressor_options.h"

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/options_parser.h"
#include "riegeli/brotli/brotli_writer.h"
#include "riegeli/chunk_encoding/constants.h"
#include "riegeli/snappy/snappy_writer.h"
#include "riegeli/zstd/zstd_writer.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if !__cpp_inline_variables
constexpr int CompressorOptions::kMinBrotli;
constexpr int CompressorOptions::kMaxBrotli;
constexpr int CompressorOptions::kDefaultBrotli;
constexpr int CompressorOptions::kMinZstd;
constexpr int CompressorOptions::kMaxZstd;
constexpr int CompressorOptions::kDefaultZstd;
constexpr int CompressorOptions::kMinSnappy;
constexpr int CompressorOptions::kMaxSnappy;
constexpr int CompressorOptions::kDefaultSnappy;
constexpr int CompressorOptions::kMinWindowLog;
constexpr int CompressorOptions::kMaxWindowLog;
#endif

absl::Status CompressorOptions::FromString(absl::string_view text) {
  // Set just `compression_type_` first because other parsers depend on
  // `compression_type_`.
  {
    OptionsParser options_parser;
    options_parser.AddOption(
        "uncompressed",
        ValueParser::And(ValueParser::FailIfSeen("brotli", "zstd", "snappy"),
                         [this](ValueParser& value_parser) {
                           compression_type_ = CompressionType::kNone;
                           return true;
                         }));
    options_parser.AddOption(
        "brotli", ValueParser::And(
                      ValueParser::FailIfSeen("uncompressed", "zstd", "snappy"),
                      [this](ValueParser& value_parser) {
                        compression_type_ = CompressionType::kBrotli;
                        return true;
                      }));
    options_parser.AddOption(
        "zstd", ValueParser::And(
                    ValueParser::FailIfSeen("uncompressed", "brotli", "snappy"),
                    [this](ValueParser& value_parser) {
                      compression_type_ = CompressionType::kZstd;
                      return true;
                    }));
    options_parser.AddOption(
        "snappy", ValueParser::And(
                      ValueParser::FailIfSeen("uncompressed", "brotli", "zstd"),
                      [this](ValueParser& value_parser) {
                        compression_type_ = CompressionType::kSnappy;
                        return true;
                      }));
    options_parser.AddOption("window_log",
                             [](ValueParser& value_parser) { return true; });
    options_parser.AddOption("brotli_encoder",
                             [](ValueParser& value_parser) { return true; });
    if (ABSL_PREDICT_FALSE(!options_parser.FromString(text))) {
      return options_parser.status();
    }
  }
  int window_log;
  OptionsParser options_parser;
  options_parser.AddOption(
      "uncompressed",
      ValueParser::And(ValueParser::FailIfSeen("window_log"),
                       ValueParser::Empty(0, &compression_level_)));
  options_parser.AddOption(
      "brotli",
      ValueParser::Or(
          ValueParser::Empty(
              BrotliWriterBase::Options::kDefaultCompressionLevel,
              &compression_level_),
          ValueParser::Int(BrotliWriterBase::Options::kMinCompressionLevel,
                           BrotliWriterBase::Options::kMaxCompressionLevel,
                           &compression_level_)));
  options_parser.AddOption(
      "zstd",
      ValueParser::Or(
          ValueParser::Empty(ZstdWriterBase::Options::kDefaultCompressionLevel,
                             &compression_level_),
          ValueParser::Int(ZstdWriterBase::Options::kMinCompressionLevel,
                           ZstdWriterBase::Options::kMaxCompressionLevel,
                           &compression_level_)));
  options_parser.AddOption(
      "snappy",
      ValueParser::And(
          ValueParser::FailIfSeen("window_log"),
          ValueParser::Or(
              ValueParser::Empty(
                  SnappyWriterBase::Options::kDefaultCompressionLevel,
                  &compression_level_),
              ValueParser::Int(SnappyWriterBase::Options::kMinCompressionLevel,
                               SnappyWriterBase::Options::kMaxCompressionLevel,
                               &compression_level_))));
  options_parser.AddOption("window_log", [&] {
    switch (compression_type_) {
      case CompressionType::kNone:
        return ValueParser::FailIfSeen("uncompressed");
      case CompressionType::kBrotli:
        return ValueParser::Or(
            ValueParser::Enum({{"auto", absl::nullopt}}, &window_log_),
            ValueParser::And(
                ValueParser::Int(BrotliWriterBase::Options::kMinWindowLog,
                                 BrotliWriterBase::Options::kMaxWindowLog,
                                 &window_log),
                [this, &window_log](ValueParser& value_parser) {
                  window_log_ = window_log;
                  return true;
                }));
      case CompressionType::kZstd:
        return ValueParser::Or(
            ValueParser::Enum({{"auto", absl::nullopt}}, &window_log_),
            ValueParser::And(
                ValueParser::Int(ZstdWriterBase::Options::kMinWindowLog,
                                 ZstdWriterBase::Options::kMaxWindowLog,
                                 &window_log),
                [this, &window_log](ValueParser& value_parser) {
                  window_log_ = window_log;
                  return true;
                }));
      case CompressionType::kSnappy:
        return ValueParser::FailIfSeen("snappy");
    }
    RIEGELI_ASSUME_UNREACHABLE() << "Unknown compression type: "
                                 << static_cast<unsigned>(compression_type_);
  }());
  options_parser.AddOption(
      "brotli_encoder",
      ValueParser::Enum(
          {{"rbrotli_or_cbrotli", BrotliEncoder::kRBrotliOrCBrotli},
           {"cbrotli", BrotliEncoder::kCBrotli},
           {"rbrotli", BrotliEncoder::kRBrotli}},
          &brotli_encoder_));
  if (ABSL_PREDICT_FALSE(!options_parser.FromString(text))) {
    return options_parser.status();
  }
  return absl::OkStatus();
}

int CompressorOptions::brotli_window_log() const {
  RIEGELI_ASSERT_EQ(compression_type_, CompressionType::kBrotli)
      << "Failed precondition of CompressorOptions::brotli_window_log(): "
         "compression type must be Brotli";
  if (window_log_ == absl::nullopt) {
    return BrotliWriterBase::Options::kDefaultWindowLog;
  } else {
    RIEGELI_ASSERT_GE(*window_log_, BrotliWriterBase::Options::kMinWindowLog)
        << "Failed precondition of CompressorOptions::set_window_log(): "
           "window log out of range for Brotli";
    RIEGELI_ASSERT_LE(*window_log_, BrotliWriterBase::Options::kMaxWindowLog)
        << "Failed precondition of CompressorOptions::set_window_log(): "
           "window log out of range for Brotli";
    return *window_log_;
  }
}

absl::optional<int> CompressorOptions::zstd_window_log() const {
  RIEGELI_ASSERT_EQ(compression_type_, CompressionType::kZstd)
      << "Failed precondition of CompressorOptions::zstd_window_log(): "
         "compression type must be Zstd";
  if (window_log_ != absl::nullopt) {
    RIEGELI_ASSERT_GE(*window_log_, ZstdWriterBase::Options::kMinWindowLog)
        << "Failed precondition of CompressorOptions::set_window_log(): "
           "window log out of range for Zstd";
    RIEGELI_ASSERT_LE(*window_log_, ZstdWriterBase::Options::kMaxWindowLog)
        << "Failed precondition of CompressorOptions::set_window_log(): "
           "window log out of range for Zstd";
  }
  return window_log_;
}

}  // namespace riegeli
