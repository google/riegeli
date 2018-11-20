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

#include <string>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/options_parser.h"
#include "riegeli/bytes/brotli_writer.h"
#include "riegeli/bytes/zstd_writer.h"
#include "riegeli/chunk_encoding/constants.h"

namespace riegeli {

bool CompressorOptions::FromString(absl::string_view text,
                                   std::string* error_message) {
  // Set just compression_type_ first because other parsers depend on
  // compression_type_.
  {
    OptionsParser options_parser;
    options_parser.AddOption(
        "uncompressed",
        ValueParser::And(ValueParser::FailIfSeen("brotli", "zstd"),
                         [this](ValueParser* value_parser) {
                           compression_type_ = CompressionType::kNone;
                           return true;
                         }));
    options_parser.AddOption(
        "brotli",
        ValueParser::And(ValueParser::FailIfSeen("uncompressed", "zstd"),
                         [this](ValueParser* value_parser) {
                           compression_type_ = CompressionType::kBrotli;
                           return true;
                         }));
    options_parser.AddOption(
        "zstd",
        ValueParser::And(ValueParser::FailIfSeen("uncompressed", "brotli"),
                         [this](ValueParser* value_parser) {
                           compression_type_ = CompressionType::kZstd;
                           return true;
                         }));
    options_parser.AddOption("window_log",
                             [](ValueParser* value_parser) { return true; });
    if (ABSL_PREDICT_FALSE(!options_parser.FromString(text))) {
      if (error_message != nullptr) {
        *error_message = std::string(options_parser.message());
      }
      return false;
    }
  }
  OptionsParser options_parser;
  options_parser.AddOption(
      "uncompressed",
      ValueParser::And(ValueParser::FailIfSeen("window_log"),
                       ValueParser::Empty(&compression_level_, 0)));
  options_parser.AddOption(
      "brotli",
      ValueParser::Or(
          ValueParser::Empty(
              &compression_level_,
              BrotliWriterBase::Options::kDefaultCompressionLevel()),
          ValueParser::Int(&compression_level_,
                           BrotliWriterBase::Options::kMinCompressionLevel(),
                           BrotliWriterBase::Options::kMaxCompressionLevel())));
  options_parser.AddOption(
      "zstd",
      ValueParser::Or(
          ValueParser::Empty(
              &compression_level_,
              ZstdWriterBase::Options::kDefaultCompressionLevel()),
          ValueParser::Int(&compression_level_,
                           ZstdWriterBase::Options::kMinCompressionLevel(),
                           ZstdWriterBase::Options::kMaxCompressionLevel())));
  options_parser.AddOption("window_log", [&] {
    switch (compression_type_) {
      case CompressionType::kNone:
        return ValueParser::FailIfSeen("uncompressed");
      case CompressionType::kBrotli:
        return ValueParser::Or(
            ValueParser::Enum(&window_log_, {{"auto", kDefaultWindowLog()}}),
            ValueParser::Int(&window_log_,
                             BrotliWriterBase::Options::kMinWindowLog(),
                             BrotliWriterBase::Options::kMaxWindowLog()));
      case CompressionType::kZstd:
        return ValueParser::Or(
            ValueParser::Enum(&window_log_, {{"auto", kDefaultWindowLog()}}),
            ValueParser::Int(&window_log_,
                             ZstdWriterBase::Options::kMinWindowLog(),
                             ZstdWriterBase::Options::kMaxWindowLog()));
    }
    RIEGELI_ASSERT_UNREACHABLE() << "Unknown compression type: "
                                 << static_cast<unsigned>(compression_type_);
  }());
  if (ABSL_PREDICT_FALSE(!options_parser.FromString(text))) {
    if (error_message != nullptr) {
      *error_message = std::string(options_parser.message());
    }
    return false;
  }
  return true;
}

int CompressorOptions::window_log() const {
  switch (compression_type_) {
    case CompressionType::kNone:
      RIEGELI_ASSERT_UNREACHABLE()
          << "Failed precondition of CompressorOptions::window_log(): "
             "uncompressed";
    case CompressionType::kBrotli:
      if (window_log_ == kDefaultWindowLog()) {
        return BrotliWriterBase::Options::kDefaultWindowLog();
      } else {
        RIEGELI_ASSERT_GE(window_log_,
                          BrotliWriterBase::Options::kMinWindowLog())
            << "Failed precondition of CompressorOptions::set_window_log(): "
               "window log out of range for brotli";
        RIEGELI_ASSERT_LE(window_log_,
                          BrotliWriterBase::Options::kMaxWindowLog())
            << "Failed precondition of CompressorOptions::set_window_log(): "
               "window log out of range for brotli";
        return window_log_;
      }
    case CompressionType::kZstd:
      if (window_log_ == kDefaultWindowLog()) {
        return ZstdWriterBase::Options::kDefaultWindowLog();
      } else {
        RIEGELI_ASSERT_GE(window_log_, ZstdWriterBase::Options::kMinWindowLog())
            << "Failed precondition of CompressorOptions::set_window_log(): "
               "window log out of range for zstd";
        RIEGELI_ASSERT_LE(window_log_, ZstdWriterBase::Options::kMaxWindowLog())
            << "Failed precondition of CompressorOptions::set_window_log(): "
               "window log out of range for zstd";
        return window_log_;
      }
  }
  RIEGELI_ASSERT_UNREACHABLE() << "Unknown compression type: "
                               << static_cast<unsigned>(compression_type_);
}

}  // namespace riegeli
