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

namespace riegeli {

bool CompressorOptions::Parse(absl::string_view text, std::string* message) {
  // Set just compression_type_ first because other parsers depend on
  // compression_type_.
  {
    OptionsParser parser;
    parser.AddOption("uncompressed", [this, &parser](absl::string_view value) {
      if (ABSL_PREDICT_FALSE(!parser.FailIfSeen("brotli", "zstd"))) {
        return false;
      }
      compression_type_ = CompressionType::kNone;
      return true;
    });
    parser.AddOption("brotli", [this, &parser](absl::string_view value) {
      if (ABSL_PREDICT_FALSE(!parser.FailIfSeen("uncompressed", "zstd"))) {
        return false;
      }
      compression_type_ = CompressionType::kBrotli;
      return true;
    });
    parser.AddOption("zstd", [this, &parser](absl::string_view value) {
      if (ABSL_PREDICT_FALSE(!parser.FailIfSeen("uncompressed", "brotli"))) {
        return false;
      }
      compression_type_ = CompressionType::kZstd;
      return true;
    });
    parser.AddOption("window_log",
                     [this](absl::string_view value) { return true; });
    if (ABSL_PREDICT_FALSE(!parser.Parse(text))) {
      *message = std::string(parser.message());
      return false;
    }
  }
  OptionsParser parser;
  parser.AddOption("uncompressed", parser.Empty([this, &parser] {
    if (ABSL_PREDICT_FALSE(!parser.FailIfSeen("window_log"))) return false;
    compression_level_ = 0;
    return true;
  }));
  parser.AddOption(
      "brotli", OptionsParser::Or(
                    parser.Empty([this] {
                      compression_level_ =
                          BrotliWriter::Options::kDefaultCompressionLevel();
                      return true;
                    }),
                    parser.Int(&compression_level_,
                               BrotliWriter::Options::kMinCompressionLevel(),
                               BrotliWriter::Options::kMaxCompressionLevel())));
  parser.AddOption(
      "zstd", OptionsParser::Or(
                  parser.Empty([this] {
                    compression_level_ =
                        ZstdWriter::Options::kDefaultCompressionLevel();
                    return true;
                  }),
                  parser.Int(&compression_level_,
                             ZstdWriter::Options::kMinCompressionLevel(),
                             ZstdWriter::Options::kMaxCompressionLevel())));
  parser.AddOption("window_log", [&] {
    switch (compression_type_) {
      case CompressionType::kNone:
        return OptionsParser::ValueParser([&parser](absl::string_view value) {
          return parser.FailIfSeen("uncompressed");
        });
      case CompressionType::kBrotli:
        return OptionsParser::Or(
            parser.Enum(&window_log_, {{"auto", kDefaultWindowLog()}}),
            parser.Int(&window_log_, BrotliWriter::Options::kMinWindowLog(),
                       BrotliWriter::Options::kMaxWindowLog()));
      case CompressionType::kZstd:
        return OptionsParser::Or(
            parser.Enum(&window_log_, {{"auto", kDefaultWindowLog()}}),
            parser.Int(&window_log_, ZstdWriter::Options::kMinWindowLog(),
                       ZstdWriter::Options::kMaxWindowLog()));
    }
    RIEGELI_ASSERT_UNREACHABLE() << "Unknown compression type: "
                                 << static_cast<unsigned>(compression_type_);
  }());
  if (ABSL_PREDICT_FALSE(!parser.Parse(text))) {
    *message = std::string(parser.message());
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
        return BrotliWriter::Options::kDefaultWindowLog();
      } else {
        RIEGELI_ASSERT_GE(window_log_, BrotliWriter::Options::kMinWindowLog())
            << "Failed precondition of CompressorOptions::set_window_log(): "
               "window log out of range for brotli";
        RIEGELI_ASSERT_LE(window_log_, BrotliWriter::Options::kMaxWindowLog())
            << "Failed precondition of CompressorOptions::set_window_log(): "
               "window log out of range for brotli";
        return window_log_;
      }
    case CompressionType::kZstd:
      if (window_log_ == kDefaultWindowLog()) {
        return ZstdWriter::Options::kDefaultWindowLog();
      } else {
        RIEGELI_ASSERT_GE(window_log_, ZstdWriter::Options::kMinWindowLog())
            << "Failed precondition of CompressorOptions::set_window_log(): "
               "window log out of range for zstd";
        RIEGELI_ASSERT_LE(window_log_, ZstdWriter::Options::kMaxWindowLog())
            << "Failed precondition of CompressorOptions::set_window_log(): "
               "window log out of range for zstd";
        return window_log_;
      }
  }
  RIEGELI_ASSERT_UNREACHABLE() << "Unknown compression type: "
                               << static_cast<unsigned>(compression_type_);
}

}  // namespace riegeli
