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

#include "riegeli/base/base.h"
#include "riegeli/base/options_parser.h"
#include "riegeli/base/str_cat.h"
#include "riegeli/base/string_view.h"
#include "riegeli/bytes/brotli_writer.h"
#include "riegeli/bytes/zstd_writer.h"

namespace riegeli {

bool CompressorOptions::Parse(string_view text, std::string* message) {
  // Set just compression_type_ first because other parsers depend on
  // compression_type_.
  // TODO: Conflicting options like "brotli,zstd" should probably be
  // errors.
  return RIEGELI_LIKELY(ParseOptions(
             {
                 {"uncompressed",
                  [this](string_view value, std::string* valid_values) {
                    compression_type_ = CompressionType::kNone;
                    return true;
                  }},
                 {"brotli",
                  [this](string_view value, std::string* valid_values) {
                    compression_type_ = CompressionType::kBrotli;
                    return true;
                  }},
                 {"zstd",
                  [this](string_view value, std::string* valid_values) {
                    compression_type_ = CompressionType::kZstd;
                    return true;
                  }},
                 {"window_log", [this](string_view value,
                                       std::string* valid_values) { return true; }},
             },
             text, message)) &&
         ParseOptions(
             {
                 {"uncompressed", EnumOption(&compression_level_, {{"", 0}})},
                 {"brotli",
                  AltOption(
                      EnumOption(&compression_level_,
                                 {{"", BrotliWriter::Options::
                                           kDefaultCompressionLevel()}}),
                      IntOption(
                          &compression_level_,
                          BrotliWriter::Options::kMinCompressionLevel(),
                          BrotliWriter::Options::kMaxCompressionLevel()))},
                 {"zstd",
                  AltOption(
                      EnumOption(
                          &compression_level_,
                          {{"",
                            ZstdWriter::Options::kDefaultCompressionLevel()}}),
                      IntOption(&compression_level_,
                                ZstdWriter::Options::kMinCompressionLevel(),
                                ZstdWriter::Options::kMaxCompressionLevel()))},
                 {"window_log",
                  [this] {
                    OptionParser auto_parser = EnumOption(
                        &window_log_, {{"auto", kDefaultWindowLog()}});
                    switch (compression_type_) {
                      case CompressionType::kNone:
                        return auto_parser;
                      case CompressionType::kBrotli:
                        return AltOption(
                            std::move(auto_parser),
                            IntOption(&window_log_,
                                      BrotliWriter::Options::kMinWindowLog(),
                                      BrotliWriter::Options::kMaxWindowLog()));
                      case CompressionType::kZstd:
                        return AltOption(
                            std::move(auto_parser),
                            IntOption(&window_log_,
                                      ZstdWriter::Options::kMinWindowLog(),
                                      ZstdWriter::Options::kMaxWindowLog()));
                    }
                    RIEGELI_ASSERT_UNREACHABLE()
                        << "Unknown compression type: "
                        << static_cast<unsigned>(compression_type_);
                  }()},
             },
             text, message);
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
