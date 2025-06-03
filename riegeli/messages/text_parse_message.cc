// Copyright 2022 Google LLC
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

#include "riegeli/messages/text_parse_message.h"

#include <memory>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "google/protobuf/io/tokenizer.h"
#include "google/protobuf/message.h"
#include "google/protobuf/text_format.h"
#include "riegeli/base/bytes_ref.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/cord_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/string_reader.h"
#include "riegeli/messages/parse_message.h"

namespace riegeli {

namespace text_parse_message_internal {

void StringErrorCollector::RecordError(
    int line, google::protobuf::io::ColumnNumber column,
    absl::string_view message) {
  if (line >= 0) {
    absl::StrAppend(&errors_, "\nAt ", line + 1, ":", column + 1, ": ",
                    message);
  } else {
    absl::StrAppend(&errors_, "\n", message);
  }
}

}  // namespace text_parse_message_internal

TextParseMessageOptions::TextParseMessageOptions()
    : error_collector_(std::make_unique<
                       text_parse_message_internal::StringErrorCollector>()) {
  parser_.RecordErrorsTo(error_collector_.get());
}

namespace text_parse_message_internal {

absl::Status TextParseMessageImpl(Reader& src, google::protobuf::Message& dest,
                                  const TextParseMessageOptions& options) {
  ReaderInputStream input_stream(&src);
  google::protobuf::TextFormat::Parser parser = options.parser();
  const bool parse_ok = options.merge() ? parser.Merge(&input_stream, &dest)
                                        : parser.Parse(&input_stream, &dest);
  if (ABSL_PREDICT_FALSE(!src.ok())) return src.status();
  if (ABSL_PREDICT_FALSE(!parse_ok)) {
    return src.AnnotateStatus(absl::InvalidArgumentError(
        absl::StrCat("Failed to text-parse message of type ",
                     dest.GetTypeName(), options.error_collector_->errors())));
  }
  return absl::OkStatus();
}

}  // namespace text_parse_message_internal

absl::Status TextParseMessage(BytesRef src, google::protobuf::Message& dest,
                              const TextParseMessageOptions& options) {
  return TextParseMessage(StringReader<>(src), dest, options);
}

absl::Status TextParseMessage(const Chain& src, google::protobuf::Message& dest,
                              const TextParseMessageOptions& options) {
  return TextParseMessage(ChainReader<>(&src), dest, options);
}

absl::Status TextParseMessage(const absl::Cord& src,
                              google::protobuf::Message& dest,
                              const TextParseMessageOptions& options) {
  return TextParseMessage(CordReader<>(&src), dest, options);
}

}  // namespace riegeli
