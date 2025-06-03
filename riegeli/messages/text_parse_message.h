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

#ifndef RIEGELI_MESSAGES_TEXT_PARSE_MESSAGE_H_
#define RIEGELI_MESSAGES_TEXT_PARSE_MESSAGE_H_

#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "google/protobuf/io/tokenizer.h"
#include "google/protobuf/message.h"
#include "google/protobuf/text_format.h"
#include "riegeli/base/bytes_ref.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

class TextParseMessageOptions;

namespace text_parse_message_internal {

class StringErrorCollector : public google::protobuf::io::ErrorCollector {
 public:
  void RecordError(int line, google::protobuf::io::ColumnNumber column,
                   absl::string_view message) override;

  absl::string_view errors() const { return errors_; }

 private:
  std::string errors_;
};

absl::Status TextParseMessageImpl(Reader& src, google::protobuf::Message& dest,
                                  const TextParseMessageOptions& options);

}  // namespace text_parse_message_internal

class TextParseMessageOptions {
 public:
  TextParseMessageOptions();

  // If `false`, replaces existing contents of the destination, clearing it
  // first.
  //
  // If `true`, merges to existing contents of the destination.
  //
  // Default: `false`.
  TextParseMessageOptions& set_merge(bool merge) &
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    merge_ = merge;
    return *this;
  }
  TextParseMessageOptions&& set_merge(bool merge) &&
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return std::move(set_merge(merge));
  }
  bool merge() const { return merge_; }

  // Other text parsing options.
  //
  // The default `ErrorCollector` is set up such that errors are returned as
  // `absl::InvalidArgumengError()` instead of being logged.
  google::protobuf::TextFormat::Parser& parser() ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return parser_;
  }
  const google::protobuf::TextFormat::Parser& parser() const
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return parser_;
  }

 private:
  // For `error_collector_`.
  friend absl::Status text_parse_message_internal::TextParseMessageImpl(
      Reader& src, google::protobuf::Message& dest,
      const TextParseMessageOptions& options);

  bool merge_ = false;
  std::unique_ptr<text_parse_message_internal::StringErrorCollector>
      error_collector_;
  google::protobuf::TextFormat::Parser parser_;
};

using TextParseOptions ABSL_DEPRECATE_AND_INLINE() = TextParseMessageOptions;

// Reads a message in text format from the given `Reader`. If successful, the
// entire input will be consumed.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the `Reader`. `Src` must support
// `DependencyRef<Reader*, Src>`, e.g.  `Reader&` (not owned),
// `ChainReader<>` (owned), `std::unique_ptr<Reader>` (owned),
// `AnyRef<Reader*>` (maybe owned).
//
// Returns status:
//  * `status.ok()`  - success (`dest` is filled)
//  * `!status.ok()` - failure (`dest` is unspecified)
template <
    typename Src,
    std::enable_if_t<TargetRefSupportsDependency<Reader*, Src>::value, int> = 0>
absl::Status TextParseMessage(
    Src&& src, google::protobuf::Message& dest,
    const TextParseMessageOptions& options = TextParseMessageOptions());

// Reads a message in text format from `src`.
//
// Returns status:
//  * `status.ok()`  - success (`dest` is filled)
//  * `!status.ok()` - failure (`dest` is unspecified)
absl::Status TextParseMessage(
    BytesRef src, google::protobuf::Message& dest,
    const TextParseMessageOptions& options = TextParseMessageOptions());
absl::Status TextParseMessage(
    const Chain& src, google::protobuf::Message& dest,
    const TextParseMessageOptions& options = TextParseMessageOptions());
absl::Status TextParseMessage(
    const absl::Cord& src, google::protobuf::Message& dest,
    const TextParseMessageOptions& options = TextParseMessageOptions());

// Implementation details follow.

namespace text_parse_message_internal {

absl::Status TextParseMessageImpl(Reader& src, google::protobuf::Message& dest,
                                  const TextParseMessageOptions& options);

}  // namespace text_parse_message_internal

template <
    typename Src,
    std::enable_if_t<TargetRefSupportsDependency<Reader*, Src>::value, int>>
inline absl::Status TextParseMessage(Src&& src, google::protobuf::Message& dest,
                                     const TextParseMessageOptions& options) {
  DependencyRef<Reader*, Src> src_dep(std::forward<Src>(src));
  if (src_dep.IsOwning()) src_dep->SetReadAllHint(true);
  absl::Status status = text_parse_message_internal::TextParseMessageImpl(
      *src_dep, dest, options);
  if (src_dep.IsOwning()) {
    if (ABSL_PREDICT_TRUE(status.ok())) src_dep->VerifyEnd();
    if (ABSL_PREDICT_FALSE(!src_dep->Close())) status.Update(src_dep->status());
  }
  return status;
}

}  // namespace riegeli

#endif  // RIEGELI_MESSAGES_TEXT_PARSE_MESSAGE_H_
