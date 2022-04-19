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

#ifndef RIEGELI_MESSAGES_TEXT_PARSE_H_
#define RIEGELI_MESSAGES_TEXT_PARSE_H_

#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "google/protobuf/io/tokenizer.h"
#include "google/protobuf/message.h"
#include "google/protobuf/text_format.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

class TextParseOptions;

namespace messages_internal {

class StringErrorCollector : public google::protobuf::io::ErrorCollector {
 public:
  void AddError(int line, google::protobuf::io::ColumnNumber column,
                const std::string& message) override;

  absl::string_view errors() const { return errors_; }

 private:
  std::string errors_;
};

absl::Status TextParseFromReaderImpl(Reader& src,
                                     google::protobuf::Message& dest,
                                     const TextParseOptions& options);

}  // namespace messages_internal

class TextParseOptions {
 public:
  TextParseOptions();

  // If `false`, replaces existing contents of the destination, clearing it
  // first.
  //
  // If `true`, merges to existing contents of the destination.
  //
  // Default: `false`.
  TextParseOptions& set_merge(bool merge) & {
    merge_ = merge;
    return *this;
  }
  TextParseOptions&& set_merge(bool merge) && {
    return std::move(set_merge(merge));
  }
  bool merge() const { return merge_; }

  // Other text parsing options.
  //
  // The default `ErrorCollector` is set up such that errors are returned as
  // `absl::InvalidArgumengError()` instead of being logged.
  google::protobuf::TextFormat::Parser& parser() { return parser_; }
  const google::protobuf::TextFormat::Parser& parser() const { return parser_; }

 private:
  // For `error_collector_`.
  friend absl::Status messages_internal::TextParseFromReaderImpl(
      Reader& src, google::protobuf::Message& dest,
      const TextParseOptions& options);

  bool merge_ = false;
  std::unique_ptr<messages_internal::StringErrorCollector> error_collector_;
  google::protobuf::TextFormat::Parser parser_;
};

// Reads a message in text format from the given `Reader`. If successful, the
// entire input will be consumed.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the `Reader`. `Src` must support
// `Dependency<Reader*, Src&&>`, e.g. `Reader&` (not owned),
// `ChainReader<>` (owned), `std::unique_ptr<Reader>` (owned).
//
// With a `src_args` parameter, reads from a `Src` constructed from elements of
// `src_args`. This avoids constructing a temporary `Src` and moving from it.
//
// Returns status:
//  * `status.ok()`  - success (`dest` is filled)
//  * `!status.ok()` - failure (`dest` is unspecified)
template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int> = 0>
absl::Status TextParseFromReader(
    Src&& src, google::protobuf::Message& dest,
    const TextParseOptions& options = TextParseOptions());

// Reads a message in text format from the given `absl::string_view`.
//
// Returns status:
//  * `status.ok()`  - success (`dest` is filled)
//  * `!status.ok()` - failure (`dest` is unspecified)
absl::Status TextParseFromString(
    absl::string_view src, google::protobuf::Message& dest,
    const TextParseOptions& options = TextParseOptions());

// Reads a message in text format from the given `Chain`.
//
// Returns status:
//  * `status.ok()`  - success (`dest` is filled)
//  * `!status.ok()` - failure (`dest` is unspecified)
absl::Status TextParseFromChain(
    const Chain& src, google::protobuf::Message& dest,
    const TextParseOptions& options = TextParseOptions());

// Reads a message in text format from the given `absl::Cord`.
//
// Returns status:
//  * `status.ok()`  - success (`dest` is filled)
//  * `!status.ok()` - failure (`dest` is unspecified)
absl::Status TextParseFromCord(
    const absl::Cord& src, google::protobuf::Message& dest,
    const TextParseOptions& options = TextParseOptions());

// Implementation details follow.

namespace messages_internal {

absl::Status TextParseFromReaderImpl(Reader& src,
                                     google::protobuf::Message& dest,
                                     const TextParseOptions& options);

}  // namespace messages_internal

template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int>>
inline absl::Status TextParseFromReader(Src&& src,
                                        google::protobuf::Message& dest,
                                        const TextParseOptions& options) {
  Dependency<Reader*, Src&&> src_ref(std::forward<Src>(src));
  absl::Status status =
      messages_internal::TextParseFromReaderImpl(*src_ref, dest, options);
  if (src_ref.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_ref->VerifyEndAndClose())) {
      status.Update(src_ref->status());
    }
  }
  return status;
}

}  // namespace riegeli

#endif  // RIEGELI_MESSAGES_TEXT_PARSE_H_
