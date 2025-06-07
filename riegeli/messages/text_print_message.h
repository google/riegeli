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

#ifndef RIEGELI_MESSAGES_TEXT_PRINT_MESSAGE_H_
#define RIEGELI_MESSAGES_TEXT_PRINT_MESSAGE_H_

#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "google/protobuf/message.h"
#include "google/protobuf/text_format.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

class TextPrintMessageOptions {
 public:
  TextPrintMessageOptions() noexcept {}

  // If `false`, all required fields must be set. This is verified in debug
  // mode.
  //
  // If `true`, missing required fields result in a partial serialized message,
  // not having these fields.
  //
  // Default: `false`.
  TextPrintMessageOptions& set_partial(bool partial) &
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    partial_ = partial;
    return *this;
  }
  TextPrintMessageOptions&& set_partial(bool partial) &&
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return std::move(set_partial(partial));
  }
  bool partial() const { return partial_; }

  // If `false`, print just the message.
  //
  // If `true`, prefix the output with comments informing about the filename and
  // message name, to aid tools and humans in interpreting the file:
  //
  // ```
  // # proto-file: path/filename.proto
  // # proto-message: MessageName
  // ```
  //
  // See
  // https://developers.google.com/protocol-buffers/docs/text-format-spec#header
  // for details.
  //
  // Default: `false`.
  TextPrintMessageOptions& set_header(bool header) &
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    header_ = header;
    return *this;
  }
  TextPrintMessageOptions&& set_header(bool header) &&
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return std::move(set_header(header));
  }
  bool header() const { return header_; }

  // Other text printing options.
  google::protobuf::TextFormat::Printer& printer()
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return printer_;
  }
  const google::protobuf::TextFormat::Printer& printer() const
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return printer_;
  }

 private:
  bool partial_ = false;
  bool header_ = false;
  google::protobuf::TextFormat::Printer printer_;
};

// Writes the message in text format to the given `Writer`.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the `Writer`. `Dest` must support
// `DependencyRef<Writer*, Dest>`, e.g. `Writer&` (not owned),
// `ChainWriter<>` (owned), `std::unique_ptr<Writer>` (owned),
// `AnyRef<Writer*>` (maybe owned).
//
// Returns status:
//  * `status.ok()`  - success (`dest` is written to)
//  * `!status.ok()` - failure (`dest` is unspecified)
template <typename Dest,
          std::enable_if_t<TargetRefSupportsDependency<Writer*, Dest>::value,
                           int> = 0>
absl::Status TextPrintMessage(
    const google::protobuf::Message& src, Dest&& dest,
    const TextPrintMessageOptions& options = TextPrintMessageOptions());

// Writes the message in text format to `dest`, clearing any existing data in
// `dest`.
//
// Returns status:
//  * `status.ok()`  - success (`dest` is filled)
//  * `!status.ok()` - failure (`dest` is unspecified)
absl::Status TextPrintMessage(
    const google::protobuf::Message& src, std::string& dest,
    const TextPrintMessageOptions& options = TextPrintMessageOptions());
absl::Status TextPrintMessage(
    const google::protobuf::Message& src, Chain& dest,
    const TextPrintMessageOptions& options = TextPrintMessageOptions());
absl::Status TextPrintMessage(
    const google::protobuf::Message& src, absl::Cord& dest,
    const TextPrintMessageOptions& options = TextPrintMessageOptions());

// Implementation details follow.

namespace text_print_message_internal {

absl::Status TextPrintMessageImpl(const google::protobuf::Message& src,
                                  Writer& dest,
                                  const TextPrintMessageOptions& options);

}  // namespace text_print_message_internal

template <
    typename Dest,
    std::enable_if_t<TargetRefSupportsDependency<Writer*, Dest>::value, int>>
inline absl::Status TextPrintMessage(const google::protobuf::Message& src,
                                     Dest&& dest,
                                     const TextPrintMessageOptions& options) {
  DependencyRef<Writer*, Dest> dest_dep(std::forward<Dest>(dest));
  absl::Status status = text_print_message_internal::TextPrintMessageImpl(
      src, *dest_dep, options);
  if (dest_dep.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_dep->Close())) {
      status.Update(dest_dep->status());
    }
  }
  return status;
}

}  // namespace riegeli

#endif  // RIEGELI_MESSAGES_TEXT_PRINT_MESSAGE_H_
