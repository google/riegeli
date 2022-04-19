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

#ifndef RIEGELI_MESSAGES_TEXT_PRINT_H_
#define RIEGELI_MESSAGES_TEXT_PRINT_H_

#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "google/protobuf/message.h"
#include "google/protobuf/text_format.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

class TextPrintOptions {
 public:
  TextPrintOptions() noexcept {}

  // If `false`, all required fields must be set. This is verified in debug
  // mode.
  //
  // If `true`, missing required fields result in a partial serialized message,
  // not having these fields.
  //
  // Default: `false`.
  TextPrintOptions& set_partial(bool partial) & {
    partial_ = partial;
    return *this;
  }
  TextPrintOptions&& set_partial(bool partial) && {
    return std::move(set_partial(partial));
  }
  bool partial() const { return partial_; }

  // Other text printing options.
  google::protobuf::TextFormat::Printer& printer() { return printer_; }
  const google::protobuf::TextFormat::Printer& printer() const {
    return printer_;
  }

 private:
  bool partial_ = false;
  google::protobuf::TextFormat::Printer printer_;
};

// Writes the message in text format to the given `Writer`.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the `Writer`. `Dest` must support
// `Dependency<Writer*, Dest&&>`, e.g. `Writer&` (not owned),
// `ChainWriter<>` (owned), `std::unique_ptr<Writer>` (owned).
//
// With a `dest_args` parameter, writes to a `Dest` constructed from elements of
// `dest_args`. This avoids constructing a temporary `Dest` and moving from it.
//
// Returns status:
//  * `status.ok()`  - success (`dest` is written to)
//  * `!status.ok()` - failure (`dest` is unspecified)
template <typename Dest,
          std::enable_if_t<IsValidDependency<Writer*, Dest&&>::value, int> = 0>
absl::Status TextPrintToWriter(
    const google::protobuf::Message& src, Dest&& dest,
    const TextPrintOptions& options = TextPrintOptions());

// Writes the message in text format to the given `std::string`, clearing it
// first.
//
// Returns status:
//  * `status.ok()`  - success (`dest` is filled)
//  * `!status.ok()` - failure (`dest` is unspecified)
absl::Status TextPrintToString(
    const google::protobuf::Message& src, std::string& dest,
    const TextPrintOptions& options = TextPrintOptions());

// Writes the message in text format to the given `Chain`, clearing it first.
//
// Returns status:
//  * `status.ok()`  - success (`dest` is filled)
//  * `!status.ok()` - failure (`dest` is unspecified)
absl::Status TextPrintToChain(
    const google::protobuf::Message& src, Chain& dest,
    const TextPrintOptions& options = TextPrintOptions());

// Writes the message in text format to the given `absl::Cord`, clearing it
// first.
//
// Returns status:
//  * `status.ok()`  - success (`dest` is filled)
//  * `!status.ok()` - failure (`dest` is unspecified)
absl::Status TextPrintToCord(
    const google::protobuf::Message& src, absl::Cord& dest,
    const TextPrintOptions& options = TextPrintOptions());

// Implementation details follow.

namespace messages_internal {

absl::Status TextPrintToWriterImpl(const google::protobuf::Message& src,
                                   Writer& dest,
                                   const TextPrintOptions& options);

}  // namespace messages_internal

template <typename Dest,
          std::enable_if_t<IsValidDependency<Writer*, Dest&&>::value, int>>
inline absl::Status TextPrintToWriter(const google::protobuf::Message& src,
                                      Dest&& dest,
                                      const TextPrintOptions& options) {
  Dependency<Writer*, Dest&&> dest_ref(std::forward<Dest>(dest));
  absl::Status status =
      messages_internal::TextPrintToWriterImpl(src, *dest_ref, options);
  if (dest_ref.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_ref->Close())) {
      status.Update(dest_ref->status());
    }
  }
  return status;
}

}  // namespace riegeli

#endif  // RIEGELI_MESSAGES_TEXT_PRINT_H_
