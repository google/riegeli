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

#ifndef RIEGELI_MESSAGES_MESSAGE_PARSE_H_
#define RIEGELI_MESSAGES_MESSAGE_PARSE_H_

#include <stdint.h>

#include <type_traits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "google/protobuf/io/zero_copy_stream.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

class ParseOptions {
 public:
  ParseOptions() noexcept {}

  // If `false`, replaces existing contents of the destination, clearing it
  // first.
  //
  // If `true`, merges to existing contents of the destination.
  //
  // Default: `false`.
  ParseOptions& set_merge(bool merge) & {
    merge_ = merge;
    return *this;
  }
  ParseOptions&& set_merge(bool merge) && {
    return std::move(set_merge(merge));
  }
  bool merge() const { return merge_; }

  // If `false`, missing required fields cause a failure.
  //
  // If `true`, missing required fields result in a partial parsed message,
  // not having these fields.
  //
  // Default: `false`.
  ParseOptions& set_partial(bool partial) & {
    partial_ = partial;
    return *this;
  }
  ParseOptions&& set_partial(bool partial) && {
    return std::move(set_partial(partial));
  }
  bool partial() const { return partial_; }

 private:
  bool merge_ = false;
  bool partial_ = false;
};

// Reads a message in binary format from the given `Reader`. If successful, the
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
absl::Status ParseFromReader(Src&& src, google::protobuf::MessageLite& dest,
                             ParseOptions options = ParseOptions());

// Reads a message in binary format from the given `absl::string_view`.
//
// Returns status:
//  * `status.ok()`  - success (`dest` is filled)
//  * `!status.ok()` - failure (`dest` is unspecified)
absl::Status ParseFromString(absl::string_view src,
                             google::protobuf::MessageLite& dest,
                             ParseOptions options = ParseOptions());

// Reads a message in binary format from the given `Chain`.
//
// Returns status:
//  * `status.ok()`  - success (`dest` is filled)
//  * `!status.ok()` - failure (`dest` is unspecified)
absl::Status ParseFromChain(const Chain& src,
                            google::protobuf::MessageLite& dest,
                            ParseOptions options = ParseOptions());

// Reads a message in binary format from the given `absl::Cord`.
//
// Returns status:
//  * `status.ok()`  - success (`dest` is filled)
//  * `!status.ok()` - failure (`dest` is unspecified)
absl::Status ParseFromCord(const absl::Cord& src,
                           google::protobuf::MessageLite& dest,
                           ParseOptions options = ParseOptions());

// Adapts a `Reader` to a `google::protobuf::io::ZeroCopyInputStream`.
class ReaderInputStream : public google::protobuf::io::ZeroCopyInputStream {
 public:
  explicit ReaderInputStream(Reader* src) : src_(RIEGELI_ASSERT_NOTNULL(src)) {}

  bool Next(const void** data, int* size) override;
  void BackUp(int length) override;
  bool Skip(int length) override;
  int64_t ByteCount() const override;

 private:
  Reader* src_;
};

// Implementation details follow.

namespace messages_internal {

absl::Status ParseFromReaderImpl(Reader& src,
                                 google::protobuf::MessageLite& dest,
                                 ParseOptions options);

}  // namespace messages_internal

template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int>>
inline absl::Status ParseFromReader(Src&& src,
                                    google::protobuf::MessageLite& dest,
                                    ParseOptions options) {
  Dependency<Reader*, Src&&> src_ref(std::forward<Src>(src));
  absl::Status status =
      messages_internal::ParseFromReaderImpl(*src_ref, dest, options);
  if (src_ref.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_ref->VerifyEndAndClose())) {
      status.Update(src_ref->status());
    }
  }
  return status;
}

}  // namespace riegeli

#endif  // RIEGELI_MESSAGES_MESSAGE_PARSE_H_
