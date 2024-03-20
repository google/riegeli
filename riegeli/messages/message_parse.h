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

#include <stddef.h>
#include <stdint.h>

#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/io/zero_copy_stream.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/assert.h"
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

  // Maximum depth of submessages allowed.
  //
  // `recursion_limit` must be non-negative.
  // Default:
  // `google::protobuf::io::CodedInputStream::GetDefaultRecursionLimit()`
  // (usually 100).
  ParseOptions& set_recursion_limit(int recursion_limit) & {
    RIEGELI_ASSERT_GE(recursion_limit, 0)
        << "Failed precondition of ParseOptions::set_recursion_limit(): "
           "recursion limit out of range";
    recursion_limit_ = recursion_limit;
    return *this;
  }
  ParseOptions&& set_recursion_limit(int recursion_limit) && {
    return std::move(set_recursion_limit(recursion_limit));
  }
  int recursion_limit() const { return recursion_limit_; }

 private:
  bool merge_ = false;
  bool partial_ = false;
  int recursion_limit_ =
      google::protobuf::io::CodedInputStream::GetDefaultRecursionLimit();
};

// Reads a message in binary format from the given `Reader`. If successful, the
// entire input will be consumed.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the `Reader`. `Src` must support
// `Dependency<Reader*, Src&&>`, e.g. `Reader&` (not owned),
// `ChainReader<>` (owned), `std::unique_ptr<Reader>` (owned),
// `AnyDependency<Reader*>` (maybe owned).
//
// Returns status:
//  * `status.ok()`  - success (`dest` is filled)
//  * `!status.ok()` - failure (`dest` is unspecified)
template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int> = 0>
absl::Status ParseFromReader(Src&& src, google::protobuf::MessageLite& dest,
                             ParseOptions options = ParseOptions());

// Reads a message in binary format with the given `length` from the given
// `Reader`. If successful, exactly `length` bytes will be consumed.
//
// Returns status:
//  * `status.ok()`  - success (`dest` is filled)
//  * `!status.ok()` - failure (`dest` is unspecified)
//
// This is a more efficient equivalent of:
//
// ```
// ParseFromReader(
//     LimitingReader<>(&src,
//                      LimitingReaderBase::Options().set_exact_length(length)),
//     dest, options)
// ```
absl::Status ParseFromReaderWithLength(Reader& src, size_t length,
                                       google::protobuf::MessageLite& dest,
                                       ParseOptions options = ParseOptions());

// Reads a message length as varint32 (at most 2G), then a message in binary
// format with that length from the given `Reader`.
//
// Returns status:
//  * `status.ok()`  - success (`dest` is filled)
//  * `!status.ok()` - failure (`dest` is unspecified)
absl::Status ParseLengthPrefixedFromReader(
    Reader& src, google::protobuf::MessageLite& dest,
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
  // Creates a dummy `ReaderInputStream`. Use `Reset(src)` to initialize it to
  // usable state.
  ReaderInputStream() = default;

  // Will read from `*src`.
  explicit ReaderInputStream(Reader* src) : src_(RIEGELI_ASSERT_NOTNULL(src)) {}

  ABSL_ATTRIBUTE_REINITIALIZES void Reset() { src_ = nullptr; }
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Reader* src) {
    src_ = RIEGELI_ASSERT_NOTNULL(src);
  }

  bool Next(const void** data, int* size) override;
  void BackUp(int length) override;
  bool Skip(int length) override;
  int64_t ByteCount() const override;

 private:
  Reader* src_ = nullptr;
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
  Dependency<Reader*, Src&&> src_dep(std::forward<Src>(src));
  if (src_dep.IsOwning()) src_dep->SetReadAllHint(true);
  absl::Status status = messages_internal::ParseFromReaderImpl(
      *src_dep, dest, std::move(options));
  if (src_dep.IsOwning()) {
    if (ABSL_PREDICT_TRUE(status.ok())) src_dep->VerifyEnd();
    if (ABSL_PREDICT_FALSE(!src_dep->Close())) status.Update(src_dep->status());
  }
  return status;
}

}  // namespace riegeli

#endif  // RIEGELI_MESSAGES_MESSAGE_PARSE_H_
