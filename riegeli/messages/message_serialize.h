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

#ifndef RIEGELI_MESSAGES_MESSAGE_SERIALIZE_H_
#define RIEGELI_MESSAGES_MESSAGE_SERIALIZE_H_

#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/io/zero_copy_stream.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

class SerializeOptions {
 public:
  SerializeOptions() noexcept {}

  // If `false`, all required fields must be set. This is verified in debug
  // mode.
  //
  // If `true`, missing required fields result in a partial serialized message,
  // not having these fields.
  //
  // Default: `false`.
  SerializeOptions& set_partial(bool partial) & {
    partial_ = partial;
    return *this;
  }
  SerializeOptions&& set_partial(bool partial) && {
    return std::move(set_partial(partial));
  }
  bool partial() const { return partial_; }

  // If `false`, a deterministic result is not guaranteed but serialization can
  // be faster.
  //
  // If `true`, a deterministic result is guaranteed (as long as the schema
  // does not change in inappropriate ways and there are no unknown fields)
  // but serialization can be slower.
  //
  // This matches
  // `google::protobuf::io::CodedOutputStream::SetSerializationDeterministic()`.
  //
  // Default:
  // `google::protobuf::io::CodedOutputStream::IsDefaultSerializationDeterministic()`
  // (usually `false`).
  SerializeOptions& set_deterministic(bool deterministic) & {
    deterministic_ = deterministic;
    return *this;
  }
  SerializeOptions&& set_deterministic(bool deterministic) && {
    return std::move(set_deterministic(deterministic));
  }
  bool deterministic() const { return deterministic_; }

  // If `true`, promises that `ByteSizeLong()` has been called on the message
  // being serialized after its last modification, and that its result does not
  // exceed `std::numeric_limits<int32_t>::max()`.
  //
  // This makes serialization faster by allowing to use `GetCachedSize()`
  // instead of `ByteSizeLong()`.
  //
  // Default: `false`.
  SerializeOptions& set_has_cached_size(bool has_cached_size) & {
    has_cached_size_ = has_cached_size;
    return *this;
  }
  SerializeOptions&& set_has_cached_size(bool has_cached_size) && {
    return std::move(set_has_cached_size(has_cached_size));
  }
  bool has_cached_size() const { return has_cached_size_; }

  // Returns `message.ByteSizeLong()` faster.
  //
  // Requires that these `SerializeOptions` will be used only for serializing
  // this `message`, which will not be modified in the meantime.
  //
  // This consults and updates `has_cached_size()` in these `SerializeOptions`.
  size_t GetByteSize(const google::protobuf::MessageLite& message);

 private:
  bool partial_ = false;
  bool deterministic_ = google::protobuf::io::CodedOutputStream::
      IsDefaultSerializationDeterministic();
  bool has_cached_size_ = false;
};

// Writes the message in binary format to the given `Writer`.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the `Writer`. `Dest` must support
// `Dependency<Writer*, Dest&&>`, e.g. `Writer&` (not owned),
// `ChainWriter<>` (owned), `std::unique_ptr<Writer>` (owned),
// `AnyDependency<Writer*>` (maybe owned).
//
// With a `dest_args` parameter, writes to a `Dest` constructed from elements of
// `dest_args`. This avoids constructing a temporary `Dest` and moving from it.
//
// Returns status:
//  * `status.ok()`  - success (`dest` is written to)
//  * `!status.ok()` - failure (`dest` is unspecified)
template <typename Dest,
          std::enable_if_t<IsValidDependency<Writer*, Dest&&>::value, int> = 0>
absl::Status SerializeToWriter(const google::protobuf::MessageLite& src,
                               Dest&& dest,
                               SerializeOptions options = SerializeOptions());

// Writes the message length as varint32, then the message in binary format to
// the given `Writer`.
//
// Returns status:
//  * `status.ok()`  - success (`dest` is written to)
//  * `!status.ok()` - failure (`dest` is unspecified)
absl::Status SerializeLengthPrefixedToWriter(
    const google::protobuf::MessageLite& src, Writer& dest,
    SerializeOptions options = SerializeOptions());

// Writes the message in binary format to the given `std::string`, clearing it
// first.
//
// Returns status:
//  * `status.ok()`  - success (`dest` is filled)
//  * `!status.ok()` - failure (`dest` is unspecified)
absl::Status SerializeToString(const google::protobuf::MessageLite& src,
                               std::string& dest,
                               SerializeOptions options = SerializeOptions());

// Writes the message in binary format to the given `Chain`, clearing it first.
//
// Returns status:
//  * `status.ok()`  - success (`dest` is filled)
//  * `!status.ok()` - failure (`dest` is unspecified)
absl::Status SerializeToChain(const google::protobuf::MessageLite& src,
                              Chain& dest,
                              SerializeOptions options = SerializeOptions());

// Writes the message in binary format to the given `absl::Cord`, clearing it
// first.
//
// Returns status:
//  * `status.ok()`  - success (`dest` is filled)
//  * `!status.ok()` - failure (`dest` is unspecified)
absl::Status SerializeToCord(const google::protobuf::MessageLite& src,
                             absl::Cord& dest,
                             SerializeOptions options = SerializeOptions());

// Adapts a `Writer` to a `google::protobuf::io::ZeroCopyOutputStream`.
class WriterOutputStream : public google::protobuf::io::ZeroCopyOutputStream {
 public:
  explicit WriterOutputStream(Writer* dest)
      : dest_(RIEGELI_ASSERT_NOTNULL(dest)) {}

  bool Next(void** data, int* size) override;
  void BackUp(int length) override;
  int64_t ByteCount() const override;

 private:
  Writer* dest_;
};

// Implementation details follow.

inline size_t SerializeOptions::GetByteSize(
    const google::protobuf::MessageLite& message) {
  if (has_cached_size()) return IntCast<size_t>(message.GetCachedSize());
  const size_t size = message.ByteSizeLong();
  if (ABSL_PREDICT_TRUE(size <=
                        uint32_t{std::numeric_limits<int32_t>::max()})) {
    set_has_cached_size(true);
  }
  return size;
}

namespace messages_internal {

absl::Status SerializeToWriterImpl(const google::protobuf::MessageLite& src,
                                   Writer& dest, SerializeOptions options,
                                   bool set_write_hint);

}  // namespace messages_internal

template <typename Dest,
          std::enable_if_t<IsValidDependency<Writer*, Dest&&>::value, int>>
inline absl::Status SerializeToWriter(const google::protobuf::MessageLite& src,
                                      Dest&& dest, SerializeOptions options) {
  Dependency<Writer*, Dest&&> dest_dep(std::forward<Dest>(dest));
  absl::Status status = messages_internal::SerializeToWriterImpl(
      src, *dest_dep, std::move(options), dest_dep.is_owning());
  if (dest_dep.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_dep->Close())) {
      status.Update(dest_dep->status());
    }
  }
  return status;
}

}  // namespace riegeli

#endif  // RIEGELI_MESSAGES_MESSAGE_SERIALIZE_H_
