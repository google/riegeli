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

#ifndef RIEGELI_MESSAGES_SERIALIZE_MESSAGE_H_
#define RIEGELI_MESSAGES_SERIALIZE_MESSAGE_H_

#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/io/zero_copy_stream.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/compact_string.h"
#include "riegeli/base/dependency.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

class SerializeMessageOptions {
 public:
  SerializeMessageOptions() noexcept {}

  // If `false`, all required fields must be set. This is verified in debug
  // mode.
  //
  // If `true`, missing required fields result in a partial serialized message,
  // not having these fields.
  //
  // Default: `false`.
  SerializeMessageOptions& set_partial(bool partial) &
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    partial_ = partial;
    return *this;
  }
  SerializeMessageOptions&& set_partial(bool partial) &&
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
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
  SerializeMessageOptions& set_deterministic(bool deterministic) &
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    deterministic_ = deterministic;
    return *this;
  }
  SerializeMessageOptions&& set_deterministic(bool deterministic) &&
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
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
  SerializeMessageOptions& set_has_cached_size(bool has_cached_size) &
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    has_cached_size_ = has_cached_size;
    return *this;
  }
  SerializeMessageOptions&& set_has_cached_size(bool has_cached_size) &&
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return std::move(set_has_cached_size(has_cached_size));
  }
  bool has_cached_size() const { return has_cached_size_; }

  // Returns `message.ByteSizeLong()` faster.
  //
  // Requires that these `SerializeMessageOptions` will be used only for
  // serializing this `message`, which will not be modified in the meantime.
  //
  // This consults and updates `has_cached_size()` in these
  // `SerializeMessageOptions`.
  size_t GetByteSize(const google::protobuf::MessageLite& message);

 private:
  bool partial_ = false;
  bool deterministic_ = google::protobuf::io::CodedOutputStream::
      IsDefaultSerializationDeterministic();
  bool has_cached_size_ = false;
};

using SerializeOptions ABSL_DEPRECATE_AND_INLINE() = SerializeMessageOptions;

// Writes the message in binary format to the given `Writer`.
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
absl::Status SerializeMessage(
    const google::protobuf::MessageLite& src, Dest&& dest,
    SerializeMessageOptions options = SerializeMessageOptions());

// Writes the message in binary format to the given `BackwardWriter`.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the `BackwardWriter`. `Dest` must support
// `DependencyRef<BackwardWriter*, Dest>`, e.g. `BackwardWriter&` (not owned),
// `ChainBackwardWriter<>` (owned), `std::unique_ptr<BackwardWriter>` (owned),
// `AnyRef<BackwardWriter*>` (maybe owned).
//
// Returns status:
//  * `status.ok()`  - success (`dest` is written to)
//  * `!status.ok()` - failure (`dest` is unspecified)
template <
    typename Dest,
    std::enable_if_t<TargetRefSupportsDependency<BackwardWriter*, Dest>::value,
                     int> = 0>
absl::Status SerializeMessage(
    const google::protobuf::MessageLite& src, Dest&& dest,
    SerializeMessageOptions options = SerializeMessageOptions());

// Writes the message length as varint32, then the message in binary format to
// the given `Writer`.
//
// Returns status:
//  * `status.ok()`  - success (`dest` is written to)
//  * `!status.ok()` - failure (`dest` is unspecified)
absl::Status SerializeLengthPrefixedMessage(
    const google::protobuf::MessageLite& src, Writer& dest,
    SerializeMessageOptions options = SerializeMessageOptions());

// Writes the message in binary format to `dest`, clearing any existing data in
// `dest`.
//
// Returns status:
//  * `status.ok()`  - success (`dest` is filled)
//  * `!status.ok()` - failure (`dest` is unspecified)
absl::Status SerializeMessage(
    const google::protobuf::MessageLite& src, std::string& dest,
    SerializeMessageOptions options = SerializeMessageOptions());
absl::Status SerializeMessage(
    const google::protobuf::MessageLite& src, CompactString& dest,
    SerializeMessageOptions options = SerializeMessageOptions());
absl::Status SerializeMessage(
    const google::protobuf::MessageLite& src, Chain& dest,
    SerializeMessageOptions options = SerializeMessageOptions());
absl::Status SerializeMessage(
    const google::protobuf::MessageLite& src, absl::Cord& dest,
    SerializeMessageOptions options = SerializeMessageOptions());

// Adapts a `Writer` to a `google::protobuf::io::ZeroCopyOutputStream`.
class WriterOutputStream : public google::protobuf::io::ZeroCopyOutputStream {
 public:
  // Creates a dummy `WriterOutputStream`. Use `Reset(dest)` to initialize it to
  // usable state.
  WriterOutputStream() = default;

  // Will write to `*dest`.
  explicit WriterOutputStream(Writer* dest ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : dest_(RIEGELI_EVAL_ASSERT_NOTNULL(dest)) {}

  ABSL_ATTRIBUTE_REINITIALIZES void Reset() { dest_ = nullptr; }
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Writer* dest) {
    dest_ = RIEGELI_EVAL_ASSERT_NOTNULL(dest);
  }

  bool Next(void** data, int* size) override;
  void BackUp(int length) override;
  int64_t ByteCount() const override;
  bool WriteCord(const absl::Cord& src) override;

 private:
  Writer* dest_ = nullptr;
};

// Implementation details follow.

inline size_t SerializeMessageOptions::GetByteSize(
    const google::protobuf::MessageLite& message) {
  if (has_cached_size()) return IntCast<size_t>(message.GetCachedSize());
  const size_t size = message.ByteSizeLong();
  if (ABSL_PREDICT_TRUE(size <=
                        uint32_t{std::numeric_limits<int32_t>::max()})) {
    set_has_cached_size(true);
  }
  return size;
}

namespace serialize_message_internal {

absl::Status SerializeMessageImpl(const google::protobuf::MessageLite& src,
                                  Writer& dest, SerializeMessageOptions options,
                                  bool set_write_hint);

absl::Status SerializeMessageImpl(const google::protobuf::MessageLite& src,
                                  BackwardWriter& dest,
                                  SerializeMessageOptions options,
                                  bool set_write_hint);

}  // namespace serialize_message_internal

template <
    typename Dest,
    std::enable_if_t<TargetRefSupportsDependency<Writer*, Dest>::value, int>>
inline absl::Status SerializeMessage(const google::protobuf::MessageLite& src,
                                     Dest&& dest,
                                     SerializeMessageOptions options) {
  DependencyRef<Writer*, Dest> dest_dep(std::forward<Dest>(dest));
  absl::Status status = serialize_message_internal::SerializeMessageImpl(
      src, *dest_dep, options, dest_dep.IsOwning());
  if (dest_dep.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_dep->Close())) {
      status.Update(dest_dep->status());
    }
  }
  return status;
}

template <typename Dest,
          std::enable_if_t<
              TargetRefSupportsDependency<BackwardWriter*, Dest>::value, int>>
inline absl::Status SerializeMessage(const google::protobuf::MessageLite& src,
                                     Dest&& dest,
                                     SerializeMessageOptions options) {
  DependencyRef<BackwardWriter*, Dest> dest_dep(std::forward<Dest>(dest));
  absl::Status status = serialize_message_internal::SerializeMessageImpl(
      src, *dest_dep, options, dest_dep.IsOwning());
  if (dest_dep.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_dep->Close())) {
      status.Update(dest_dep->status());
    }
  }
  return status;
}

}  // namespace riegeli

#endif  // RIEGELI_MESSAGES_SERIALIZE_MESSAGE_H_
