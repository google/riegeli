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

#ifndef RIEGELI_CHUNK_ENCODING_TRANSPOSE_INTERNAL_H_
#define RIEGELI_CHUNK_ENCODING_TRANSPOSE_INTERNAL_H_

#include <stdint.h>

#include "riegeli/base/assert.h"
#include "riegeli/base/constexpr.h"
#include "riegeli/messages/message_wire_format.h"
#include "riegeli/varint/varint_writing.h"

namespace riegeli {
namespace chunk_encoding_internal {

enum class MessageId : uint32_t {
  kNoOp,
  kNonProto,
  kStartOfSubmessage,
  kStartOfMessage,
  // `kRoot` marks the root node in memory. It is never encoded.
  kRoot,
  // Remaining message ids are assigned sequentially one per `NodeId`.
};

inline MessageId operator+(MessageId a, uint32_t b) {
  return static_cast<MessageId>(static_cast<uint32_t>(a) + b);
}

inline MessageId& operator++(MessageId& a) { return a = a + 1; }

static_assert(static_cast<uint32_t>(MessageId::kRoot) <= 8,
              "Reserved ids must not overlap valid proto tags");

// `kSubmessageWireType` does marks the end of a submessage, distinguishing it
// from the end of a string or bytes field, which is encoded using
// `WireType::kLengthDelimited`.
RIEGELI_INLINE_CONSTEXPR(WireType, kSubmessageWireType,
                         static_cast<WireType>(6));

enum class Subtype : uint8_t {
  kTrivial = 0,

  // Subtypes of `WireType::kVarint`:
  // Varint of the given length, in the buffer.
  kVarint1 = 0,
  kVarintMax = static_cast<uint8_t>(kVarint1) + kMaxLengthVarint64 - 1,
  // Varint of the given value, inline.
  kVarintInline0 = static_cast<uint8_t>(kVarintMax) + 1,
  kVarintInlineMax = static_cast<uint8_t>(kVarintInline0) + 0x7f,

  // Subtypes of `WireType::kLengthDelimited`:
  kLengthDelimitedString = 0,
  kLengthDelimitedStartOfSubmessage = 1,
  kLengthDelimitedEndOfSubmessage = 2,
};

inline Subtype operator+(Subtype a, uint8_t b) {
  return static_cast<Subtype>(static_cast<uint8_t>(a) + b);
}

inline uint8_t operator-(Subtype a, Subtype b) {
  return static_cast<uint8_t>(a) - static_cast<uint8_t>(b);
}

// Returns whether `tag`/`subtype` pair has a data buffer.
// Precondition: `tag` is a valid proto tag.
inline bool HasDataBuffer(uint32_t tag, Subtype subtype) {
  switch (GetTagWireType(tag)) {
    case WireType::kVarint:
      // Protocol buffer has buffer if value is not inlined.
      return subtype < Subtype::kVarintInline0;
    case WireType::kFixed32:
    case WireType::kFixed64:
      return true;
    case WireType::kLengthDelimited:
      // If subtype is `kLengthDelimitedStartOfSubmessage` or
      // `kLengthDelimitedEndOfSubmessage`, we have no buffer.
      return subtype == Subtype::kLengthDelimitedString;
    case WireType::kStartGroup:
    case WireType::kEndGroup:
      return false;
    default:
      RIEGELI_ASSERT_UNREACHABLE() << "Unknown wire type in " << tag;
  }
}

// Returns `true` if this tag is followed by subtype.
// Precondition: `tag` is a valid proto tag.
inline bool HasSubtype(uint32_t tag) {
  switch (GetTagWireType(tag)) {
    case WireType::kVarint:
      return true;
      // A `kLengthDelimited` tag is not followed by subtype, even though
      // `kLengthDelimited` nodes have subtypes, because submessage start is
      // encoded as `MessageId::kStartOfSubmessage`, and submessage end is
      // encoded with `kSubmessageWireType` that is taken into account before
      // calling this method.
    case WireType::kFixed32:
    case WireType::kFixed64:
    case WireType::kLengthDelimited:
    case WireType::kStartGroup:
    case WireType::kEndGroup:
      return false;
    default:
      RIEGELI_ASSERT_UNREACHABLE() << "Unknown wire type in " << tag;
  }
}

}  // namespace chunk_encoding_internal
}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_TRANSPOSE_INTERNAL_H_
