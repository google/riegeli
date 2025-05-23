// Copyright 2020 Google LLC
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

#ifndef RIEGELI_MESSAGES_MESSAGE_WIRE_FORMAT_H_
#define RIEGELI_MESSAGES_MESSAGE_WIRE_FORMAT_H_

#include <stddef.h>
#include <stdint.h>

#include <limits>

#include "absl/base/attributes.h"
#include "absl/base/casts.h"
#include "absl/base/optimization.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/constexpr.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/endian/endian_writing.h"
#include "riegeli/varint/varint_writing.h"

namespace riegeli {

// Low level functions for writing and reading serialized proto messages
// directly.
//
// They mostly correspond to selected members of
// `google::protobuf::internal::WireFormatLite`.

// The part of a field tag which denotes the representation of the field value
// which follows the tag.
enum class WireType : uint32_t {
  kVarint = 0,
  kFixed64 = 1,
  kLengthDelimited = 2,
  kStartGroup = 3,
  kEndGroup = 4,
  kFixed32 = 5,
};

// Composes/decomposes a field tag.
constexpr uint32_t MakeTag(int field_number, WireType wire_type);
constexpr WireType GetTagWireType(uint32_t tag);
constexpr int GetTagFieldNumber(uint32_t tag);

// Write a scalar field, prefixed with its tag.
ABSL_DEPRECATED("Use SerializedMessageWriter::WriteUInt32() instead.")
bool WriteVarint32WithTag(int field_number, uint32_t data, Writer& dest);
ABSL_DEPRECATED(
    "Use SerializedMessageWriter::WriteUInt64(), WriteInt64(), or "
    "WriteInt32() instead.")
bool WriteVarint64WithTag(int field_number, uint64_t data, Writer& dest);
ABSL_DEPRECATED("Use SerializedMessageWriter::WriteSInt32() instead.")
bool WriteVarintSigned32WithTag(int field_number, int32_t data, Writer& dest);
ABSL_DEPRECATED("Use SerializedMessageWriter::WriteSInt64() instead.")
bool WriteVarintSigned64WithTag(int field_number, int64_t data, Writer& dest);
ABSL_DEPRECATED("Use SerializedMessageWriter::WriteFixed32() instead.")
bool WriteFixed32WithTag(int field_number, uint32_t data, Writer& dest);
ABSL_DEPRECATED("Use SerializedMessageWriter::WriteFixed64() instead.")
bool WriteFixed64WithTag(int field_number, uint64_t data, Writer& dest);
ABSL_DEPRECATED("Use SerializedMessageWriter::WriteSFixed32() instead.")
bool WriteFixedSigned32WithTag(int field_number, int32_t data, Writer& dest);
ABSL_DEPRECATED("Use SerializedMessageWriter::WriteSFixed64() instead.")
bool WriteFixedSigned64WithTag(int field_number, int64_t data, Writer& dest);
ABSL_DEPRECATED("Use SerializedMessageWriter::WriteFloat() instead.")
bool WriteFloatWithTag(int field_number, float data, Writer& dest);
ABSL_DEPRECATED("Use SerializedMessageWriter::WriteDouble() instead.")
bool WriteDoubleWithTag(int field_number, double data, Writer& dest);

// Write the length of a length-delimited field, prefixed with its tag.
ABSL_DEPRECATED(
    "Use SerializedMessageWriter::WriteString(), CopyString(), "
    "WriteSerializedMessage(), {Open,Close}LengthDelimited(), or "
    "WriteLengthUnchecked() instead.")
bool WriteLengthWithTag(int field_number, Position length, Writer& dest);

// Write a scalar field, prefixed with its tag.
ABSL_DEPRECATED("Use SerializedMessageBackwardWriter::WriteUInt32() instead.")
bool WriteVarint32WithTag(int field_number, uint32_t data,
                          BackwardWriter& dest);
ABSL_DEPRECATED(
    "Use SerializedMessageBackwardWriter::WriteUInt64(), WriteInt64(), or "
    "WriteInt32() instead.")
bool WriteVarint64WithTag(int field_number, uint64_t data,
                          BackwardWriter& dest);
ABSL_DEPRECATED("Use SerializedMessageBackwardWriter::WriteSInt32() instead.")
bool WriteVarintSigned32WithTag(int field_number, int32_t data,
                                BackwardWriter& dest);
ABSL_DEPRECATED("Use SerializedMessageBackwardWriter::WriteSInt64() instead.")
bool WriteVarintSigned64WithTag(int field_number, int64_t data,
                                BackwardWriter& dest);
ABSL_DEPRECATED("Use SerializedMessageBackwardWriter::WriteFixed32() instead.")
bool WriteFixed32WithTag(int field_number, uint32_t data, BackwardWriter& dest);
ABSL_DEPRECATED("Use SerializedMessageBackwardWriter::WriteFixed64() instead.")
bool WriteFixed64WithTag(int field_number, uint64_t data, BackwardWriter& dest);
ABSL_DEPRECATED("Use SerializedMessageBackwardWriter::WriteSFixed32() instead.")
bool WriteFixedSigned32WithTag(int field_number, int32_t data,
                               BackwardWriter& dest);
ABSL_DEPRECATED("Use SerializedMessageBackwardWriter::WriteSFixed64() instead.")
bool WriteFixedSigned64WithTag(int field_number, int64_t data,
                               BackwardWriter& dest);
ABSL_DEPRECATED("Use SerializedMessageBackwardWriter::WriteFloat() instead.")
bool WriteFloatWithTag(int field_number, float data, BackwardWriter& dest);
ABSL_DEPRECATED("Use SerializedMessageBackwardWriter::WriteDouble() instead.")
bool WriteDoubleWithTag(int field_number, double data, BackwardWriter& dest);

// Write the length of a length-delimited field, prefixed with its tag.
ABSL_DEPRECATED(
    "Use SerializedMessageBackwardWriter::WriteString(), CopyString(), "
    "WriteSerializedMessage(), {Open,Close}LengthDelimited(), or "
    "WriteLengthUnchecked() instead.")
bool WriteLengthWithTag(int field_number, Position length,
                        BackwardWriter& dest);

// Implementation details follow.

constexpr uint32_t MakeTag(int field_number, WireType wire_type) {
  return (static_cast<uint32_t>(field_number) << 3) |
         static_cast<uint32_t>(wire_type);
}

constexpr WireType GetTagWireType(uint32_t tag) {
  return static_cast<WireType>(tag & 7);
}

constexpr int GetTagFieldNumber(uint32_t tag) {
  return static_cast<int>(tag >> 3);
}

ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool WriteVarint32WithTag(int field_number,
                                                              uint32_t data,
                                                              Writer& dest) {
  const uint32_t tag = MakeTag(field_number, WireType::kVarint);
  if (ABSL_PREDICT_FALSE(
          !dest.Push((RIEGELI_IS_CONSTANT(tag) ||
                              (RIEGELI_IS_CONSTANT(tag < 0x80) && tag < 0x80)
                          ? LengthVarint32(tag)
                          : kMaxLengthVarint32) +
                     (RIEGELI_IS_CONSTANT(data) ||
                              (RIEGELI_IS_CONSTANT(data < 0x80) && data < 0x80)
                          ? LengthVarint32(data)
                          : kMaxLengthVarint32)))) {
    return false;
  }
  char* ptr = WriteVarint32(tag, dest.cursor());
  ptr = WriteVarint32(data, ptr);
  dest.set_cursor(ptr);
  return true;
}

ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool WriteVarint64WithTag(int field_number,
                                                              uint64_t data,
                                                              Writer& dest) {
  const uint32_t tag = MakeTag(field_number, WireType::kVarint);
  if (ABSL_PREDICT_FALSE(
          !dest.Push((RIEGELI_IS_CONSTANT(tag) ||
                              (RIEGELI_IS_CONSTANT(tag < 0x80) && tag < 0x80)
                          ? LengthVarint32(tag)
                          : kMaxLengthVarint32) +
                     (RIEGELI_IS_CONSTANT(data) ||
                              (RIEGELI_IS_CONSTANT(data < 0x80) && data < 0x80)
                          ? LengthVarint64(data)
                          : kMaxLengthVarint64)))) {
    return false;
  }
  char* ptr = WriteVarint32(tag, dest.cursor());
  ptr = WriteVarint64(data, ptr);
  dest.set_cursor(ptr);
  return true;
}

ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool WriteVarintSigned32WithTag(
    int field_number, int32_t data, Writer& dest) {
  return WriteVarint32WithTag(field_number, EncodeVarintSigned32(data), dest);
}

ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool WriteVarintSigned64WithTag(
    int field_number, int64_t data, Writer& dest) {
  return WriteVarint64WithTag(field_number, EncodeVarintSigned64(data), dest);
}

ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool WriteFixed32WithTag(int field_number,
                                                             uint32_t data,
                                                             Writer& dest) {
  const uint32_t tag = MakeTag(field_number, WireType::kFixed32);
  if (ABSL_PREDICT_FALSE(!dest.Push(LengthVarint32(tag) + sizeof(uint32_t)))) {
    return false;
  }
  char* ptr = WriteVarint32(tag, dest.cursor());
  WriteLittleEndian32(data, ptr);
  ptr += sizeof(uint32_t);
  dest.set_cursor(ptr);
  return true;
}

ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool WriteFixed64WithTag(int field_number,
                                                             uint64_t data,
                                                             Writer& dest) {
  const uint32_t tag = MakeTag(field_number, WireType::kFixed64);
  if (ABSL_PREDICT_FALSE(!dest.Push(LengthVarint32(tag) + sizeof(uint64_t)))) {
    return false;
  }
  char* ptr = WriteVarint32(tag, dest.cursor());
  WriteLittleEndian64(data, ptr);
  ptr += sizeof(uint64_t);
  dest.set_cursor(ptr);
  return true;
}

ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool WriteFixedSigned32WithTag(
    int field_number, int32_t data, Writer& dest) {
  return WriteFixed32WithTag(field_number, static_cast<uint32_t>(data), dest);
}

ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool WriteFixedSigned64WithTag(
    int field_number, int64_t data, Writer& dest) {
  return WriteFixed64WithTag(field_number, static_cast<uint64_t>(data), dest);
}

ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool WriteFloatWithTag(int field_number,
                                                           float data,
                                                           Writer& dest) {
  return WriteFixed32WithTag(field_number, absl::bit_cast<uint32_t>(data),
                             dest);
}

ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool WriteDoubleWithTag(int field_number,
                                                            double data,
                                                            Writer& dest) {
  return WriteFixed64WithTag(field_number, absl::bit_cast<uint64_t>(data),
                             dest);
}

namespace message_wire_format_internal {

ABSL_ATTRIBUTE_COLD bool FailLengthOverflow(Object& dest, Position length);

}  // namespace message_wire_format_internal

ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool WriteLengthWithTag(int field_number,
                                                            Position length,
                                                            Writer& dest) {
  if (ABSL_PREDICT_FALSE(length >
                         uint32_t{std::numeric_limits<int32_t>::max()})) {
    return message_wire_format_internal::FailLengthOverflow(dest, length);
  }
  const uint32_t tag = MakeTag(field_number, WireType::kLengthDelimited);
  if (ABSL_PREDICT_FALSE(!dest.Push(
          (RIEGELI_IS_CONSTANT(tag) ||
                   (RIEGELI_IS_CONSTANT(tag < 0x80) && tag < 0x80)
               ? LengthVarint32(tag)
               : kMaxLengthVarint32) +
          (RIEGELI_IS_CONSTANT(length) ||
                   (RIEGELI_IS_CONSTANT(length < 0x80) && length < 0x80)
               ? LengthVarint32(IntCast<uint32_t>(length))
               : kMaxLengthVarint32)))) {
    return false;
  }
  char* ptr = WriteVarint32(tag, dest.cursor());
  ptr = WriteVarint32(IntCast<uint32_t>(length), ptr);
  dest.set_cursor(ptr);
  return true;
}

ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool WriteVarint32WithTag(
    int field_number, uint32_t data, BackwardWriter& dest) {
  const uint32_t tag = MakeTag(field_number, WireType::kVarint);
  const size_t length = LengthVarint32(tag) + LengthVarint32(data);
  if (ABSL_PREDICT_FALSE(!dest.Push(length))) return false;
  dest.move_cursor(length);
  char* const ptr = WriteVarint32(tag, dest.cursor());
  WriteVarint32(data, ptr);
  return true;
}

ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool WriteVarint64WithTag(
    int field_number, uint64_t data, BackwardWriter& dest) {
  const uint32_t tag = MakeTag(field_number, WireType::kVarint);
  const size_t length = LengthVarint32(tag) + LengthVarint64(data);
  if (ABSL_PREDICT_FALSE(!dest.Push(length))) return false;
  dest.move_cursor(length);
  char* const ptr = WriteVarint32(tag, dest.cursor());
  WriteVarint64(data, ptr);
  return true;
}

ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool WriteVarintSigned32WithTag(
    int field_number, int32_t data, BackwardWriter& dest) {
  return WriteVarint32WithTag(field_number, EncodeVarintSigned32(data), dest);
}

ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool WriteVarintSigned64WithTag(
    int field_number, int64_t data, BackwardWriter& dest) {
  return WriteVarint64WithTag(field_number, EncodeVarintSigned64(data), dest);
}

ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool WriteFixed32WithTag(
    int field_number, uint32_t data, BackwardWriter& dest) {
  const uint32_t tag = MakeTag(field_number, WireType::kFixed32);
  const size_t length = LengthVarint32(tag) + sizeof(uint32_t);
  if (ABSL_PREDICT_FALSE(!dest.Push(length))) return false;
  dest.move_cursor(length);
  char* const ptr = WriteVarint32(tag, dest.cursor());
  WriteLittleEndian32(data, ptr);
  return true;
}

ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool WriteFixed64WithTag(
    int field_number, uint64_t data, BackwardWriter& dest) {
  const uint32_t tag = MakeTag(field_number, WireType::kFixed64);
  const size_t length = LengthVarint32(tag) + sizeof(uint64_t);
  if (ABSL_PREDICT_FALSE(!dest.Push(length))) return false;
  dest.move_cursor(length);
  char* const ptr = WriteVarint32(tag, dest.cursor());
  WriteLittleEndian64(data, ptr);
  return true;
}

ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool WriteFixedSigned32WithTag(
    int field_number, int32_t data, BackwardWriter& dest) {
  return WriteFixed32WithTag(field_number, static_cast<uint32_t>(data), dest);
}

ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool WriteFixedSigned64WithTag(
    int field_number, int64_t data, BackwardWriter& dest) {
  return WriteFixed64WithTag(field_number, static_cast<uint64_t>(data), dest);
}

ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool WriteFloatWithTag(
    int field_number, float data, BackwardWriter& dest) {
  return WriteFixed32WithTag(field_number, absl::bit_cast<uint32_t>(data),
                             dest);
}

ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool WriteDoubleWithTag(
    int field_number, double data, BackwardWriter& dest) {
  return WriteFixed64WithTag(field_number, absl::bit_cast<uint64_t>(data),
                             dest);
}

ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool WriteLengthWithTag(
    int field_number, Position length, BackwardWriter& dest) {
  if (ABSL_PREDICT_FALSE(length >
                         uint32_t{std::numeric_limits<int32_t>::max()})) {
    return message_wire_format_internal::FailLengthOverflow(dest, length);
  }
  const uint32_t tag = MakeTag(field_number, WireType::kLengthDelimited);
  const size_t header_length =
      LengthVarint32(tag) + LengthVarint32(IntCast<uint32_t>(length));
  if (ABSL_PREDICT_FALSE(!dest.Push(header_length))) return false;
  dest.move_cursor(header_length);
  char* const ptr = WriteVarint32(tag, dest.cursor());
  WriteVarint32(IntCast<uint32_t>(length), ptr);
  return true;
}

}  // namespace riegeli

#endif  // RIEGELI_MESSAGES_MESSAGE_WIRE_FORMAT_H_
