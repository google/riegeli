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

#ifndef RIEGELI_BYTES_MESSAGE_WIRE_FORMAT_H_
#define RIEGELI_BYTES_MESSAGE_WIRE_FORMAT_H_

#include <stdint.h>

#include "absl/base/casts.h"

namespace riegeli {

// Low level functions for writing and reading serialized proto messages
// directly.
//
// They correspond to selected members of
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
WireType GetTagWireType(uint32_t tag);
int GetTagFieldNumber(uint32_t tag);

// Encodes/decodes the value of a `float` field.
// Used together with `{Write,Read}LittleEndian32()`.
uint32_t EncodeFloat(float value);
float DecodeFloat(uint32_t repr);
// Encodes/decodes the value of a `double` field.
// Used together with `{Write,Read}LittleEndian64()`.
uint64_t EncodeDouble(double value);
double DecodeDouble(uint64_t repr);

// Encodes/decodes the value of a `sint32` field.
// Used together with `{Write,Read}Varint32()`.
uint32_t EncodeSint32(int32_t value);
int32_t DecodeSint32(uint32_t repr);
// Encodes/decodes the value of a `sint64` field.
// Used together with `{Write,Read}Varint64()`.
uint64_t EncodeSint64(int64_t value);
int64_t DecodeSint64(uint64_t repr);

// Implementation details follow.

constexpr inline uint32_t MakeTag(int field_number, WireType wire_type) {
  return (static_cast<uint32_t>(field_number) << 3) |
         static_cast<uint32_t>(wire_type);
}

inline WireType GetTagWireType(uint32_t tag) {
  return static_cast<WireType>(tag & 7);
}

inline int GetTagFieldNumber(uint32_t tag) {
  return static_cast<int>(tag >> 3);
}

inline uint32_t EncodeFloat(float value) {
  return absl::bit_cast<uint32_t>(value);
}

inline float DecodeFloat(uint32_t repr) { return absl::bit_cast<float>(repr); }

inline uint64_t EncodeDouble(double value) {
  return absl::bit_cast<uint64_t>(value);
}

inline double DecodeDouble(uint64_t repr) {
  return absl::bit_cast<double>(repr);
}

inline uint32_t EncodeSint32(int32_t value) {
  return (static_cast<uint32_t>(value) << 1) ^
         static_cast<uint32_t>(value >> 31);
}

inline int32_t DecodeSint32(uint32_t repr) {
  return static_cast<int32_t>((repr >> 1) ^ (~(repr & 1) + 1));
}

inline uint64_t EncodeSint64(int64_t value) {
  return (static_cast<uint64_t>(value) << 1) ^
         static_cast<uint64_t>(value >> 63);
}

inline int64_t DecodeSint64(uint64_t repr) {
  return static_cast<int64_t>((repr >> 1) ^ (~(repr & 1) + 1));
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_MESSAGE_WIRE_FORMAT_H_
