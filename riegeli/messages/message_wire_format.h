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

#include <stdint.h>

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
  kFixed32 = 5,
  kFixed64 = 1,
  kLengthDelimited = 2,
  kStartGroup = 3,
  kEndGroup = 4,
  kInvalid6 = 6,
  kInvalid7 = 7,
};

// Composes/decomposes a field tag.
constexpr uint32_t MakeTag(int field_number, WireType wire_type);
constexpr WireType GetTagWireType(uint32_t tag);
constexpr int GetTagFieldNumber(uint32_t tag);

// Represents a set of wire types.

enum class WireTypeSet : uint32_t {
  kVarint = 1 << static_cast<uint32_t>(WireType::kVarint),
  kFixed32 = 1 << static_cast<uint32_t>(WireType::kFixed32),
  kFixed64 = 1 << static_cast<uint32_t>(WireType::kFixed64),
  kLengthDelimited = 1 << static_cast<uint32_t>(WireType::kLengthDelimited),
  kStartGroup = 1 << static_cast<uint32_t>(WireType::kStartGroup),
  kEndGroup = 1 << static_cast<uint32_t>(WireType::kEndGroup),
  kInvalid6 = 1 << static_cast<uint32_t>(WireType::kInvalid6),
  kInvalid7 = 1 << static_cast<uint32_t>(WireType::kInvalid7),
};

constexpr WireTypeSet operator|(WireTypeSet a, WireTypeSet b) {
  return WireTypeSet{static_cast<uint32_t>(a) | static_cast<uint32_t>(b)};
}

constexpr WireTypeSet operator^(WireTypeSet a, WireTypeSet b) {
  return WireTypeSet{static_cast<uint32_t>(a) ^ static_cast<uint32_t>(b)};
}

constexpr WireTypeSet operator&(WireTypeSet a, WireTypeSet b) {
  return WireTypeSet{static_cast<uint32_t>(a) & static_cast<uint32_t>(b)};
}

template <WireType... wire_types>
constexpr WireTypeSet WireTypeSetOf() {
  return (WireTypeSet{0} | ... |
          WireTypeSet{uint32_t{1} << static_cast<uint32_t>(wire_types)});
}

constexpr WireTypeSet NoWireTypes = WireTypeSetOf<>();

constexpr WireTypeSet AllWireTypes =
    WireTypeSetOf<WireType::kVarint, WireType::kFixed32, WireType::kFixed64,
                  WireType::kLengthDelimited, WireType::kStartGroup,
                  WireType::kEndGroup, WireType::kInvalid6,
                  WireType::kInvalid7>();

constexpr WireTypeSet operator~(WireTypeSet a) { return AllWireTypes ^ a; }

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

}  // namespace riegeli

#endif  // RIEGELI_MESSAGES_MESSAGE_WIRE_FORMAT_H_
