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

#include "riegeli/chunk_encoding/transpose_decoder.h"

#include <stddef.h>
#include <stdint.h>
#include <cstring>
#include <limits>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/memory/memory.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/memory.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/backward_writer_utils.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/limiting_backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/reader_utils.h"
#include "riegeli/bytes/writer_utils.h"
#include "riegeli/chunk_encoding/decompressor.h"
#include "riegeli/chunk_encoding/transpose_internal.h"
#include "riegeli/chunk_encoding/types.h"

namespace riegeli {

namespace {

ChainReader* kEmptyChainReader() {
  static NoDestructor<ChainReader> kStaticEmptyChainReader((Chain()));
  RIEGELI_ASSERT(kStaticEmptyChainReader->healthy())
      << "kEmptyChainReader() has been closed";
  return kStaticEmptyChainReader.get();
}

constexpr uint32_t kInvalidPos = std::numeric_limits<uint32_t>::max();

// Hash pair of uint32_ts.
struct PairHasher {
  size_t operator()(std::pair<uint32_t, uint32_t> p) const {
    return internal::Murmur3_64((uint64_t{p.first} << 32) | uint64_t{p.second});
  }
};

// Information about one data bucket used in filtering.
struct DataBucket {
  // Contains sizes of data buffers in the bucket if "decompressed" is false.
  std::vector<size_t> buffer_sizes;
  // Decompressed data buffers if "decompressed" is true.
  std::vector<ChainReader> buffers;
  // Raw bucket data.
  Chain compressed_data;
  // True if the bucket was already decompressed.
  bool decompressed = false;
};

// Should the data content of the field be decoded?
enum class FieldIncluded {
  kYes,
  kNo,
  kExistenceOnly,
};

// Return true if "tag" is a valid protocol buffer tag.
bool ValidTag(uint32_t tag) {
  switch (static_cast<internal::WireType>(tag & 7)) {
    case internal::WireType::kVarint:
    case internal::WireType::kFixed32:
    case internal::WireType::kFixed64:
    case internal::WireType::kLengthDelimited:
    case internal::WireType::kStartGroup:
    case internal::WireType::kEndGroup:
      return tag >= 8;
    default:
      return false;
  }
}

}  // namespace

namespace internal {

// The types of callbacks in state machine states.
// NOTE: CallbackType is used to index labels array in DecodeToBuffer method so
// the ordering of CallbackType must not change without a corresponding change
// in DecodeToBuffer.
enum class CallbackType : uint8_t {
  kNoOp,
  kMessageStart,
  kSubmessageStart,
  kSubmessageEnd,
  kSelectCallback,
  kSkippedSubmessageStart,
  kSkippedSubmessageEnd,
  kNonProto,
  kFailure,

// CopyTag_ has to be the first CallbackType in TYPES_FOR_TAG_LEN for
// GetCopyTagCallbackType() to work.
#define TYPES_FOR_TAG_LEN(tag_length)                                         \
  kCopyTag_##tag_length, kVarint_1_##tag_length, kVarint_2_##tag_length,      \
      kVarint_3_##tag_length, kVarint_4_##tag_length, kVarint_5_##tag_length, \
      kVarint_6_##tag_length, kVarint_7_##tag_length, kVarint_8_##tag_length, \
      kVarint_9_##tag_length, kVarint_10_##tag_length, kFixed32_##tag_length, \
      kFixed64_##tag_length, kString_##tag_length,                            \
      kStartFilterGroup_##tag_length, kEndFilterGroup_##tag_length

  TYPES_FOR_TAG_LEN(1),
  TYPES_FOR_TAG_LEN(2),
  TYPES_FOR_TAG_LEN(3),
  TYPES_FOR_TAG_LEN(4),
  TYPES_FOR_TAG_LEN(5),
#undef TYPES_FOR_TAG_LEN

  // We need kCopyTag callback for length 6 as well because of inline numerics.
  // kCopyTag_6 has to be the first CallbackType after TYPES_FOR_TAG_LEN for
  // GetCopyTagCallbackType() to work.
  kCopyTag_6,
  kUnknown,
  // Implicit callback type is added to any of the above types if the transition
  // from the node should go to "node->next_node" without reading the transition
  // byte.
  kImplicit = 0x80,
};

constexpr CallbackType operator+(CallbackType a, uint8_t b) {
  return static_cast<CallbackType>(static_cast<uint8_t>(a) + b);
}

constexpr uint8_t operator-(CallbackType a, CallbackType b) {
  return static_cast<uint8_t>(a) - static_cast<uint8_t>(b);
}

constexpr CallbackType operator|(CallbackType a, CallbackType b) {
  return static_cast<CallbackType>(static_cast<uint8_t>(a) |
                                   static_cast<uint8_t>(b));
}

constexpr CallbackType operator&(CallbackType a, CallbackType b) {
  return static_cast<CallbackType>(static_cast<uint8_t>(a) &
                                   static_cast<uint8_t>(b));
}

inline CallbackType& operator|=(CallbackType& a, CallbackType b) {
  return a = a | b;
}

// Returns copy_tag callback type for "tag_length".
inline CallbackType GetCopyTagCallbackType(size_t tag_length) {
  RIEGELI_ASSERT_GT(tag_length, 0u) << "Zero tag length";
  RIEGELI_ASSERT_LE(tag_length, kMaxLengthVarint32() + 1)
      << "Tag length too large";
  return CallbackType::kCopyTag_1 +
         (tag_length - 1) *
             (CallbackType::kCopyTag_2 - CallbackType::kCopyTag_1);
}

// Returns numeric callback type for "subtype" and "tag_length".
inline CallbackType GetVarintCallbackType(Subtype subtype, size_t tag_length) {
  RIEGELI_ASSERT_GT(tag_length, 0u) << "Zero tag length";
  RIEGELI_ASSERT_LE(tag_length, kMaxLengthVarint32()) << "Tag length too large";
  if (subtype > Subtype::kVarintInlineMax) {
    return CallbackType::kUnknown;
  }
  if (subtype >= Subtype::kVarintInline0) {
    return GetCopyTagCallbackType(tag_length + 1);
  }
  return CallbackType::kVarint_1_1 +
         (subtype - Subtype::kVarint1) *
             (CallbackType::kVarint_2_1 - CallbackType::kVarint_1_1) +
         (tag_length - 1) *
             (CallbackType::kVarint_1_2 - CallbackType::kVarint_1_1);
}

// Returns FLOAT binary callback type for "tag_length".
inline CallbackType GetFixed32CallbackType(size_t tag_length) {
  RIEGELI_ASSERT_GT(tag_length, 0u) << "Zero tag length";
  RIEGELI_ASSERT_LE(tag_length, kMaxLengthVarint32()) << "Tag length too large";
  return CallbackType::kFixed32_1 +
         (tag_length - 1) *
             (CallbackType::kFixed32_2 - CallbackType::kFixed32_1);
}

// Returns DOUBLE binary callback type for "tag_length".
inline CallbackType GetFixed64CallbackType(size_t tag_length) {
  RIEGELI_ASSERT_GT(tag_length, 0u) << "Zero tag length";
  RIEGELI_ASSERT_LE(tag_length, kMaxLengthVarint32()) << "Tag length too large";
  return CallbackType::kFixed64_1 +
         (tag_length - 1) *
             (CallbackType::kFixed64_2 - CallbackType::kFixed64_1);
}

// Returns string callback type for "subtype" and "tag_length".
inline CallbackType GetStringCallbackType(Subtype subtype, size_t tag_length) {
  RIEGELI_ASSERT_GT(tag_length, 0u) << "Zero tag length";
  RIEGELI_ASSERT_LE(tag_length, kMaxLengthVarint32()) << "Tag length too large";
  switch (subtype) {
    case Subtype::kLengthDelimitedString:
      return CallbackType::kString_1 +
             (tag_length - 1) *
                 (CallbackType::kString_2 - CallbackType::kString_1);
    case Subtype::kLengthDelimitedEndOfSubmessage:
      return CallbackType::kSubmessageEnd;
    default:
      // Note: Nodes with kLengthDelimitedStartOfSubmessage are not created.
      // Start of submessage is indicated with ReservedId
      // "kReservedIdStartOfSubmessage" and uses CallbackType::kSubmessageStart.
      return CallbackType::kUnknown;
  }
}

// Returns string callback type for "subtype" and "tag_length" to exclude field.
inline CallbackType GetStringExcludeCallbackType(Subtype subtype,
                                                 size_t tag_length) {
  RIEGELI_ASSERT_GT(tag_length, 0u) << "Zero tag length";
  RIEGELI_ASSERT_LE(tag_length, kMaxLengthVarint32()) << "Tag length too large";
  switch (subtype) {
    case Subtype::kLengthDelimitedString:
      return CallbackType::kNoOp;
    case Subtype::kLengthDelimitedEndOfSubmessage:
      return CallbackType::kSkippedSubmessageEnd;
    default:
      return CallbackType::kUnknown;
  }
}

// Returns string callback type for "subtype" and "tag_length" for existence
// only.
inline CallbackType GetStringExistenceCallbackType(Subtype subtype,
                                                   size_t tag_length) {
  RIEGELI_ASSERT_GT(tag_length, 0u) << "Zero tag length";
  RIEGELI_ASSERT_LE(tag_length, kMaxLengthVarint32()) << "Tag length too large";
  switch (subtype) {
    case Subtype::kLengthDelimitedString:
      // We use the fact that there is a zero stored in TagData. This decodes as
      // an empty string in proto decoder.
      // Note: Only submessages can be "existence only" filtered, but we encode
      // empty submessages as strings so we need to handle this callback type.
      return GetCopyTagCallbackType(tag_length + 1);
    case Subtype::kLengthDelimitedEndOfSubmessage:
      return CallbackType::kSubmessageEnd;
    default:
      return CallbackType::kUnknown;
  }
}

inline CallbackType GetStartFilterGroupCallbackType(size_t tag_length) {
  RIEGELI_ASSERT_GT(tag_length, 0u) << "Zero tag length";
  RIEGELI_ASSERT_LE(tag_length, kMaxLengthVarint32()) << "Tag length too large";
  return CallbackType::kStartFilterGroup_1 +
         (tag_length - 1) * (CallbackType::kStartFilterGroup_2 -
                             CallbackType::kStartFilterGroup_1);
}

inline CallbackType GetEndFilterGroupCallbackType(size_t tag_length) {
  RIEGELI_ASSERT_GT(tag_length, 0u) << "Zero tag length";
  RIEGELI_ASSERT_LE(tag_length, kMaxLengthVarint32()) << "Tag length too large";
  return CallbackType::kEndFilterGroup_1 +
         (tag_length - 1) * (CallbackType::kEndFilterGroup_2 -
                             CallbackType::kEndFilterGroup_1);
}

// Get callback for node.
inline CallbackType GetCallbackType(FieldIncluded field_included, uint32_t tag,
                                    Subtype subtype, size_t tag_length,
                                    bool filtering_enabled) {
  RIEGELI_ASSERT_GT(tag_length, 0u) << "Zero tag length";
  RIEGELI_ASSERT_LE(tag_length, kMaxLengthVarint32()) << "Tag length too large";
  switch (field_included) {
    case FieldIncluded::kYes:
      switch (static_cast<WireType>(tag & 7)) {
        case WireType::kVarint:
          return GetVarintCallbackType(subtype, tag_length);
        case WireType::kFixed32:
          return GetFixed32CallbackType(tag_length);
        case WireType::kFixed64:
          return GetFixed64CallbackType(tag_length);
        case WireType::kLengthDelimited:
          return GetStringCallbackType(subtype, tag_length);
        case WireType::kStartGroup:
          return filtering_enabled ? GetStartFilterGroupCallbackType(tag_length)
                                   : GetCopyTagCallbackType(tag_length);
        case WireType::kEndGroup:
          return filtering_enabled ? GetEndFilterGroupCallbackType(tag_length)
                                   : GetCopyTagCallbackType(tag_length);
        default:
          return CallbackType::kUnknown;
      }
    case FieldIncluded::kNo:
      switch (static_cast<WireType>(tag & 7)) {
        case WireType::kVarint:
        case WireType::kFixed32:
        case WireType::kFixed64:
          return CallbackType::kNoOp;
        case WireType::kLengthDelimited:
          return GetStringExcludeCallbackType(subtype, tag_length);
        case WireType::kStartGroup:
          return CallbackType::kSkippedSubmessageStart;
        case WireType::kEndGroup:
          return CallbackType::kSkippedSubmessageEnd;
        default:
          return CallbackType::kUnknown;
      }
    case FieldIncluded::kExistenceOnly:
      switch (static_cast<WireType>(tag & 7)) {
        case WireType::kVarint:
        case WireType::kFixed32:
        case WireType::kFixed64:
          return CallbackType::kUnknown;
        case WireType::kLengthDelimited:
          // Only submessage fields should be allowed to be "existence only".
          return GetStringExistenceCallbackType(subtype, tag_length);
        case WireType::kStartGroup:
          return GetStartFilterGroupCallbackType(tag_length);
        case WireType::kEndGroup:
          return GetEndFilterGroupCallbackType(tag_length);
        default:
          return CallbackType::kUnknown;
      }
  }
  RIEGELI_ASSERT_UNREACHABLE()
      << "Unknown FieldIncluded: " << static_cast<int>(field_included);
}

inline bool IsImplicit(CallbackType callback_type) {
  return (callback_type & CallbackType::kImplicit) == CallbackType::kImplicit;
}

}  // namespace internal

struct TransposeDecoder::Context {
  // Compression type of the input.
  CompressionType compression_type = CompressionType::kNone;
  // Buffer containing all the data.
  // Note: Used only when filtering is disabled.
  std::vector<ChainReader> buffers;
  // Buffer for lengths of nonproto messages.
  ChainReader* nonproto_lengths = nullptr;
  // State machine read from the input.
  std::vector<StateMachineNode> state_machine_nodes;
  // Node to start decoding from.
  uint32_t first_node = 0;
  // State machine transitions. One byte = one transition.
  internal::Decompressor transitions;

  // --- Fields used in filtering. ---
  // We number used fields with indices into "existence_only" vector below.
  // Fields form a tree structure stored in "include_fields" map. If "p" is
  // the index of parent submessage then "include_fields[<p,f>]" is the index
  // of the child with field number "f". The root index is assumed to be
  // "kInvalidPos".
  std::unordered_map<std::pair<uint32_t, uint32_t>, uint32_t, PairHasher>
      include_fields;
  // Are we interested in the existence of the field only?
  std::vector<bool> existence_only;
  // Data buckets.
  std::vector<DataBucket> buckets;
  // Template that can later be used later to finalize StateMachineNode.
  std::vector<StateMachineNodeTemplate> node_templates;
};

void TransposeDecoder::Done() {}

bool TransposeDecoder::Reset(Reader* src, uint64_t num_records,
                             uint64_t decoded_data_size,
                             const FieldFilter& field_filter,
                             BackwardWriter* dest,
                             std::vector<size_t>* limits) {
  RIEGELI_ASSERT_EQ(dest->pos(), 0u)
      << "Failed precondition of TransposeDecoder::Reset(): "
         "non-zero destination position";
  MarkHealthy();
  if (ABSL_PREDICT_FALSE(num_records > limits->max_size())) {
    return Fail("Too many records");
  }
  if (ABSL_PREDICT_FALSE(decoded_data_size >
                         std::numeric_limits<size_t>::max())) {
    return Fail("Records too large");
  }

  Context context;
  if (ABSL_PREDICT_FALSE(!Parse(&context, src, field_filter))) return false;
  LimitingBackwardWriter limiting_dest(dest, decoded_data_size);
  if (ABSL_PREDICT_FALSE(
          !Decode(&context, num_records, &limiting_dest, limits))) {
    limiting_dest.Close();
    return false;
  }
  if (ABSL_PREDICT_FALSE(!limiting_dest.Close())) return Fail(limiting_dest);
  RIEGELI_ASSERT_LE(dest->pos(), decoded_data_size)
      << "Decoded data size larger than expected";
  if (field_filter.include_all() &&
      ABSL_PREDICT_FALSE(dest->pos() != decoded_data_size)) {
    return Fail("Decoded data size smaller than expected");
  }
  return true;
}

inline bool TransposeDecoder::Parse(Context* context, Reader* src,
                                    const FieldFilter& field_filter) {
  const bool filtering_enabled = !field_filter.include_all();
  if (filtering_enabled) {
    for (const auto& include_field : field_filter.fields()) {
      uint32_t current_index = kInvalidPos;
      for (size_t i = 0; i < include_field.path().size(); ++i) {
        const auto insert_result = context->include_fields.emplace(
            std::make_pair(current_index, include_field.path()[i]),
            IntCast<uint32_t>(context->existence_only.size()));
        if (insert_result.second) context->existence_only.push_back(true);
        current_index = insert_result.first->second;
      }
      context->existence_only[current_index] = false;
    }
  }

  uint8_t compression_type_byte;
  if (ABSL_PREDICT_FALSE(!ReadByte(src, &compression_type_byte))) {
    return Fail("Reading compression type failed", *src);
  }
  context->compression_type =
      static_cast<CompressionType>(compression_type_byte);

  uint64_t header_size;
  if (ABSL_PREDICT_FALSE(!ReadVarint64(src, &header_size))) {
    return Fail("Reading header size failed", *src);
  }
  Chain header;
  if (ABSL_PREDICT_FALSE(!src->Read(&header, header_size))) {
    return Fail("Reading header failed", *src);
  }
  internal::Decompressor header_decompressor(
      absl::make_unique<ChainReader>(&header), context->compression_type);
  if (ABSL_PREDICT_FALSE(!header_decompressor.healthy())) {
    return Fail(header_decompressor);
  }

  uint32_t num_buffers;
  std::vector<uint32_t> first_buffer_indices;
  std::vector<uint32_t> bucket_indices;
  if (filtering_enabled) {
    if (ABSL_PREDICT_FALSE(
            !ParseBuffersForFitering(context, header_decompressor.reader(), src,
                                     &first_buffer_indices, &bucket_indices))) {
      return false;
    }
    num_buffers = IntCast<uint32_t>(bucket_indices.size());
  } else {
    if (ABSL_PREDICT_FALSE(
            !ParseBuffers(context, header_decompressor.reader(), src))) {
      return false;
    }
    num_buffers = IntCast<uint32_t>(context->buffers.size());
  }

  uint32_t state_machine_size;
  if (ABSL_PREDICT_FALSE(
          !ReadVarint32(header_decompressor.reader(), &state_machine_size))) {
    return Fail("Reading state machine size failed",
                *header_decompressor.reader());
  }
  // Additional 0xff nodes to correctly handle invalid/malicious inputs.
  // TODO: Handle overflow.
  context->state_machine_nodes.resize(state_machine_size + 0xff);
  if (filtering_enabled) {
    context->node_templates.resize(state_machine_size);
  }
  std::vector<StateMachineNode>& state_machine_nodes =
      context->state_machine_nodes;
  bool has_nonproto_op = false;
  size_t num_subtypes = 0;
  std::vector<uint32_t> tags;
  tags.reserve(state_machine_size);
  for (size_t i = 0; i < state_machine_size; ++i) {
    uint32_t tag;
    if (ABSL_PREDICT_FALSE(!ReadVarint32(header_decompressor.reader(), &tag))) {
      return Fail("Reading field tag failed", *header_decompressor.reader());
    }
    tags.push_back(tag);
    if (ValidTag(tag) && internal::HasSubtype(tag)) ++num_subtypes;
  }
  std::vector<uint32_t> next_node_indices;
  next_node_indices.reserve(state_machine_size);
  for (size_t i = 0; i < state_machine_size; ++i) {
    uint32_t next_node;
    if (ABSL_PREDICT_FALSE(
            !ReadVarint32(header_decompressor.reader(), &next_node))) {
      return Fail("Reading next node index failed",
                  *header_decompressor.reader());
    }
    next_node_indices.push_back(next_node);
  }
  std::string subtypes;
  if (ABSL_PREDICT_FALSE(
          !header_decompressor.reader()->Read(&subtypes, num_subtypes))) {
    return Fail("Reading subtypes failed", *header_decompressor.reader());
  }
  size_t subtype_index = 0;
  for (size_t i = 0; i < state_machine_size; ++i) {
    uint32_t tag = tags[i];
    StateMachineNode& state_machine_node = state_machine_nodes[i];
    state_machine_node.buffer = nullptr;
    switch (static_cast<internal::MessageId>(tag)) {
      case internal::MessageId::kNoOp:
        state_machine_node.callback_type = internal::CallbackType::kNoOp;
        break;
      case internal::MessageId::kNonProto: {
        state_machine_node.callback_type = internal::CallbackType::kNonProto;
        uint32_t buffer_index;
        if (ABSL_PREDICT_FALSE(
                !ReadVarint32(header_decompressor.reader(), &buffer_index))) {
          return Fail("Reading buffer index failed",
                      *header_decompressor.reader());
        }
        if (ABSL_PREDICT_FALSE(buffer_index >= num_buffers)) {
          return Fail("Buffer index too large");
        }
        if (filtering_enabled) {
          const uint32_t bucket = bucket_indices[buffer_index];
          state_machine_node.buffer = GetBuffer(
              context, bucket, buffer_index - first_buffer_indices[bucket]);
          if (ABSL_PREDICT_FALSE(state_machine_node.buffer == nullptr)) {
            return false;
          }
        } else {
          state_machine_node.buffer = &context->buffers[buffer_index];
        }
        has_nonproto_op = true;
      } break;
      case internal::MessageId::kStartOfMessage:
        state_machine_node.callback_type =
            internal::CallbackType::kMessageStart;
        break;
      case internal::MessageId::kStartOfSubmessage:
        if (filtering_enabled) {
          context->node_templates[i].tag =
              static_cast<uint32_t>(internal::MessageId::kStartOfSubmessage);
          state_machine_node.node_template = &context->node_templates[i];
          state_machine_node.callback_type =
              internal::CallbackType::kSelectCallback;
        } else {
          state_machine_node.callback_type =
              internal::CallbackType::kSubmessageStart;
        }
        break;
      default: {
        internal::Subtype subtype = internal::Subtype::kTrivial;
        static_assert(
            internal::Subtype::kLengthDelimitedString ==
                internal::Subtype::kTrivial,
            "Subtypes kLengthDelimitedString and kTrivial must be equal");
        // End of submessage is encoded as WireType::kSubmessage.
        if (static_cast<internal::WireType>(tag & 7) ==
            internal::WireType::kSubmessage) {
          tag -= internal::WireType::kSubmessage -
                 internal::WireType::kLengthDelimited;
          subtype = internal::Subtype::kLengthDelimitedEndOfSubmessage;
        }
        if (ABSL_PREDICT_FALSE((!ValidTag(tag)))) return Fail("Invalid tag");
        char* const tag_end =
            WriteVarint32(state_machine_node.tag_data.data, tag);
        const size_t tag_length =
            PtrDistance(state_machine_node.tag_data.data, tag_end);
        if (internal::HasSubtype(tag)) {
          subtype = static_cast<internal::Subtype>(subtypes[subtype_index++]);
        }
        if (filtering_enabled) {
          if (internal::HasDataBuffer(tag, subtype)) {
            uint32_t buffer_index;
            if (ABSL_PREDICT_FALSE(!ReadVarint32(header_decompressor.reader(),
                                                 &buffer_index))) {
              return Fail("Reading buffer index failed",
                          *header_decompressor.reader());
            }
            if (ABSL_PREDICT_FALSE(buffer_index >= num_buffers)) {
              return Fail("Buffer index too large");
            }
            const uint32_t bucket = bucket_indices[buffer_index];
            context->node_templates[i].bucket_index = bucket;
            context->node_templates[i].buffer_within_bucket_index =
                buffer_index - first_buffer_indices[bucket];
          } else {
            context->node_templates[i].bucket_index = kInvalidPos;
          }
          context->node_templates[i].tag = tag;
          context->node_templates[i].subtype = subtype;
          context->node_templates[i].tag_length = IntCast<uint8_t>(tag_length);
          state_machine_node.node_template = &context->node_templates[i];
          state_machine_node.callback_type =
              internal::CallbackType::kSelectCallback;
        } else {
          if (internal::HasDataBuffer(tag, subtype)) {
            uint32_t buffer_index;
            if (ABSL_PREDICT_FALSE(!ReadVarint32(header_decompressor.reader(),
                                                 &buffer_index))) {
              return Fail("Reading buffer index failed",
                          *header_decompressor.reader());
            }
            if (ABSL_PREDICT_FALSE(buffer_index >= num_buffers)) {
              return Fail("Buffer index too large");
            }
            state_machine_node.buffer = &context->buffers[buffer_index];
          }
          state_machine_node.callback_type = internal::GetCallbackType(
              FieldIncluded::kYes, tag, subtype, tag_length, filtering_enabled);
          if (ABSL_PREDICT_FALSE(state_machine_node.callback_type ==
                                 internal::CallbackType::kUnknown)) {
            return Fail("Invalid node");
          }
        }
        // Store subtype right past tag in case this is inline numeric.
        if (static_cast<internal::WireType>(tag & 7) ==
                internal::WireType::kVarint &&
            subtype >= internal::Subtype::kVarintInline0) {
          state_machine_node.tag_data.data[tag_length] =
              subtype - internal::Subtype::kVarintInline0;
        } else {
          state_machine_node.tag_data.data[tag_length] = 0;
        }
        state_machine_node.tag_data.size = IntCast<uint8_t>(tag_length);
      }
    }
    uint32_t next_node_id = next_node_indices[i];
    if (next_node_id >= state_machine_size) {
      // Callback is implicit.
      next_node_id -= state_machine_size;
      state_machine_node.callback_type =
          state_machine_node.callback_type | internal::CallbackType::kImplicit;
    }
    if (ABSL_PREDICT_FALSE(next_node_id >= state_machine_size)) {
      return Fail("Node index too large");
    }

    state_machine_node.next_node = &state_machine_nodes[next_node_id];
  }

  if (has_nonproto_op) {
    // If non-proto state exists then the last buffer is the
    // nonproto_lengths buffer.
    if (ABSL_PREDICT_FALSE(num_buffers == 0)) {
      return Fail("Missing buffer for non-proto records");
    }
    if (filtering_enabled) {
      const uint32_t bucket = bucket_indices[num_buffers - 1];
      context->nonproto_lengths = GetBuffer(
          context, bucket, num_buffers - 1 - first_buffer_indices[bucket]);
      if (ABSL_PREDICT_FALSE(context->nonproto_lengths == nullptr)) {
        return false;
      }
    } else {
      context->nonproto_lengths = &context->buffers.back();
    }
  }

  if (ABSL_PREDICT_FALSE(
          !ReadVarint32(header_decompressor.reader(), &context->first_node))) {
    return Fail("Reading first node index failed",
                *header_decompressor.reader());
  }
  if (ABSL_PREDICT_FALSE(context->first_node >= state_machine_size)) {
    return Fail("First node index too large");
  }

  // Add 0xff failure nodes so we never overflow this array.
  for (uint64_t i = state_machine_size; i < state_machine_size + 0xff; ++i) {
    state_machine_nodes[i].callback_type = internal::CallbackType::kFailure;
  }

  if (ABSL_PREDICT_FALSE(ContainsImplicitLoop(&state_machine_nodes))) {
    return Fail("Nodes contain an implicit loop");
  }

  if (ABSL_PREDICT_FALSE(!header_decompressor.VerifyEndAndClose())) {
    return Fail(header_decompressor);
  }
  context->transitions = internal::Decompressor(src, context->compression_type);
  if (ABSL_PREDICT_FALSE(!context->transitions.healthy())) {
    return Fail(context->transitions);
  }
  return true;
}

inline bool TransposeDecoder::ParseBuffers(Context* context,
                                           Reader* header_reader, Reader* src) {
  uint32_t num_buckets;
  if (ABSL_PREDICT_FALSE(!ReadVarint32(header_reader, &num_buckets))) {
    return Fail("Reading number of buckets failed");
  }
  uint32_t num_buffers;
  if (ABSL_PREDICT_FALSE(!ReadVarint32(header_reader, &num_buffers))) {
    return Fail("Reading number of buffers failed");
  }
  if (ABSL_PREDICT_FALSE(num_buffers > context->buffers.max_size())) {
    return Fail("Too many buffers");
  }
  if (num_buckets == 0) {
    if (ABSL_PREDICT_FALSE(num_buffers != 0)) return Fail("Too few buckets");
    return true;
  }
  context->buffers.reserve(num_buffers);
  std::vector<internal::Decompressor> bucket_decompressors;
  if (ABSL_PREDICT_FALSE(num_buckets > bucket_decompressors.max_size())) {
    return Fail("Too many buckets");
  }
  bucket_decompressors.reserve(num_buckets);
  for (uint32_t bucket_index = 0; bucket_index < num_buckets; ++bucket_index) {
    uint64_t bucket_length;
    if (ABSL_PREDICT_FALSE(!ReadVarint64(header_reader, &bucket_length))) {
      return Fail("Reading bucket length failed");
    }
    if (ABSL_PREDICT_FALSE(bucket_length >
                           std::numeric_limits<size_t>::max())) {
      return Fail("Bucket too large");
    }
    Chain bucket;
    if (ABSL_PREDICT_FALSE(
            !src->Read(&bucket, IntCast<size_t>(bucket_length)))) {
      return Fail("Reading bucket failed", *src);
    }
    bucket_decompressors.emplace_back(
        absl::make_unique<ChainReader>(std::move(bucket)),
        context->compression_type);
    if (ABSL_PREDICT_FALSE(!bucket_decompressors.back().healthy())) {
      return Fail(bucket_decompressors.back());
    }
  }

  uint32_t bucket_index = 0;
  for (size_t buffer_index = 0; buffer_index < num_buffers; ++buffer_index) {
    uint64_t buffer_length;
    if (ABSL_PREDICT_FALSE(!ReadVarint64(header_reader, &buffer_length))) {
      return Fail("Reading buffer length failed");
    }
    if (ABSL_PREDICT_FALSE(buffer_length >
                           std::numeric_limits<size_t>::max())) {
      return Fail("Buffer too large");
    }
    Chain buffer;
    if (ABSL_PREDICT_FALSE(!bucket_decompressors[bucket_index].reader()->Read(
            &buffer, IntCast<size_t>(buffer_length)))) {
      return Fail("Reading buffer failed",
                  *bucket_decompressors[bucket_index].reader());
    }
    context->buffers.emplace_back(std::move(buffer));
    while (!bucket_decompressors[bucket_index].reader()->Pull() &&
           bucket_index + 1 < num_buckets) {
      if (ABSL_PREDICT_FALSE(
              !bucket_decompressors[bucket_index].VerifyEndAndClose())) {
        return Fail(bucket_decompressors[bucket_index]);
      }
      ++bucket_index;
    }
  }
  if (ABSL_PREDICT_FALSE(bucket_index + 1 < num_buckets)) {
    return Fail("Too few buckets");
  }
  if (ABSL_PREDICT_FALSE(
          !bucket_decompressors[bucket_index].VerifyEndAndClose())) {
    return Fail(bucket_decompressors[bucket_index]);
  }
  return true;
}

inline bool TransposeDecoder::ParseBuffersForFitering(
    Context* context, Reader* header_reader, Reader* src,
    std::vector<uint32_t>* first_buffer_indices,
    std::vector<uint32_t>* bucket_indices) {
  uint32_t num_buckets;
  if (ABSL_PREDICT_FALSE(!ReadVarint32(header_reader, &num_buckets))) {
    return Fail("Reading number of buckets failed", *header_reader);
  }
  if (ABSL_PREDICT_FALSE(num_buckets > context->buckets.max_size())) {
    return Fail("Too many buckets");
  }
  uint32_t num_buffers;
  if (ABSL_PREDICT_FALSE(!ReadVarint32(header_reader, &num_buffers))) {
    return Fail("Reading number of buffers failed", *header_reader);
  }
  if (ABSL_PREDICT_FALSE(num_buffers > bucket_indices->max_size())) {
    return Fail("Too many buffers");
  }
  if (num_buckets == 0) {
    if (ABSL_PREDICT_FALSE(num_buffers != 0)) {
      return Fail("Too few buckets");
    }
    return true;
  }
  first_buffer_indices->reserve(num_buckets);
  bucket_indices->reserve(num_buffers);
  context->buckets.reserve(num_buckets);
  for (uint32_t bucket_index = 0; bucket_index < num_buckets; ++bucket_index) {
    uint64_t bucket_length;
    if (ABSL_PREDICT_FALSE(!ReadVarint64(header_reader, &bucket_length))) {
      return Fail("Reading bucket length failed");
    }
    if (ABSL_PREDICT_FALSE(bucket_length >
                           std::numeric_limits<size_t>::max())) {
      return Fail("Bucket too large");
    }
    context->buckets.emplace_back();
    if (ABSL_PREDICT_FALSE(!src->Read(&context->buckets.back().compressed_data,
                                      IntCast<size_t>(bucket_length)))) {
      return Fail("Reading bucket failed", *src);
    }
  }

  uint32_t bucket_index = 0;
  uint64_t remaining_bucket_size = 0;
  first_buffer_indices->push_back(0);
  if (ABSL_PREDICT_FALSE(!internal::Decompressor::UncompressedSize(
          context->buckets[0].compressed_data, context->compression_type,
          &remaining_bucket_size))) {
    return Fail("Reading uncompressed size failed");
  }
  for (uint32_t buffer_index = 0; buffer_index < num_buffers; ++buffer_index) {
    uint64_t buffer_length;
    if (ABSL_PREDICT_FALSE(!ReadVarint64(header_reader, &buffer_length))) {
      return Fail("Reading buffer length failed", *header_reader);
    }
    if (ABSL_PREDICT_FALSE(buffer_length >
                           std::numeric_limits<size_t>::max())) {
      return Fail("Buffer too large");
    }
    context->buckets[bucket_index].buffer_sizes.push_back(
        IntCast<size_t>(buffer_length));
    if (ABSL_PREDICT_FALSE(buffer_length > remaining_bucket_size)) {
      return Fail("Buffer does not fit in bucket");
    }
    remaining_bucket_size -= buffer_length;
    bucket_indices->push_back(bucket_index);
    while (remaining_bucket_size == 0 && bucket_index + 1 < num_buckets) {
      ++bucket_index;
      first_buffer_indices->push_back(buffer_index + 1);
      if (ABSL_PREDICT_FALSE(!internal::Decompressor::UncompressedSize(
              context->buckets[bucket_index].compressed_data,
              context->compression_type, &remaining_bucket_size))) {
        return Fail("Reading uncompressed size failed");
      }
    }
  }
  if (ABSL_PREDICT_FALSE(bucket_index + 1 < num_buckets)) {
    return Fail("Too few buckets");
  }
  if (ABSL_PREDICT_FALSE(remaining_bucket_size > 0)) {
    return Fail("End of data expected");
  }
  return true;
}

inline ChainReader* TransposeDecoder::GetBuffer(Context* context,
                                                uint32_t bucket_index,
                                                uint32_t index_within_bucket) {
  RIEGELI_ASSERT_LT(bucket_index, context->buckets.size())
      << "Bucket index out of range";
  DataBucket& bucket = context->buckets[bucket_index];
  if (bucket.decompressed) {
    RIEGELI_ASSERT_LT(index_within_bucket, bucket.buffers.size())
        << "Index within bucket out of range";
  } else {
    RIEGELI_ASSERT_LT(index_within_bucket, bucket.buffer_sizes.size())
        << "Index within bucket out of range";
    internal::Decompressor decompressor(
        absl::make_unique<ChainReader>(&bucket.compressed_data),
        context->compression_type);
    if (ABSL_PREDICT_FALSE(!decompressor.healthy())) {
      Fail(decompressor);
      return nullptr;
    }
    bucket.buffers.reserve(bucket.buffer_sizes.size());
    for (auto buffer_size : bucket.buffer_sizes) {
      Chain buffer;
      if (ABSL_PREDICT_FALSE(
              !decompressor.reader()->Read(&buffer, buffer_size))) {
        Fail("Reading buffer failed", *decompressor.reader());
        return nullptr;
      }
      bucket.buffers.emplace_back(std::move(buffer));
    }
    if (ABSL_PREDICT_FALSE(!decompressor.VerifyEndAndClose())) {
      Fail(decompressor);
      return nullptr;
    }
    // Free memory of fields which are no longer needed.
    bucket.buffer_sizes = std::vector<size_t>();
    bucket.compressed_data = Chain();
    bucket.decompressed = true;
  }
  return &bucket.buffers[index_within_bucket];
}

inline bool TransposeDecoder::ContainsImplicitLoop(
    std::vector<StateMachineNode>* state_machine_nodes) {
  for (auto& node : *state_machine_nodes) {
    node.implicit_loop_id = 0;
  }
  size_t next_loop_id = 1;
  for (auto& node : *state_machine_nodes) {
    if (node.implicit_loop_id != 0) {
      continue;
    }
    StateMachineNode* node_ptr = &node;
    node_ptr->implicit_loop_id = next_loop_id;
    while (internal::IsImplicit(node_ptr->callback_type)) {
      node_ptr = node_ptr->next_node;
      if (node_ptr->implicit_loop_id == next_loop_id) {
        return true;
      }
      if (node_ptr->implicit_loop_id != 0) {
        break;
      }
      node_ptr->implicit_loop_id = next_loop_id;
    }
    ++next_loop_id;
  }
  return false;
}

// Copy tag from *node to *dest.
#define COPY_TAG_CALLBACK(tag_length)                               \
  do {                                                              \
    if (ABSL_PREDICT_FALSE(!dest->Write(                            \
            absl::string_view(node->tag_data.data, tag_length)))) { \
      return Fail(*dest);                                           \
    }                                                               \
  } while (false)

// Decode varint value from *node to *dest.
#define VARINT_CALLBACK(tag_length, data_length)                            \
  do {                                                                      \
    if (ABSL_PREDICT_TRUE(dest->available() >= tag_length + data_length)) { \
      dest->set_cursor(dest->cursor() - (tag_length + data_length));        \
      char* const buffer = dest->cursor();                                  \
      if (ABSL_PREDICT_FALSE(                                               \
              !node->buffer->Read(buffer + tag_length, data_length))) {     \
        return Fail("Reading varint field failed", *node->buffer);          \
      }                                                                     \
      for (size_t i = 0; i < data_length - 1; ++i) {                        \
        buffer[tag_length + i] |= 0x80;                                     \
      }                                                                     \
      std::memcpy(buffer, node->tag_data.data, tag_length);                 \
    } else {                                                                \
      char buffer[tag_length + data_length];                                \
      if (ABSL_PREDICT_FALSE(                                               \
              !node->buffer->Read(buffer + tag_length, data_length))) {     \
        return Fail("Reading varint field failed", *node->buffer);          \
      }                                                                     \
      for (size_t i = 0; i < data_length - 1; ++i) {                        \
        buffer[tag_length + i] |= 0x80;                                     \
      }                                                                     \
      std::memcpy(buffer, node->tag_data.data, tag_length);                 \
      if (ABSL_PREDICT_FALSE(!dest->Write(                                  \
              absl::string_view(buffer, tag_length + data_length)))) {      \
        return Fail(*dest);                                                 \
      }                                                                     \
    }                                                                       \
  } while (false)

// Decode fixed32 or fixed64 value from *node to *dest.
#define FIXED_CALLBACK(tag_length, data_length)                             \
  do {                                                                      \
    if (ABSL_PREDICT_TRUE(dest->available() >= tag_length + data_length)) { \
      dest->set_cursor(dest->cursor() - (tag_length + data_length));        \
      char* const buffer = dest->cursor();                                  \
      if (ABSL_PREDICT_FALSE(                                               \
              !node->buffer->Read(buffer + tag_length, data_length))) {     \
        return Fail("Reading fixed field failed", *node->buffer);           \
      }                                                                     \
      std::memcpy(buffer, node->tag_data.data, tag_length);                 \
    } else {                                                                \
      char buffer[tag_length + data_length];                                \
      if (ABSL_PREDICT_FALSE(                                               \
              !node->buffer->Read(buffer + tag_length, data_length))) {     \
        return Fail("Reading fixed field failed", *node->buffer);           \
      }                                                                     \
      std::memcpy(buffer, node->tag_data.data, tag_length);                 \
      if (ABSL_PREDICT_FALSE(!dest->Write(                                  \
              absl::string_view(buffer, tag_length + data_length)))) {      \
        return Fail(*dest);                                                 \
      }                                                                     \
    }                                                                       \
  } while (false)

// Decode string value from *node to *dest.
#define STRING_CALLBACK(tag_length)                                         \
  do {                                                                      \
    uint32_t length;                                                        \
    size_t length_length;                                                   \
    if (ABSL_PREDICT_TRUE(node->buffer->available() >=                      \
                          kMaxLengthVarint32())) {                          \
      const char* cursor = node->buffer->cursor();                          \
      if (ABSL_PREDICT_FALSE(!ReadVarint32(&cursor, &length))) {            \
        node->buffer->set_cursor(cursor);                                   \
        return Fail("Reading string length failed");                        \
      }                                                                     \
      length_length = PtrDistance(node->buffer->cursor(), cursor);          \
    } else {                                                                \
      const Position pos_before = node->buffer->pos();                      \
      if (ABSL_PREDICT_FALSE(!ReadVarint32(node->buffer, &length))) {       \
        return Fail("Reading string length failed", *node->buffer);         \
      }                                                                     \
      length_length = IntCast<size_t>(node->buffer->pos() - pos_before);    \
      if (!node->buffer->Seek(pos_before)) {                                \
        RIEGELI_ASSERT_UNREACHABLE()                                        \
            << "Seeking buffer failed: " << node->buffer->Message();        \
      }                                                                     \
    }                                                                       \
    if (ABSL_PREDICT_FALSE(length > std::numeric_limits<uint32_t>::max() -  \
                                        length_length)) {                   \
      return Fail("String length overflow");                                \
    }                                                                       \
    if (ABSL_PREDICT_FALSE(                                                 \
            !node->buffer->CopyTo(dest, length_length + size_t{length}))) { \
      if (!dest->healthy()) return Fail(*dest);                             \
      return Fail("Reading string field failed", *node->buffer);            \
    }                                                                       \
    if (ABSL_PREDICT_FALSE(!dest->Write(                                    \
            absl::string_view(node->tag_data.data, tag_length)))) {         \
      return Fail(*dest);                                                   \
    }                                                                       \
  } while (false)

inline bool TransposeDecoder::Decode(Context* context, uint64_t num_records,
                                     BackwardWriter* dest,
                                     std::vector<size_t>* limits) {
  // For now positions reported by *dest are pushed to limits directly.
  // Later limits will be reversed and complemented.
  limits->clear();

  // Set current node to the initial node.
  StateMachineNode* node = &context->state_machine_nodes[context->first_node];
  // The depth of the current field relative to the parent submessage that was
  // excluded in filtering.
  int skipped_submessage_level = 0;

  Reader* const transitions_reader = context->transitions.reader();
  // Stack of all open sub-messages.
  std::vector<SubmessageStackElement> submessage_stack;
  // Number of following iteration that go directly to node->next_node without
  // reading transition byte.
  int num_iters = 0;

  // NOTE: CallbackType is used to index labels so the ordering of labels
  // must not change without a corresponding change in CallbackType enum.
  static constexpr void* labels[] = {
      &&do_transition,  // NoOp
      &&message_start,
      &&submessage_start,
      &&submessage_end,
      &&select_callback,
      &&skipped_submessage_start,
      &&skipped_submessage_end,
      &&non_proto,
      &&failure,

#define LABELS_FOR_TAG_LEN(tag_length)                                       \
  &&copy_tag_##tag_length, &&varint_1_##tag_length, &&varint_2_##tag_length, \
      &&varint_3_##tag_length, &&varint_4_##tag_length,                      \
      &&varint_5_##tag_length, &&varint_6_##tag_length,                      \
      &&varint_7_##tag_length, &&varint_8_##tag_length,                      \
      &&varint_9_##tag_length, &&varint_10_##tag_length,                     \
      &&fixed32_##tag_length, &&fixed64_##tag_length, &&string_##tag_length, \
      &&start_filter_group_##tag_length, &&end_filter_group_##tag_length

      LABELS_FOR_TAG_LEN(1),
      LABELS_FOR_TAG_LEN(2),
      LABELS_FOR_TAG_LEN(3),
      LABELS_FOR_TAG_LEN(4),
      LABELS_FOR_TAG_LEN(5),
#undef LABELS_FOR_TAG_LEN

      &&copy_tag_6,
      &&failure,  // Unknown
  };

  for (auto& state : context->state_machine_nodes) {
    state.callback =
        labels[static_cast<uint8_t>(state.callback_type) &
               ~static_cast<uint8_t>(internal::CallbackType::kImplicit)];
  }

  if (internal::IsImplicit(node->callback_type)) ++num_iters;
  goto * node->callback;

select_callback:
  if (ABSL_PREDICT_FALSE(!SetCallbackType(context, skipped_submessage_level,
                                          submessage_stack, node))) {
    return false;
  }
  node->callback =
      labels[static_cast<uint8_t>(node->callback_type) &
             ~static_cast<uint8_t>(internal::CallbackType::kImplicit)];
  goto * node->callback;

skipped_submessage_end:
  ++skipped_submessage_level;
  goto do_transition;

skipped_submessage_start:
  if (ABSL_PREDICT_FALSE(skipped_submessage_level == 0)) {
    return Fail("Skipped submessage stack underflow");
  }
  --skipped_submessage_level;
  goto do_transition;

submessage_end:
  submessage_stack.emplace_back(IntCast<size_t>(dest->pos()), node->tag_data);
  goto do_transition;

failure:
  return false;

submessage_start : {
  if (ABSL_PREDICT_FALSE(submessage_stack.empty())) {
    return Fail("Submessage stack underflow");
  }
  const SubmessageStackElement& elem = submessage_stack.back();
  RIEGELI_ASSERT_GE(dest->pos(), elem.end_of_submessage)
      << "Destination position decreased";
  const size_t length = IntCast<size_t>(dest->pos()) - elem.end_of_submessage;
  if (ABSL_PREDICT_FALSE(length > std::numeric_limits<uint32_t>::max())) {
    return Fail("Message too large");
  }
  if (ABSL_PREDICT_FALSE(!WriteVarint32(dest, IntCast<uint32_t>(length)))) {
    return Fail(*dest);
  }
  if (ABSL_PREDICT_FALSE(!dest->Write(
          absl::string_view(elem.tag_data.data, elem.tag_data.size)))) {
    return Fail(*dest);
  }
  submessage_stack.pop_back();
}
  goto do_transition;

#define ACTIONS_FOR_TAG_LEN(tag_length)                             \
  copy_tag_##tag_length : COPY_TAG_CALLBACK(tag_length);            \
  goto do_transition;                                               \
  varint_1_##tag_length : VARINT_CALLBACK(tag_length, 1);           \
  goto do_transition;                                               \
  varint_2_##tag_length : VARINT_CALLBACK(tag_length, 2);           \
  goto do_transition;                                               \
  varint_3_##tag_length : VARINT_CALLBACK(tag_length, 3);           \
  goto do_transition;                                               \
  varint_4_##tag_length : VARINT_CALLBACK(tag_length, 4);           \
  goto do_transition;                                               \
  varint_5_##tag_length : VARINT_CALLBACK(tag_length, 5);           \
  goto do_transition;                                               \
  varint_6_##tag_length : VARINT_CALLBACK(tag_length, 6);           \
  goto do_transition;                                               \
  varint_7_##tag_length : VARINT_CALLBACK(tag_length, 7);           \
  goto do_transition;                                               \
  varint_8_##tag_length : VARINT_CALLBACK(tag_length, 8);           \
  goto do_transition;                                               \
  varint_9_##tag_length : VARINT_CALLBACK(tag_length, 9);           \
  goto do_transition;                                               \
  varint_10_##tag_length : VARINT_CALLBACK(tag_length, 10);         \
  goto do_transition;                                               \
  fixed32_##tag_length : FIXED_CALLBACK(tag_length, 4);             \
  goto do_transition;                                               \
  fixed64_##tag_length : FIXED_CALLBACK(tag_length, 8);             \
  goto do_transition;                                               \
  string_##tag_length : STRING_CALLBACK(tag_length);                \
  goto do_transition;                                               \
  start_filter_group_##tag_length                                   \
      : if (ABSL_PREDICT_FALSE(submessage_stack.empty())) {         \
    return Fail("Submessage stack underflow");                      \
  }                                                                 \
  submessage_stack.pop_back();                                      \
  COPY_TAG_CALLBACK(tag_length);                                    \
  goto do_transition;                                               \
  end_filter_group_##tag_length                                     \
      : submessage_stack.emplace_back(IntCast<size_t>(dest->pos()), \
                                      node->tag_data);              \
  COPY_TAG_CALLBACK(tag_length);                                    \
  goto do_transition

  ACTIONS_FOR_TAG_LEN(1);
  ACTIONS_FOR_TAG_LEN(2);
  ACTIONS_FOR_TAG_LEN(3);
  ACTIONS_FOR_TAG_LEN(4);
  ACTIONS_FOR_TAG_LEN(5);
#undef ACTIONS_FOR_TAG_LEN

copy_tag_6:
  COPY_TAG_CALLBACK(6);
  goto do_transition;

non_proto : {
  uint32_t length;
  if (ABSL_PREDICT_FALSE(!ReadVarint32(context->nonproto_lengths, &length))) {
    return Fail("Reading non-proto record length failed",
                *context->nonproto_lengths);
  }
  if (ABSL_PREDICT_FALSE(!node->buffer->CopyTo(dest, length))) {
    if (!dest->healthy()) return Fail(*dest);
    return Fail("Reading non-proto record failed", *node->buffer);
  }
}
  // Fall through to message_start.

message_start:
  if (ABSL_PREDICT_FALSE(!submessage_stack.empty())) {
    return Fail("Submessages still open");
  }
  if (ABSL_PREDICT_FALSE(limits->size() == num_records)) {
    return Fail("Too many records");
  }
  limits->push_back(IntCast<size_t>(dest->pos()));
  // Fall through to do_transition.

do_transition:
  node = node->next_node;
  if (num_iters == 0) {
    uint8_t transition_byte;
    if (ABSL_PREDICT_FALSE(!ReadByte(transitions_reader, &transition_byte))) {
      goto done;
    }
    node += (transition_byte >> 2);
    num_iters = transition_byte & 3;
    if (internal::IsImplicit(node->callback_type)) ++num_iters;
    goto * node->callback;
  } else {
    if (!internal::IsImplicit(node->callback_type)) --num_iters;
    goto * node->callback;
  }

done:
  if (ABSL_PREDICT_FALSE(!context->transitions.VerifyEndAndClose())) {
    return Fail(context->transitions);
  }
  if (ABSL_PREDICT_FALSE(!submessage_stack.empty())) {
    return Fail("Submessages still open");
  }
  if (ABSL_PREDICT_FALSE(skipped_submessage_level != 0)) {
    return Fail("Skipped submessages still open");
  }
  if (ABSL_PREDICT_FALSE(limits->size() != num_records)) {
    return Fail("Too few records");
  }
  const size_t size = limits->empty() ? 0u : limits->back();
  if (ABSL_PREDICT_FALSE(size != dest->pos())) {
    return Fail("Unfinished message");
  }

  // Reverse limits and complement them, but keep the last limit unchanged
  // (because both old and new limits exclude 0 at the beginning and include
  // size at the end), e.g. for records of sizes {10, 20, 30, 40}:
  // {40, 70, 90, 100} -> {10, 30, 60, 100}.
  auto first = limits->begin();
  auto last = limits->end();
  if (first != last) {
    --last;
    while (first < last) {
      --last;
      const size_t tmp = size - *first;
      *first = size - *last;
      *last = tmp;
      ++first;
    }
  }
  return true;
}

// Do not inline this function. This helps Clang to generate better code for
// the main loop in Decode().
ABSL_ATTRIBUTE_NOINLINE inline bool TransposeDecoder::SetCallbackType(
    Context* context, int skipped_submessage_level,
    const std::vector<SubmessageStackElement>& submessage_stack,
    StateMachineNode* node) {
  const bool is_implicit = internal::IsImplicit(node->callback_type);
  StateMachineNodeTemplate* node_template = node->node_template;
  if (node_template->tag ==
      static_cast<uint32_t>(internal::MessageId::kStartOfSubmessage)) {
    if (skipped_submessage_level > 0) {
      node->callback_type = internal::CallbackType::kSkippedSubmessageStart;
    } else {
      node->callback_type = internal::CallbackType::kSubmessageStart;
    }
  } else {
    FieldIncluded field_included = FieldIncluded::kNo;
    uint32_t index = kInvalidPos;
    if (skipped_submessage_level == 0) {
      field_included = FieldIncluded::kExistenceOnly;
      for (const SubmessageStackElement& elem : submessage_stack) {
        uint32_t tag;
        const char* cursor = elem.tag_data.data;
        if (!ReadVarint32(&cursor, &tag)) {
          RIEGELI_ASSERT_UNREACHABLE() << "Invalid tag";
        }
        auto it = context->include_fields.find(std::make_pair(index, tag >> 3));
        if (it == context->include_fields.end()) {
          field_included = FieldIncluded::kNo;
          break;
        }
        index = it->second;
        if (!context->existence_only[index]) {
          field_included = FieldIncluded::kYes;
          break;
        }
      }
    }
    // If tag is a STARTGROUP, there are two options:
    // 1. Either related ENDGROUP was skipped and
    //    "skipped_submessage_level > 0".
    //    In this case field_included is already set to kNo.
    // 2. If ENDGROUP was not skipped, then its tag is on the top of the
    //    "submessage_stack" and in that case we already checked its tag in
    //    "include_fields" in the loop above.
    const bool start_group_tag =
        static_cast<internal::WireType>(node_template->tag & 7) ==
        internal::WireType::kStartGroup;
    if (!start_group_tag && field_included == FieldIncluded::kExistenceOnly) {
      uint32_t tag;
      const char* cursor = node->tag_data.data;
      if (!ReadVarint32(&cursor, &tag)) {
        RIEGELI_ASSERT_UNREACHABLE() << "Invalid tag";
      }
      auto it = context->include_fields.find(std::make_pair(index, tag >> 3));
      if (it == context->include_fields.end()) {
        field_included = FieldIncluded::kNo;
      } else {
        index = it->second;
        if (!context->existence_only[index]) {
          field_included = FieldIncluded::kYes;
        }
      }
    }
    if (node_template->bucket_index != kInvalidPos) {
      switch (field_included) {
        case FieldIncluded::kYes:
          node->buffer = GetBuffer(context, node_template->bucket_index,
                                   node_template->buffer_within_bucket_index);
          if (ABSL_PREDICT_FALSE(node->buffer == nullptr)) return false;
          break;
        case FieldIncluded::kNo:
          node->buffer = kEmptyChainReader();
          break;
        case FieldIncluded::kExistenceOnly:
          node->buffer = kEmptyChainReader();
          break;
      }
    } else {
      node->buffer = kEmptyChainReader();
    }
    node->callback_type = GetCallbackType(field_included, node_template->tag,
                                          node_template->subtype,
                                          node_template->tag_length, true);
  }
  if (is_implicit) node->callback_type |= internal::CallbackType::kImplicit;
  return true;
}

}  // namespace riegeli
