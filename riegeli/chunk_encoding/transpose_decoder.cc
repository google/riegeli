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

#include <algorithm>
#include <cstring>
#include <iterator>
#include <limits>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/container/flat_hash_map.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/canonical_errors.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/memory.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/backward_writer_utils.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/limiting_backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/reader_utils.h"
#include "riegeli/bytes/string_reader.h"
#include "riegeli/bytes/writer_utils.h"
#include "riegeli/chunk_encoding/constants.h"
#include "riegeli/chunk_encoding/decompressor.h"
#include "riegeli/chunk_encoding/field_projection.h"
#include "riegeli/chunk_encoding/transpose_internal.h"

namespace riegeli {

namespace {

Reader* kEmptyReader() {
  static NoDestructor<StringReader<>> kStaticEmptyReader((absl::string_view()));
  RIEGELI_ASSERT(kStaticEmptyReader->healthy())
      << "kEmptyReader() has been closed";
  return kStaticEmptyReader.get();
}

constexpr uint32_t kInvalidPos = std::numeric_limits<uint32_t>::max();

// Information about one data bucket used in projection.
struct DataBucket {
  // Raw bucket data, valid if not all buffers are already decompressed,
  // otherwise empty.
  Chain compressed_data;
  // Sizes of data buffers in the bucket, valid if not all buffers are already
  // decompressed, otherwise empty.
  std::vector<size_t> buffer_sizes;
  // Decompressor for the remaining data, valid if some but not all buffers are
  // already decompressed, otherwise closed.
  internal::Decompressor<ChainReader<>> decompressor;
  // A prefix of decompressed data buffers, lazily extended.
  std::vector<ChainReader<Chain>> buffers;
};

// Should the data content of the field be decoded?
enum class FieldIncluded {
  kYes,
  kNo,
  kExistenceOnly,
};

// Returns `true` if `tag` is a valid protocol buffer tag.
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
// Note: `CallbackType` is used to index labels array in
// `TransposeDecoder::Decode()` method so the ordering of `CallbackType` must
// not change without a corresponding change in `TransposeDecoder::Decode()`.
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

// `kCopyTag_*` has to be the first `CallbackType` in `TYPES_FOR_TAG_LEN` for
// `GetCopyTagCallbackType()` to work.
#define TYPES_FOR_TAG_LEN(tag_length)                                         \
  kCopyTag_##tag_length, kVarint_1_##tag_length, kVarint_2_##tag_length,      \
      kVarint_3_##tag_length, kVarint_4_##tag_length, kVarint_5_##tag_length, \
      kVarint_6_##tag_length, kVarint_7_##tag_length, kVarint_8_##tag_length, \
      kVarint_9_##tag_length, kVarint_10_##tag_length, kFixed32_##tag_length, \
      kFixed64_##tag_length, kFixed32Existence_##tag_length,                  \
      kFixed64Existence_##tag_length, kString_##tag_length,                   \
      kStartProjectionGroup_##tag_length, kEndProjectionGroup_##tag_length

  TYPES_FOR_TAG_LEN(1),
  TYPES_FOR_TAG_LEN(2),
  TYPES_FOR_TAG_LEN(3),
  TYPES_FOR_TAG_LEN(4),
  TYPES_FOR_TAG_LEN(5),
#undef TYPES_FOR_TAG_LEN

  // We need `kCopyTag_*` callback for length 6 as well because of inline
  // numerics. `kCopyTag_6` has to be the first `CallbackType` after
  // `TYPES_FOR_TAG_LEN` for `GetCopyTagCallbackType()` to work.
  kCopyTag_6,
  kUnknown,
  // Implicit callback type is added to any of the above types if the transition
  // from the node should go to `node->next_node` without reading the transition
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

// Returns copy tag callback type for `tag_length`.
inline CallbackType GetCopyTagCallbackType(size_t tag_length) {
  RIEGELI_ASSERT_GT(tag_length, 0u) << "Zero tag length";
  RIEGELI_ASSERT_LE(tag_length, kMaxLengthVarint32 + 1)
      << "Tag length too large";
  return CallbackType::kCopyTag_1 +
         (tag_length - 1) *
             (CallbackType::kCopyTag_2 - CallbackType::kCopyTag_1);
}

// Returns varint callback type for `subtype` and `tag_length`.
inline CallbackType GetVarintCallbackType(Subtype subtype, size_t tag_length) {
  RIEGELI_ASSERT_GT(tag_length, 0u) << "Zero tag length";
  RIEGELI_ASSERT_LE(tag_length, kMaxLengthVarint32) << "Tag length too large";
  if (subtype > Subtype::kVarintInlineMax) return CallbackType::kUnknown;
  if (subtype >= Subtype::kVarintInline0) {
    return GetCopyTagCallbackType(tag_length + 1);
  }
  return CallbackType::kVarint_1_1 +
         (subtype - Subtype::kVarint1) *
             (CallbackType::kVarint_2_1 - CallbackType::kVarint_1_1) +
         (tag_length - 1) *
             (CallbackType::kVarint_1_2 - CallbackType::kVarint_1_1);
}

// Returns fixed32 callback type for `tag_length`.
inline CallbackType GetFixed32CallbackType(size_t tag_length) {
  RIEGELI_ASSERT_GT(tag_length, 0u) << "Zero tag length";
  RIEGELI_ASSERT_LE(tag_length, kMaxLengthVarint32) << "Tag length too large";
  return CallbackType::kFixed32_1 +
         (tag_length - 1) *
             (CallbackType::kFixed32_2 - CallbackType::kFixed32_1);
}

// Returns fixed64 callback type for `tag_length`.
inline CallbackType GetFixed64CallbackType(size_t tag_length) {
  RIEGELI_ASSERT_GT(tag_length, 0u) << "Zero tag length";
  RIEGELI_ASSERT_LE(tag_length, kMaxLengthVarint32) << "Tag length too large";
  return CallbackType::kFixed64_1 +
         (tag_length - 1) *
             (CallbackType::kFixed64_2 - CallbackType::kFixed64_1);
}

// Returns fixed32 existence callback type for `tag_length`.
inline CallbackType GetFixed32ExistenceCallbackType(size_t tag_length) {
  RIEGELI_ASSERT_GT(tag_length, 0u) << "Zero tag length";
  RIEGELI_ASSERT_LE(tag_length, kMaxLengthVarint32) << "Tag length too large";
  return CallbackType::kFixed32Existence_1 +
         (tag_length - 1) * (CallbackType::kFixed32Existence_2 -
                             CallbackType::kFixed32Existence_1);
}

// Returns fixed64 existence callback type for `tag_length`.
inline CallbackType GetFixed64ExistenceCallbackType(size_t tag_length) {
  RIEGELI_ASSERT_GT(tag_length, 0u) << "Zero tag length";
  RIEGELI_ASSERT_LE(tag_length, kMaxLengthVarint32) << "Tag length too large";
  return CallbackType::kFixed64Existence_1 +
         (tag_length - 1) * (CallbackType::kFixed64Existence_2 -
                             CallbackType::kFixed64Existence_1);
}

// Returns string callback type for `subtype` and `tag_length`.
inline CallbackType GetStringCallbackType(Subtype subtype, size_t tag_length) {
  RIEGELI_ASSERT_GT(tag_length, 0u) << "Zero tag length";
  RIEGELI_ASSERT_LE(tag_length, kMaxLengthVarint32) << "Tag length too large";
  switch (subtype) {
    case Subtype::kLengthDelimitedString:
      return CallbackType::kString_1 +
             (tag_length - 1) *
                 (CallbackType::kString_2 - CallbackType::kString_1);
    case Subtype::kLengthDelimitedEndOfSubmessage:
      return CallbackType::kSubmessageEnd;
    default:
      // Note: Nodes with `kLengthDelimitedStartOfSubmessage` are not created.
      // Start of submessage is indicated with `MessageId::kStartOfSubmessage`
      // and uses `CallbackType::kSubmessageStart`.
      return CallbackType::kUnknown;
  }
}

// Returns string callback type for `subtype` and `tag_length` to exclude field.
inline CallbackType GetStringExcludeCallbackType(Subtype subtype,
                                                 size_t tag_length) {
  RIEGELI_ASSERT_GT(tag_length, 0u) << "Zero tag length";
  RIEGELI_ASSERT_LE(tag_length, kMaxLengthVarint32) << "Tag length too large";
  switch (subtype) {
    case Subtype::kLengthDelimitedString:
      return CallbackType::kNoOp;
    case Subtype::kLengthDelimitedEndOfSubmessage:
      return CallbackType::kSkippedSubmessageEnd;
    default:
      return CallbackType::kUnknown;
  }
}

// Returns string existence callback type for `subtype` and `tag_length`.
inline CallbackType GetStringExistenceCallbackType(Subtype subtype,
                                                   size_t tag_length) {
  RIEGELI_ASSERT_GT(tag_length, 0u) << "Zero tag length";
  RIEGELI_ASSERT_LE(tag_length, kMaxLengthVarint32) << "Tag length too large";
  switch (subtype) {
    case Subtype::kLengthDelimitedString:
      // We use the fact that there is a zero stored in TagData. This decodes as
      // an empty string in proto decoder.
      return GetCopyTagCallbackType(tag_length + 1);
    case Subtype::kLengthDelimitedEndOfSubmessage:
      return CallbackType::kSubmessageEnd;
    default:
      return CallbackType::kUnknown;
  }
}

inline CallbackType GetStartProjectionGroupCallbackType(size_t tag_length) {
  RIEGELI_ASSERT_GT(tag_length, 0u) << "Zero tag length";
  RIEGELI_ASSERT_LE(tag_length, kMaxLengthVarint32) << "Tag length too large";
  return CallbackType::kStartProjectionGroup_1 +
         (tag_length - 1) * (CallbackType::kStartProjectionGroup_2 -
                             CallbackType::kStartProjectionGroup_1);
}

inline CallbackType GetEndProjectionGroupCallbackType(size_t tag_length) {
  RIEGELI_ASSERT_GT(tag_length, 0u) << "Zero tag length";
  RIEGELI_ASSERT_LE(tag_length, kMaxLengthVarint32) << "Tag length too large";
  return CallbackType::kEndProjectionGroup_1 +
         (tag_length - 1) * (CallbackType::kEndProjectionGroup_2 -
                             CallbackType::kEndProjectionGroup_1);
}

// Get callback for node.
inline CallbackType GetCallbackType(FieldIncluded field_included, uint32_t tag,
                                    Subtype subtype, size_t tag_length,
                                    bool projection_enabled) {
  RIEGELI_ASSERT_GT(tag_length, 0u) << "Zero tag length";
  RIEGELI_ASSERT_LE(tag_length, kMaxLengthVarint32) << "Tag length too large";
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
          return projection_enabled
                     ? GetStartProjectionGroupCallbackType(tag_length)
                     : GetCopyTagCallbackType(tag_length);
        case WireType::kEndGroup:
          return projection_enabled
                     ? GetEndProjectionGroupCallbackType(tag_length)
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
          return GetCopyTagCallbackType(tag_length + 1);
        case WireType::kFixed32:
          return GetFixed32ExistenceCallbackType(tag_length);
        case WireType::kFixed64:
          return GetFixed64ExistenceCallbackType(tag_length);
        case WireType::kLengthDelimited:
          return GetStringExistenceCallbackType(subtype, tag_length);
        case WireType::kStartGroup:
          return GetStartProjectionGroupCallbackType(tag_length);
        case WireType::kEndGroup:
          return GetEndProjectionGroupCallbackType(tag_length);
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
  // Note: Used only when projection is disabled.
  std::vector<ChainReader<Chain>> buffers;
  // Buffer for lengths of nonproto messages.
  Reader* nonproto_lengths = nullptr;
  // State machine read from the input.
  std::vector<StateMachineNode> state_machine_nodes;
  // Node to start decoding from.
  uint32_t first_node = 0;
  // State machine transitions. One byte = one transition.
  internal::Decompressor<> transitions;

  enum class IncludeType : uint8_t {
    // Field is included.
    kIncludeFully,
    // Some child fields are included.
    kIncludeChild,
    // Field is existence only.
    kExistenceOnly,
  };

  // --- Fields used in projection. ---
  // Holds information about included field.
  struct IncludedField {
    // IDs are sequentially assigned to fields from FieldProjection.
    uint32_t field_id;
    IncludeType include_type;
  };
  // Fields form a tree structure stored in `include_fields` map. If `p` is
  // the ID of parent submessage then `include_fields[std::make_pair(p, f)]`
  // holds the include information of the child with field number `f`. The root
  // ID is assumed to be `kInvalidPos` and the root `IncludeType` is assumed to
  // be `kIncludeChild`.
  absl::flat_hash_map<std::pair<uint32_t, uint32_t>, IncludedField>
      include_fields;
  // Data buckets.
  std::vector<DataBucket> buckets;
  // Template that can later be used later to finalize `StateMachineNode`.
  std::vector<StateMachineNodeTemplate> node_templates;
};

bool TransposeDecoder::Decode(Reader* src, uint64_t num_records,
                              uint64_t decoded_data_size,
                              const FieldProjection& field_projection,
                              BackwardWriter* dest,
                              std::vector<size_t>* limits) {
  RIEGELI_ASSERT_EQ(dest->pos(), 0u)
      << "Failed precondition of TransposeDecoder::Reset(): "
         "non-zero destination position";
  Object::Reset(kInitiallyOpen);
  if (ABSL_PREDICT_FALSE(num_records > limits->max_size())) {
    return Fail(ResourceExhaustedError("Too many records"));
  }
  if (ABSL_PREDICT_FALSE(decoded_data_size >
                         std::numeric_limits<size_t>::max())) {
    return Fail(ResourceExhaustedError("Records too large"));
  }

  Context context;
  if (ABSL_PREDICT_FALSE(!Parse(&context, src, field_projection))) return false;
  LimitingBackwardWriter<> limiting_dest(dest, decoded_data_size);
  if (ABSL_PREDICT_FALSE(
          !Decode(&context, num_records, &limiting_dest, limits))) {
    limiting_dest.Close();
    return false;
  }
  if (ABSL_PREDICT_FALSE(!limiting_dest.Close())) return Fail(limiting_dest);
  RIEGELI_ASSERT_LE(dest->pos(), decoded_data_size)
      << "Decoded data size larger than expected";
  if (field_projection.includes_all() &&
      ABSL_PREDICT_FALSE(dest->pos() != decoded_data_size)) {
    return Fail(DataLossError("Decoded data size smaller than expected"));
  }
  return true;
}

inline bool TransposeDecoder::Parse(Context* context, Reader* src,
                                    const FieldProjection& field_projection) {
  bool projection_enabled = true;
  for (const Field& include_field : field_projection.fields()) {
    if (include_field.path().empty()) {
      projection_enabled = false;
      break;
    }
    size_t path_len = include_field.path().size();
    bool existence_only =
        include_field.path()[path_len - 1] == Field::kExistenceOnly;
    if (existence_only) {
      --path_len;
      if (path_len == 0) continue;
    }
    uint32_t current_id = kInvalidPos;
    for (size_t i = 0; i < path_len; ++i) {
      const uint32_t tag = include_field.path()[i];
      if (tag == Field::kExistenceOnly) {
        return false;
      }
      uint32_t next_id = context->include_fields.size();
      Context::IncludeType include_type = Context::IncludeType::kIncludeChild;
      if (i + 1 == path_len) {
        include_type = existence_only ? Context::IncludeType::kExistenceOnly
                                      : Context::IncludeType::kIncludeFully;
      }
      Context::IncludedField& val =
          context->include_fields
              .emplace(std::make_pair(current_id, tag),
                       Context::IncludedField{next_id, include_type})
              .first->second;
      current_id = val.field_id;
      static_assert(Context::IncludeType::kExistenceOnly >
                            Context::IncludeType::kIncludeChild &&
                        Context::IncludeType::kIncludeChild >
                            Context::IncludeType::kIncludeFully,
                    "Statement below assumes this ordering");
      val.include_type = std::min(val.include_type, include_type);
    }
  }

  uint8_t compression_type_byte;
  if (ABSL_PREDICT_FALSE(!ReadByte(src, &compression_type_byte))) {
    return Fail(*src, DataLossError("Reading compression type failed"));
  }
  context->compression_type =
      static_cast<CompressionType>(compression_type_byte);

  uint64_t header_size;
  if (ABSL_PREDICT_FALSE(!ReadVarint64(src, &header_size))) {
    return Fail(*src, DataLossError("Reading header size failed"));
  }
  Chain header;
  if (ABSL_PREDICT_FALSE(!src->Read(&header, header_size))) {
    return Fail(*src, DataLossError("Reading header failed"));
  }
  internal::Decompressor<ChainReader<>> header_decompressor(
      std::forward_as_tuple(&header), context->compression_type);
  if (ABSL_PREDICT_FALSE(!header_decompressor.healthy())) {
    return Fail(header_decompressor);
  }

  uint32_t num_buffers;
  std::vector<uint32_t> first_buffer_indices;
  std::vector<uint32_t> bucket_indices;
  if (projection_enabled) {
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
    return Fail(*header_decompressor.reader(),
                DataLossError("Reading state machine size failed"));
  }
  // Additional `0xff` nodes to correctly handle invalid/malicious inputs.
  // TODO: Handle overflow.
  context->state_machine_nodes.resize(state_machine_size + 0xff);
  if (projection_enabled) context->node_templates.resize(state_machine_size);
  std::vector<StateMachineNode>& state_machine_nodes =
      context->state_machine_nodes;
  bool has_nonproto_op = false;
  size_t num_subtypes = 0;
  std::vector<uint32_t> tags;
  tags.reserve(state_machine_size);
  for (size_t i = 0; i < state_machine_size; ++i) {
    uint32_t tag;
    if (ABSL_PREDICT_FALSE(!ReadVarint32(header_decompressor.reader(), &tag))) {
      return Fail(*header_decompressor.reader(),
                  DataLossError("Reading field tag failed"));
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
      return Fail(*header_decompressor.reader(),
                  DataLossError("Reading next node index failed"));
    }
    next_node_indices.push_back(next_node);
  }
  std::string subtypes;
  if (ABSL_PREDICT_FALSE(
          !header_decompressor.reader()->Read(&subtypes, num_subtypes))) {
    return Fail(*header_decompressor.reader(),
                DataLossError("Reading subtypes failed"));
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
          return Fail(*header_decompressor.reader(),
                      DataLossError("Reading buffer index failed"));
        }
        if (ABSL_PREDICT_FALSE(buffer_index >= num_buffers)) {
          return Fail(DataLossError("Buffer index too large"));
        }
        if (projection_enabled) {
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
        if (projection_enabled) {
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
        // End of submessage is encoded as `WireType::kSubmessage`.
        if (static_cast<internal::WireType>(tag & 7) ==
            internal::WireType::kSubmessage) {
          tag -= internal::WireType::kSubmessage -
                 internal::WireType::kLengthDelimited;
          subtype = internal::Subtype::kLengthDelimitedEndOfSubmessage;
        }
        if (ABSL_PREDICT_FALSE((!ValidTag(tag)))) {
          return Fail(DataLossError("Invalid tag"));
        }
        char* const tag_end =
            WriteVarint32(state_machine_node.tag_data.data, tag);
        const size_t tag_length =
            PtrDistance(state_machine_node.tag_data.data, tag_end);
        if (internal::HasSubtype(tag)) {
          subtype = static_cast<internal::Subtype>(subtypes[subtype_index++]);
        }
        if (projection_enabled) {
          if (internal::HasDataBuffer(tag, subtype)) {
            uint32_t buffer_index;
            if (ABSL_PREDICT_FALSE(!ReadVarint32(header_decompressor.reader(),
                                                 &buffer_index))) {
              return Fail(*header_decompressor.reader(),
                          DataLossError("Reading buffer index failed"));
            }
            if (ABSL_PREDICT_FALSE(buffer_index >= num_buffers)) {
              return Fail(DataLossError("Buffer index too large"));
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
              return Fail(*header_decompressor.reader(),
                          DataLossError("Reading buffer index failed"));
            }
            if (ABSL_PREDICT_FALSE(buffer_index >= num_buffers)) {
              return Fail(DataLossError("Buffer index too large"));
            }
            state_machine_node.buffer = &context->buffers[buffer_index];
          }
          state_machine_node.callback_type =
              internal::GetCallbackType(FieldIncluded::kYes, tag, subtype,
                                        tag_length, projection_enabled);
          if (ABSL_PREDICT_FALSE(state_machine_node.callback_type ==
                                 internal::CallbackType::kUnknown)) {
            return Fail(DataLossError("Invalid node"));
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
      return Fail(DataLossError("Node index too large"));
    }

    state_machine_node.next_node = &state_machine_nodes[next_node_id];
  }

  if (has_nonproto_op) {
    // If non-proto state exists then the last buffer is the
    // `nonproto_lengths` buffer.
    if (ABSL_PREDICT_FALSE(num_buffers == 0)) {
      return Fail(DataLossError("Missing buffer for non-proto records"));
    }
    if (projection_enabled) {
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
    return Fail(*header_decompressor.reader(),
                DataLossError("Reading first node index failed"));
  }
  if (ABSL_PREDICT_FALSE(context->first_node >= state_machine_size)) {
    return Fail(DataLossError("First node index too large"));
  }

  // Add `0xff` failure nodes so we never overflow this array.
  for (uint64_t i = state_machine_size; i < state_machine_size + 0xff; ++i) {
    state_machine_nodes[i].callback_type = internal::CallbackType::kFailure;
  }

  if (ABSL_PREDICT_FALSE(ContainsImplicitLoop(&state_machine_nodes))) {
    return Fail(DataLossError("Nodes contain an implicit loop"));
  }

  if (ABSL_PREDICT_FALSE(!header_decompressor.VerifyEndAndClose())) {
    return Fail(header_decompressor);
  }
  context->transitions.Reset(src, context->compression_type);
  if (ABSL_PREDICT_FALSE(!context->transitions.healthy())) {
    return Fail(context->transitions);
  }
  return true;
}

inline bool TransposeDecoder::ParseBuffers(Context* context,
                                           Reader* header_reader, Reader* src) {
  uint32_t num_buckets;
  if (ABSL_PREDICT_FALSE(!ReadVarint32(header_reader, &num_buckets))) {
    return Fail(*header_reader,
                DataLossError("Reading number of buckets failed"));
  }
  uint32_t num_buffers;
  if (ABSL_PREDICT_FALSE(!ReadVarint32(header_reader, &num_buffers))) {
    return Fail(*header_reader,
                DataLossError("Reading number of buffers failed"));
  }
  if (ABSL_PREDICT_FALSE(num_buffers > context->buffers.max_size())) {
    return Fail(DataLossError("Too many buffers"));
  }
  if (num_buckets == 0) {
    if (ABSL_PREDICT_FALSE(num_buffers != 0)) {
      return Fail(DataLossError("Too few buckets"));
    }
    return true;
  }
  context->buffers.reserve(num_buffers);
  std::vector<internal::Decompressor<ChainReader<Chain>>> bucket_decompressors;
  if (ABSL_PREDICT_FALSE(num_buckets > bucket_decompressors.max_size())) {
    return Fail(ResourceExhaustedError("Too many buckets"));
  }
  bucket_decompressors.reserve(num_buckets);
  for (uint32_t bucket_index = 0; bucket_index < num_buckets; ++bucket_index) {
    uint64_t bucket_length;
    if (ABSL_PREDICT_FALSE(!ReadVarint64(header_reader, &bucket_length))) {
      return Fail(*header_reader,
                  DataLossError("Reading bucket length failed"));
    }
    if (ABSL_PREDICT_FALSE(bucket_length >
                           std::numeric_limits<size_t>::max())) {
      return Fail(ResourceExhaustedError("Bucket too large"));
    }
    Chain bucket;
    if (ABSL_PREDICT_FALSE(
            !src->Read(&bucket, IntCast<size_t>(bucket_length)))) {
      return Fail(*src, DataLossError("Reading bucket failed"));
    }
    bucket_decompressors.emplace_back(std::forward_as_tuple(std::move(bucket)),
                                      context->compression_type);
    if (ABSL_PREDICT_FALSE(!bucket_decompressors.back().healthy())) {
      return Fail(bucket_decompressors.back());
    }
  }

  uint32_t bucket_index = 0;
  for (size_t buffer_index = 0; buffer_index < num_buffers; ++buffer_index) {
    uint64_t buffer_length;
    if (ABSL_PREDICT_FALSE(!ReadVarint64(header_reader, &buffer_length))) {
      return Fail(*header_reader,
                  DataLossError("Reading buffer length failed"));
    }
    if (ABSL_PREDICT_FALSE(buffer_length >
                           std::numeric_limits<size_t>::max())) {
      return Fail(ResourceExhaustedError("Buffer too large"));
    }
    Chain buffer;
    if (ABSL_PREDICT_FALSE(!bucket_decompressors[bucket_index].reader()->Read(
            &buffer, IntCast<size_t>(buffer_length)))) {
      return Fail(*bucket_decompressors[bucket_index].reader(),
                  DataLossError("Reading buffer failed"));
    }
    context->buffers.emplace_back(std::move(buffer));
    while (!bucket_decompressors[bucket_index].reader()->Pull() &&
           bucket_index + 1 < num_buckets) {
      if (ABSL_PREDICT_FALSE(
              !bucket_decompressors[bucket_index].VerifyEndAndClose())) {
        return Fail(bucket_decompressors[bucket_index].status());
      }
      ++bucket_index;
    }
  }
  if (ABSL_PREDICT_FALSE(bucket_index + 1 < num_buckets)) {
    return Fail(DataLossError("Too few buckets"));
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
    return Fail(*header_reader,
                DataLossError("Reading number of buckets failed"));
  }
  if (ABSL_PREDICT_FALSE(num_buckets > context->buckets.max_size())) {
    return Fail(ResourceExhaustedError("Too many buckets"));
  }
  uint32_t num_buffers;
  if (ABSL_PREDICT_FALSE(!ReadVarint32(header_reader, &num_buffers))) {
    return Fail(*header_reader,
                DataLossError("Reading number of buffers failed"));
  }
  if (ABSL_PREDICT_FALSE(num_buffers > bucket_indices->max_size())) {
    return Fail(ResourceExhaustedError("Too many buffers"));
  }
  if (num_buckets == 0) {
    if (ABSL_PREDICT_FALSE(num_buffers != 0)) {
      return Fail(DataLossError("Too few buckets"));
    }
    return true;
  }
  first_buffer_indices->reserve(num_buckets);
  bucket_indices->reserve(num_buffers);
  context->buckets.reserve(num_buckets);
  for (uint32_t bucket_index = 0; bucket_index < num_buckets; ++bucket_index) {
    uint64_t bucket_length;
    if (ABSL_PREDICT_FALSE(!ReadVarint64(header_reader, &bucket_length))) {
      return Fail(*header_reader,
                  DataLossError("Reading bucket length failed"));
    }
    if (ABSL_PREDICT_FALSE(bucket_length >
                           std::numeric_limits<size_t>::max())) {
      return Fail(ResourceExhaustedError("Bucket too large"));
    }
    context->buckets.emplace_back();
    if (ABSL_PREDICT_FALSE(!src->Read(&context->buckets.back().compressed_data,
                                      IntCast<size_t>(bucket_length)))) {
      return Fail(*src, DataLossError("Reading bucket failed"));
    }
  }

  uint32_t bucket_index = 0;
  uint64_t remaining_bucket_size = 0;
  first_buffer_indices->push_back(0);
  if (ABSL_PREDICT_FALSE(!internal::UncompressedSize(
          context->buckets[0].compressed_data, context->compression_type,
          &remaining_bucket_size))) {
    return Fail(DataLossError("Reading uncompressed size failed"));
  }
  for (uint32_t buffer_index = 0; buffer_index < num_buffers; ++buffer_index) {
    uint64_t buffer_length;
    if (ABSL_PREDICT_FALSE(!ReadVarint64(header_reader, &buffer_length))) {
      return Fail(*header_reader,
                  DataLossError("Reading buffer length failed"));
    }
    if (ABSL_PREDICT_FALSE(buffer_length >
                           std::numeric_limits<size_t>::max())) {
      return Fail(ResourceExhaustedError("Buffer too large"));
    }
    context->buckets[bucket_index].buffer_sizes.push_back(
        IntCast<size_t>(buffer_length));
    if (ABSL_PREDICT_FALSE(buffer_length > remaining_bucket_size)) {
      return Fail(DataLossError("Buffer does not fit in bucket"));
    }
    remaining_bucket_size -= buffer_length;
    bucket_indices->push_back(bucket_index);
    while (remaining_bucket_size == 0 && bucket_index + 1 < num_buckets) {
      ++bucket_index;
      first_buffer_indices->push_back(buffer_index + 1);
      if (ABSL_PREDICT_FALSE(!internal::UncompressedSize(
              context->buckets[bucket_index].compressed_data,
              context->compression_type, &remaining_bucket_size))) {
        return Fail(DataLossError("Reading uncompressed size failed"));
      }
    }
  }
  if (ABSL_PREDICT_FALSE(bucket_index + 1 < num_buckets)) {
    return Fail(DataLossError("Too few buckets"));
  }
  if (ABSL_PREDICT_FALSE(remaining_bucket_size > 0)) {
    return Fail(DataLossError("End of data expected"));
  }
  return true;
}

inline Reader* TransposeDecoder::GetBuffer(Context* context,
                                           uint32_t bucket_index,
                                           uint32_t index_within_bucket) {
  RIEGELI_ASSERT_LT(bucket_index, context->buckets.size())
      << "Bucket index out of range";
  DataBucket& bucket = context->buckets[bucket_index];
  RIEGELI_ASSERT_LT(index_within_bucket, !bucket.buffer_sizes.empty()
                                             ? bucket.buffer_sizes.size()
                                             : bucket.buffers.size())
      << "Index within bucket out of range";
  while (index_within_bucket >= bucket.buffers.size()) {
    if (bucket.buffers.empty()) {
      // This is the first buffer to be decompressed from this bucket.
      bucket.decompressor.Reset(std::forward_as_tuple(&bucket.compressed_data),
                                context->compression_type);
      if (ABSL_PREDICT_FALSE(!bucket.decompressor.healthy())) {
        Fail(bucket.decompressor);
        return nullptr;
      }
      // Important to prevent invalidating pointers by `emplace_back()`.
      bucket.buffers.reserve(bucket.buffer_sizes.size());
    }
    Chain buffer;
    if (ABSL_PREDICT_FALSE(!bucket.decompressor.reader()->Read(
            &buffer, bucket.buffer_sizes[bucket.buffers.size()]))) {
      Fail(*bucket.decompressor.reader(),
           DataLossError("Reading buffer failed"));
      return nullptr;
    }
    bucket.buffers.emplace_back(std::move(buffer));
    if (bucket.buffers.size() == bucket.buffer_sizes.size()) {
      // This was the last decompressed buffer from this bucket.
      if (ABSL_PREDICT_FALSE(!bucket.decompressor.VerifyEndAndClose())) {
        Fail(bucket.decompressor);
        return nullptr;
      }
      // Free memory of fields which are no longer needed.
      bucket.compressed_data = Chain();
      bucket.buffer_sizes = std::vector<size_t>();
    }
  }
  return &bucket.buffers[index_within_bucket];
}

inline bool TransposeDecoder::ContainsImplicitLoop(
    std::vector<StateMachineNode>* state_machine_nodes) {
  for (StateMachineNode& node : *state_machine_nodes) node.implicit_loop_id = 0;
  size_t next_loop_id = 1;
  for (StateMachineNode& node : *state_machine_nodes) {
    if (node.implicit_loop_id != 0) continue;
    StateMachineNode* node_ptr = &node;
    node_ptr->implicit_loop_id = next_loop_id;
    while (internal::IsImplicit(node_ptr->callback_type)) {
      node_ptr = node_ptr->next_node;
      if (node_ptr->implicit_loop_id == next_loop_id) return true;
      if (node_ptr->implicit_loop_id != 0) break;
      node_ptr->implicit_loop_id = next_loop_id;
    }
    ++next_loop_id;
  }
  return false;
}

// Copy tag from `*node` to `*dest`.
#define COPY_TAG_CALLBACK(tag_length)                               \
  do {                                                              \
    if (ABSL_PREDICT_FALSE(!dest->Write(                            \
            absl::string_view(node->tag_data.data, tag_length)))) { \
      return Fail(*dest);                                           \
    }                                                               \
  } while (false)

// Decode varint value from `*node` to `*dest`.
#define VARINT_CALLBACK(tag_length, data_length)                      \
  do {                                                                \
    if (ABSL_PREDICT_FALSE(!dest->Push(tag_length + data_length))) {  \
      return Fail(*dest);                                             \
    }                                                                 \
    dest->move_cursor(tag_length + data_length);                      \
    char* const buffer = dest->cursor();                              \
    if (ABSL_PREDICT_FALSE(                                           \
            !node->buffer->Read(buffer + tag_length, data_length))) { \
      return Fail(*node->buffer,                                      \
                  DataLossError("Reading varint field failed"));      \
    }                                                                 \
    for (size_t i = 0; i < data_length - 1; ++i) {                    \
      buffer[tag_length + i] |= 0x80;                                 \
    }                                                                 \
    std::memcpy(buffer, node->tag_data.data, tag_length);             \
  } while (false)

// Decode fixed32 or fixed64 value from `*node` to `*dest`.
#define FIXED_CALLBACK(tag_length, data_length)                                \
  do {                                                                         \
    if (ABSL_PREDICT_FALSE(!dest->Push(tag_length + data_length))) {           \
      return Fail(*dest);                                                      \
    }                                                                          \
    dest->move_cursor(tag_length + data_length);                               \
    char* const buffer = dest->cursor();                                       \
    if (ABSL_PREDICT_FALSE(                                                    \
            !node->buffer->Read(buffer + tag_length, data_length))) {          \
      return Fail(*node->buffer, DataLossError("Reading fixed field failed")); \
    }                                                                          \
    std::memcpy(buffer, node->tag_data.data, tag_length);                      \
  } while (false)

// Create zero fixed32 or fixed64 value in `*dest`.
#define FIXED_EXISTENCE_CALLBACK(tag_length, data_length)            \
  do {                                                               \
    if (ABSL_PREDICT_FALSE(!dest->Push(tag_length + data_length))) { \
      return Fail(*dest);                                            \
    }                                                                \
    dest->move_cursor(tag_length + data_length);                     \
    char* const buffer = dest->cursor();                             \
    std::memset(buffer + tag_length, '\0', data_length);             \
    std::memcpy(buffer, node->tag_data.data, tag_length);            \
  } while (false)

// Decode string value from `*node` to `*dest`.
#define STRING_CALLBACK(tag_length)                                           \
  do {                                                                        \
    node->buffer->Pull(kMaxLengthVarint32);                                   \
    uint32_t length;                                                          \
    const char* cursor = node->buffer->cursor();                              \
    if (ABSL_PREDICT_FALSE(                                                   \
            !ReadVarint32(&cursor, node->buffer->limit(), &length))) {        \
      return Fail(DataLossError("Reading string length failed"));             \
    }                                                                         \
    const size_t length_length = PtrDistance(node->buffer->cursor(), cursor); \
    if (ABSL_PREDICT_FALSE(length > std::numeric_limits<uint32_t>::max() -    \
                                        length_length)) {                     \
      return Fail(DataLossError("String length overflow"));                   \
    }                                                                         \
    if (ABSL_PREDICT_FALSE(                                                   \
            !node->buffer->CopyTo(dest, length_length + size_t{length}))) {   \
      if (!dest->healthy()) return Fail(*dest);                               \
      return Fail(*node->buffer,                                              \
                  DataLossError("Reading string field failed"));              \
    }                                                                         \
    if (ABSL_PREDICT_FALSE(!dest->Write(                                      \
            absl::string_view(node->tag_data.data, tag_length)))) {           \
      return Fail(*dest);                                                     \
    }                                                                         \
  } while (false)

inline bool TransposeDecoder::Decode(Context* context, uint64_t num_records,
                                     BackwardWriter* dest,
                                     std::vector<size_t>* limits) {
  // For now positions reported by `*dest` are pushed to `*limits` directly.
  // Later `*limits` will be reversed and complemented.
  limits->clear();
  limits->reserve(num_records);

  // Set current node to the initial node.
  StateMachineNode* node = &context->state_machine_nodes[context->first_node];
  // The depth of the current field relative to the parent submessage that
  // was excluded in projection.
  int skipped_submessage_level = 0;

  Reader* const transitions_reader = context->transitions.reader();
  // Stack of all open sub-messages.
  std::vector<SubmessageStackElement> submessage_stack;
  submessage_stack.reserve(16);
  // Number of following iteration that go directly to `node->next_node`
  // without reading transition byte.
  int num_iters = 0;

  // Note: `CallbackType` is used to index labels so the ordering of labels
  // must not change without a corresponding change in `CallbackType` enum.
  static constexpr void* labels[] = {
      &&do_transition,  // `CallbackType::kNoOp`
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
      &&fixed32_##tag_length, &&fixed64_##tag_length,                        \
      &&fixed32_existence_##tag_length, &&fixed64_existence_##tag_length,    \
      &&string_##tag_length, &&start_projection_group_##tag_length,          \
      &&end_projection_group_##tag_length

      LABELS_FOR_TAG_LEN(1),
      LABELS_FOR_TAG_LEN(2),
      LABELS_FOR_TAG_LEN(3),
      LABELS_FOR_TAG_LEN(4),
      LABELS_FOR_TAG_LEN(5),
#undef LABELS_FOR_TAG_LEN

      &&copy_tag_6,
      &&failure,  // `CallbackType::kUnknown`
  };

  for (StateMachineNode& state : context->state_machine_nodes) {
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
    return Fail(DataLossError("Skipped submessage stack underflow"));
  }
  --skipped_submessage_level;
  goto do_transition;

submessage_end:
  submessage_stack.push_back({IntCast<size_t>(dest->pos()), node->tag_data});
  goto do_transition;

failure:
  return Fail(DataLossError("Invalid node index"));

submessage_start : {
  if (ABSL_PREDICT_FALSE(submessage_stack.empty())) {
    return Fail(DataLossError("Submessage stack underflow"));
  }
  const SubmessageStackElement& elem = submessage_stack.back();
  RIEGELI_ASSERT_GE(dest->pos(), elem.end_of_submessage)
      << "Destination position decreased";
  const size_t length = IntCast<size_t>(dest->pos()) - elem.end_of_submessage;
  if (ABSL_PREDICT_FALSE(length > std::numeric_limits<uint32_t>::max())) {
    return Fail(DataLossError("Message too large"));
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

#define ACTIONS_FOR_TAG_LEN(tag_length)                                     \
  copy_tag_##tag_length : COPY_TAG_CALLBACK(tag_length);                    \
  goto do_transition;                                                       \
  varint_1_##tag_length : VARINT_CALLBACK(tag_length, 1);                   \
  goto do_transition;                                                       \
  varint_2_##tag_length : VARINT_CALLBACK(tag_length, 2);                   \
  goto do_transition;                                                       \
  varint_3_##tag_length : VARINT_CALLBACK(tag_length, 3);                   \
  goto do_transition;                                                       \
  varint_4_##tag_length : VARINT_CALLBACK(tag_length, 4);                   \
  goto do_transition;                                                       \
  varint_5_##tag_length : VARINT_CALLBACK(tag_length, 5);                   \
  goto do_transition;                                                       \
  varint_6_##tag_length : VARINT_CALLBACK(tag_length, 6);                   \
  goto do_transition;                                                       \
  varint_7_##tag_length : VARINT_CALLBACK(tag_length, 7);                   \
  goto do_transition;                                                       \
  varint_8_##tag_length : VARINT_CALLBACK(tag_length, 8);                   \
  goto do_transition;                                                       \
  varint_9_##tag_length : VARINT_CALLBACK(tag_length, 9);                   \
  goto do_transition;                                                       \
  varint_10_##tag_length : VARINT_CALLBACK(tag_length, 10);                 \
  goto do_transition;                                                       \
  fixed32_##tag_length : FIXED_CALLBACK(tag_length, 4);                     \
  goto do_transition;                                                       \
  fixed64_##tag_length : FIXED_CALLBACK(tag_length, 8);                     \
  goto do_transition;                                                       \
  fixed32_existence_##tag_length : FIXED_EXISTENCE_CALLBACK(tag_length, 4); \
  goto do_transition;                                                       \
  fixed64_existence_##tag_length : FIXED_EXISTENCE_CALLBACK(tag_length, 8); \
  goto do_transition;                                                       \
  string_##tag_length : STRING_CALLBACK(tag_length);                        \
  goto do_transition;                                                       \
  start_projection_group_##tag_length                                       \
      : if (ABSL_PREDICT_FALSE(submessage_stack.empty())) {                 \
    return Fail(DataLossError("Submessage stack underflow"));               \
  }                                                                         \
  submessage_stack.pop_back();                                              \
  COPY_TAG_CALLBACK(tag_length);                                            \
  goto do_transition;                                                       \
  end_projection_group_##tag_length                                         \
      : submessage_stack.push_back(                                         \
            {IntCast<size_t>(dest->pos()), node->tag_data});                \
  COPY_TAG_CALLBACK(tag_length);                                            \
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
    return Fail(*context->nonproto_lengths,
                DataLossError("Reading non-proto record length failed"));
  }
  if (ABSL_PREDICT_FALSE(!node->buffer->CopyTo(dest, length))) {
    if (!dest->healthy()) return Fail(*dest);
    return Fail(*node->buffer,
                DataLossError("Reading non-proto record failed"));
  }
}
  // Fall through to `message_start`.

message_start:
  if (ABSL_PREDICT_FALSE(!submessage_stack.empty())) {
    return Fail(DataLossError("Submessages still open"));
  }
  if (ABSL_PREDICT_FALSE(limits->size() == num_records)) {
    return Fail(DataLossError("Too many records"));
  }
  limits->push_back(IntCast<size_t>(dest->pos()));
  // Fall through to `do_transition`.

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
    return Fail(DataLossError("Submessages still open"));
  }
  if (ABSL_PREDICT_FALSE(skipped_submessage_level != 0)) {
    return Fail(DataLossError("Skipped submessages still open"));
  }
  if (ABSL_PREDICT_FALSE(limits->size() != num_records)) {
    return Fail(DataLossError("Too few records"));
  }
  const size_t size = limits->empty() ? size_t{0} : limits->back();
  if (ABSL_PREDICT_FALSE(size != dest->pos())) {
    return Fail(DataLossError("Unfinished message"));
  }

  // Reverse limits and complement them, but keep the last limit unchanged
  // (because both old and new limits exclude 0 at the beginning and include
  // size at the end), e.g. for records of sizes {10, 20, 30, 40}:
  // {40, 70, 90, 100} -> {10, 30, 60, 100}.
  std::vector<size_t>::iterator first = limits->begin();
  std::vector<size_t>::iterator last = limits->end();
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
// the main loop in `Decode()`.
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
    uint32_t field_id = kInvalidPos;
    if (skipped_submessage_level == 0) {
      field_included = FieldIncluded::kExistenceOnly;
      for (const SubmessageStackElement& elem : submessage_stack) {
        uint32_t tag;
        const char* cursor = elem.tag_data.data;
        if (!ReadVarint32(&cursor, elem.tag_data.data + kMaxLengthVarint32,
                          &tag)) {
          RIEGELI_ASSERT_UNREACHABLE() << "Invalid tag";
        }
        const absl::flat_hash_map<std::pair<uint32_t, uint32_t>,
                                  Context::IncludedField>::const_iterator iter =
            context->include_fields.find(std::make_pair(field_id, tag >> 3));
        if (iter == context->include_fields.end()) {
          field_included = FieldIncluded::kNo;
          break;
        }
        if (iter->second.include_type == Context::IncludeType::kIncludeFully) {
          field_included = FieldIncluded::kYes;
          break;
        }
        field_id = iter->second.field_id;
      }
    }
    // If tag is a `kStartGroup`, there are two options:
    // 1. Either related `kEndGroup` was skipped and
    //    `skipped_submessage_level > 0`.
    //    In this case `field_included` is already set to `kNo`.
    // 2. If `kEndGroup` was not skipped, then its tag is on the top of the
    //    `submessage_stack` and in that case we already checked its tag in
    //    `include_fields` in the loop above.
    const bool start_group_tag =
        static_cast<internal::WireType>(node_template->tag & 7) ==
        internal::WireType::kStartGroup;
    if (!start_group_tag && field_included == FieldIncluded::kExistenceOnly) {
      uint32_t tag;
      const char* cursor = node->tag_data.data;
      if (!ReadVarint32(&cursor, node->tag_data.data + kMaxLengthVarint32,
                        &tag)) {
        RIEGELI_ASSERT_UNREACHABLE() << "Invalid tag";
      }
      const absl::flat_hash_map<std::pair<uint32_t, uint32_t>,
                                Context::IncludedField>::const_iterator iter =
          context->include_fields.find(std::make_pair(field_id, tag >> 3));
      if (iter == context->include_fields.end()) {
        field_included = FieldIncluded::kNo;
      } else {
        if (iter->second.include_type == Context::IncludeType::kIncludeFully ||
            iter->second.include_type == Context::IncludeType::kIncludeChild) {
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
          node->buffer = kEmptyReader();
          break;
        case FieldIncluded::kExistenceOnly:
          node->buffer = kEmptyReader();
          break;
      }
    } else {
      node->buffer = kEmptyReader();
    }
    node->callback_type = GetCallbackType(field_included, node_template->tag,
                                          node_template->subtype,
                                          node_template->tag_length, true);
    if (field_included == FieldIncluded::kExistenceOnly &&
        static_cast<internal::WireType>(node_template->tag & 7) ==
            internal::WireType::kVarint) {
      // The tag in `TagData` was followed by a subtype but must be followed by
      // zero now.
      node->tag_data.data[node_template->tag_length] = 0;
    }
  }
  if (is_implicit) node->callback_type |= internal::CallbackType::kImplicit;
  return true;
}

}  // namespace riegeli
