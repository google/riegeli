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
#include <limits>
#include <memory>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/no_destructor.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/limiting_backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/string_reader.h"
#include "riegeli/chunk_encoding/constants.h"
#include "riegeli/chunk_encoding/decompressor.h"
#include "riegeli/chunk_encoding/field_projection.h"
#include "riegeli/chunk_encoding/transpose_internal.h"
#include "riegeli/messages/message_wire_format.h"
#include "riegeli/varint/varint_reading.h"
#include "riegeli/varint/varint_writing.h"

namespace riegeli {
namespace {

Reader* kEmptyReader() {
  static NoDestructor<StringReader<>> kStaticEmptyReader;
  RIEGELI_ASSERT(kStaticEmptyReader->ok()) << "kEmptyReader() has been closed";
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
  chunk_encoding_internal::Decompressor<ChainReader<>> decompressor{kClosed};
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
  switch (GetTagWireType(tag)) {
    case WireType::kVarint:
    case WireType::kFixed32:
    case WireType::kFixed64:
    case WireType::kLengthDelimited:
    case WireType::kStartGroup:
    case WireType::kEndGroup:
      return tag >= 8;
    default:
      return false;
  }
}

// The types of callbacks in state machine states.
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
};

static_assert(static_cast<uint8_t>(CallbackType::kUnknown) < (1 << 7),
              "CallbackType has too many cases to fit in 7 bits.");

constexpr CallbackType operator+(CallbackType a, uint8_t b) {
  return static_cast<CallbackType>(static_cast<uint8_t>(a) + b);
}

constexpr uint8_t operator-(CallbackType a, CallbackType b) {
  return static_cast<uint8_t>(a) - static_cast<uint8_t>(b);
}

// Node template that can be used to resolve the `CallbackType` of the node in
// decoding phase.
struct StateMachineNodeTemplate {
  // `bucket_index` and `buffer_within_bucket_index` identify the decoder
  // to read data from.
  uint32_t bucket_index;
  uint32_t buffer_within_bucket_index;
  // Proto tag of the node.
  uint32_t tag;
  // Tag subtype.
  chunk_encoding_internal::Subtype subtype;
  // Length of the varint encoded tag.
  uint8_t tag_length;
};

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
inline CallbackType GetVarintCallbackType(
    chunk_encoding_internal::Subtype subtype, size_t tag_length) {
  RIEGELI_ASSERT_GT(tag_length, 0u) << "Zero tag length";
  RIEGELI_ASSERT_LE(tag_length, kMaxLengthVarint32) << "Tag length too large";
  if (subtype > chunk_encoding_internal::Subtype::kVarintInlineMax)
    return CallbackType::kUnknown;
  if (subtype >= chunk_encoding_internal::Subtype::kVarintInline0) {
    return GetCopyTagCallbackType(tag_length + 1);
  }
  return CallbackType::kVarint_1_1 +
         (subtype - chunk_encoding_internal::Subtype::kVarint1) *
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
inline CallbackType GetStringCallbackType(
    chunk_encoding_internal::Subtype subtype, size_t tag_length) {
  RIEGELI_ASSERT_GT(tag_length, 0u) << "Zero tag length";
  RIEGELI_ASSERT_LE(tag_length, kMaxLengthVarint32) << "Tag length too large";
  switch (subtype) {
    case chunk_encoding_internal::Subtype::kLengthDelimitedString:
      return CallbackType::kString_1 +
             (tag_length - 1) *
                 (CallbackType::kString_2 - CallbackType::kString_1);
    case chunk_encoding_internal::Subtype::kLengthDelimitedEndOfSubmessage:
      return CallbackType::kSubmessageEnd;
    default:
      // Note: Nodes with `kLengthDelimitedStartOfSubmessage` are not created.
      // Start of submessage is indicated with `MessageId::kStartOfSubmessage`
      // and uses `CallbackType::kSubmessageStart`.
      return CallbackType::kUnknown;
  }
}

// Returns string callback type for `subtype` and `tag_length` to exclude field.
inline CallbackType GetStringExcludeCallbackType(
    chunk_encoding_internal::Subtype subtype, size_t tag_length) {
  RIEGELI_ASSERT_GT(tag_length, 0u) << "Zero tag length";
  RIEGELI_ASSERT_LE(tag_length, kMaxLengthVarint32) << "Tag length too large";
  switch (subtype) {
    case chunk_encoding_internal::Subtype::kLengthDelimitedString:
      return CallbackType::kNoOp;
    case chunk_encoding_internal::Subtype::kLengthDelimitedEndOfSubmessage:
      return CallbackType::kSkippedSubmessageEnd;
    default:
      return CallbackType::kUnknown;
  }
}

// Returns string existence callback type for `subtype` and `tag_length`.
inline CallbackType GetStringExistenceCallbackType(
    chunk_encoding_internal::Subtype subtype, size_t tag_length) {
  RIEGELI_ASSERT_GT(tag_length, 0u) << "Zero tag length";
  RIEGELI_ASSERT_LE(tag_length, kMaxLengthVarint32) << "Tag length too large";
  switch (subtype) {
    case chunk_encoding_internal::Subtype::kLengthDelimitedString:
      // We use the fact that there is a zero stored in `tag_data`. This decodes
      // as an empty string in proto decoder.
      return GetCopyTagCallbackType(tag_length + 1);
    case chunk_encoding_internal::Subtype::kLengthDelimitedEndOfSubmessage:
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
                                    chunk_encoding_internal::Subtype subtype,
                                    size_t tag_length,
                                    bool projection_enabled) {
  RIEGELI_ASSERT_GT(tag_length, 0u) << "Zero tag length";
  RIEGELI_ASSERT_LE(tag_length, kMaxLengthVarint32) << "Tag length too large";
  switch (field_included) {
    case FieldIncluded::kYes:
      switch (GetTagWireType(tag)) {
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
      switch (GetTagWireType(tag)) {
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
      switch (GetTagWireType(tag)) {
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

}  // namespace

// Node of the state machine read from input.
struct TransposeDecoder::StateMachineNode {
  // Tag for the field decoded by this node, may benefit from being aligned.
  //
  // `tag_data` contains varint encoded tag (1 to 5 bytes) followed by inline
  // numeric (if any) or zero otherwise.
  char tag_data[kMaxLengthVarint32 + 1];
  // Size of the tag data. Must be in the range [1..5].
  uint8_t tag_data_size : 7;
  // Whether the callback is implicit.
  bool is_implicit : 1;
  CallbackType callback_type;
  union {
    // Buffer to read data from.
    Reader* buffer;
    // In projection mode, the node is updated in decoding phase based on the
    // current submessage stack and this template.
    StateMachineNodeTemplate* node_template;
  };
  // Node to move to after finishing the callback for this node.
  StateMachineNode* next_node;
};

struct TransposeDecoder::Context {
  static_assert(sizeof(StateMachineNode) == 8 + 2 * sizeof(void*),
                "Unexpected padding in StateMachineNode.");
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
  chunk_encoding_internal::Decompressor<> transitions{kClosed};

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
  absl::flat_hash_map<std::pair<uint32_t, int>, IncludedField> include_fields;
  // Data buckets.
  std::vector<DataBucket> buckets;
  // Template that can later be used later to finalize `StateMachineNode`.
  std::vector<StateMachineNodeTemplate> node_templates;
};

bool TransposeDecoder::Decode(uint64_t num_records, uint64_t decoded_data_size,
                              const FieldProjection& field_projection,
                              Reader& src, BackwardWriter& dest,
                              std::vector<size_t>& limits) {
  RIEGELI_ASSERT_EQ(dest.pos(), 0u)
      << "Failed precondition of TransposeDecoder::Reset(): "
         "non-zero destination position";
  Object::Reset();
  if (ABSL_PREDICT_FALSE(num_records > limits.max_size())) {
    return Fail(absl::ResourceExhaustedError("Too many records"));
  }
  if (ABSL_PREDICT_FALSE(decoded_data_size >
                         std::numeric_limits<size_t>::max())) {
    return Fail(absl::ResourceExhaustedError("Records too large"));
  }

  Context context;
  if (ABSL_PREDICT_FALSE(!Parse(context, src, field_projection))) return false;
  LimitingBackwardWriter<> limiting_dest(
      &dest, LimitingBackwardWriterBase::Options()
                 .set_max_length(decoded_data_size)
                 .set_exact(field_projection.includes_all()));
  if (ABSL_PREDICT_FALSE(
          !Decode(context, num_records, limiting_dest, limits))) {
    limiting_dest.Close();
    return false;
  }
  if (ABSL_PREDICT_FALSE(!limiting_dest.Close())) {
    return Fail(limiting_dest.status());
  }
  return true;
}

inline bool TransposeDecoder::Parse(Context& context, Reader& src,
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
      const int field_number = include_field.path()[i];
      if (field_number == Field::kExistenceOnly) return false;
      uint32_t next_id = context.include_fields.size();
      Context::IncludeType include_type = Context::IncludeType::kIncludeChild;
      if (i + 1 == path_len) {
        include_type = existence_only ? Context::IncludeType::kExistenceOnly
                                      : Context::IncludeType::kIncludeFully;
      }
      Context::IncludedField& val =
          context.include_fields
              .emplace(std::make_pair(current_id, field_number),
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
  if (ABSL_PREDICT_FALSE(!src.ReadByte(compression_type_byte))) {
    return Fail(src.StatusOrAnnotate(
        absl::InvalidArgumentError("Reading compression type failed")));
  }
  context.compression_type =
      static_cast<CompressionType>(compression_type_byte);

  uint64_t header_size;
  Chain header;
  if (ABSL_PREDICT_FALSE(!ReadVarint64(src, header_size) ||
                         !src.Read(header_size, header))) {
    return Fail(src.StatusOrAnnotate(
        absl::InvalidArgumentError("Reading header failed")));
  }
  chunk_encoding_internal::Decompressor<ChainReader<>> header_decompressor(
      std::forward_as_tuple(&header), context.compression_type);
  if (ABSL_PREDICT_FALSE(!header_decompressor.ok())) {
    return Fail(header_decompressor.status());
  }

  uint32_t num_buffers;
  std::vector<uint32_t> first_buffer_indices;
  std::vector<uint32_t> bucket_indices;
  if (projection_enabled) {
    if (ABSL_PREDICT_FALSE(!ParseBuffersForFiltering(
            context, header_decompressor.reader(), src, first_buffer_indices,
            bucket_indices))) {
      return false;
    }
    num_buffers = IntCast<uint32_t>(bucket_indices.size());
  } else {
    if (ABSL_PREDICT_FALSE(
            !ParseBuffers(context, header_decompressor.reader(), src))) {
      return false;
    }
    num_buffers = IntCast<uint32_t>(context.buffers.size());
  }

  uint32_t state_machine_size;
  if (ABSL_PREDICT_FALSE(
          !ReadVarint32(header_decompressor.reader(), state_machine_size))) {
    return Fail(header_decompressor.reader().StatusOrAnnotate(
        absl::InvalidArgumentError("Reading state machine size failed")));
  }
  // Additional `0xff` nodes to correctly handle invalid/malicious inputs.
  // TODO: Handle overflow.
  context.state_machine_nodes.resize(state_machine_size + 0xff);
  if (projection_enabled) context.node_templates.resize(state_machine_size);
  std::vector<StateMachineNode>& state_machine_nodes =
      context.state_machine_nodes;
  bool has_nonproto_op = false;
  size_t num_subtypes = 0;
  std::vector<uint32_t> tags;
  tags.reserve(state_machine_size);
  for (size_t i = 0; i < state_machine_size; ++i) {
    uint32_t tag;
    if (ABSL_PREDICT_FALSE(!ReadVarint32(header_decompressor.reader(), tag))) {
      return Fail(header_decompressor.reader().StatusOrAnnotate(
          absl::InvalidArgumentError("Reading field tag failed")));
    }
    tags.push_back(tag);
    if (ValidTag(tag) && chunk_encoding_internal::HasSubtype(tag)) {
      ++num_subtypes;
    }
  }
  std::vector<uint32_t> next_node_indices;
  next_node_indices.reserve(state_machine_size);
  for (size_t i = 0; i < state_machine_size; ++i) {
    uint32_t next_node;
    if (ABSL_PREDICT_FALSE(
            !ReadVarint32(header_decompressor.reader(), next_node))) {
      return Fail(header_decompressor.reader().StatusOrAnnotate(
          absl::InvalidArgumentError("Reading next node index failed")));
    }
    next_node_indices.push_back(next_node);
  }
  std::string subtypes;
  if (ABSL_PREDICT_FALSE(
          !header_decompressor.reader().Read(num_subtypes, subtypes))) {
    return Fail(header_decompressor.reader().StatusOrAnnotate(
        absl::InvalidArgumentError("Reading subtypes failed")));
  }
  size_t subtype_index = 0;
  for (size_t i = 0; i < state_machine_size; ++i) {
    uint32_t tag = tags[i];
    StateMachineNode& state_machine_node = state_machine_nodes[i];
    state_machine_node.buffer = nullptr;
    switch (static_cast<chunk_encoding_internal::MessageId>(tag)) {
      case chunk_encoding_internal::MessageId::kNoOp:
        state_machine_node.callback_type = CallbackType::kNoOp;
        break;
      case chunk_encoding_internal::MessageId::kNonProto: {
        state_machine_node.callback_type = CallbackType::kNonProto;
        uint32_t buffer_index;
        if (ABSL_PREDICT_FALSE(
                !ReadVarint32(header_decompressor.reader(), buffer_index))) {
          return Fail(header_decompressor.reader().StatusOrAnnotate(
              absl::InvalidArgumentError("Reading buffer index failed")));
        }
        if (ABSL_PREDICT_FALSE(buffer_index >= num_buffers)) {
          return Fail(absl::InvalidArgumentError("Buffer index too large"));
        }
        if (projection_enabled) {
          const uint32_t bucket = bucket_indices[buffer_index];
          state_machine_node.buffer = GetBuffer(
              context, bucket, buffer_index - first_buffer_indices[bucket]);
          if (ABSL_PREDICT_FALSE(state_machine_node.buffer == nullptr)) {
            return false;
          }
        } else {
          state_machine_node.buffer = &context.buffers[buffer_index];
        }
        has_nonproto_op = true;
      } break;
      case chunk_encoding_internal::MessageId::kStartOfMessage:
        state_machine_node.callback_type = CallbackType::kMessageStart;
        break;
      case chunk_encoding_internal::MessageId::kStartOfSubmessage:
        if (projection_enabled) {
          context.node_templates[i].tag = static_cast<uint32_t>(
              chunk_encoding_internal::MessageId::kStartOfSubmessage);
          state_machine_node.node_template = &context.node_templates[i];
          state_machine_node.callback_type = CallbackType::kSelectCallback;
        } else {
          state_machine_node.callback_type = CallbackType::kSubmessageStart;
        }
        break;
      default: {
        chunk_encoding_internal::Subtype subtype =
            chunk_encoding_internal::Subtype::kTrivial;
        static_assert(
            chunk_encoding_internal::Subtype::kLengthDelimitedString ==
                chunk_encoding_internal::Subtype::kTrivial,
            "chunk_encoding_internal::Subtypes kLengthDelimitedString and "
            "kTrivial must be equal");
        // End of submessage is encoded as `kSubmessageWireType`.
        if (GetTagWireType(tag) ==
            chunk_encoding_internal::kSubmessageWireType) {
          tag -= static_cast<uint32_t>(
                     chunk_encoding_internal::kSubmessageWireType) -
                 static_cast<uint32_t>(WireType::kLengthDelimited);
          subtype =
              chunk_encoding_internal::Subtype::kLengthDelimitedEndOfSubmessage;
        }
        if (ABSL_PREDICT_FALSE(!ValidTag(tag))) {
          return Fail(absl::InvalidArgumentError("Invalid tag"));
        }
        char* const tag_end = WriteVarint32(tag, state_machine_node.tag_data);
        const size_t tag_length =
            PtrDistance(state_machine_node.tag_data, tag_end);
        if (chunk_encoding_internal::HasSubtype(tag)) {
          subtype = static_cast<chunk_encoding_internal::Subtype>(
              subtypes[subtype_index++]);
        }
        if (projection_enabled) {
          if (chunk_encoding_internal::HasDataBuffer(tag, subtype)) {
            uint32_t buffer_index;
            if (ABSL_PREDICT_FALSE(!ReadVarint32(header_decompressor.reader(),
                                                 buffer_index))) {
              return Fail(header_decompressor.reader().StatusOrAnnotate(
                  absl::InvalidArgumentError("Reading buffer index failed")));
            }
            if (ABSL_PREDICT_FALSE(buffer_index >= num_buffers)) {
              return Fail(absl::InvalidArgumentError("Buffer index too large"));
            }
            const uint32_t bucket = bucket_indices[buffer_index];
            context.node_templates[i].bucket_index = bucket;
            context.node_templates[i].buffer_within_bucket_index =
                buffer_index - first_buffer_indices[bucket];
          } else {
            context.node_templates[i].bucket_index = kInvalidPos;
          }
          context.node_templates[i].tag = tag;
          context.node_templates[i].subtype = subtype;
          context.node_templates[i].tag_length = IntCast<uint8_t>(tag_length);
          state_machine_node.node_template = &context.node_templates[i];
          state_machine_node.callback_type = CallbackType::kSelectCallback;
        } else {
          if (chunk_encoding_internal::HasDataBuffer(tag, subtype)) {
            uint32_t buffer_index;
            if (ABSL_PREDICT_FALSE(!ReadVarint32(header_decompressor.reader(),
                                                 buffer_index))) {
              return Fail(header_decompressor.reader().StatusOrAnnotate(
                  absl::InvalidArgumentError("Reading buffer index failed")));
            }
            if (ABSL_PREDICT_FALSE(buffer_index >= num_buffers)) {
              return Fail(absl::InvalidArgumentError("Buffer index too large"));
            }
            state_machine_node.buffer = &context.buffers[buffer_index];
          }
          state_machine_node.callback_type =
              GetCallbackType(FieldIncluded::kYes, tag, subtype, tag_length,
                              projection_enabled);
          if (ABSL_PREDICT_FALSE(state_machine_node.callback_type ==
                                 CallbackType::kUnknown)) {
            return Fail(absl::InvalidArgumentError("Invalid node"));
          }
        }
        // Store subtype right past tag in case this is inline numeric.
        if (GetTagWireType(tag) == WireType::kVarint &&
            subtype >= chunk_encoding_internal::Subtype::kVarintInline0) {
          state_machine_node.tag_data[tag_length] =
              subtype - chunk_encoding_internal::Subtype::kVarintInline0;
        } else {
          state_machine_node.tag_data[tag_length] = 0;
        }
        state_machine_node.tag_data_size = IntCast<uint8_t>(tag_length);
      }
    }
    uint32_t next_node_id = next_node_indices[i];
    if (next_node_id >= state_machine_size) {
      // Callback is implicit.
      next_node_id -= state_machine_size;
      state_machine_node.is_implicit = true;
    } else {
      state_machine_node.is_implicit = false;
    }
    if (ABSL_PREDICT_FALSE(next_node_id >= state_machine_size)) {
      return Fail(absl::InvalidArgumentError("Node index too large"));
    }

    state_machine_node.next_node = &state_machine_nodes[next_node_id];
  }

  if (has_nonproto_op) {
    // If non-proto state exists then the last buffer is the
    // `nonproto_lengths` buffer.
    if (ABSL_PREDICT_FALSE(num_buffers == 0)) {
      return Fail(
          absl::InvalidArgumentError("Missing buffer for non-proto records"));
    }
    if (projection_enabled) {
      const uint32_t bucket = bucket_indices[num_buffers - 1];
      context.nonproto_lengths = GetBuffer(
          context, bucket, num_buffers - 1 - first_buffer_indices[bucket]);
      if (ABSL_PREDICT_FALSE(context.nonproto_lengths == nullptr)) return false;
    } else {
      context.nonproto_lengths = &context.buffers.back();
    }
  }

  if (ABSL_PREDICT_FALSE(
          !ReadVarint32(header_decompressor.reader(), context.first_node))) {
    return Fail(header_decompressor.reader().StatusOrAnnotate(
        absl::InvalidArgumentError("Reading first node index failed")));
  }
  if (ABSL_PREDICT_FALSE(context.first_node >= state_machine_size)) {
    return Fail(absl::InvalidArgumentError("First node index too large"));
  }

  // Add `0xff` failure nodes so we never overflow this array.
  for (uint64_t i = state_machine_size; i < state_machine_size + 0xff; ++i) {
    state_machine_nodes[i].callback_type = CallbackType::kFailure;
  }

  if (ABSL_PREDICT_FALSE(ContainsImplicitLoop(&state_machine_nodes))) {
    return Fail(absl::InvalidArgumentError("Nodes contain an implicit loop"));
  }

  if (ABSL_PREDICT_FALSE(!header_decompressor.VerifyEndAndClose())) {
    return Fail(header_decompressor.status());
  }
  context.transitions.Reset(&src, context.compression_type);
  if (ABSL_PREDICT_FALSE(!context.transitions.ok())) {
    return Fail(context.transitions.status());
  }
  return true;
}

inline bool TransposeDecoder::ParseBuffers(Context& context,
                                           Reader& header_reader, Reader& src) {
  uint32_t num_buckets;
  if (ABSL_PREDICT_FALSE(!ReadVarint32(header_reader, num_buckets))) {
    return Fail(header_reader.StatusOrAnnotate(
        absl::InvalidArgumentError("Reading number of buckets failed")));
  }
  uint32_t num_buffers;
  if (ABSL_PREDICT_FALSE(!ReadVarint32(header_reader, num_buffers))) {
    return Fail(header_reader.StatusOrAnnotate(
        absl::InvalidArgumentError("Reading number of buffers failed")));
  }
  if (ABSL_PREDICT_FALSE(num_buffers > context.buffers.max_size())) {
    return Fail(absl::InvalidArgumentError("Too many buffers"));
  }
  if (num_buckets == 0) {
    if (ABSL_PREDICT_FALSE(num_buffers != 0)) {
      return Fail(absl::InvalidArgumentError("Too few buckets"));
    }
    return true;
  }
  context.buffers.reserve(num_buffers);
  std::vector<chunk_encoding_internal::Decompressor<ChainReader<Chain>>>
      bucket_decompressors;
  // Explicitly convert `num_buckets` to `size_t` to avoid a warning
  // `[-Wtautological-constant-out-of-range-compare]` if `max_size()` is
  // constexpr.
  if (ABSL_PREDICT_FALSE(size_t{num_buckets} >
                         bucket_decompressors.max_size())) {
    return Fail(absl::ResourceExhaustedError("Too many buckets"));
  }
  bucket_decompressors.reserve(num_buckets);
  for (uint32_t bucket_index = 0; bucket_index < num_buckets; ++bucket_index) {
    uint64_t bucket_length;
    if (ABSL_PREDICT_FALSE(!ReadVarint64(header_reader, bucket_length))) {
      return Fail(header_reader.StatusOrAnnotate(
          absl::InvalidArgumentError("Reading bucket length failed")));
    }
    if (ABSL_PREDICT_FALSE(bucket_length >
                           std::numeric_limits<size_t>::max())) {
      return Fail(absl::ResourceExhaustedError("Bucket too large"));
    }
    Chain bucket;
    if (ABSL_PREDICT_FALSE(!src.Read(IntCast<size_t>(bucket_length), bucket))) {
      return Fail(src.StatusOrAnnotate(
          absl::InvalidArgumentError("Reading bucket failed")));
    }
    bucket_decompressors.emplace_back(std::forward_as_tuple(std::move(bucket)),
                                      context.compression_type);
    if (ABSL_PREDICT_FALSE(!bucket_decompressors.back().ok())) {
      return Fail(bucket_decompressors.back().status());
    }
  }

  uint32_t bucket_index = 0;
  for (size_t buffer_index = 0; buffer_index < num_buffers; ++buffer_index) {
    uint64_t buffer_length;
    if (ABSL_PREDICT_FALSE(!ReadVarint64(header_reader, buffer_length))) {
      return Fail(header_reader.StatusOrAnnotate(
          absl::InvalidArgumentError("Reading buffer length failed")));
    }
    if (ABSL_PREDICT_FALSE(buffer_length >
                           std::numeric_limits<size_t>::max())) {
      return Fail(absl::ResourceExhaustedError("Buffer too large"));
    }
    Chain buffer;
    if (ABSL_PREDICT_FALSE(!bucket_decompressors[bucket_index].reader().Read(
            IntCast<size_t>(buffer_length), buffer))) {
      return Fail(bucket_decompressors[bucket_index].reader().StatusOrAnnotate(
          absl::InvalidArgumentError("Reading buffer failed")));
    }
    context.buffers.emplace_back(std::move(buffer));
    while (!bucket_decompressors[bucket_index].reader().Pull() &&
           bucket_index + 1 < num_buckets) {
      if (ABSL_PREDICT_FALSE(
              !bucket_decompressors[bucket_index].VerifyEndAndClose())) {
        return Fail(bucket_decompressors[bucket_index].status());
      }
      ++bucket_index;
    }
  }
  if (ABSL_PREDICT_FALSE(bucket_index + 1 < num_buckets)) {
    return Fail(absl::InvalidArgumentError("Too few buckets"));
  }
  if (ABSL_PREDICT_FALSE(
          !bucket_decompressors[bucket_index].VerifyEndAndClose())) {
    return Fail(bucket_decompressors[bucket_index].status());
  }
  return true;
}

inline bool TransposeDecoder::ParseBuffersForFiltering(
    Context& context, Reader& header_reader, Reader& src,
    std::vector<uint32_t>& first_buffer_indices,
    std::vector<uint32_t>& bucket_indices) {
  uint32_t num_buckets;
  if (ABSL_PREDICT_FALSE(!ReadVarint32(header_reader, num_buckets))) {
    return Fail(header_reader.StatusOrAnnotate(
        absl::InvalidArgumentError("Reading number of buckets failed")));
  }
  if (ABSL_PREDICT_FALSE(num_buckets > context.buckets.max_size())) {
    return Fail(absl::ResourceExhaustedError("Too many buckets"));
  }
  uint32_t num_buffers;
  if (ABSL_PREDICT_FALSE(!ReadVarint32(header_reader, num_buffers))) {
    return Fail(header_reader.StatusOrAnnotate(
        absl::InvalidArgumentError("Reading number of buffers failed")));
  }
  if (ABSL_PREDICT_FALSE(num_buffers > bucket_indices.max_size())) {
    return Fail(absl::ResourceExhaustedError("Too many buffers"));
  }
  if (num_buckets == 0) {
    if (ABSL_PREDICT_FALSE(num_buffers != 0)) {
      return Fail(absl::InvalidArgumentError("Too few buckets"));
    }
    return true;
  }
  first_buffer_indices.reserve(num_buckets);
  bucket_indices.reserve(num_buffers);
  context.buckets.reserve(num_buckets);
  for (uint32_t bucket_index = 0; bucket_index < num_buckets; ++bucket_index) {
    uint64_t bucket_length;
    if (ABSL_PREDICT_FALSE(!ReadVarint64(header_reader, bucket_length))) {
      return Fail(header_reader.StatusOrAnnotate(
          absl::InvalidArgumentError("Reading bucket length failed")));
    }
    if (ABSL_PREDICT_FALSE(bucket_length >
                           std::numeric_limits<size_t>::max())) {
      return Fail(absl::ResourceExhaustedError("Bucket too large"));
    }
    context.buckets.emplace_back();
    if (ABSL_PREDICT_FALSE(!src.Read(IntCast<size_t>(bucket_length),
                                     context.buckets.back().compressed_data))) {
      return Fail(src.StatusOrAnnotate(
          absl::InvalidArgumentError("Reading bucket failed")));
    }
  }

  uint32_t bucket_index = 0;
  first_buffer_indices.push_back(0);
  absl::optional<uint64_t> remaining_bucket_size =
      chunk_encoding_internal::UncompressedSize(
          context.buckets[0].compressed_data, context.compression_type);
  if (ABSL_PREDICT_FALSE(remaining_bucket_size == absl::nullopt)) {
    return Fail(absl::InvalidArgumentError("Reading uncompressed size failed"));
  }
  for (uint32_t buffer_index = 0; buffer_index < num_buffers; ++buffer_index) {
    uint64_t buffer_length;
    if (ABSL_PREDICT_FALSE(!ReadVarint64(header_reader, buffer_length))) {
      return Fail(header_reader.StatusOrAnnotate(
          absl::InvalidArgumentError("Reading buffer length failed")));
    }
    if (ABSL_PREDICT_FALSE(buffer_length >
                           std::numeric_limits<size_t>::max())) {
      return Fail(absl::ResourceExhaustedError("Buffer too large"));
    }
    context.buckets[bucket_index].buffer_sizes.push_back(
        IntCast<size_t>(buffer_length));
    if (ABSL_PREDICT_FALSE(buffer_length > *remaining_bucket_size)) {
      return Fail(absl::InvalidArgumentError("Buffer does not fit in bucket"));
    }
    *remaining_bucket_size -= buffer_length;
    bucket_indices.push_back(bucket_index);
    while (*remaining_bucket_size == 0 && bucket_index + 1 < num_buckets) {
      ++bucket_index;
      first_buffer_indices.push_back(buffer_index + 1);
      remaining_bucket_size = chunk_encoding_internal::UncompressedSize(
          context.buckets[bucket_index].compressed_data,
          context.compression_type);
      if (ABSL_PREDICT_FALSE(remaining_bucket_size == absl::nullopt)) {
        return Fail(
            absl::InvalidArgumentError("Reading uncompressed size failed"));
      }
    }
  }
  if (ABSL_PREDICT_FALSE(bucket_index + 1 < num_buckets)) {
    return Fail(absl::InvalidArgumentError("Too few buckets"));
  }
  if (ABSL_PREDICT_FALSE(*remaining_bucket_size > 0)) {
    return Fail(absl::InvalidArgumentError("End of data expected"));
  }
  return true;
}

inline Reader* TransposeDecoder::GetBuffer(Context& context,
                                           uint32_t bucket_index,
                                           uint32_t index_within_bucket) {
  RIEGELI_ASSERT_LT(bucket_index, context.buckets.size())
      << "Bucket index out of range";
  DataBucket& bucket = context.buckets[bucket_index];
  RIEGELI_ASSERT_LT(index_within_bucket, !bucket.buffer_sizes.empty()
                                             ? bucket.buffer_sizes.size()
                                             : bucket.buffers.size())
      << "Index within bucket out of range";
  while (index_within_bucket >= bucket.buffers.size()) {
    if (bucket.buffers.empty()) {
      // This is the first buffer to be decompressed from this bucket.
      bucket.decompressor.Reset(std::forward_as_tuple(&bucket.compressed_data),
                                context.compression_type);
      if (ABSL_PREDICT_FALSE(!bucket.decompressor.ok())) {
        Fail(bucket.decompressor.status());
        return nullptr;
      }
      // Important to prevent invalidating pointers by `emplace_back()`.
      bucket.buffers.reserve(bucket.buffer_sizes.size());
    }
    Chain buffer;
    if (ABSL_PREDICT_FALSE(!bucket.decompressor.reader().Read(
            bucket.buffer_sizes[bucket.buffers.size()], buffer))) {
      Fail(bucket.decompressor.reader().StatusOrAnnotate(
          absl::InvalidArgumentError("Reading buffer failed")));
      return nullptr;
    }
    bucket.buffers.emplace_back(std::move(buffer));
    if (bucket.buffers.size() == bucket.buffer_sizes.size()) {
      // This was the last decompressed buffer from this bucket.
      if (ABSL_PREDICT_FALSE(!bucket.decompressor.VerifyEndAndClose())) {
        Fail(bucket.decompressor.status());
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
  std::vector<size_t> implicit_loop_ids(state_machine_nodes->size(), 0);
  size_t next_loop_id = 1;
  for (size_t i = 0; i < state_machine_nodes->size(); ++i) {
    if (implicit_loop_ids[i] != 0) continue;
    StateMachineNode* node = &(*state_machine_nodes)[i];
    implicit_loop_ids[i] = next_loop_id;
    while (node->is_implicit) {
      node = node->next_node;
      const size_t j = node - state_machine_nodes->data();
      if (implicit_loop_ids[j] == next_loop_id) return true;
      if (implicit_loop_ids[j] != 0) break;
      implicit_loop_ids[j] = next_loop_id;
    }
    ++next_loop_id;
  }
  return false;
}

// InvalidArgumentError that is not inlined. This reduces register pressure for
// the Decode loop.
ABSL_ATTRIBUTE_NOINLINE absl::Status InvalidArgumentError(
    absl::string_view msg) {
  return absl::InvalidArgumentError(msg);
}

struct TransposeDecoder::DecodingState {
  // `SubmessageStack` is used to keep information about started nested
  // submessages. Decoding works in non-recursive loop and this class keeps the
  // information needed to finalize one submessages.
  //
  // A manual structure is used instead of `std::vector` to avoid unnecessary
  // zeroing or object construction in the submessage hot path.
  class SubmessageStack {
   public:
    ABSL_ATTRIBUTE_ALWAYS_INLINE void Push(size_t position,
                                           StateMachineNode* node) {
      EnsureSpaceForOne();
      positions_[size_] = position;
      nodes_[size_] = node;
      ++size_;
    }

    struct SubmessageStackElement {
      size_t end_of_submessage;
      StateMachineNode* submessage_node;
    };

    ABSL_ATTRIBUTE_ALWAYS_INLINE SubmessageStackElement Pop() {
      RIEGELI_ASSERT(!Empty());
      --size_;
      return SubmessageStackElement{positions_[size_], nodes_[size_]};
    }

    ABSL_ATTRIBUTE_ALWAYS_INLINE bool Empty() const { return size_ == 0; }

    ABSL_ATTRIBUTE_ALWAYS_INLINE absl::Span<const StateMachineNode* const>
    NodeSpan() const {
      return absl::MakeConstSpan(nodes_.get(), size_);
    }

   private:
    ABSL_ATTRIBUTE_ALWAYS_INLINE void EnsureSpaceForOne() {
      if (ABSL_PREDICT_FALSE(size_ == capacity_)) {
        if (ABSL_PREDICT_TRUE(capacity_ == 0)) {
          capacity_ = 16;
          positions_ = std::unique_ptr<size_t[]>(new size_t[capacity_]);
          nodes_ = std::unique_ptr<StateMachineNode*[]>(
              new StateMachineNode*[capacity_]);
        } else {
          capacity_ *= 2;
          std::unique_ptr<size_t[]> new_positions(new size_t[capacity_]);
          std::unique_ptr<StateMachineNode*[]> new_nodes(
              new StateMachineNode*[capacity_]);
          std::memcpy(new_positions.get(), positions_.get(),
                      size_ * sizeof(size_t));
          std::memcpy(new_nodes.get(), nodes_.get(),
                      size_ * sizeof(StateMachineNode*));
          positions_ = std::move(new_positions);
          nodes_ = std::move(new_nodes);
        }
      }
    }

    std::unique_ptr<size_t[]> positions_;
    std::unique_ptr<StateMachineNode*[]> nodes_;
    size_t size_ = 0;
    size_t capacity_ = 0;
  };

  // All pointers are non-null and owned elsewhere.
  explicit DecodingState(TransposeDecoder* decoder, Context* context,
                         uint64_t num_records, BackwardWriter* dest,
                         std::vector<size_t>* limits)
      : decoder(decoder),
        context(context),
        num_records(num_records),
        dest(dest),
        limits(limits),
        transitions_reader(&context->transitions.reader()),
        node(&context->state_machine_nodes[context->first_node]),
        num_iters(node->is_implicit ? 1 : 0) {
    // For now positions reported by `dest` are pushed to `limits` directly.
    // Later `limits` will be reversed and complemented.
    limits->clear();
    limits->reserve(num_records);
  }

  DecodingState(const DecodingState&) = delete;
  DecodingState& operator=(const DecodingState&) = delete;

  ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool SetCallbackType() {
    return decoder->SetCallbackType(*context, skipped_submessage_level,
                                    submessage_stack.NodeSpan(), *node);
  }

  // Copy tag from `*node` to `dest`.
  template <size_t tag_length>
  ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool CopyTagCallback() {
    if (ABSL_PREDICT_FALSE(
            !dest->Write(absl::string_view(node->tag_data, tag_length)))) {
      return decoder->Fail(dest->status());
    }
    return true;
  }

  template <uint8_t data_length, uint8_t low, uint8_t high>
  using EnableIfBetween =
      std::enable_if_t<(data_length >= low && data_length <= high), int>;

  template <uint8_t data_length>
  using Uint8Constant = std::integral_constant<uint8_t, data_length>;

  // Masks the first `data_length - 1` elements in `src` with 0x80, and copies
  // to `dest`.
  //
  // TODO: Replace with `if constexpr` in C++17.
  template <uint8_t data_length, EnableIfBetween<data_length, 9, 10> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE inline void MaskBuffer(
      const char* src, char* dest,
      std::integral_constant<uint8_t, data_length>) {
    uint64_t val;
    std::memcpy(&val, src, 8);
    val |= uint64_t{0x8080808080808080};
    std::memcpy(dest, &val, 8);
    MaskBuffer(src + 8, dest + 8, Uint8Constant<data_length - 8>());
  }

  template <uint8_t data_length, EnableIfBetween<data_length, 5, 8> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE inline void MaskBuffer(
      const char* src, char* dest,
      std::integral_constant<uint8_t, data_length>) {
    uint32_t val;
    std::memcpy(&val, src, 4);
    val |= uint32_t{0x80808080};
    std::memcpy(dest, &val, 4);
    MaskBuffer(src + 4, dest + 4, Uint8Constant<data_length - 4>());
  }

  template <uint8_t data_length, EnableIfBetween<data_length, 3, 4> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE inline void MaskBuffer(
      const char* src, char* dest,
      std::integral_constant<uint8_t, data_length>) {
    uint16_t val;
    std::memcpy(&val, src, 2);
    val |= uint16_t{0x8080};
    std::memcpy(dest, &val, 2);
    MaskBuffer(src + 2, dest + 2, Uint8Constant<data_length - 2>());
  }

  template <uint8_t data_length, EnableIfBetween<data_length, 2, 2> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE inline void MaskBuffer(
      const char* src, char* dest,
      std::integral_constant<uint8_t, data_length>) {
    *dest = static_cast<char>(static_cast<uint8_t>(*src) | uint8_t{0x80});
    MaskBuffer(src + 1, dest + 1, Uint8Constant<data_length - 1>());
  }

  template <uint8_t data_length, EnableIfBetween<data_length, 1, 1> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE inline void MaskBuffer(
      const char* src, char* dest,
      std::integral_constant<uint8_t, data_length>) {
    *dest = *src;
  }

  // Decode varint value from `*node` to `dest`.
  template <size_t tag_length, uint8_t data_length>
  ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool VarintCallback() {
    if (ABSL_PREDICT_FALSE(!dest->Push(tag_length + data_length))) {
      return decoder->Fail(dest->status());
    }
    dest->move_cursor(tag_length + data_length);
    // Use a temporary buffer to allow it to be in a register for processing.
    char unmasked[data_length];
    char* const buffer = dest->cursor();
    if (ABSL_PREDICT_FALSE(!node->buffer->Read(data_length, unmasked))) {
      return decoder->Fail(node->buffer->StatusOrAnnotate(
          InvalidArgumentError("Reading varint field failed")));
    }
    MaskBuffer(unmasked, buffer + tag_length, Uint8Constant<data_length>());
    std::memcpy(buffer, node->tag_data, tag_length);
    return true;
  }

  // Decode fixed32 or fixed64 value from `*node` to `dest`.
  template <size_t tag_length, size_t data_length>
  ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool FixedCallback() {
    if (ABSL_PREDICT_FALSE(!dest->Push(tag_length + data_length))) {
      return decoder->Fail(dest->status());
    }
    dest->move_cursor(tag_length + data_length);
    char* const buffer = dest->cursor();
    if (ABSL_PREDICT_FALSE(
            !node->buffer->Read(data_length, buffer + tag_length))) {
      return decoder->Fail(node->buffer->StatusOrAnnotate(
          InvalidArgumentError("Reading fixed field failed")));
    }
    std::memcpy(buffer, node->tag_data, tag_length);
    return true;
  }

  // Create zero fixed32 or fixed64 value in `dest`.
  template <size_t tag_length, size_t data_length>
  ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool FixedExistenceCallback() {
    if (ABSL_PREDICT_FALSE(!dest->Push(tag_length + data_length))) {
      return decoder->Fail(dest->status());
    }
    dest->move_cursor(tag_length + data_length);
    char* const buffer = dest->cursor();
    std::memset(buffer + tag_length, '\0', data_length);
    std::memcpy(buffer, node->tag_data, tag_length);
    return true;
  }

  // Decode string value from `*node` to `dest`.
  template <size_t tag_length>
  ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool StringCallback() {
    node->buffer->Pull(kMaxLengthVarint32);
    uint32_t length;
    const absl::optional<const char*> cursor =
        ReadVarint32(node->buffer->cursor(), node->buffer->limit(), length);
    if (ABSL_PREDICT_FALSE(cursor == absl::nullopt)) {
      return decoder->Fail(node->buffer->StatusOrAnnotate(
          InvalidArgumentError("Reading string length failed")));
    }
    const size_t length_length = PtrDistance(node->buffer->cursor(), *cursor);
    if (ABSL_PREDICT_FALSE(length > std::numeric_limits<uint32_t>::max() -
                                        length_length)) {
      return decoder->Fail(InvalidArgumentError("String length overflow"));
    }
    if (ABSL_PREDICT_FALSE(
            !node->buffer->Copy(length_length + length, *dest))) {
      if (!dest->ok()) return decoder->Fail(dest->status());
      return decoder->Fail(node->buffer->StatusOrAnnotate(
          InvalidArgumentError("Reading string field failed")));
    }
    if (ABSL_PREDICT_FALSE(
            !dest->Write(absl::string_view(node->tag_data, tag_length)))) {
      return decoder->Fail(dest->status());
    }
    return true;
  }

  template <size_t tag_length>
  ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool StartProjectionGroupCallback() {
    if (ABSL_PREDICT_FALSE(submessage_stack.Empty())) {
      return decoder->Fail(InvalidArgumentError("Submessage stack underflow"));
    }
    submessage_stack.Pop();
    return CopyTagCallback<tag_length>();
  }

  template <size_t tag_length>
  ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool EndProjectionGroupCallback() {
    submessage_stack.Push(IntCast<size_t>(dest->pos()), node);
    return CopyTagCallback<tag_length>();
  }

  ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool SubmessageStartCallback() {
    if (ABSL_PREDICT_FALSE(submessage_stack.Empty())) {
      return decoder->Fail(InvalidArgumentError("Submessage stack underflow"));
    }
    auto elem = submessage_stack.Pop();
    RIEGELI_ASSERT_GE(dest->pos(), elem.end_of_submessage)
        << "Destination position decreased";
    const size_t length = IntCast<size_t>(dest->pos()) - elem.end_of_submessage;
    if (ABSL_PREDICT_FALSE(length > std::numeric_limits<uint32_t>::max())) {
      return decoder->Fail(InvalidArgumentError("Message too large"));
    }
    const size_t varint_length = LengthVarint32(IntCast<uint32_t>(length));
    uint8_t tag_data_size = elem.submessage_node->tag_data_size;
    size_t header_length = varint_length + tag_data_size;
    if (ABSL_PREDICT_FALSE(!dest->Push(header_length))) {
      return decoder->Fail(dest->status());
    }
    dest->move_cursor(header_length);
    char* cursor = dest->cursor();
    std::memcpy(cursor, elem.submessage_node->tag_data, tag_data_size);
    WriteVarint32(IntCast<uint32_t>(length), cursor + tag_data_size);
    return true;
  }

  ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool MessageStartCallback() {
    if (ABSL_PREDICT_FALSE(!submessage_stack.Empty())) {
      return decoder->Fail(InvalidArgumentError("Submessages still open"));
    }
    if (ABSL_PREDICT_FALSE(limits->size() == num_records)) {
      return decoder->Fail(InvalidArgumentError("Too many records"));
    }
    limits->push_back(IntCast<size_t>(dest->pos()));
    return true;
  }

  ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool NonprotoCallback() {
    uint32_t length;
    if (ABSL_PREDICT_FALSE(!ReadVarint32(*context->nonproto_lengths, length))) {
      return decoder->Fail(context->nonproto_lengths->StatusOrAnnotate(
          InvalidArgumentError("Reading non-proto record length failed")));
    }
    if (ABSL_PREDICT_FALSE(!node->buffer->Copy(length, *dest))) {
      if (!dest->ok()) return decoder->Fail(dest->status());
      return decoder->Fail(node->buffer->StatusOrAnnotate(
          InvalidArgumentError("Reading non-proto record failed")));
    }
    return MessageStartCallback();
  }

  ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool HandleNode() {
    // Use a loop instead of recursion to avoid unbounded stack growth.
    // for loop is only executed more than once in the case of kSelectCallback.
    for (;;) {
      switch (node->callback_type) {
        case CallbackType::kSelectCallback:
          if (ABSL_PREDICT_FALSE(!SetCallbackType())) return false;
          continue;

        case CallbackType::kSkippedSubmessageEnd:
          ++skipped_submessage_level;
          return true;

        case CallbackType::kSkippedSubmessageStart:
          if (ABSL_PREDICT_FALSE(skipped_submessage_level == 0)) {
            return decoder->Fail(
                InvalidArgumentError("Skipped submessage stack underflow"));
          }
          --skipped_submessage_level;
          return true;

        case CallbackType::kSubmessageEnd:
          submessage_stack.Push(IntCast<size_t>(dest->pos()), node);
          return true;

        case CallbackType::kSubmessageStart:
          return SubmessageStartCallback();

#define ACTIONS_FOR_TAG_LEN(tag_length)                  \
  case CallbackType::kCopyTag_##tag_length:              \
    return CopyTagCallback<tag_length>();                \
  case CallbackType::kVarint_1_##tag_length:             \
    return VarintCallback<tag_length, 1>();              \
  case CallbackType::kVarint_2_##tag_length:             \
    return VarintCallback<tag_length, 2>();              \
  case CallbackType::kVarint_3_##tag_length:             \
    return VarintCallback<tag_length, 3>();              \
  case CallbackType::kVarint_4_##tag_length:             \
    return VarintCallback<tag_length, 4>();              \
  case CallbackType::kVarint_5_##tag_length:             \
    return VarintCallback<tag_length, 5>();              \
  case CallbackType::kVarint_6_##tag_length:             \
    return VarintCallback<tag_length, 6>();              \
  case CallbackType::kVarint_7_##tag_length:             \
    return VarintCallback<tag_length, 7>();              \
  case CallbackType::kVarint_8_##tag_length:             \
    return VarintCallback<tag_length, 8>();              \
  case CallbackType::kVarint_9_##tag_length:             \
    return VarintCallback<tag_length, 9>();              \
  case CallbackType::kVarint_10_##tag_length:            \
    return VarintCallback<tag_length, 10>();             \
  case CallbackType::kFixed32_##tag_length:              \
    return FixedCallback<tag_length, 4>();               \
  case CallbackType::kFixed64_##tag_length:              \
    return FixedCallback<tag_length, 8>();               \
  case CallbackType::kFixed32Existence_##tag_length:     \
    return FixedExistenceCallback<tag_length, 4>();      \
  case CallbackType::kFixed64Existence_##tag_length:     \
    return FixedExistenceCallback<tag_length, 8>();      \
  case CallbackType::kString_##tag_length:               \
    return StringCallback<tag_length>();                 \
  case CallbackType::kStartProjectionGroup_##tag_length: \
    return StartProjectionGroupCallback<tag_length>();   \
  case CallbackType::kEndProjectionGroup_##tag_length:   \
    return EndProjectionGroupCallback<tag_length>()

          ACTIONS_FOR_TAG_LEN(1);
          ACTIONS_FOR_TAG_LEN(2);
          ACTIONS_FOR_TAG_LEN(3);
          ACTIONS_FOR_TAG_LEN(4);
          ACTIONS_FOR_TAG_LEN(5);
#undef ACTIONS_FOR_TAG_LEN

        case CallbackType::kCopyTag_6:
          return CopyTagCallback<6>();

        case CallbackType::kUnknown:
        case CallbackType::kFailure:
          return decoder->Fail(InvalidArgumentError("Invalid node index"));

        case CallbackType::kNonProto:
          return NonprotoCallback();

        case CallbackType::kMessageStart:
          return MessageStartCallback();

        case CallbackType::kNoOp:
          return true;
      }
      RIEGELI_ASSERT_UNREACHABLE()
          << "Unknown callback type: " << static_cast<int>(node->callback_type);
    }
  }

  ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool TransitionNode() {
    node = node->next_node;
    if (num_iters == 0) {
      uint8_t transition_byte;
      if (ABSL_PREDICT_FALSE(!transitions_reader->ReadByte(transition_byte))) {
        return false;
      }
      node += (transition_byte >> 2);
      num_iters = (transition_byte & 3) + (node->is_implicit ? 1 : 0);
    } else {
      num_iters -= node->is_implicit ? 0 : 1;
    }
    return true;
  }

  ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool Finish() {
    if (ABSL_PREDICT_FALSE(!context->transitions.VerifyEndAndClose())) {
      return decoder->Fail(context->transitions.status());
    }
    if (ABSL_PREDICT_FALSE(!submessage_stack.Empty())) {
      return decoder->Fail(InvalidArgumentError("Submessages still open"));
    }
    if (ABSL_PREDICT_FALSE(skipped_submessage_level != 0)) {
      return decoder->Fail(
          InvalidArgumentError("Skipped submessages still open"));
    }
    if (ABSL_PREDICT_FALSE(limits->size() != num_records)) {
      return decoder->Fail(InvalidArgumentError("Too few records"));
    }
    const size_t size = limits->empty() ? size_t{0} : limits->back();
    if (ABSL_PREDICT_FALSE(size != dest->pos())) {
      return decoder->Fail(InvalidArgumentError("Unfinished message"));
    }
    // Reverse `limits` and complement them, but keep the last limit unchanged
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

  // Initial state
  TransposeDecoder* decoder;
  Context* context;
  uint64_t num_records;
  BackwardWriter* dest;
  std::vector<size_t>* limits;
  Reader* transitions_reader;

  // Variable state

  // The current node.
  StateMachineNode* node;
  // The depth of the current field relative to the parent submessage that
  // was excluded in projection.
  int skipped_submessage_level = 0;
  // Stack of all open sub-messages.
  SubmessageStack submessage_stack;
  // Number of following iteration that go directly to `node->next_node`
  // without reading transition byte. Maximum value is 4.
  int8_t num_iters;
};

inline bool TransposeDecoder::Decode(Context& context, uint64_t num_records,
                                     BackwardWriter& dest,
                                     std::vector<size_t>& limits) {
  DecodingState state(this, &context, num_records, &dest, &limits);
  do {
    if (ABSL_PREDICT_FALSE(!state.HandleNode())) return false;
  } while (ABSL_PREDICT_TRUE(state.TransitionNode()));
  return state.Finish();
}

// Do not inline this function. This helps Clang to generate better code for
// the main loop in `Decode()`.
ABSL_ATTRIBUTE_NOINLINE inline bool TransposeDecoder::SetCallbackType(
    Context& context, int skipped_submessage_level,
    absl::Span<const StateMachineNode* const> submessage_stack,
    StateMachineNode& node) {
  StateMachineNodeTemplate* node_template = node.node_template;
  if (node_template->tag ==
      static_cast<uint32_t>(
          chunk_encoding_internal::MessageId::kStartOfSubmessage)) {
    if (skipped_submessage_level > 0) {
      node.callback_type = CallbackType::kSkippedSubmessageStart;
    } else {
      node.callback_type = CallbackType::kSubmessageStart;
    }
  } else {
    FieldIncluded field_included = FieldIncluded::kNo;
    uint32_t field_id = kInvalidPos;
    if (skipped_submessage_level == 0) {
      field_included = FieldIncluded::kExistenceOnly;
      for (const StateMachineNode* elem : submessage_stack) {
        uint32_t tag;
        if (ReadVarint32(elem->tag_data, elem->tag_data + kMaxLengthVarint32,
                         tag) == absl::nullopt) {
          RIEGELI_ASSERT_UNREACHABLE() << "Invalid tag";
        }
        const absl::flat_hash_map<std::pair<uint32_t, int>,
                                  Context::IncludedField>::const_iterator iter =
            context.include_fields.find(
                std::make_pair(field_id, GetTagFieldNumber(tag)));
        if (iter == context.include_fields.end()) {
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
        GetTagWireType(node_template->tag) == WireType::kStartGroup;
    if (!start_group_tag && field_included == FieldIncluded::kExistenceOnly) {
      uint32_t tag;
      if (ReadVarint32(node.tag_data, node.tag_data + kMaxLengthVarint32,
                       tag) == absl::nullopt) {
        RIEGELI_ASSERT_UNREACHABLE() << "Invalid tag";
      }
      const absl::flat_hash_map<std::pair<uint32_t, int>,
                                Context::IncludedField>::const_iterator iter =
          context.include_fields.find(
              std::make_pair(field_id, GetTagFieldNumber(tag)));
      if (iter == context.include_fields.end()) {
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
          node.buffer = GetBuffer(context, node_template->bucket_index,
                                  node_template->buffer_within_bucket_index);
          if (ABSL_PREDICT_FALSE(node.buffer == nullptr)) return false;
          break;
        case FieldIncluded::kNo:
          node.buffer = kEmptyReader();
          break;
        case FieldIncluded::kExistenceOnly:
          node.buffer = kEmptyReader();
          break;
      }
    } else {
      node.buffer = kEmptyReader();
    }
    node.callback_type = GetCallbackType(field_included, node_template->tag,
                                         node_template->subtype,
                                         node_template->tag_length, true);
    if (field_included == FieldIncluded::kExistenceOnly &&
        GetTagWireType(node_template->tag) == WireType::kVarint) {
      // The tag in `tag_data` was followed by a subtype but must be followed by
      // zero now.
      node.tag_data[node_template->tag_length] = 0;
    }
  }
  return true;
}

}  // namespace riegeli
