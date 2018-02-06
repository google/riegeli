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
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/memory.h"
#include "riegeli/base/object.h"
#include "riegeli/base/string_view.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/backward_writer_utils.h"
#include "riegeli/bytes/brotli_reader.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/reader_utils.h"
#include "riegeli/bytes/writer_utils.h"
#include "riegeli/bytes/zstd_reader.h"
#include "riegeli/chunk_encoding/internal_types.h"
#include "riegeli/chunk_encoding/transpose_internal.h"

namespace riegeli {

namespace {

#define RETURN_FALSE_IF(x)                 \
  do {                                     \
    if (RIEGELI_UNLIKELY(x)) return false; \
  } while (false)

ChainReader* kEmptyChainReader() {
  static NoDestructor<ChainReader> kStaticEmptyChainReader((Chain()));
  RIEGELI_ASSERT(kStaticEmptyChainReader->healthy())
      << "kEmptyChainReader() has been closed";
  return kStaticEmptyChainReader.get();
}

class Decompressor {
 public:
  bool Initialize(ChainReader src, internal::CompressionType compression_type,
                  std::string* error_message);
  bool Initialize(Reader* src, internal::CompressionType compression_type,
                  std::string* error_message);

  Reader* reader() const { return reader_; }

  bool VerifyEndAndClose();

 private:
  bool Initialize(internal::CompressionType compression_type,
                  std::string* error_message);

  ChainReader owned_src_;
  Reader* src_;
  std::unique_ptr<Reader> owned_reader_;
  Reader* reader_;
};

bool Decompressor::Initialize(ChainReader src,
                              internal::CompressionType compression_type,
                              std::string* error_message) {
  owned_src_ = std::move(src);
  src_ = &owned_src_;
  return Initialize(compression_type, error_message);
}

bool Decompressor::Initialize(Reader* src,
                              internal::CompressionType compression_type,
                              std::string* error_message) {
  owned_src_ = ChainReader();
  src_ = RIEGELI_ASSERT_NOTNULL(src);
  return Initialize(compression_type, error_message);
}

bool Decompressor::Initialize(internal::CompressionType compression_type,
                              std::string* error_message) {
  if (compression_type == internal::CompressionType::kNone) {
    reader_ = src_;
    return true;
  }
  uint64_t uncompressed_size;
  RETURN_FALSE_IF(!ReadVarint64(src_, &uncompressed_size));
  switch (compression_type) {
    case internal::CompressionType::kNone:
      RIEGELI_ASSERT_UNREACHABLE();
    case internal::CompressionType::kBrotli:
      owned_reader_ = riegeli::make_unique<BrotliReader>(src_);
      reader_ = owned_reader_.get();
      return true;
    case internal::CompressionType::kZstd:
      owned_reader_ = riegeli::make_unique<ZstdReader>(src_);
      reader_ = owned_reader_.get();
      return true;
  }
  *error_message = "Unknown compression type: " +
                   std::to_string(static_cast<int>(compression_type));
  return false;
}

bool Decompressor::VerifyEndAndClose() {
  // Verify that decompression succeeded, there are no extra compressed data,
  // and in case the compression type was not kNone that there are no extra data
  // after the compressed stream.
  if (owned_reader_ != nullptr) {
    if (RIEGELI_UNLIKELY(!owned_reader_->VerifyEndAndClose())) return false;
  }
  if (src_ == &owned_src_) {
    if (RIEGELI_UNLIKELY(!owned_src_.VerifyEndAndClose())) return false;
  }
  return true;
}

// Returns decompressed size of data in "compressed_data" in "size".
bool DecompressedSize(internal::CompressionType compression_type,
                      const Chain& compressed_data,
                      uint64_t* uncompressed_size) {
  if (compression_type == internal::CompressionType::kNone) {
    *uncompressed_size = compressed_data.size();
    return true;
  }
  ChainReader compressed_data_reader(&compressed_data);
  return ReadVarint64(&compressed_data_reader, uncompressed_size);
}

constexpr uint32_t kInvalidPos = std::numeric_limits<uint32_t>::max();

// Information about one proto tag.
struct TagData {
  // "data" contains varint encoded tag (1 to 5 bytes) followed by inline
  // numeric (if any) or zero otherwise.
  char data[kMaxLengthVarint32() + 1];
  // Length of the varint encoded tag.
  uint8_t size;
};

// SubmessageStackElement is used to keep information about started nested
// submessages. Decoding works in non-recursive loop and this class keeps the
// information needed to finalize one submessage.
struct SubmessageStackElement {
  SubmessageStackElement(Position end_of_submessage, TagData other_tag_data)
      : end_of_submessage(end_of_submessage), tag_data(other_tag_data) {}
  // The position of the end of submessage.
  Position end_of_submessage;
  // Tag of this submessage.
  TagData tag_data;
};

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
// GetCopyTagCallbackType to work.
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
  // GetCopyTagCallbackType to work.
  kCopyTag_6,
  kUnknown,
  // Implicit callback type is added to any of the above types if the transition
  // from the node should go to "node->next_node" without reading the transition
  // byte.
  kImplicit = 0x80,
};

constexpr inline CallbackType operator+(CallbackType a, uint8_t b) {
  return static_cast<CallbackType>(static_cast<uint8_t>(a) + b);
}

constexpr inline uint8_t operator-(CallbackType a, CallbackType b) {
  return static_cast<uint8_t>(a) - static_cast<uint8_t>(b);
}

constexpr inline CallbackType operator|(CallbackType a, CallbackType b) {
  return static_cast<CallbackType>(static_cast<uint8_t>(a) |
                                   static_cast<uint8_t>(b));
}

constexpr inline CallbackType operator&(CallbackType a, CallbackType b) {
  return static_cast<CallbackType>(static_cast<uint8_t>(a) &
                                   static_cast<uint8_t>(b));
}

inline CallbackType& operator|=(CallbackType& a, CallbackType b) {
  return a = a | b;
}

// Note: If we need more callback types in the future, is_implicit can be made
// a separate state machine node member.
static_assert(static_cast<uint8_t>(CallbackType::kUnknown) <
                  static_cast<uint8_t>(CallbackType::kImplicit),
              "The total number of types overflows CallbackType.");

static_assert(CallbackType::kCopyTag_2 - CallbackType::kCopyTag_1 ==
                  CallbackType::kCopyTag_6 - CallbackType::kCopyTag_5,
              "Bad ordering of CallbackType");

// Node template that can be used to resolve the CallbackType of the node in
// decoding phase.
struct StateMachineNodeTemplate {
  // "bucket_index" and "buffer_within_bucket_index" identify the decoder
  // to read data from.
  uint32_t bucket_index;
  uint32_t buffer_within_bucket_index;
  // Proto tag of the node.
  uint32_t tag;
  // Tag subtype.
  internal::Subtype subtype;
  // Length of the varint encoded tag.
  uint8_t tag_length;
};

// Node of the state machine read from input.
struct StateMachineNode {
  union {
    // Every state has callback assigned to it that performs the state action.
    // This is an address of a label in DecodeToBuffer method which is filled
    // when DecodeToBuffer is called.
    void* callback;
    // Used to verify there are no implicit loops in the state machine.
    size_t implicit_loop_id;
  };
  // Tag for the field decoded by this node.
  TagData tag_data;
  // Note: callback_type is after tag_data which is 7 bytes and may benefit from
  // being aligned.
  CallbackType callback_type;
  union {
    // Buffer to read data from.
    ChainReader* buffer;
    // In filtering mode, the node is updated in decoding phase based on the
    // current submessage stack and this template.
    StateMachineNodeTemplate* node_template;
  };
  // Node to move to after finishing the callback for this node.
  StateMachineNode* next_node;
};

// Note: If more bytes is needed in StateMachineNode, callback_type can be moved
// to a separate vector with some refactoring.
static_assert(sizeof(StateMachineNode) == 3 * sizeof(void*) + 8,
              "Unexpected padding in StateMachineNode.");

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

// Copy tag from tag_data to writer.
#define COPY_TAG_CALLBACK(tag_length)                                  \
  do {                                                                 \
    RETURN_FALSE_IF(                                                   \
        !writer->Write(string_view(node->tag_data.data, tag_length))); \
  } while (0)

// Decode one varint value from reader to writer.
#define VARINT_CALLBACK(tag_length, data_length)                              \
  do {                                                                        \
    if (RIEGELI_LIKELY(writer->available() >= tag_length + data_length)) {    \
      writer->set_cursor(writer->cursor() - (tag_length + data_length));      \
      char* const buffer = writer->cursor();                                  \
      RETURN_FALSE_IF(!node->buffer->Read(buffer + tag_length, data_length)); \
      for (size_t i = 0; i < data_length - 1; ++i) {                          \
        buffer[tag_length + i] |= 0x80;                                       \
      }                                                                       \
      std::memcpy(buffer, node->tag_data.data, tag_length);                   \
    } else {                                                                  \
      char buffer[tag_length + data_length];                                  \
      RETURN_FALSE_IF(!node->buffer->Read(buffer + tag_length, data_length)); \
      for (size_t i = 0; i < data_length - 1; ++i) {                          \
        buffer[tag_length + i] |= 0x80;                                       \
      }                                                                       \
      std::memcpy(buffer, node->tag_data.data, tag_length);                   \
      RETURN_FALSE_IF(                                                        \
          !writer->Write(string_view(buffer, tag_length + data_length)));     \
    }                                                                         \
  } while (0)

// Decode one fixed32 or fixed64 value from reader to writer.
#define FIXED_CALLBACK(tag_length, data_length)                               \
  do {                                                                        \
    if (RIEGELI_LIKELY(writer->available() >= tag_length + data_length)) {    \
      writer->set_cursor(writer->cursor() - (tag_length + data_length));      \
      char* const buffer = writer->cursor();                                  \
      RETURN_FALSE_IF(!node->buffer->Read(buffer + tag_length, data_length)); \
      std::memcpy(buffer, node->tag_data.data, tag_length);                   \
    } else {                                                                  \
      char buffer[tag_length + data_length];                                  \
      RETURN_FALSE_IF(!node->buffer->Read(buffer + tag_length, data_length)); \
      std::memcpy(buffer, node->tag_data.data, tag_length);                   \
      RETURN_FALSE_IF(                                                        \
          !writer->Write(string_view(buffer, tag_length + data_length)));     \
    }                                                                         \
  } while (0)

// Decode string from decoder to writer.
#define STRING_CALLBACK(tag_length)                                            \
  do {                                                                         \
    size_t total_length;                                                       \
    if (RIEGELI_LIKELY(node->buffer->available() >= kMaxLengthVarint32())) {   \
      uint32_t length;                                                         \
      const char* cursor = node->buffer->cursor();                             \
      if (RIEGELI_UNLIKELY(!ReadVarint32(&cursor, &length))) {                 \
        node->buffer->set_cursor(cursor);                                      \
        return false;                                                          \
      }                                                                        \
      const size_t length_length =                                             \
          PtrDistance(node->buffer->cursor(), cursor);                         \
      total_length = length_length + IntCast<size_t>(length);                  \
    } else {                                                                   \
      const Position pos_before = node->buffer->pos();                         \
      uint32_t length;                                                         \
      RETURN_FALSE_IF(!ReadVarint32(node->buffer, &length));                   \
      RIEGELI_ASSERT_GT(node->buffer->pos(), pos_before);                      \
      const Position length_length = node->buffer->pos() - pos_before;         \
      total_length = IntCast<size_t>(length_length) + IntCast<size_t>(length); \
      if (!node->buffer->Seek(pos_before)) RIEGELI_ASSERT_UNREACHABLE();       \
    }                                                                          \
    RETURN_FALSE_IF(!node->buffer->CopyTo(writer, total_length));              \
    RETURN_FALSE_IF(                                                           \
        !writer->Write(string_view(node->tag_data.data, tag_length)));         \
  } while (0)

// Should the data content of the field be decoded?
enum class FieldIncluded {
  kYes,
  kNo,
  kExistenceOnly,
};

// Returns copy_tag callback type for "tag_length".
CallbackType GetCopyTagCallbackType(size_t tag_length) {
  return CallbackType::kCopyTag_1 +
         IntCast<uint8_t>((tag_length - 1) * (CallbackType::kCopyTag_2 -
                                              CallbackType::kCopyTag_1));
}

// Returns string callback type for "subtype" and "tag_length".
CallbackType GetStringCallbackType(internal::Subtype subtype,
                                   size_t tag_length) {
  switch (subtype) {
    case internal::Subtype::kLengthDelimitedString:
      return CallbackType::kString_1 +
             IntCast<uint8_t>((tag_length - 1) * (CallbackType::kString_2 -
                                                  CallbackType::kString_1));
    case internal::Subtype::kLengthDelimitedEndOfSubmessage:
      return CallbackType::kSubmessageEnd;
    default:
      // Note: Nodes with kLengthDelimitedStartOfSubmessage are not created.
      // Start of submessage is indicated with ReservedId
      // "kReservedIdStartOfSubmessage" and uses CallbackType::kSubmessageStart.
      return CallbackType::kUnknown;
  }
}

// Returns string callback type for "subtype" and "tag_length" to exclude field.
CallbackType GetStringExcludeCallbackType(internal::Subtype subtype,
                                          size_t tag_length) {
  switch (subtype) {
    case internal::Subtype::kLengthDelimitedString:
      return CallbackType::kNoOp;
    case internal::Subtype::kLengthDelimitedEndOfSubmessage:
      return CallbackType::kSkippedSubmessageEnd;
    default:
      return CallbackType::kUnknown;
  }
}

// Returns string callback type for "subtype" and "tag_length" for existence
// only.
CallbackType GetStringExistenceCallbackType(internal::Subtype subtype,
                                            size_t tag_length) {
  switch (subtype) {
    case internal::Subtype::kLengthDelimitedString:
      // We use the fact that there is a zero stored in TagData. This decodes as
      // an empty string in proto decoder.
      // Note: Only submessages can be "existence only" filtered, but we encode
      // empty submessages as strings so we need to handle this callback type.
      return GetCopyTagCallbackType(tag_length + 1);
    case internal::Subtype::kLengthDelimitedEndOfSubmessage:
      return CallbackType::kSubmessageEnd;
    default:
      return CallbackType::kUnknown;
  }
}

// Returns FLOAT binary callback type for "tag_length".
CallbackType GetBinaryCallbackType32(size_t tag_length) {
  return CallbackType::kFixed32_1 +
         IntCast<uint8_t>((tag_length - 1) * (CallbackType::kFixed32_2 -
                                              CallbackType::kFixed32_1));
}

// Returns DOUBLE binary callback type for "tag_length".
CallbackType GetBinaryCallbackType64(size_t tag_length) {
  return CallbackType::kFixed64_1 +
         IntCast<uint8_t>((tag_length - 1) * (CallbackType::kFixed64_2 -
                                              CallbackType::kFixed64_1));
}

// Returns numeric callback type for "subtype" and "tag_length".
CallbackType GetNumericCallbackType(internal::Subtype subtype,
                                    size_t tag_length) {
  if (subtype > internal::Subtype::kVarintInlineMax) {
    return CallbackType::kUnknown;
  }
  if (subtype >= internal::Subtype::kVarintInline0) {
    return GetCopyTagCallbackType(tag_length + 1);
  }
  return CallbackType::kVarint_1_1 +
         IntCast<uint8_t>(
             (subtype - internal::Subtype::kVarint1) *
                 (CallbackType::kVarint_2_1 - CallbackType::kVarint_1_1) +
             (tag_length - 1) *
                 (CallbackType::kVarint_1_2 - CallbackType::kVarint_1_1));
}

CallbackType GetStartFilterGroupCallbackType(size_t tag_length) {
  return CallbackType::kStartFilterGroup_1 +
         IntCast<uint8_t>((tag_length - 1) *
                          (CallbackType::kStartFilterGroup_2 -
                           CallbackType::kStartFilterGroup_1));
}

CallbackType GetEndFilterGroupCallbackType(size_t tag_length) {
  return CallbackType::kEndFilterGroup_1 +
         IntCast<uint8_t>((tag_length - 1) * (CallbackType::kEndFilterGroup_2 -
                                              CallbackType::kEndFilterGroup_1));
}

// Get callback for node.
CallbackType GetCallbackType(FieldIncluded field_included, uint32_t tag,
                             internal::Subtype subtype, size_t tag_length,
                             bool filtering_enabled) {
  if (tag_length < 1 || tag_length > kMaxLengthVarint32()) {
    return CallbackType::kUnknown;
  }
  switch (field_included) {
    case FieldIncluded::kYes:
      switch (static_cast<internal::WireType>(tag & 7)) {
        case internal::WireType::kVarint:
          return GetNumericCallbackType(subtype, tag_length);
        case internal::WireType::kFixed32:
          return GetBinaryCallbackType32(tag_length);
        case internal::WireType::kFixed64:
          return GetBinaryCallbackType64(tag_length);
        case internal::WireType::kLengthDelimited:
          return GetStringCallbackType(subtype, tag_length);
        case internal::WireType::kStartGroup:
          return filtering_enabled ? GetStartFilterGroupCallbackType(tag_length)
                                   : GetCopyTagCallbackType(tag_length);
        case internal::WireType::kEndGroup:
          return filtering_enabled ? GetEndFilterGroupCallbackType(tag_length)
                                   : GetCopyTagCallbackType(tag_length);
        default:
          return CallbackType::kUnknown;
      }
    case FieldIncluded::kNo:
      switch (static_cast<internal::WireType>(tag & 7)) {
        case internal::WireType::kVarint:
        case internal::WireType::kFixed32:
        case internal::WireType::kFixed64:
          return CallbackType::kNoOp;
        case internal::WireType::kLengthDelimited:
          return GetStringExcludeCallbackType(subtype, tag_length);
        case internal::WireType::kStartGroup:
          return CallbackType::kSkippedSubmessageStart;
        case internal::WireType::kEndGroup:
          return CallbackType::kSkippedSubmessageEnd;
        default:
          return CallbackType::kUnknown;
      }
    case FieldIncluded::kExistenceOnly:
      switch (static_cast<internal::WireType>(tag & 7)) {
        case internal::WireType::kVarint:
        case internal::WireType::kFixed32:
        case internal::WireType::kFixed64:
          return CallbackType::kUnknown;
        case internal::WireType::kLengthDelimited:
          // Only submessage fields should be allowed to be "existence only".
          return GetStringExistenceCallbackType(subtype, tag_length);
        case internal::WireType::kStartGroup:
          return GetStartFilterGroupCallbackType(tag_length);
        case internal::WireType::kEndGroup:
          return GetEndFilterGroupCallbackType(tag_length);
        default:
          return CallbackType::kUnknown;
      }
  }
  RIEGELI_ASSERT_UNREACHABLE()
      << "Unknown FieldIncluded: " << static_cast<int>(field_included);
}

bool IsImplicit(CallbackType callback_type) {
  static_assert(static_cast<uint8_t>(CallbackType::kImplicit) == 0x80,
                "Implicit is not the highest bit in uint8_t enum.");
  return (callback_type & CallbackType::kImplicit) == CallbackType::kImplicit;
}

bool ContainsImplicitLoop(std::vector<StateMachineNode>* state_machine_nodes) {
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
    while (IsImplicit(node_ptr->callback_type)) {
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

}  // namespace

struct TransposeDecoder::Context {
  // Precondition filtering_enabled == true.
  ChainReader* GetBuffer(uint32_t bucket_index, uint32_t index_within_bucket);

  // Set callback_type in "node" based on "skipped_submessage_level",
  // "submessage_stack" and "node->node_template".
  bool SetCallbackType(
      int skipped_submessage_level,
      const std::vector<SubmessageStackElement>& submessage_stack,
      StateMachineNode* node);

  // TODO: Expose message, probably by making TransposeDecoder an Object
  // and possibly moving Context contents to TransposeDecoder.
  std::string message;

  // --- Fields used by state machine nodes. ---
  // Buffer for lengths of nonproto messages.
  ChainReader* nonproto_lengths;
  // Node to start decoding from.
  uint32_t first_node;

  // --- Fields used by TransposeDecoder. ---
  // State machine read from the input.
  std::vector<StateMachineNode> state_machine_nodes;
  // Buffer containing all the data.
  // Note: Used only when filtering is disabled.
  std::vector<ChainReader> buffers;
  // State machine transitions. One byte = one transition.
  Decompressor transitions;
  // Compression type of the input.
  internal::CompressionType compression_type;

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
  // Template that can later be used later to finalize StateMachineNode.
  std::vector<StateMachineNodeTemplate> node_templates;
  // Data buckets.
  std::vector<DataBucket> buckets;
};

inline ChainReader* TransposeDecoder::Context::GetBuffer(
    uint32_t bucket_index, uint32_t index_within_bucket) {
  RIEGELI_ASSERT_LT(bucket_index, buckets.size());
  DataBucket& bucket = buckets[bucket_index];
  if (bucket.decompressed) {
    RIEGELI_ASSERT_LT(index_within_bucket, bucket.buffers.size());
  } else {
    RIEGELI_ASSERT_LT(index_within_bucket, bucket.buffer_sizes.size());
    Decompressor decompressor;
    if (!decompressor.Initialize(ChainReader(&bucket.compressed_data),
                                 compression_type, &message)) {
      return nullptr;
    }
    bucket.buffers.reserve(bucket.buffer_sizes.size());
    for (auto buffer_size : bucket.buffer_sizes) {
      Chain buffer;
      if (!decompressor.reader()->Read(&buffer, buffer_size)) return nullptr;
      bucket.buffers.emplace_back(std::move(buffer));
    }
    if (!decompressor.VerifyEndAndClose()) return nullptr;
    // Clear buffer_sizes which are no longer needed.
    bucket.buffer_sizes = std::vector<size_t>();
    bucket.compressed_data = Chain();
    bucket.decompressed = true;
  }
  return &bucket.buffers[index_within_bucket];
}

// Do not inline this function. This helps Clang to generate better code for
// the main loop in Decode().
RIEGELI_ATTRIBUTE_NOINLINE inline bool
TransposeDecoder::Context::SetCallbackType(
    int skipped_submessage_level,
    const std::vector<SubmessageStackElement>& submessage_stack,
    StateMachineNode* node) {
  const bool is_implicit = IsImplicit(node->callback_type);
  StateMachineNodeTemplate* node_template = node->node_template;
  if (node_template->tag ==
      static_cast<uint32_t>(internal::MessageId::kStartOfSubmessage)) {
    if (skipped_submessage_level > 0) {
      node->callback_type = CallbackType::kSkippedSubmessageStart;
    } else {
      node->callback_type = CallbackType::kSubmessageStart;
    }
  } else {
    FieldIncluded field_included = FieldIncluded::kNo;
    uint32_t index = kInvalidPos;
    if (skipped_submessage_level == 0) {
      field_included = FieldIncluded::kExistenceOnly;
      for (const SubmessageStackElement& elem : submessage_stack) {
        uint32_t tag;
        const char* cursor = elem.tag_data.data;
        if (!ReadVarint32(&cursor, &tag)) RIEGELI_ASSERT_UNREACHABLE();
        auto it = include_fields.find(std::make_pair(index, tag >> 3));
        if (it == include_fields.end()) {
          field_included = FieldIncluded::kNo;
          break;
        }
        index = it->second;
        if (!existence_only[index]) {
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
      if (!ReadVarint32(&cursor, &tag)) RIEGELI_ASSERT_UNREACHABLE();
      auto it = include_fields.find(std::make_pair(index, tag >> 3));
      if (it == include_fields.end()) {
        field_included = FieldIncluded::kNo;
      } else {
        index = it->second;
        if (!existence_only[index]) {
          field_included = FieldIncluded::kYes;
        }
      }
    }
    if (node_template->bucket_index != kInvalidPos) {
      switch (field_included) {
        case FieldIncluded::kYes:
          node->buffer = GetBuffer(node_template->bucket_index,
                                   node_template->buffer_within_bucket_index);
          RETURN_FALSE_IF(node->buffer == nullptr);
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
  if (is_implicit) node->callback_type |= CallbackType::kImplicit;
  return true;
}

TransposeDecoder::TransposeDecoder() = default;
TransposeDecoder::TransposeDecoder(TransposeDecoder&&) noexcept = default;
TransposeDecoder& TransposeDecoder::operator=(TransposeDecoder&&) noexcept =
    default;
TransposeDecoder::~TransposeDecoder() = default;

bool TransposeDecoder::Initialize(Reader* reader,
                                  const FieldFilter& field_filter) {
  context_ = riegeli::make_unique<Context>();
  const bool filtering_enabled = !field_filter.include_all();
  if (filtering_enabled) {
    for (const auto& include_field : field_filter.fields()) {
      RETURN_FALSE_IF(include_field.empty());
      uint32_t n = include_field[0];
      RETURN_FALSE_IF(n == 0);
      auto insert_result = context_->include_fields.emplace(
          std::make_pair(kInvalidPos, n),
          IntCast<uint32_t>(context_->existence_only.size()));
      if (insert_result.second) context_->existence_only.push_back(true);
      for (size_t i = 1; i < include_field.size(); ++i) {
        n = include_field[i];
        RETURN_FALSE_IF(n == 0);
        const uint32_t current_index = insert_result.first->second;
        insert_result = context_->include_fields.emplace(
            std::make_pair(current_index, n),
            IntCast<uint32_t>(context_->existence_only.size()));
        if (insert_result.second) context_->existence_only.push_back(true);
      }
      context_->existence_only[insert_result.first->second] = false;
    }
  }

  uint8_t compression_type_byte;
  RETURN_FALSE_IF(!ReadByte(reader, &compression_type_byte));
  context_->compression_type =
      static_cast<internal::CompressionType>(compression_type_byte);

  uint64_t header_size;
  RETURN_FALSE_IF(!ReadVarint64(reader, &header_size));
  Chain header;
  RETURN_FALSE_IF(!reader->Read(&header, header_size));
  Decompressor header_decompressor;
  RETURN_FALSE_IF(!header_decompressor.Initialize(
      ChainReader(&header), context_->compression_type, &context_->message));

  uint32_t num_buffers;
  std::vector<uint32_t> bucket_start;
  std::vector<uint32_t> bucket_indices;
  if (filtering_enabled) {
    RETURN_FALSE_IF(!ParseBuffersForFitering(
        header_decompressor.reader(), reader, &bucket_start, &bucket_indices));
    num_buffers = IntCast<uint32_t>(bucket_indices.size());
  } else {
    RETURN_FALSE_IF(!ParseBuffers(header_decompressor.reader(), reader));
    num_buffers = IntCast<uint32_t>(context_->buffers.size());
  }

  uint32_t state_machine_size;
  RETURN_FALSE_IF(
      !ReadVarint32(header_decompressor.reader(), &state_machine_size));
  // Additional 0xff nodes to correctly handle invalid/malicious inputs.
  context_->state_machine_nodes.resize(state_machine_size + 0xff);
  if (filtering_enabled) {
    context_->node_templates.resize(state_machine_size);
  }
  std::vector<StateMachineNode>& state_machine_nodes =
      context_->state_machine_nodes;
  bool has_nonproto_op = false;
  size_t num_subtypes = 0;
  std::vector<uint32_t> tags;
  tags.reserve(state_machine_size);
  for (size_t i = 0; i < state_machine_size; ++i) {
    uint32_t tag;
    RETURN_FALSE_IF(!ReadVarint32(header_decompressor.reader(), &tag));
    tags.push_back(tag);
    if (ValidTag(tag) && internal::HasSubtype(tag)) ++num_subtypes;
  }
  std::vector<uint32_t> next_node_indices;
  next_node_indices.reserve(state_machine_size);
  for (size_t i = 0; i < state_machine_size; ++i) {
    uint32_t next_node;
    RETURN_FALSE_IF(!ReadVarint32(header_decompressor.reader(), &next_node));
    next_node_indices.push_back(next_node);
  }
  std::string subtypes;
  RETURN_FALSE_IF(!header_decompressor.reader()->Read(&subtypes, num_subtypes));
  size_t subtype_index = 0;
  for (size_t i = 0; i < state_machine_size; ++i) {
    uint32_t tag = tags[i];
    StateMachineNode& state_machine_node = state_machine_nodes[i];
    state_machine_node.buffer = nullptr;
    switch (static_cast<internal::MessageId>(tag)) {
      case internal::MessageId::kNoOp:
        state_machine_node.callback_type = CallbackType::kNoOp;
        break;
      case internal::MessageId::kNonProto: {
        state_machine_node.callback_type = CallbackType::kNonProto;
        uint32_t decoder_index;
        RETURN_FALSE_IF(
            !ReadVarint32(header_decompressor.reader(), &decoder_index));
        RETURN_FALSE_IF(decoder_index >= num_buffers);
        if (filtering_enabled) {
          const uint32_t bucket = bucket_indices[decoder_index];
          state_machine_node.buffer =
              context_->GetBuffer(bucket, decoder_index - bucket_start[bucket]);
          RETURN_FALSE_IF(state_machine_node.buffer == nullptr);
        } else {
          state_machine_node.buffer = &context_->buffers[decoder_index];
        }
        has_nonproto_op = true;
      } break;
      case internal::MessageId::kStartOfMessage:
        state_machine_node.callback_type = CallbackType::kMessageStart;
        break;
      case internal::MessageId::kStartOfSubmessage:
        if (filtering_enabled) {
          context_->node_templates[i].tag =
              static_cast<uint32_t>(internal::MessageId::kStartOfSubmessage);
          state_machine_node.node_template = &context_->node_templates[i];
          state_machine_node.callback_type = CallbackType::kSelectCallback;
        } else {
          state_machine_node.callback_type = CallbackType::kSubmessageStart;
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
        RETURN_FALSE_IF(!ValidTag(tag));
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
            RETURN_FALSE_IF(
                !ReadVarint32(header_decompressor.reader(), &buffer_index));
            RETURN_FALSE_IF(buffer_index >= num_buffers);
            const uint32_t bucket = bucket_indices[buffer_index];
            context_->node_templates[i].bucket_index = bucket;
            context_->node_templates[i].buffer_within_bucket_index =
                buffer_index - bucket_start[bucket];
          } else {
            context_->node_templates[i].bucket_index = kInvalidPos;
          }
          context_->node_templates[i].tag = tag;
          context_->node_templates[i].subtype = subtype;
          context_->node_templates[i].tag_length = IntCast<uint8_t>(tag_length);
          state_machine_node.node_template = &context_->node_templates[i];
          state_machine_node.callback_type = CallbackType::kSelectCallback;
        } else {
          if (internal::HasDataBuffer(tag, subtype)) {
            uint32_t buffer_index;
            RETURN_FALSE_IF(
                !ReadVarint32(header_decompressor.reader(), &buffer_index));
            RETURN_FALSE_IF(buffer_index >= num_buffers);
            state_machine_node.buffer = &context_->buffers[buffer_index];
          }
          state_machine_node.callback_type = GetCallbackType(
              FieldIncluded::kYes, tag, subtype, tag_length, filtering_enabled);
          RETURN_FALSE_IF(state_machine_node.callback_type ==
                          CallbackType::kUnknown);
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
          state_machine_node.callback_type | CallbackType::kImplicit;
    }
    RETURN_FALSE_IF(next_node_id >= state_machine_size);

    state_machine_node.next_node = &state_machine_nodes[next_node_id];
  }

  if (has_nonproto_op) {
    // If non-proto state exists then the last buffer is the
    // nonproto_lengths buffer.
    RETURN_FALSE_IF(num_buffers == 0);
    if (filtering_enabled) {
      const uint32_t bucket = bucket_indices[num_buffers - 1];
      context_->nonproto_lengths =
          context_->GetBuffer(bucket, num_buffers - 1 - bucket_start[bucket]);
      RETURN_FALSE_IF(context_->nonproto_lengths == nullptr);
    } else {
      context_->nonproto_lengths = &context_->buffers.back();
    }
  }

  RETURN_FALSE_IF(
      !ReadVarint32(header_decompressor.reader(), &context_->first_node));
  RETURN_FALSE_IF(context_->first_node >= state_machine_size);

  // Add 0xff failure nodes so we never overflow this array.
  for (uint64_t i = state_machine_size; i < state_machine_size + 0xff; ++i) {
    state_machine_nodes[i].callback_type = CallbackType::kFailure;
  }

  RETURN_FALSE_IF(ContainsImplicitLoop(&state_machine_nodes));

  RETURN_FALSE_IF(!context_->transitions.Initialize(
      reader, context_->compression_type, &context_->message));

  RETURN_FALSE_IF(!header_decompressor.VerifyEndAndClose());
  return true;
}

bool TransposeDecoder::ParseBuffers(Reader* header_reader, Reader* reader) {
  uint32_t num_buffers;
  RETURN_FALSE_IF(!ReadVarint32(header_reader, &num_buffers));
  uint32_t num_buckets;
  RETURN_FALSE_IF(!ReadVarint32(header_reader, &num_buckets));
  if (num_buckets == 0) {
    RETURN_FALSE_IF(num_buffers != 0);
    return true;
  }
  context_->buffers.reserve(num_buffers);
  std::vector<Chain> buckets;
  buckets.resize(num_buckets);
  std::vector<Decompressor> bucket_decompressors;
  bucket_decompressors.reserve(num_buckets);
  for (uint32_t i = 0; i < num_buckets; ++i) {
    uint64_t bucket_length;
    RETURN_FALSE_IF(!ReadVarint64(header_reader, &bucket_length));
    RETURN_FALSE_IF(!reader->Read(&buckets[i], bucket_length));
    bucket_decompressors.emplace_back();
    RETURN_FALSE_IF(!bucket_decompressors.back().Initialize(
        ChainReader(&buckets[i]), context_->compression_type,
        &context_->message));
  }

  uint32_t bucket_index = 0;
  for (size_t i = 0; i < num_buffers; ++i) {
    uint64_t buffer_length;
    RETURN_FALSE_IF(!ReadVarint64(header_reader, &buffer_length));
    Chain buffer;
    RETURN_FALSE_IF(!bucket_decompressors[bucket_index].reader()->Read(
        &buffer, buffer_length));
    context_->buffers.emplace_back(std::move(buffer));
    while (!bucket_decompressors[bucket_index].reader()->Pull() &&
           bucket_index + 1 < num_buckets) {
      RETURN_FALSE_IF(!bucket_decompressors[bucket_index].VerifyEndAndClose());
      ++bucket_index;
    }
  }
  RETURN_FALSE_IF(bucket_index + 1 != num_buckets);
  RETURN_FALSE_IF(!bucket_decompressors[bucket_index].VerifyEndAndClose());
  return true;
}

bool TransposeDecoder::ParseBuffersForFitering(
    Reader* header_reader, Reader* reader, std::vector<uint32_t>* bucket_start,
    std::vector<uint32_t>* bucket_indices) {
  uint32_t num_buffers;
  RETURN_FALSE_IF(!ReadVarint32(header_reader, &num_buffers));
  uint32_t num_buckets;
  RETURN_FALSE_IF(!ReadVarint32(header_reader, &num_buckets));
  if (num_buckets == 0) {
    RETURN_FALSE_IF(num_buffers != 0);
    return true;
  }
  bucket_start->reserve(num_buckets);
  bucket_indices->reserve(num_buffers);
  context_->buckets.reserve(num_buckets);
  for (uint32_t i = 0; i < num_buckets; ++i) {
    uint64_t bucket_length;
    RETURN_FALSE_IF(!ReadVarint64(header_reader, &bucket_length));
    context_->buckets.emplace_back();
    RETURN_FALSE_IF(!reader->Read(&context_->buckets.back().compressed_data,
                                  bucket_length));
  }

  uint32_t bucket_index = 0;
  uint64_t remaining_bucket_size = 0;
  if (num_buckets > 0) {
    bucket_start->push_back(0);
    RETURN_FALSE_IF(!DecompressedSize(context_->compression_type,
                                      context_->buckets[0].compressed_data,
                                      &remaining_bucket_size));
  }
  for (uint32_t i = 0; i < num_buffers; ++i) {
    uint64_t buffer_length;
    RETURN_FALSE_IF(!ReadVarint64(header_reader, &buffer_length));
    context_->buckets[bucket_index].buffer_sizes.push_back(buffer_length);
    RETURN_FALSE_IF(buffer_length > remaining_bucket_size);
    remaining_bucket_size -= buffer_length;
    bucket_indices->push_back(bucket_index);
    while (remaining_bucket_size == 0 && bucket_index + 1 < num_buckets) {
      ++bucket_index;
      bucket_start->push_back(i + 1);
      RETURN_FALSE_IF(
          !DecompressedSize(context_->compression_type,
                            context_->buckets[bucket_index].compressed_data,
                            &remaining_bucket_size));
    }
  }
  RETURN_FALSE_IF(bucket_index + 1 != num_buckets);
  RETURN_FALSE_IF(remaining_bucket_size != 0);
  return true;
}

bool TransposeDecoder::Decode(BackwardWriter* writer,
                              std::vector<size_t>* boundaries) {
  Context* context = context_.get();

  // Set current node to the initial node.
  StateMachineNode* node =
      context->state_machine_nodes.data() + context->first_node;

  // For now positions reported by writer are pushed to boundaries directly.
  // At the end boundaries will be reversed and normalized such that
  // boundaries->front() == 0.
  boundaries->clear();
  boundaries->push_back(IntCast<size_t>(writer->pos()));
  // The depth of the current field relative to the parent submessage that was
  // excluded in filtering.
  int skipped_submessage_level = 0;

  Reader* const transitions_reader = context_->transitions.reader();
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
    state.callback = labels[static_cast<uint8_t>(state.callback_type) &
                            ~static_cast<uint8_t>(CallbackType::kImplicit)];
  }

  if (IsImplicit(node->callback_type)) ++num_iters;
  goto * node->callback;

select_callback:
  RETURN_FALSE_IF(!context_->SetCallbackType(skipped_submessage_level,
                                             submessage_stack, node));
  node->callback = labels[static_cast<uint8_t>(node->callback_type) &
                          ~static_cast<uint8_t>(CallbackType::kImplicit)];
  goto * node->callback;

skipped_submessage_end:
  ++skipped_submessage_level;
  goto do_transition;

skipped_submessage_start:
  RETURN_FALSE_IF(skipped_submessage_level == 0);
  --skipped_submessage_level;
  goto do_transition;

submessage_end:
  submessage_stack.emplace_back(writer->pos(), node->tag_data);
  goto do_transition;

failure:
  return false;

submessage_start : {
  RETURN_FALSE_IF(submessage_stack.empty());
  SubmessageStackElement elem = submessage_stack.back();
  RIEGELI_ASSERT_GE(writer->pos(), elem.end_of_submessage);
  const Position length = writer->pos() - elem.end_of_submessage;
  RETURN_FALSE_IF(length > std::numeric_limits<uint32_t>::max());
  RETURN_FALSE_IF(!WriteVarint32(writer, IntCast<uint32_t>(length)));
  RETURN_FALSE_IF(
      !writer->Write(string_view(elem.tag_data.data, elem.tag_data.size)));
  submessage_stack.pop_back();
}
  goto do_transition;

#define ACTIONS_FOR_TAG_LEN(tag_length)                                        \
  copy_tag_##tag_length : COPY_TAG_CALLBACK(tag_length);                       \
  goto do_transition;                                                          \
  varint_1_##tag_length : VARINT_CALLBACK(tag_length, 1);                      \
  goto do_transition;                                                          \
  varint_2_##tag_length : VARINT_CALLBACK(tag_length, 2);                      \
  goto do_transition;                                                          \
  varint_3_##tag_length : VARINT_CALLBACK(tag_length, 3);                      \
  goto do_transition;                                                          \
  varint_4_##tag_length : VARINT_CALLBACK(tag_length, 4);                      \
  goto do_transition;                                                          \
  varint_5_##tag_length : VARINT_CALLBACK(tag_length, 5);                      \
  goto do_transition;                                                          \
  varint_6_##tag_length : VARINT_CALLBACK(tag_length, 6);                      \
  goto do_transition;                                                          \
  varint_7_##tag_length : VARINT_CALLBACK(tag_length, 7);                      \
  goto do_transition;                                                          \
  varint_8_##tag_length : VARINT_CALLBACK(tag_length, 8);                      \
  goto do_transition;                                                          \
  varint_9_##tag_length : VARINT_CALLBACK(tag_length, 9);                      \
  goto do_transition;                                                          \
  varint_10_##tag_length : VARINT_CALLBACK(tag_length, 10);                    \
  goto do_transition;                                                          \
  fixed32_##tag_length : FIXED_CALLBACK(tag_length, 4);                        \
  goto do_transition;                                                          \
  fixed64_##tag_length : FIXED_CALLBACK(tag_length, 8);                        \
  goto do_transition;                                                          \
  string_##tag_length : STRING_CALLBACK(tag_length);                           \
  goto do_transition;                                                          \
  start_filter_group_##tag_length : RETURN_FALSE_IF(submessage_stack.empty()); \
  submessage_stack.pop_back();                                                 \
  COPY_TAG_CALLBACK(tag_length);                                               \
  goto do_transition;                                                          \
  end_filter_group_##tag_length                                                \
      : submessage_stack.emplace_back(writer->pos(), node->tag_data);          \
  COPY_TAG_CALLBACK(tag_length);                                               \
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
  RETURN_FALSE_IF(!ReadVarint32(context_->nonproto_lengths, &length));
  RETURN_FALSE_IF(!node->buffer->CopyTo(writer, length));
}
  // Fall through to message_start.

message_start:
  RETURN_FALSE_IF(!submessage_stack.empty());
  boundaries->push_back(IntCast<size_t>(writer->pos()));
  // Fall through to do_transition.

do_transition:
  node = node->next_node;
  if (num_iters == 0) {
    uint8_t transition_byte;
    if (RIEGELI_UNLIKELY(!ReadByte(transitions_reader, &transition_byte))) {
      goto done;
    }
    node += (transition_byte >> 2);
    num_iters = transition_byte & 3;
    if (IsImplicit(node->callback_type)) ++num_iters;
    goto * node->callback;
  } else {
    if (!IsImplicit(node->callback_type)) --num_iters;
    goto * node->callback;
  }

done:
  RETURN_FALSE_IF(!submessage_stack.empty());
  RETURN_FALSE_IF(skipped_submessage_level != 0);

  // Reverse boundaries and normalize them such that boundaries->front() == 0.
  const size_t size = boundaries->back();
  std::vector<size_t>::iterator first = boundaries->begin();
  std::vector<size_t>::iterator last = boundaries->end();
  while (last - first > 1) {
    --last;
    const size_t tmp = size - *first;
    *first = size - *last;
    *last = tmp;
    ++first;
  }
  if (first != last) *first = size - *first;
  return true;
}

#undef RETURN_FALSE_IF

}  // namespace riegeli
