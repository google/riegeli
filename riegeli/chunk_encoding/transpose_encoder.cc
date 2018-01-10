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

#include "riegeli/chunk_encoding/transpose_encoder.h"

#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <limits>
#include <queue>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include "riegeli/base/assert.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/memory.h"
#include "riegeli/base/string_view.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/backward_writer_utils.h"
#include "riegeli/bytes/brotli_writer.h"
#include "riegeli/bytes/chain_backward_writer.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/chain_writer.h"
#include "riegeli/bytes/limiting_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/reader_utils.h"
#include "riegeli/bytes/string_reader.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/bytes/writer_utils.h"
#include "riegeli/bytes/zstd_writer.h"
#include "riegeli/chunk_encoding/canonical_varint.h"
#include "riegeli/chunk_encoding/internal_types.h"
#include "riegeli/chunk_encoding/transpose_internal.h"

namespace riegeli {

namespace {

#define RETURN_FALSE_IF(x)                 \
  do {                                     \
    if (RIEGELI_UNLIKELY(x)) return false; \
  } while (false)

constexpr uint32_t kInvalidPos = std::numeric_limits<uint32_t>::max();
// Maximum varint value to encode as varint subtype instead of using the buffer.
constexpr uint8_t kMaxVarintInline = 3;

static_assert(kMaxVarintInline < 0x80,
              "Only one byte is used to store inline varint and its value must "
              "concide with its varint encoding");

// Maximum depth of the nested message we break into columns. Submessages with
// deeper nesting are encoded as strings.
constexpr int kMaxRecursionDepth = 100;

}  // namespace

TransposeEncoder::MessageNode::MessageNode(internal::MessageId message_id)
    : message_id(message_id) {}

TransposeEncoder::NodeId::NodeId(internal::MessageId parent_message_id,
                                 uint32_t field)
    : parent_message_id(parent_message_id), field(field) {}

bool TransposeEncoder::NodeId::operator==(NodeId other) const {
  return parent_message_id == other.parent_message_id && field == other.field;
}

size_t TransposeEncoder::NodeIdHasher::operator()(NodeId node_id) const {
  return internal::Murmur3_64(
      (static_cast<uint64_t>(node_id.parent_message_id) << 32) |
      uint64_t{node_id.field});
}

TransposeEncoder::StateInfo::StateInfo()
    : etag_index(kInvalidPos),
      base(kInvalidPos),
      canonical_source(kInvalidPos) {}

TransposeEncoder::StateInfo::StateInfo(uint32_t etag_index, uint32_t base)
    : etag_index(etag_index), base(base), canonical_source(kInvalidPos) {}

TransposeEncoder::EncodedTag::EncodedTag(internal::MessageId message_id,
                                         uint32_t tag,
                                         internal::Subtype subtype)
    : message_id(message_id), tag(tag), subtype(subtype) {}

bool TransposeEncoder::EncodedTag::operator==(EncodedTag other) const {
  return message_id == other.message_id && tag == other.tag &&
         subtype == other.subtype;
}

TransposeEncoder::DestInfo::DestInfo() : pos(kInvalidPos) {}

TransposeEncoder::EncodedTagInfo::EncodedTagInfo(EncodedTag tag)
    : tag(tag),
      state_machine_pos(kInvalidPos),
      public_list_noop_pos(kInvalidPos),
      base(kInvalidPos) {}

TransposeEncoder::BufferWithMetadata::BufferWithMetadata(
    internal::MessageId message_id, uint32_t field)
    : buffer(riegeli::make_unique<Chain>()),
      message_id(message_id),
      field(field) {}

TransposeEncoder::TransposeEncoder()
    : nonproto_lengths_(riegeli::make_unique<Chain>()),
      nonproto_lengths_writer_(nonproto_lengths_.get()) {}

void TransposeEncoder::Reset() {
  tags_list_.clear();
  encoded_tags_.clear();
  encoded_tag_pos_.clear();
  // Clear message_nodes_ before data_ because writers in message_nodes_ point
  // to data_.
  message_nodes_.clear();
  for (auto& buffers : data_) buffers.clear();
  group_stack_.clear();
  nonproto_lengths_writer_.Cancel();
  nonproto_lengths_->Clear();
  nonproto_lengths_writer_ = ChainBackwardWriter(nonproto_lengths_.get());
  next_message_id_ = internal::MessageId::kRoot + 1;
}

static_assert(std::is_move_constructible<TransposeEncoder>::value,
              "TransposeEncoder must be move_constructible");

namespace {

// Returns true if "message" is a valid protocol buffer message in the canonical
// encoding. The purpose of this method is to distinguish string from a
// submessage in the proto wire format and to perform validity checks that are
// asserted later (such as that double proto field is followed by at least 8
// bytes of data).
// Note: Protocol buffer with suboptimal varint encoded tags and values (such as
// 0x87,0x00 instead of 0x07) would parse successfully with the default proto
// parser. This can happen for binary strings in proto. However, we need to
// produce exactly the same bytes in the output so we reject message encoded
// in non-canonical way.
bool IsProtoMessage(Reader* message) {
  // We validate that all started proto groups are closed with endgroup tag.
  std::vector<uint32_t> started_groups;
  while (message->Pull()) {
    uint32_t tag;
    RETURN_FALSE_IF(!ReadCanonicalVarint32(message, &tag));
    const uint32_t field = tag >> 3;
    RETURN_FALSE_IF(field == 0);
    switch (static_cast<internal::WireType>(tag & 7)) {
      case internal::WireType::kVarint:
        RETURN_FALSE_IF(!VerifyCanonicalVarint64(message));
        break;
      case internal::WireType::kFixed32:
        RETURN_FALSE_IF(!message->Skip(sizeof(uint32_t)));
        break;
      case internal::WireType::kFixed64:
        RETURN_FALSE_IF(!message->Skip(sizeof(uint64_t)));
        break;
      case internal::WireType::kLengthDelimited: {
        uint32_t length;
        RETURN_FALSE_IF(!ReadCanonicalVarint32(message, &length));
        RETURN_FALSE_IF(!message->Skip(length));
      } break;
      case internal::WireType::kStartGroup:
        started_groups.push_back(field);
        break;
      case internal::WireType::kEndGroup:
        RETURN_FALSE_IF(started_groups.empty() ||
                        started_groups.back() != field);
        started_groups.pop_back();
        break;
      default:
        return false;
    }
  }
  return message->healthy() && started_groups.empty();
}

}  // namespace

size_t TransposeEncoder::Uint32Hasher::operator()(uint32_t v) const {
  return internal::Murmur3_64(v);
}

size_t TransposeEncoder::EncodedTagHasher::operator()(
    EncodedTag encoded_tag) const {
  static_assert(static_cast<uint8_t>(internal::Subtype::kVarintInline0) +
                        kMaxVarintInline <=
                    15,
                "Update the hash function if any subtype can exceed 15");
  return internal::Murmur3_64(
      (uint64_t{encoded_tag.tag} << 32) ^
      (static_cast<uint64_t>(encoded_tag.message_id) << 4) ^
      static_cast<uint64_t>(encoded_tag.subtype));
}

void TransposeEncoder::AddMessage(string_view message) {
  StringReader reader(message.data(), message.size());
  AddMessageInternal(&reader);
}

void TransposeEncoder::AddMessage(std::string&& message) {
  if (message.size() <= kMaxBytesToCopy()) {
    AddMessage(string_view(message));
  } else {
    AddMessage(Chain(std::move(message)));
  }
}

void TransposeEncoder::AddMessage(const Chain& message) {
  ChainReader reader(&message);
  AddMessageInternal(&reader);
}

void TransposeEncoder::AddMessageInternal(Reader* message) {
  RIEGELI_ASSERT_EQ(message->pos(), 0u);
  Position size;
  if (!message->Size(&size)) RIEGELI_UNREACHABLE();
  const bool is_proto = IsProtoMessage(message);
  if (!message->Seek(0)) RIEGELI_UNREACHABLE();
  if (is_proto) {
    encoded_tags_.push_back(GetPosInTagsList(EncodedTag(
        internal::MessageId::kStartOfMessage, 0, internal::Subtype::kTrivial)));
    AddMessageInternal(message, internal::MessageId::kRoot, 0);
  } else {
    encoded_tags_.push_back(GetPosInTagsList(EncodedTag(
        internal::MessageId::kNonProto, 0, internal::Subtype::kTrivial)));
    if (!message->CopyTo(
            GetBuffer(internal::MessageId::kNonProto, 0, BufferType::kNonProto),
            size)) {
      RIEGELI_UNREACHABLE();
    }
    WriteVarint64(&nonproto_lengths_writer_, size);
  }
}

ChainBackwardWriter* TransposeEncoder::GetBuffer(
    internal::MessageId parent_message_id, uint32_t field, BufferType type) {
  auto insert_result = message_nodes_.emplace(NodeId(parent_message_id, field),
                                              MessageNode(next_message_id_));
  if (insert_result.second) {
    // New node was added.
    ++next_message_id_;
  }
  MessageNode& node = insert_result.first->second;
  if (node.writer == nullptr) {
    auto& data = data_[static_cast<uint32_t>(type)];
    data.emplace_back(parent_message_id, field);
    node.writer =
        riegeli::make_unique<ChainBackwardWriter>(data.back().buffer.get());
  }
  return node.writer.get();
}

uint32_t TransposeEncoder::GetPosInTagsList(EncodedTag etag) {
  const auto insert_result = encoded_tag_pos_.emplace(etag, tags_list_.size());
  if (insert_result.second) {
    tags_list_.emplace_back(etag);
  }
  return insert_result.first->second;
}

// Precondition: IsProtoMessage returns true for this message.
// Note: EncodedTags are appended into "encoded_tags_" but data is prepended
// into respective buffers. "encoded_tags_" will be reversed later in
// WriteToBuffer call.
void TransposeEncoder::AddMessageInternal(Reader* message,
                                          internal::MessageId parent_message_id,
                                          int depth) {
  while (message->Pull()) {
    uint32_t tag;
    if (!ReadVarint32(message, &tag)) RIEGELI_UNREACHABLE();
    const uint32_t field = tag >> 3;
    switch (static_cast<internal::WireType>(tag & 7)) {
      case internal::WireType::kVarint: {
        char value[kMaxLengthVarint64()];
        char* const value_end = CopyCanonicalVarint64(message, value);
        if (value_end == nullptr) RIEGELI_UNREACHABLE();
        const size_t value_length = value_end - value;
        RIEGELI_ASSERT_GT(value_length, 0u);
        if (static_cast<uint8_t>(value[0]) <= kMaxVarintInline) {
          encoded_tags_.push_back(
              GetPosInTagsList(EncodedTag(parent_message_id, tag,
                                          internal::Subtype::kVarintInline0 +
                                              static_cast<uint8_t>(value[0]))));
        } else {
          encoded_tags_.push_back(GetPosInTagsList(
              EncodedTag(parent_message_id, tag,
                         internal::Subtype::kVarint1 + (value_length - 1))));
          // TODO: Consider processing the whole sizeof value instead.
          for (size_t i = 0; i < value_length - 1; ++i) value[i] &= ~0x80;
          GetBuffer(parent_message_id, field, BufferType::kVarint)
              ->Write(string_view(value, value_length));
        }
      } break;
      case internal::WireType::kFixed32:
        encoded_tags_.push_back(GetPosInTagsList(
            EncodedTag(parent_message_id, tag, internal::Subtype::kTrivial)));
        if (!message->CopyTo(
                GetBuffer(parent_message_id, field, BufferType::kFixed32),
                sizeof(uint32_t))) {
          RIEGELI_UNREACHABLE();
        }
        break;
      case internal::WireType::kFixed64:
        encoded_tags_.push_back(GetPosInTagsList(
            EncodedTag(parent_message_id, tag, internal::Subtype::kTrivial)));
        if (!message->CopyTo(
                GetBuffer(parent_message_id, field, BufferType::kFixed64),
                sizeof(uint64_t))) {
          RIEGELI_UNREACHABLE();
        }
        break;
      case internal::WireType::kLengthDelimited: {
        uint32_t length;
        const Position length_pos = message->pos();
        if (!ReadVarint32(message, &length)) RIEGELI_UNREACHABLE();
        const Position value_pos = message->pos();
        LimitingReader value(message, value_pos + length);
        // Non-toplevel empty strings are treated as strings, not messages.
        // They have a simpler encoding this way (one node instead of two).
        if (depth < kMaxRecursionDepth && length != 0 &&
            IsProtoMessage(&value)) {
          encoded_tags_.push_back(GetPosInTagsList(EncodedTag(
              parent_message_id, tag,
              internal::Subtype::kLengthDelimitedStartOfSubmessage)));
          auto insert_result = message_nodes_.emplace(
              NodeId(parent_message_id, field), MessageNode(next_message_id_));
          if (insert_result.second) {
            // New node was added.
            ++next_message_id_;
          }
          if (!value.Seek(value_pos)) RIEGELI_UNREACHABLE();
          AddMessageInternal(&value, insert_result.first->second.message_id,
                             depth + 1);
          encoded_tags_.push_back(GetPosInTagsList(
              EncodedTag(parent_message_id, tag,
                         internal::Subtype::kLengthDelimitedEndOfSubmessage)));
        } else {
          value.Cancel();
          encoded_tags_.push_back(GetPosInTagsList(
              EncodedTag(parent_message_id, tag,
                         internal::Subtype::kLengthDelimitedString)));
          if (!message->Seek(length_pos)) RIEGELI_UNREACHABLE();
          if (!message->CopyTo(
                  GetBuffer(parent_message_id, field, BufferType::kString),
                  value_pos - length_pos + length)) {
            RIEGELI_UNREACHABLE();
          }
        }
      } break;
      case internal::WireType::kStartGroup: {
        encoded_tags_.push_back(GetPosInTagsList(
            EncodedTag(parent_message_id, tag, internal::Subtype::kTrivial)));
        auto insert_result = message_nodes_.emplace(
            NodeId(parent_message_id, field), MessageNode(next_message_id_));
        if (insert_result.second) {
          // New node was added.
          ++next_message_id_;
        }
        group_stack_.push_back(parent_message_id);
        ++depth;
        parent_message_id = insert_result.first->second.message_id;
      } break;
      case internal::WireType::kEndGroup:
        parent_message_id = group_stack_.back();
        group_stack_.pop_back();
        --depth;
        encoded_tags_.push_back(GetPosInTagsList(
            EncodedTag(parent_message_id, tag, internal::Subtype::kTrivial)));
        break;
      default:
        RIEGELI_UNREACHABLE() << "Bug in IsProtoMessage()?";
    }
  }
  RIEGELI_ASSERT(message->healthy());
}

struct TransposeEncoder::BufferWithMetadataSizeComparator {
  bool operator()(const BufferWithMetadata& a,
                  const BufferWithMetadata& b) const {
    if (a.buffer->size() != b.buffer->size()) {
      return a.buffer->size() > b.buffer->size();
    }
    // Break ties for reproducible ordering.
    if (a.message_id != b.message_id) {
      return a.message_id < b.message_id;
    }
    return a.field < b.field;
  }
};

// TODO: Consider reducing indirections (writing directly to the
// compressor, and/or letting the compressor write directly to dest if
// prepend_compressed_size is false).
void TransposeEncoder::AppendCompressedBuffer(bool prepend_compressed_size,
                                              const Chain& input,
                                              Writer* dest) const {
  switch (compression_type_) {
    case internal::CompressionType::kNone:
      if (prepend_compressed_size) WriteVarint64(dest, input.size());
      dest->Write(input);
      return;
    case internal::CompressionType::kBrotli: {
      Chain compressed;
      ChainWriter compressed_writer(&compressed);
      BrotliWriter compressor(&compressed_writer,
                              BrotliWriter::Options()
                                  .set_compression_level(compression_level_)
                                  .set_size_hint(input.size()));
      compressor.Write(input);
      // TODO: Expose compressor failures by TransposeEncoder.
      if (!compressor.Close()) RIEGELI_UNREACHABLE();
      if (!compressed_writer.Close()) RIEGELI_UNREACHABLE();
      if (prepend_compressed_size) {
        WriteVarint64(dest, LengthVarint64(input.size()) + compressed.size());
      }
      WriteVarint64(dest, input.size());
      dest->Write(std::move(compressed));
      return;
    }
    case internal::CompressionType::kZstd: {
      Chain compressed;
      ChainWriter compressed_writer(&compressed);
      ZstdWriter compressor(&compressed_writer,
                            ZstdWriter::Options()
                                .set_compression_level(compression_level_)
                                .set_size_hint(input.size()));
      compressor.Write(input);
      // TODO: Expose compressor failures by TransposeEncoder.
      if (!compressor.Close()) RIEGELI_UNREACHABLE();
      if (!compressed_writer.Close()) RIEGELI_UNREACHABLE();
      if (prepend_compressed_size) {
        WriteVarint64(dest, LengthVarint64(input.size()) + compressed.size());
      }
      WriteVarint64(dest, input.size());
      dest->Write(std::move(compressed));
      return;
    }
  }
  RIEGELI_UNREACHABLE() << "Unknown compression type: "
                        << static_cast<int>(compression_type_);
}

void TransposeEncoder::AddBuffer(bool force_new_bucket, const Chain& next_chunk,
                                 Chain* bucket_buffer,
                                 ChainWriter* bucket_writer,
                                 ChainWriter* data_writer,
                                 std::vector<size_t>* bucket_lengths,
                                 std::vector<size_t>* buffer_lengths) {
  buffer_lengths->push_back(next_chunk.size());
  if (compression_type_ != internal::CompressionType::kNone &&
      bucket_writer->pos() > 0 &&
      (force_new_bucket ||
       bucket_writer->pos() + next_chunk.size() > desired_bucket_size_)) {
    if (!bucket_writer->Close()) RIEGELI_UNREACHABLE();
    const size_t pos_before = data_writer->pos();
    AppendCompressedBuffer(/*prepend_compressed_size=*/false, *bucket_buffer,
                           data_writer);
    bucket_buffer->Clear();
    *bucket_writer = ChainWriter(bucket_buffer);
    bucket_lengths->push_back(data_writer->pos() - pos_before);
  }
  bucket_writer->Write(next_chunk);
}

std::unordered_map<TransposeEncoder::NodeId, uint32_t,
                   TransposeEncoder::NodeIdHasher>
TransposeEncoder::WriteBuffers(ChainWriter* header_writer,
                               ChainWriter* data_writer) {
  size_t num_buffers = 0;
  for (size_t i = 0; i < kNumBufferTypes; ++i) {
    // Sort data_ by length, largest to smallest.
    std::sort(data_[i].begin(), data_[i].end(),
              BufferWithMetadataSizeComparator());
    num_buffers += data_[i].size();
  }
  if (!nonproto_lengths_->empty()) ++num_buffers;

  std::vector<size_t> buffer_lengths;
  buffer_lengths.reserve(num_buffers);
  std::vector<size_t> bucket_lengths;

  Chain bucket_buffer;
  ChainWriter bucket_writer(&bucket_buffer);
  std::unordered_map<NodeId, uint32_t, NodeIdHasher> buffer_pos;
  // Write all buffer lengths to the header and data to "bucket_buffer".
  for (size_t i = 0; i < kNumBufferTypes; ++i) {
    for (size_t j = 0; j < data_[i].size(); ++j) {
      const auto& x = data_[i][j];
      AddBuffer(j == 0, *x.buffer, &bucket_buffer, &bucket_writer, data_writer,
                &bucket_lengths, &buffer_lengths);
      const uint32_t pos = buffer_pos.size();
      buffer_pos[NodeId(x.message_id, x.field)] = pos;
    }
  }
  if (!nonproto_lengths_->empty()) {
    // nonproto_lengths_ is the last buffer if non-empty.
    AddBuffer(/*force_new_bucket=*/true, *nonproto_lengths_, &bucket_buffer,
              &bucket_writer, data_writer, &bucket_lengths, &buffer_lengths);
    // Note: nonproto_lengths_ needs no buffer_pos.
  }

  if (bucket_writer.pos() > 0) {
    // Last bucket.
    if (!bucket_writer.Close()) RIEGELI_UNREACHABLE();
    const Position pos_before = data_writer->pos();
    AppendCompressedBuffer(/*prepend_compressed_size=*/false, bucket_buffer,
                           data_writer);
    bucket_lengths.push_back(data_writer->pos() - pos_before);
  }

  RIEGELI_ASSERT_EQ(num_buffers, buffer_lengths.size());
  WriteVarint64(header_writer, num_buffers);
  WriteVarint32(header_writer, bucket_lengths.size());
  for (size_t length : bucket_lengths) {
    WriteVarint64(header_writer, length);
  }
  for (size_t length : buffer_lengths) {
    WriteVarint64(header_writer, length);
  }

  return buffer_pos;
}

void TransposeEncoder::WriteStatesAndData(
    uint32_t max_transition, const std::vector<StateInfo>& state_machine,
    ChainWriter* header_writer, ChainWriter* data_writer) {
  if (!encoded_tags_.empty() &&
      tags_list_[encoded_tags_[0]].dest_info.size() == 1) {
    // There should be no implicit transition from the last state. If there was
    // one, then it would not be obvious whether to stop or continue decoding.
    // Only if transition is explicit we check whether there is more transition
    // bytes.
    auto& dest_info = tags_list_[encoded_tags_[0]].dest_info;
    const auto first_key = dest_info.begin()->first;
    dest_info[first_key + 1];
    RIEGELI_ASSERT_NE(tags_list_[encoded_tags_[0]].dest_info.size(), 1u);
  }
  const std::unordered_map<NodeId, uint32_t, NodeIdHasher> buffer_pos =
      WriteBuffers(header_writer, data_writer);

  std::string subtype_to_write;
  std::vector<uint32_t> buffer_index_to_write;
  std::vector<uint32_t> base_to_write;

  base_to_write.reserve(state_machine.size());

  WriteVarint32(header_writer, state_machine.size());
  for (auto state_info : state_machine) {
    if (state_info.etag_index == kInvalidPos) {
      // NoOp state.
      WriteVarint32(header_writer,
                    static_cast<uint32_t>(internal::MessageId::kNoOp));
      base_to_write.push_back(state_info.base);
      continue;
    }
    EncodedTag etag = tags_list_[state_info.etag_index].tag;
    if (etag.tag != 0) {
      const bool is_string = static_cast<internal::WireType>(etag.tag & 7) ==
                             internal::WireType::kLengthDelimited;
      if (is_string &&
          etag.subtype ==
              internal::Subtype::kLengthDelimitedStartOfSubmessage) {
        WriteVarint32(
            header_writer,
            static_cast<uint32_t>(internal::MessageId::kStartOfSubmessage));
      } else if (is_string &&
                 etag.subtype ==
                     internal::Subtype::kLengthDelimitedEndOfSubmessage) {
        // End of submessage is encoded as WireType::kSubmessage instead of
        // WireType::kLengthDelimited.
        WriteVarint32(header_writer,
                      etag.tag + (internal::WireType::kSubmessage -
                                  internal::WireType::kLengthDelimited));
      } else {
        WriteVarint32(header_writer, etag.tag);
        if (internal::HasSubtype(etag.tag)) {
          subtype_to_write.push_back(static_cast<char>(etag.subtype));
        }
        if (internal::HasDataBuffer(etag.tag, etag.subtype)) {
          auto it2 = buffer_pos.find(NodeId(etag.message_id, etag.tag >> 3));
          RIEGELI_ASSERT(it2 != buffer_pos.end());
          buffer_index_to_write.push_back(it2->second);
        }
      }
    } else {
      // NonProto and StartOfMessage special IDs.
      WriteVarint32(header_writer, static_cast<uint32_t>(etag.message_id));
      if (etag.message_id == internal::MessageId::kNonProto) {
        // NonProto has data buffer.
        auto it2 = buffer_pos.find(NodeId(internal::MessageId::kNonProto, 0));
        RIEGELI_ASSERT(it2 != buffer_pos.end());
        buffer_index_to_write.push_back(it2->second);
      } else {
        RIEGELI_ASSERT_EQ(
            static_cast<uint32_t>(etag.message_id),
            static_cast<uint32_t>(internal::MessageId::kStartOfMessage));
      }
    }
    if (tags_list_[state_info.etag_index].base != kInvalidPos) {
      // Signal implicit transition by adding "state_machine.size()".
      base_to_write.push_back(
          tags_list_[state_info.etag_index].base +
          (tags_list_[state_info.etag_index].dest_info.size() == 1
               ? state_machine.size()
               : 0));
    } else {
      // If there is no outgoing transition from this state, just output zero.
      base_to_write.push_back(0);
    }
  }
  for (auto v : base_to_write) {
    WriteVarint32(header_writer, v);
  }
  header_writer->Write(subtype_to_write);
  for (auto v : buffer_index_to_write) {
    WriteVarint32(header_writer, v);
  }

  // Find the smallest index that has first tag.
  // Note: encoded_tags_ is stored in reverse order so we look for the last
  // element of encoded_tags_.
  uint32_t first_tag_pos = 0;
  if (!encoded_tags_.empty()) {
    while (state_machine[first_tag_pos].etag_index != encoded_tags_.back()) {
      ++first_tag_pos;
    }
  }
  WriteVarint32(header_writer, first_tag_pos);

  Chain transitions_buffer;
  ChainWriter transitions_writer(&transitions_buffer);
  WriteTransitions(max_transition, state_machine, &transitions_writer);
  if (!transitions_writer.Close()) RIEGELI_UNREACHABLE();
  const Position pos_before = data_writer->pos();
  AppendCompressedBuffer(/*prepend_compressed_size=*/false, transitions_buffer,
                         data_writer);
  WriteVarint64(header_writer, data_writer->pos() - pos_before);
}

void TransposeEncoder::WriteTransitions(
    uint32_t max_transition, const std::vector<StateInfo>& state_machine,
    ChainWriter* transitions_writer) {
  if (encoded_tags_.empty()) return;
  uint32_t prev_etag = encoded_tags_.back();
  uint32_t current_base = tags_list_[prev_etag].base;
  // Assuming an approximately balanced tree of NoOp nodes covering transitions
  // from the given node in the state machine, the maximal number of bytes
  // needed to encode one transition should be the depth of the tree, i.e.
  // O(log_max_transition(state_machine_size)). We allocate buffer of size
  // kWriteBufSize to store the entire encoded transition.
  // For experiments with low max_transition we use much larger buffer then
  // needed for optimal max_transition == 63.
  constexpr size_t kWriteBufSize = 32;
  uint8_t write[kWriteBufSize];
  bool have_last_transition = false;
  uint8_t last_transition;
  // Go through all transitions and encode them.
  for (uint32_t i = encoded_tags_.size() - 1; i > 0; --i) {
    // There are multiple options how transition may be encoded:
    // 1. Transition is common and it's in the private list for the previous
    //    node.
    // 2. Transition is common and is served from public list. This can have two
    //    forms:
    //      a) Previous node has no private list so we simply serve the
    //         transition using the public node list.
    //      b) Node has private list so we first make a NoOp transition to the
    //         public list and then continue as above.
    //
    uint32_t tag = encoded_tags_[i - 1];
    // Check whether this is implicit transition.
    if (tags_list_[prev_etag].dest_info.size() != 1) {
      // Position in the private list.
      uint32_t pos = tags_list_[prev_etag].dest_info[tag].pos;
      if (pos == kInvalidPos) {
        // "pos" is not in the private list, go to public_list_noop_pos if
        // available.
        // Otherwise base is already in the public list (option 2a).
        pos = tags_list_[prev_etag].public_list_noop_pos;
        if (pos != kInvalidPos) {
          // Option 2b.
          const uint32_t orig_pos = pos;
          uint32_t write_start = kWriteBufSize;
          // Encode transition from "current_base" to "public_list_noop_pos"
          // which is a NoOp that would lead us to the public list.
          while (pos - current_base > max_transition) {
            // While desired pos is not reachable using one transition, move to
            // "canonical_source".
            const uint32_t cs = state_machine[pos].canonical_source;
            RIEGELI_ASSERT_LT(cs, state_machine.size()) << pos;
            RIEGELI_ASSERT_LE(state_machine[cs].base, pos);
            RIEGELI_ASSERT_LE(pos - state_machine[cs].base, max_transition);
            RIEGELI_ASSERT_NE(write_start, 0u);
            write[--write_start] = pos - state_machine[cs].base;
            pos = cs;
          }
          RIEGELI_ASSERT_NE(write_start, 0u);
          write[--write_start] = pos - current_base;

          for (uint32_t i = write_start; i < kWriteBufSize; ++i) {
            if (write[i] == 0 && have_last_transition &&
                (last_transition & 3) < 3) {
              ++last_transition;
            } else {
              if (have_last_transition) {
                WriteByte(transitions_writer, last_transition);
              }
              have_last_transition = true;
              last_transition = write[i] << 2;
            }
          }
          // "current_base" is the base of the NoOp that we reached using the
          // transitions so far.
          current_base = state_machine[orig_pos].base;
        }
        // "pos" becomes the position of the state in the public list.
        pos = tags_list_[tag].state_machine_pos;
      }
      RIEGELI_ASSERT(current_base != kInvalidPos);
      RIEGELI_ASSERT_LT(pos, state_machine.size());
      uint32_t write_start = kWriteBufSize;
      // Encode transition from "current_base" to "pos".
      while (pos - current_base > max_transition) {
        // While desired pos is not reachable using one transition, move to
        // "canonical_source".
        const uint32_t cs = state_machine[pos].canonical_source;
        RIEGELI_ASSERT_LT(cs, state_machine.size()) << pos;
        RIEGELI_ASSERT_LE(state_machine[cs].base, pos);
        RIEGELI_ASSERT_LE(pos - state_machine[cs].base, max_transition);
        RIEGELI_ASSERT_NE(write_start, 0u);
        write[--write_start] = pos - state_machine[cs].base;
        pos = cs;
      }
      RIEGELI_ASSERT_NE(write_start, 0u);
      write[--write_start] = pos - current_base;
      for (uint32_t i = write_start; i < kWriteBufSize; ++i) {
        if (write[i] == 0 && have_last_transition &&
            (last_transition & 3) < 3) {
          ++last_transition;
        } else {
          if (have_last_transition) {
            WriteByte(transitions_writer, last_transition);
          }
          have_last_transition = true;
          last_transition = write[i] << 2;
        }
      }
    } else {
      RIEGELI_ASSERT_EQ(state_machine[tags_list_[prev_etag].base].etag_index,
                        tag);
    }
    prev_etag = tag;
    current_base = tags_list_[prev_etag].base;
  }
  if (have_last_transition) {
    WriteByte(transitions_writer, last_transition);
  }
}

namespace {

// PriorityQueueEntry is used in priority_queue to order destinations by the
// number of transitions into them.
struct PriorityQueueEntry {
  PriorityQueueEntry() = default;

  PriorityQueueEntry(uint32_t dest_index, size_t num_transitions)
      : dest_index(dest_index), num_transitions(num_transitions) {}

  // Index of the destination in "tags_list_".
  uint32_t dest_index;
  // Number of transitions into destination.
  size_t num_transitions;
};

bool operator<(PriorityQueueEntry a, PriorityQueueEntry b) {
  // Sort by num_transitions. Largest first.
  if (a.num_transitions != b.num_transitions) {
    return a.num_transitions > b.num_transitions;
  }
  // Break ties for reproducible ordering.
  return a.dest_index < b.dest_index;
}

}  // namespace

void TransposeEncoder::CollectTransitionStatistics() {
  // Go through all the transitions from back to front and collect transition
  // distribution statistics.
  uint32_t prev_pos = encoded_tags_.back();
  for (size_t i = encoded_tags_.size() - 1; i > 0; --i) {
    const uint32_t pos = encoded_tags_[i - 1];
    ++tags_list_[prev_pos].dest_info[pos].num_transitions;
    ++tags_list_[pos].num_incoming_transitions;
    prev_pos = pos;
  }

  if (tags_list_[encoded_tags_.back()].num_incoming_transitions == 0) {
    // This guarantees that the initial state is created even if it has no other
    // incoming transition.
    tags_list_[encoded_tags_.back()].num_incoming_transitions = 1;
  }
}

void TransposeEncoder::ComputeBaseIndices(
    uint32_t max_transition, uint32_t public_list_base,
    const std::vector<std::pair<uint32_t, uint32_t>>& public_list_noops,
    std::vector<StateInfo>* state_machine_ptr) {
  std::vector<StateInfo>& state_machine = *state_machine_ptr;
  // The related transitions reach a state in the public list so the valid
  // approach would be to simply set all of these to "public_list_base".
  // However, we observe that most of the tags only target few destinations so
  // we can do better if we find the base that is closer to reachable states.
  //
  // We do this by computing "base" of the block that can reach all required
  // destination and "min_pos" of the state that is used in any such transition.

  // Compute "base" indices for NoOp states.
  for (auto tag_index_and_state_index : public_list_noops) {
    // Start of block that can reach all required destinations.
    uint32_t base = kInvalidPos;
    // Smallest position of node used in transition.
    uint32_t min_pos = kInvalidPos;
    for (const auto& dest_info :
         tags_list_[tag_index_and_state_index.first].dest_info) {
      uint32_t pos = dest_info.second.pos;
      if (pos != kInvalidPos) {
        // This tag has a node in the private list.
        continue;
      }
      // Position of the state that we need to reach.
      pos = tags_list_[dest_info.first].state_machine_pos;
      RIEGELI_ASSERT(pos != kInvalidPos);
      // Assuming we processed some states already and "base" is already set to
      // non-kInvalidPos we find the base of the block that is the common
      // ancestor for both "pos" and current "base".
      // If "base <= pos && pos - base <= max_transition" then "pos" can
      // be encoded from "base" using one byte and "base" starts the block we
      // are looking for. If this is not the case then either:
      //  - "base > pos" and "pos" is reachable from one of the common ancestor
      //    blocks of "base" and "pos". In that case we move "base" to the
      //    parent block of "base".
      //  - "pos - base > max_transition" and to reach "pos" we need more than
      //    one transition. In that case we ensure the reachability of "pos" by
      //    ensuring reachability of it's canonical_source which belongs to the
      //    parent block of "pos".
      // Note: We assume that transitions in the public list always go from
      // lower to higher indices. This is ensured by the public list generation
      // code.
      // Note: If "base" is kInvalidPos, the condition "base > pos" is true and
      // we handle the first state in there.
      while (base > pos || pos - base > max_transition) {
        if (base > pos) {
          // "cs" is the canonical_source that leads to the block we are looking
          // for.
          uint32_t cs;
          if (base == kInvalidPos) {
            // "base" not initialized yet. We'll use canonical_source of "pos".
            cs = state_machine[pos].canonical_source;
          } else {
            // Set "cs" to the NoOp that leads to "base".
            cs = state_machine[base].canonical_source;
            // If "cs" is kInvalidPos then "base" was already in the first
            // block. But then "base > pos" can't be true.
            RIEGELI_ASSERT(cs != kInvalidPos);
            // Transitions to previously added states will use "cs" so we update
            // "min_pos".
            min_pos = UnsignedMin(min_pos, cs);
            // To find "base" the block that contains "cs" we move one level
            // above.
            cs = state_machine[cs].canonical_source;
          }
          if (cs == kInvalidPos) {
            // No canonical_source means "base" is in the first block.
            base = public_list_base;
          } else {
            // Otherwise it's the base of the current "cs".
            base = state_machine[cs].base;
          }
        } else {
          // Update "pos" to canonical_source of "pos".
          const uint32_t cs = state_machine[pos].canonical_source;
          RIEGELI_ASSERT_LT(cs, state_machine.size()) << pos;
          RIEGELI_ASSERT_LE(state_machine[cs].base, pos);
          RIEGELI_ASSERT_LE(pos - state_machine[cs].base, max_transition);
          pos = cs;
        }
      }
      min_pos = UnsignedMin(min_pos, pos);
    }
    RIEGELI_ASSERT(min_pos != kInvalidPos);
    state_machine[tag_index_and_state_index.second].base = min_pos;
  }

  // The same as above for tags without private list.
  for (auto& tag : tags_list_) {
    if (tag.base != kInvalidPos) {
      // Skip tags with private list.
      continue;
    }
    uint32_t base = kInvalidPos;
    uint32_t min_pos = kInvalidPos;
    for (const auto& dest_info : tag.dest_info) {
      uint32_t pos = dest_info.second.pos;
      if (pos != kInvalidPos) {
        // Skip destinations in the private list.
        continue;
      }
      pos = tags_list_[dest_info.first].state_machine_pos;
      RIEGELI_ASSERT(pos != kInvalidPos);
      while (base > pos || pos - base > max_transition) {
        if (base > pos) {
          uint32_t cs;
          if (base == kInvalidPos) {
            cs = state_machine[pos].canonical_source;
          } else {
            cs = state_machine[base].canonical_source;
            RIEGELI_ASSERT(cs != kInvalidPos);
            min_pos = UnsignedMin(min_pos, cs);
            cs = state_machine[cs].canonical_source;
          }
          if (cs == kInvalidPos) {
            base = public_list_base;
          } else {
            base = state_machine[cs].base;
          }
        } else {
          const uint32_t cs = state_machine[pos].canonical_source;
          RIEGELI_ASSERT_LT(cs, state_machine.size()) << pos;
          RIEGELI_ASSERT_LE(state_machine[cs].base, pos);
          RIEGELI_ASSERT_LE(pos - state_machine[cs].base, max_transition);
          pos = cs;
        }
      }
      min_pos = UnsignedMin(min_pos, pos);
    }
    if (min_pos != kInvalidPos) {
      tag.base = min_pos;
    }
  }
}

std::vector<TransposeEncoder::StateInfo> TransposeEncoder::CreateStateMachine(
    uint32_t max_transition, uint32_t min_count_for_state) {
  std::vector<StateInfo> state_machine;
  if (encoded_tags_.empty()) {
    state_machine.emplace_back(kInvalidPos, 0);
    return state_machine;
  }

  CollectTransitionStatistics();

  // Go through all the tag infos and update transitions that will be included
  // in the private list for the node.
  constexpr uint32_t kInListPos = 0;
  for (EncodedTagInfo& tag_info : tags_list_) {
    for (auto& dest_and_count : tag_info.dest_info) {
      if (dest_and_count.second.num_transitions >= min_count_for_state) {
        // Subtract transitions so we have the right estimate of the remaining
        // transitions into each node.
        tags_list_[dest_and_count.first].num_incoming_transitions -=
            dest_and_count.second.num_transitions;
        // Mark transition to be included in list.
        dest_and_count.second.pos = kInListPos;
      }
    }
  }

  // priority_queue to order nodes by transition count.
  std::priority_queue<PriorityQueueEntry> tag_priority;
  // Pair of <tag_index, noop_position> where "noop_position" is the index of
  // the NoOp state created for this tag that has base index in the public node
  // list.
  std::vector<std::pair<uint32_t, uint32_t>> public_list_noops;
  // Helper vector to track the base index for NoOp nodes added in the loop
  // below.
  std::vector<uint32_t> noop_base;
  // Create private lists of states for all nodes that have one.
  // After this loop:
  //  - "state_machine" will contain states of created private lists.
  //  - "base" in "tags_list_" will be set for tags with private list.
  //  - "dest_info" in "tags_list_" will have pos != kInvalidPos for those nodes
  //    that already have state.
  //  - "public_list_noops" will have a record for all NoOp states reaching
  //    public list.
  for (uint32_t tag_id = 0; tag_id < tags_list_.size(); ++tag_id) {
    EncodedTagInfo& tag_info = tags_list_[tag_id];
    const uint32_t sz = tag_info.dest_info.size();
    // If we exclude just one state we add it instead of creating the NoOp
    // state.
    PriorityQueueEntry excluded_state;
    // Number of transitions into public list states.
    uint32_t num_excluded_transitions = 0;
    for (const auto& dest_info : tag_info.dest_info) {
      // If destination was marked as "kInListPos" or all transitions into it go
      // from this node.
      if (dest_info.second.pos == kInListPos ||
          dest_info.second.num_transitions ==
              tags_list_[dest_info.first].num_incoming_transitions) {
        if (dest_info.second.pos != kInListPos) {
          // Not yet subtracted.
          tags_list_[dest_info.first].num_incoming_transitions -=
              dest_info.second.num_transitions;
        }
        // Add to the priority queue.
        tag_priority.emplace(dest_info.first, dest_info.second.num_transitions);
      } else {
        num_excluded_transitions += dest_info.second.num_transitions;
        excluded_state = PriorityQueueEntry(dest_info.first,
                                            dest_info.second.num_transitions);
      }
    }
    uint32_t num_states = tag_priority.size();
    if (num_states == 0) {
      // No private list for this tag.
      continue;
    }
    if (num_states + 1 == sz) {
      // If only one state would go to the public list: just add it.
      ++num_states;
      tag_priority.push(excluded_state);
      tags_list_[excluded_state.dest_index].num_incoming_transitions -=
          excluded_state.num_transitions;
    }
    if (num_states != sz) {
      // If not all nodes are in the private list, we'll need NoOp into the
      // public list.
      tag_priority.emplace(kInvalidPos, num_excluded_transitions);
      ++num_states;
    }
    // update "base" for this tag.
    tag_info.base = state_machine.size();
    // Number of NoOp nodes for transitions that can't be encoded using one
    // byte.
    const uint32_t noop_nodes = num_states <= max_transition + 1
                                    ? 0
                                    : (num_states - 2) / max_transition;
    num_states += noop_nodes;
    // We create states back to front. After loop below there will be
    // "state_machine.size() + num_states" states.
    uint32_t prev_state = state_machine.size() + num_states;
    state_machine.resize(prev_state);
    // States are created in blocks. All blocks except the last one have
    // "max_transition + 1" states. "block_size" is initialized to the size of
    // the last block.
    uint32_t block_size = ((num_states - 1) % (max_transition + 1)) + 1;
    noop_base.clear();
    for (;;) {
      // Sum of all num_transitions into this block. It will be used as the
      // weight of the NoOp created for this block.
      uint32_t total_block_nodes_weight = 0;
      for (uint32_t i = 0; i < block_size; ++i) {
        RIEGELI_ASSERT(!tag_priority.empty());
        total_block_nodes_weight += tag_priority.top().num_transitions;
        const uint32_t node_index = tag_priority.top().dest_index;
        if (node_index == kInvalidPos) {
          // NoOp that goes to the public list.
          state_machine[--prev_state] = StateInfo(kInvalidPos, kInvalidPos);
          tag_info.public_list_noop_pos = prev_state;
          public_list_noops.emplace_back(tag_id, prev_state);
        } else if (node_index >= tags_list_.size()) {
          // NoOp that goes to private list.
          const uint32_t base = noop_base[node_index - tags_list_.size()];
          state_machine[--prev_state] = StateInfo(kInvalidPos, base);
          // Update canonical source for block that this node serves.
          for (uint32_t j = 0; j <= max_transition; ++j) {
            if (j + base >= state_machine.size()) {
              break;
            }
            state_machine[j + base].canonical_source = prev_state;
          }
        } else {
          // Regular state.
          state_machine[--prev_state] = StateInfo(node_index, kInvalidPos);
          tag_info.dest_info[node_index].pos = prev_state;
        }
        tag_priority.pop();
      }
      if (tag_priority.empty()) {
        break;
      }
      // Add new NoOp node into "tag_priority" to serve the block that was just
      // created. Use position greater than tags_list_.size() to distinguish it
      // from both regular state and public_list_noop.
      tag_priority.emplace(tags_list_.size() + noop_base.size(),
                           total_block_nodes_weight);
      // Set the base to the start of the block.
      noop_base.push_back(prev_state);
      // All remaining blocks are "max_transition + 1" states long.
      block_size = (max_transition + 1);
    }
  }

  // Base index of the public state list.
  const uint32_t public_list_base = state_machine.size();

  // Add all tags with non-zero incoming transition count to the priority queue.
  for (uint32_t i = 0; i < tags_list_.size(); ++i) {
    if (tags_list_[i].num_incoming_transitions != 0) {
      tag_priority.emplace(i, tags_list_[i].num_incoming_transitions);
    }
  }

  // Create a public list of states. The loop is similar to the public list
  // creation above.
  // After this loop:
  //  - All states in the state machine are created.
  //  - All tags that have an state in the public list have "state_machine_pos"
  //    set.
  uint32_t num_states = tag_priority.size();
  if (num_states > 0) {
    const uint32_t noop_nodes = num_states <= max_transition + 1
                                    ? 0
                                    : (num_states - 2) / max_transition;
    num_states += noop_nodes;
    // Note: The code that assigns "base" indices to states assumes that all
    // NoOp transitions to the child block increase the state index. This is
    // ensured by creating the blocks in reverse order.
    uint32_t prev_node = state_machine.size() + num_states;
    state_machine.resize(prev_node);
    uint32_t block_size = ((num_states - 1) % (max_transition + 1)) + 1;
    noop_base.clear();
    for (;;) {
      uint32_t total_block_nodes_weight = 0;
      for (uint32_t i = 0; i < block_size; ++i) {
        RIEGELI_ASSERT(!tag_priority.empty());
        total_block_nodes_weight += tag_priority.top().num_transitions;
        const uint32_t node_index = tag_priority.top().dest_index;
        if (node_index >= tags_list_.size()) {
          // NoOp state.
          const uint32_t base = noop_base[node_index - tags_list_.size()];
          state_machine[--prev_node] = StateInfo(kInvalidPos, base);
          for (uint32_t j = 0; j <= max_transition; ++j) {
            if (j + base >= state_machine.size()) {
              break;
            }
            state_machine[j + base].canonical_source = prev_node;
          }
        } else {
          // Regular state.
          state_machine[--prev_node] = StateInfo(node_index, kInvalidPos);
          tags_list_[node_index].state_machine_pos = prev_node;
        }
        tag_priority.pop();
      }
      if (tag_priority.empty()) {
        break;
      }
      tag_priority.emplace(tags_list_.size() + noop_base.size(),
                           total_block_nodes_weight);
      noop_base.push_back(prev_node);
      block_size = (max_transition + 1);
    }
  }

  // At this point, the only thing missing is the "base" index for tags
  // without a private list and for NoOp nodes that go to public list.
  ComputeBaseIndices(max_transition, public_list_base, public_list_noops,
                     &state_machine);

  return state_machine;
}

bool TransposeEncoder::EncodeInternal(uint32_t max_transition,
                                      uint32_t min_count_for_state,
                                      Writer* writer) {
  for (const auto& entry : message_nodes_) {
    if (entry.second.writer != nullptr && !entry.second.writer->Close()) {
      RIEGELI_UNREACHABLE();
    }
  }
  if (!nonproto_lengths_writer_.Close()) RIEGELI_UNREACHABLE();

  RIEGELI_ASSERT_LE(max_transition, 63u);
  WriteByte(writer, static_cast<uint8_t>(compression_type_));

  Chain header;
  ChainWriter header_writer(&header);

  const std::vector<StateInfo> state_machine =
      CreateStateMachine(max_transition, min_count_for_state);
  Chain data;
  ChainWriter data_writer(&data);
  WriteStatesAndData(max_transition, state_machine, &header_writer,
                     &data_writer);
  if (!header_writer.Close()) RIEGELI_UNREACHABLE();
  AppendCompressedBuffer(/*prepend_compressed_size=*/true, header, writer);
  if (!data_writer.Close()) RIEGELI_UNREACHABLE();
  return writer->Write(std::move(data));
}

// Maximum transition number. Transitions are encoded as values
// [0..max_transition].
constexpr uint32_t kMaxTransition = 63;
// Minimum number of transitions between nodes A and B for state for node B to
// appear in the private state list for node A.
constexpr uint32_t kMinCountForState = 10;

bool TransposeEncoder::Encode(Writer* writer) {
  return EncodeInternal(kMaxTransition, kMinCountForState, writer);
}

#undef RETURN_FALSE_IF

}  // namespace riegeli
