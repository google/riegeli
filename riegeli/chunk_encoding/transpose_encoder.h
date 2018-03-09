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

#ifndef RIEGELI_CHUNK_ENCODING_TRANSPOSE_ENCODER_H_
#define RIEGELI_CHUNK_ENCODING_TRANSPOSE_ENCODER_H_

#include <stddef.h>
#include <stdint.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/base/string_view.h"
#include "riegeli/bytes/chain_backward_writer.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/chunk_encoding/chunk_encoder.h"
#include "riegeli/chunk_encoding/compressor.h"
#include "riegeli/chunk_encoding/transpose_internal.h"
#include "riegeli/chunk_encoding/types.h"

namespace riegeli {

class Reader;

// Format (values are varint encoded unless indicated otherwise):
//  - Compression type
//  - Header length (compressed length if applicable)
//  - Header (possibly compressed):
//    - Number of separately compressed buckets that data buffers are split into
//      [num_buckets]
//    - Number of data buffers [num_buffers]
//    - Array of "num_buckets" varints: sizes of buckets (compressed size
//      if applicable)
//    - Array of "num_buffers" varints: lengths of buffers (uncompressed)
//    - Number of state machine states [num_state]
//    - States encoded in 4 blocks:
//      - Array of "num_state" Tags/ReservedIDs
//      - Array of "num_state" next node indices
//      - Array of subtypes (for all tags where applicable)
//      - Array of data buffer indices (for all tags/subtypes where applicable)
//    - Initial state index
//  - "num_buckets" buckets:
//    - Bucket data (possibly compressed):
//      - Concatenated data buffers in this bucket (bytes)
//  - Transitions (possibly compressed):
//    - State machine transitions (bytes)
class TransposeEncoder : public ChunkEncoder {
 public:
  // Creates an empty TransposeEncoder.
  TransposeEncoder(CompressionType compression_type, int compression_level,
                   uint64_t bucket_size);

  ~TransposeEncoder();

  void Reset() override;

  // "record" should be a protocol message in binary format. Transpose works
  // just fine even if "record" is a corrupted protocol message or an arbitrary
  // string. Such records are internally stored separately -- these are not
  // broken down into columns.
  using ChunkEncoder::AddRecord;
  bool AddRecord(string_view record) override;
  bool AddRecord(std::string&& record) override;
  bool AddRecord(const Chain& record) override;

  bool AddRecords(Chain records, std::vector<size_t> limits) override;

  bool EncodeAndClose(Writer* dest, uint64_t* num_records,
                      uint64_t* decoded_data_size) override;

  ChunkType GetChunkType() const override;

 protected:
  void Done() override;

 private:
  bool AddRecordInternal(Reader* record);

  // Encode messages added with AddRecord() calls and write the result to
  // "buffer". No messages should be added after calling this method.
  bool EncodeAndCloseInternal(uint32_t max_transition,
                              uint32_t min_count_for_state, Writer* dest,
                              uint64_t* num_records,
                              uint64_t* decoded_data_size);

  // Types of data buffers protocol buffer fields are split into.
  // The buffer type information is not used in any way except to group similar
  // buffers together hoping that this helps with compression context modeling.
  enum class BufferType : int {
    // Varint-encoded numbers, with the highest bit (signifying end of number)
    // stripped.
    kVarint,
    // Fixed width 32bit integer or floating point numbers.
    kFixed32,
    // Fixed width 64bit integer or floating point numbers.
    kFixed64,
    // String length + data.
    kString,
    // All non-proto messages.
    kNonProto,
    kNumBufferTypes,
  };

  static constexpr size_t kNumBufferTypes =
      static_cast<size_t>(BufferType::kNumBufferTypes);

  // Struct that contains information about a field with unique proto path.
  struct MessageNode {
    explicit MessageNode(internal::MessageId message_id);
    // Some nodes (such as STARTGROUP) contain no data. Buffer is assigned in
    // the first GetBuffer call when we have data to write.
    std::unique_ptr<ChainBackwardWriter> writer;
    // Unique ID for every instance of this class within Encoder.
    internal::MessageId message_id;
  };

  // We build a tree structure of protocol buffer fields. NodeId uniquely
  // identifies a node in this tree.
  // Note: We use "field" to denote numeric id of the protocol buffer field
  // assigned in the .proto file. We use "tag" for the identifier of this field
  // in the binary format. I.e., tag = (field << 3) | field_type.
  struct NodeId {
    NodeId(internal::MessageId parent_message_id, uint32_t field);
    bool operator==(NodeId other) const;
    internal::MessageId parent_message_id;
    uint32_t field;
  };

  struct NodeIdHasher {
    size_t operator()(NodeId node_id) const;
  };

  // Get ChainBackwardWriter for field "field" in message "parent_message_id".
  // "type" is used to select the right category for the buffer if not created
  // yet.
  ChainBackwardWriter* GetBuffer(internal::MessageId parent_message_id,
                                 uint32_t field, BufferType type);

  // Add message recursively to the internal data structures.
  // Precondition: "message" is a valid proto message, i.e. IsProtoMessage on
  // this message returns true.
  // "depth" is the recursion depth.
  bool AddMessage(Reader* record, internal::MessageId parent_message_id,
                  int depth);

  // Write all data buffers in "data_" to "data_buffer" (possibly compressed)
  // and buffer lengths into "header_buffer".
  // Fill map with the sequential position of each buffer written.
  bool WriteBuffers(
      Writer* data_writer,
      std::unordered_map<TransposeEncoder::NodeId, uint32_t,
                         TransposeEncoder::NodeIdHasher>* buffer_pos);

  // One state of the state machine created in encoder.
  struct StateInfo {
    StateInfo();
    StateInfo(uint32_t etag_index, uint32_t base);
    // Index of the encoded_tag in "tags_list_" represented by this state.
    // kInvalidPos for NoOp states.
    uint32_t etag_index;
    // Base index of this state. Transitions from this state can target only
    // states [base, base + kMaxTransition].
    // kInvalidPos if no outgoing transition.
    uint32_t base;
    // Usual source of transition into this state. Set if there is NoOp
    // generated to reach a group of states including this one.
    // kInvalidPos if no such state.
    uint32_t canonical_source;
  };

  // Add "next_chunk" to "bucket_buffer". If compression is enabled and either
  // the current bucket would become too large or "force_new_bucket" is true,
  // flush the bucket to "data_buffer" first and create a new bucket.
  bool AddBuffer(bool force_new_bucket, const Chain& next_chunk,
                 Writer* data_writer, std::vector<size_t>* bucket_lengths,
                 std::vector<size_t>* buffer_lengths);

  // Compute base indices for states in "state_machine" that don't have one yet.
  // "public_list_base" is the index of the start of the public list.
  // "public_list_noops" is the list of NoOp states that don't have a base set
  // yet. It contains pairs of (tag_index, state_index).
  void ComputeBaseIndices(
      uint32_t max_transition, uint32_t public_list_base,
      const std::vector<std::pair<uint32_t, uint32_t>>& public_list_noops,
      std::vector<StateInfo>* state_machine);

  // Traverse "encoded_tags_" and populate "num_incoming_transitions" and
  // "dest_info" in "tags_list_" based on transition distribution.
  void CollectTransitionStatistics();

  // Create a state machine for "encoded_tags_".
  std::vector<StateInfo> CreateStateMachine(uint32_t max_transition,
                                            uint32_t min_count_for_state);

  // Write state machine states into "header_buffer" and all data buffers into
  // "data_buffer".
  bool WriteStatesAndData(uint32_t max_transition,
                          const std::vector<StateInfo>& state_machine,
                          Writer* data_writer);

  // Write all state machine transitions from "encoded_tags_" into
  // "transitions_writer".
  bool WriteTransitions(uint32_t max_transition,
                        const std::vector<StateInfo>& state_machine);

  // Encoded tag represents a tag read from input together with the ID of the
  // message this tag belongs to and subtype extracted from the data.
  struct EncodedTag {
    EncodedTag(internal::MessageId message_id, uint32_t tag,
               internal::Subtype subtype);

    bool operator==(EncodedTag other) const;
    // ID of the message this tag belongs to.
    internal::MessageId message_id;
    // Tag as read from input.
    uint32_t tag;
    internal::Subtype subtype;
    // Always update EncodedTagHasher when changing this struct.
  };

  struct EncodedTagHasher {
    size_t operator()(EncodedTag encoded_tag) const;
  };

  // Get possition of the encoded tag in "tags_list_" adding it if not in the
  // list yet.
  uint32_t GetPosInTagsList(EncodedTag etag);

  // Information about the state machine transition destination.
  struct DestInfo {
    DestInfo();
    // Position of the destination in destination list created for this state.
    // kInvalidPos if transition destination is not in the list. In that case
    // transition is encoded using the public list of states.
    uint32_t pos;
    // Number of transition to this destination.
    size_t num_transitions = 0;
  };

  struct Uint32Hasher {
    size_t operator()(uint32_t v) const;
  };

  // Information about encoded tag.
  struct EncodedTagInfo {
    explicit EncodedTagInfo(EncodedTag tag);
    EncodedTag tag;
    // Maps all destinations reachable from this encoded tag to DestInfo.
    std::unordered_map<uint32_t, DestInfo, Uint32Hasher> dest_info;
    // Number of incoming tranitions into this state.
    size_t num_incoming_transitions = 0;
    // Index of this state in the state machine.
    uint32_t state_machine_pos;
    // Position of NoOp node in the private list that has base in public list.
    // If outgoing transitions are split into frequent and infrequent a list of
    // frequent destinations is created and NoOp node is added that serves
    // infrequent transitions.
    uint32_t public_list_noop_pos;
    // Base index of this encoded tag. Transitions from this tag can target only
    // states [base, base + kMaxTransition].
    // kInvalidPos if no outgoing transition.
    uint32_t base;
  };

  // Information about the data buffer.
  struct BufferWithMetadata {
    BufferWithMetadata(internal::MessageId message_id, uint32_t field);
    // Buffer itself, wrapped in unique_ptr so that its address remains constant
    // when additional buffers are added.
    std::unique_ptr<Chain> buffer;
    // Message ID and tag of the node in "message_nodes_" that this buffer
    // belongs to.
    internal::MessageId message_id;
    // Field number in message with ID "message_id" that the buffer belongs to.
    uint32_t field;
  };

  CompressionType compression_type_;
  // The default approximate bucket size, used if compression is enabled.
  // Finer bucket granularity (i.e. smaller size) worsens compression density
  // but makes field filtering more effective.
  uint64_t bucket_size_;

  uint64_t num_records_;
  uint64_t decoded_data_size_;
  internal::Compressor header_compressor_;
  internal::Compressor bucket_compressor_;
  internal::Compressor transitions_compressor_;
  // List of all distinct Encoded tags.
  std::vector<EncodedTagInfo> tags_list_;
  // Sequence of tags on input as indices into "tags_list_".
  std::vector<uint32_t> encoded_tags_;
  // Position of encoded tag in "tags_list_".
  std::unordered_map<EncodedTag, uint32_t, EncodedTagHasher> encoded_tag_pos_;
  // Data buffers in separate vectors per buffer type.
  std::vector<BufferWithMetadata> data_[kNumBufferTypes];
  // Every group creates a new message ID. We keep track of open groups in this
  // vector.
  std::vector<internal::MessageId> group_stack_;
  // Tree of message nodes.
  std::unordered_map<NodeId, MessageNode, NodeIdHasher> message_nodes_;
  Chain nonproto_lengths_;
  ChainBackwardWriter nonproto_lengths_writer_;
  // Counter used to assign unique IDs to the message nodes.
  internal::MessageId next_message_id_;
};

}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_TRANSPOSE_ENCODER_H_
