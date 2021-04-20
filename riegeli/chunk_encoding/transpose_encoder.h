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
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/inlined_vector.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/chain_backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/chunk_encoding/chunk_encoder.h"
#include "riegeli/chunk_encoding/compressor.h"
#include "riegeli/chunk_encoding/compressor_options.h"
#include "riegeli/chunk_encoding/constants.h"
#include "riegeli/chunk_encoding/transpose_internal.h"

namespace riegeli {

class LimitingReaderBase;

// Format (values are varint encoded unless indicated otherwise):
//  - Compression type
//  - Header length (compressed length if applicable)
//  - Header (possibly compressed):
//    - Number of separately compressed buckets that data buffers are split into
//      [`num_buckets`]
//    - Number of data buffers [`num_buffers`]
//    - Array of `num_buckets` varints: sizes of buckets (compressed size
//      if applicable)
//    - Array of `num_buffers` varints: lengths of buffers (uncompressed)
//    - Number of state machine states [`num_state`]
//    - States encoded in 4 blocks:
//      - Array of `num_state` Tags/ReservedIDs
//      - Array of `num_state` next node indices
//      - Array of subtypes (for all tags where applicable)
//      - Array of data buffer indices (for all tags/subtypes where applicable)
//    - Initial state index
//  - `num_buckets` buckets:
//    - Bucket data (possibly compressed):
//      - Concatenated data buffers in this bucket (bytes)
//  - Transitions (possibly compressed):
//    - State machine transitions (bytes)
class TransposeEncoder : public ChunkEncoder {
 public:
  // Creates an empty `TransposeEncoder`.
  explicit TransposeEncoder(CompressorOptions options, uint64_t bucket_size);

  ~TransposeEncoder();

  void Clear() override;

  // `record` should be a protocol message in binary format. Transpose works
  // just fine even if `record` is a corrupted protocol message or an arbitrary
  // string. Such records are internally stored separately -- these are not
  // broken down into columns.
  using ChunkEncoder::AddRecord;
  bool AddRecord(absl::string_view record) override;
  bool AddRecord(const Chain& record) override;
  bool AddRecord(const absl::Cord& record) override;

  bool AddRecords(Chain records, std::vector<size_t> limits) override;

  bool EncodeAndClose(Writer& dest, ChunkType& chunk_type,
                      uint64_t& num_records,
                      uint64_t& decoded_data_size) override;

 private:
  bool AddRecordInternal(Reader& record);

  // Encode messages added with `AddRecord()` calls and write the result to
  // `dest`.
  bool EncodeAndCloseInternal(uint32_t max_transition,
                              uint32_t min_count_for_state, Writer& dest,
                              uint64_t& num_records,
                              uint64_t& decoded_data_size);

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

  // Information about a field with unique proto path.
  struct MessageNode {
    explicit MessageNode(internal::MessageId message_id);
    // Some nodes (such as `kStartGroup`) contain no data. Buffer is assigned in
    // the first `GetBuffer()` call when we have data to write.
    std::unique_ptr<BackwardWriter> writer;
    // Unique ID for every instance of this class within `TransposeEncoder`.
    internal::MessageId message_id;
    // Position of encoded tag in `tags_list_` per subtype.
    // Size 14 works well with `kMaxVarintInline == 3`.
    absl::InlinedVector<uint32_t, 14> encoded_tag_pos;
  };

  // We build a tree structure of protocol buffer tags. `NodeId` uniquely
  // identifies a node in this tree.
  struct NodeId {
    explicit NodeId(internal::MessageId parent_message_id, uint32_t tag);

    friend bool operator==(NodeId a, NodeId b) {
      return a.parent_message_id == b.parent_message_id && a.tag == b.tag;
    }
    template <typename HashState>
    friend HashState AbslHashValue(HashState hash_state, NodeId self) {
      return HashState::combine(std::move(hash_state), self.parent_message_id,
                                self.tag);
    }

    internal::MessageId parent_message_id;
    uint32_t tag;
  };

  // Add message recursively to the internal data structures.
  // Precondition: `message` is a valid proto message, i.e. `IsProtoMessage()`
  // on this message returns `true`.
  // `depth` is the recursion depth.
  bool AddMessage(LimitingReaderBase& record,
                  internal::MessageId parent_message_id, int depth);

  // Write all buffer lengths to `header_writer` and data buffers in `data_` to
  // `data_writer` (compressed using `compressor_`). Fill map with the
  // sequential position of each buffer written.
  bool WriteBuffers(Writer& header_writer, Writer& data_writer,
                    absl::flat_hash_map<NodeId, uint32_t>* buffer_pos);

  // One state of the state machine created in encoder.
  struct StateInfo {
    StateInfo();
    explicit StateInfo(uint32_t etag_index, uint32_t base);
    // Index of the `encoded_tag` in `tags_list_` represented by this state.
    // `kInvalidPos` for `kNoOp` states.
    uint32_t etag_index;
    // Base index of this state. Transitions from this state can target only
    // states [`base`, `base + kMaxTransition`].
    // `kInvalidPos` if no outgoing transition.
    uint32_t base;
    // Usual source of transition into this state. Set if there is `kNoOp`
    // generated to reach a group of states including this one.
    // `kInvalidPos` if no such state.
    uint32_t canonical_source;
  };

  // Add `buffer` to `bucket_compressor.writer()`.
  // If `new_uncompressed_bucket_size` is not `absl::nullopt`, flush the current
  // bucket to `data_writer` first and create a new bucket of that size.
  bool AddBuffer(absl::optional<size_t> new_uncompressed_bucket_size,
                 const Chain& buffer, internal::Compressor& bucket_compressor,
                 Writer& data_writer,
                 std::vector<size_t>& compressed_bucket_sizes,
                 std::vector<size_t>& buffer_sizes);

  // Compute base indices for states in `state_machine` that don't have one yet.
  // `public_list_base` is the index of the start of the public list.
  // `public_list_noops` is the list of `kNoOp` states that don't have a base
  // set yet. It contains pairs of (`tag_index`, `state_index`).
  void ComputeBaseIndices(
      uint32_t max_transition, uint32_t public_list_base,
      const std::vector<std::pair<uint32_t, uint32_t>>& public_list_noops,
      std::vector<StateInfo>& state_machine);

  // Traverse `encoded_tags_` and populate `num_incoming_transitions` and
  // `dest_info` in `tags_list_` based on transition distribution.
  void CollectTransitionStatistics();

  // Create a state machine for `encoded_tags_`.
  std::vector<StateInfo> CreateStateMachine(uint32_t max_transition,
                                            uint32_t min_count_for_state);

  // Write state machine states into `header_writer` and all data buffers and
  // transitions into `data_writer` (compressed using `compressor_`).
  bool WriteStatesAndData(uint32_t max_transition,
                          const std::vector<StateInfo>& state_machine,
                          Writer& header_writer, Writer& data_writer);

  // Write all state machine transitions from `encoded_tags_` into
  // `compressor_.writer()`.
  bool WriteTransitions(uint32_t max_transition,
                        const std::vector<StateInfo>& state_machine,
                        Writer& transitions_writer);

  // Value type of node in Nodes map.
  using Node = absl::flat_hash_map<NodeId, MessageNode>::value_type;

  // Returns node pointer from `node_id`.
  Node* GetNode(NodeId node_id);

  // Get possition of the (`node`, `subtype`) pair in `tags_list_`, adding it
  // if not in the list yet.
  uint32_t GetPosInTagsList(Node* node, internal::Subtype subtype);

  // Get `BackwardWriter` for node. `type` is used to select the right category
  // for the buffer if not created yet.
  BackwardWriter* GetBuffer(Node* node, BufferType type);

  // Information about the state machine transition destination.
  struct DestInfo {
    DestInfo();
    // Position of the destination in destination list created for this state.
    // `kInvalidPos` if transition destination is not in the list. In that case
    // transition is encoded using the public list of states.
    uint32_t pos;
    // Number of transition to this destination.
    size_t num_transitions = 0;
  };

  // Information about encoded tag.
  struct EncodedTagInfo {
    explicit EncodedTagInfo(NodeId node_id, internal::Subtype subtype);
    NodeId node_id;
    internal::Subtype subtype;
    // Maps all destinations reachable from this encoded tag to `DestInfo`.
    absl::flat_hash_map<uint32_t, DestInfo> dest_info;
    // Number of incoming tranitions into this state.
    size_t num_incoming_transitions = 0;
    // Index of this state in the state machine.
    uint32_t state_machine_pos;
    // Position of `kNoOp` node in the private list that has base in public
    // list. If outgoing transitions are split into frequent and infrequent, a
    // list of frequent destinations is created and `kNoOp` node is added that
    // serves infrequent transitions.
    uint32_t public_list_noop_pos;
    // Base index of this encoded tag. Transitions from this tag can target only
    // states [`base`, `base + kMaxTransition`].
    // `kInvalidPos` if no outgoing transition.
    uint32_t base;
  };

  // Information about the data buffer.
  struct BufferWithMetadata {
    explicit BufferWithMetadata(NodeId node_id);
    // Buffer itself, wrapped in `std::unique_ptr` so that its address remains
    // constant when additional buffers are added.
    std::unique_ptr<Chain> buffer;
    // `NodeId` this buffer belongs to.
    NodeId node_id;
  };

  CompressorOptions compressor_options_;
  // The default approximate bucket size, used if compression is enabled.
  // Finer bucket granularity (i.e. smaller size) worsens compression density
  // but makes field projection more effective.
  uint64_t bucket_size_;

  // List of all distinct Encoded tags.
  std::vector<EncodedTagInfo> tags_list_;
  // Sequence of tags on input as indices into `tags_list_`.
  std::vector<uint32_t> encoded_tags_;
  // Data buffers in separate vectors per buffer type.
  std::vector<BufferWithMetadata> data_[kNumBufferTypes];
  // Every group creates a new message ID. We keep track of open groups in this
  // vector.
  std::vector<internal::MessageId> group_stack_;
  // Tree of message nodes.
  absl::flat_hash_map<NodeId, MessageNode> message_nodes_;
  ChainBackwardWriter<Chain> nonproto_lengths_writer_;
  // Counter used to assign unique IDs to the message nodes.
  internal::MessageId next_message_id_ = internal::MessageId::kRoot + 1;
};

}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_TRANSPOSE_ENCODER_H_
