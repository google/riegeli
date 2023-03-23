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

#ifndef RIEGELI_CHUNK_ENCODING_TRANSPOSE_DECODER_H_
#define RIEGELI_CHUNK_ENCODING_TRANSPOSE_DECODER_H_

#include <stddef.h>
#include <stdint.h>

#include <vector>

#include "absl/types/span.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/chunk_encoding/field_projection.h"
#include "riegeli/chunk_encoding/transpose_internal.h"
#include "riegeli/varint/varint_writing.h"

namespace riegeli {

class TransposeDecoder : public Object {
 public:
  // Creates a closed `TransposeDecoder`.
  TransposeDecoder() noexcept : Object(kClosed) {}

  TransposeDecoder(const TransposeDecoder&) = delete;
  TransposeDecoder& operator=(const TransposeDecoder&) = delete;

  // Resets the `TransposeDecoder` and parses the chunk.
  //
  // Writes concatenated record values to `dest`. Sets `limits` to sorted
  // record end positions.
  //
  // Precondition: `dest.pos() == 0`
  //
  // Return values:
  //  * `true`  - success (`ok()`)
  //  * `false` - failure (`!ok()`);
  //              if `!dest.ok()` then the problem was at `dest`
  bool Decode(uint64_t num_records, uint64_t decoded_data_size,
              const FieldProjection& field_projection, Reader& src,
              BackwardWriter& dest, std::vector<size_t>& limits);

 private:
  struct SubmessageStackElement;
  struct StateMachineNode;
  struct Context;
  struct DecodingState;

  bool Parse(Context& context, Reader& src,
             const FieldProjection& field_projection);

  // Parse data buffers in `header_reader` and `src` into `context.buffers`.
  // This method is used when projection is disabled and all buffers are
  // initially decompressed.
  bool ParseBuffers(Context& context, Reader& header_reader, Reader& src);

  // Parse data buffers in `header_reader` and `src` into `context.buckets`.
  // When projection is enabled, buckets are decompressed on demand.
  // `bucket_indices` contains bucket index for each buffer.
  // `first_buffer_indices` contains the index of first buffer for each bucket.
  bool ParseBuffersForFiltering(Context& context, Reader& header_reader,
                                Reader& src,
                                std::vector<uint32_t>& first_buffer_indices,
                                std::vector<uint32_t>& bucket_indices);

  // Precondition: `projection_enabled`.
  Reader* GetBuffer(Context& context, uint32_t bucket_index,
                    uint32_t index_within_bucket);

  static bool ContainsImplicitLoop(
      std::vector<StateMachineNode>* state_machine_nodes);

  bool Decode(Context& context, uint64_t num_records, BackwardWriter& dest,
              std::vector<size_t>& limits);

  // Set `callback_type` in `node` based on `skipped_submessage_level`,
  // `submessage_stack`, and `node.node_template`.
  bool SetCallbackType(
      Context& context, int skipped_submessage_level,
      absl::Span<const SubmessageStackElement> submessage_stack,
      StateMachineNode& node);
};

}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_TRANSPOSE_DECODER_H_
