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
#include <memory>
#include <vector>

#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/chunk_encoding/field_filter.h"
#include "riegeli/chunk_encoding/transpose_internal.h"

namespace riegeli {

class TransposeDecoder {
 public:
  TransposeDecoder();
  TransposeDecoder(TransposeDecoder&&) noexcept;
  TransposeDecoder& operator=(TransposeDecoder&&) noexcept;
  ~TransposeDecoder();

  // Initialize using "reader" (this should be the byte-by-byte output of an
  // earlier call to TransposeEncoder::Encode()).
  bool Initialize(Reader* reader,
                  const FieldFilter& field_filter = FieldFilter::All());

  // Writes concatenated messages (back to front so that they end up in their
  // natural order). Sets *boundaries to positions between messages, such that
  // boundaries->size() is the number of messages plus one, message[i] is in
  // the destination of writer between positions (*boundaries)[i] and
  // (*boundaries)[i + 1], boundaries->front() == 0, and boundaries->back() is
  // the total size written to writer.
  bool Decode(BackwardWriter* writer, std::vector<size_t>* boundaries);

 private:
  class Context;

  // Parse data buffers in "header_reader" and "reader" into
  // "context_->buffers". This method is used when filtering is disabled and all
  // filters are initially decompressed.
  bool ParseBuffers(Reader* header_reader, Reader* reader);

  // Parse data buffers in "header_reader" and "reader" into
  // "context_->data_buckets". When filtering is enabled, buckets are
  // decompressed on demand. "bucket_indices" contains bucket index for each
  // buffer. "bucket_start" contains the index of first buffer for each bucket.
  bool ParseBuffersForFitering(Reader* header_reader, Reader* reader,
                               std::vector<uint32_t>* bucket_start,
                               std::vector<uint32_t>* bucket_indices);

  // Decode context containing decode information preprocessed by one of the
  // "Initialize" calls.
  std::unique_ptr<Context> context_;
};

}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_TRANSPOSE_DECODER_H_
