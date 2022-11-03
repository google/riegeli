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

#ifndef RIEGELI_CHUNK_ENCODING_CHUNK_H_
#define RIEGELI_CHUNK_ENCODING_CHUNK_H_

#include <stddef.h>
#include <stdint.h>

#include "absl/base/attributes.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/chunk_encoding/constants.h"
#include "riegeli/endian/endian_reading.h"
#include "riegeli/endian/endian_writing.h"

namespace riegeli {

class ChunkHeader {
 public:
  ChunkHeader() = default;

  explicit ChunkHeader(const Chain& data, ChunkType chunk_type,
                       uint64_t num_records, uint64_t decoded_data_size);

  ChunkHeader(const ChunkHeader& that) = default;
  ChunkHeader& operator=(const ChunkHeader& that) = default;

  char* bytes() { return bytes_; }
  const char* bytes() const { return bytes_; }
  static constexpr size_t size() { return sizeof(bytes_); }

  uint64_t computed_header_hash() const;
  uint64_t stored_header_hash() const { return ReadLittleEndian64(bytes_); }
  uint64_t data_size() const {
    return ReadLittleEndian64(bytes_ + sizeof(uint64_t));
  }
  uint64_t data_hash() const {
    return ReadLittleEndian64(bytes_ + 2 * sizeof(uint64_t));
  }
  ChunkType chunk_type() const {
    return static_cast<ChunkType>(
        ReadLittleEndian64(bytes_ + 3 * sizeof(uint64_t)) & 0xff);
  }
  uint64_t num_records() const {
    return ReadLittleEndian64(bytes_ + 3 * sizeof(uint64_t)) >> 8;
  }
  uint64_t decoded_data_size() const {
    return ReadLittleEndian64(bytes_ + 4 * sizeof(uint64_t));
  }

 private:
  void set_header_hash(uint64_t value) { WriteLittleEndian64(value, bytes_); }
  void set_data_size(uint64_t value) {
    WriteLittleEndian64(value, bytes_ + sizeof(uint64_t));
  }
  void set_data_hash(uint64_t value) {
    WriteLittleEndian64(value, bytes_ + 2 * sizeof(uint64_t));
  }
  void set_chunk_type_and_num_records(ChunkType chunk_type,
                                      uint64_t num_records) {
    RIEGELI_ASSERT_LE(num_records, kMaxNumRecords)
        << "Failed precondition of "
           "ChunkHeader::set_chunk_type_and_num_records(): "
           "number of records out of range";
    WriteLittleEndian64(static_cast<uint64_t>(chunk_type) | (num_records << 8),
                        bytes_ + 3 * sizeof(uint64_t));
  }
  void set_decoded_data_size(uint64_t value) {
    WriteLittleEndian64(value, bytes_ + 4 * sizeof(uint64_t));
  }

  // Representation (Little Endian):
  //  - `uint64_t`: `header_hash`
  //  - `uint64_t`: `data_size`
  //  - `uint64_t`: `data_hash`
  //  - `uint64_t`: `chunk_type` (low 8 bits) | `num_records` (high 56 bits)
  //  - `uint64_t`: `decoded_data_size`
  char bytes_[5 * sizeof(uint64_t)];
};

struct Chunk {
  ABSL_ATTRIBUTE_REINITIALIZES void Reset() { data = Chain(); }
  ABSL_ATTRIBUTE_REINITIALIZES void Clear() { data.Clear(); }

  bool WriteTo(Writer& dest) const;
  bool ReadFrom(Reader& src);

  ChunkHeader header;
  Chain data;
};

}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_CHUNK_H_
