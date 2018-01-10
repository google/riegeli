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

#include "riegeli/base/chain.h"
#include "riegeli/base/endian.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

class ChunkHeader {
 public:
  ChunkHeader() = default;

  ChunkHeader(const Chain& data, uint64_t num_records,
              uint64_t decoded_data_size);

  ChunkHeader(const ChunkHeader&) noexcept = default;
  ChunkHeader& operator=(const ChunkHeader&) noexcept = default;

  char* bytes() { return reinterpret_cast<char*>(words_); }
  const char* bytes() const { return reinterpret_cast<const char*>(words_); }
  static constexpr size_t size() { return sizeof(words_); }

  uint64_t computed_header_hash() const;
  uint64_t stored_header_hash() const { return ReadLittleEndian64(words_[0]); }
  uint64_t data_size() const { return ReadLittleEndian64(words_[1]); }
  uint64_t data_hash() const { return ReadLittleEndian64(words_[2]); }
  uint64_t num_records() const { return ReadLittleEndian64(words_[3]); }
  uint64_t decoded_data_size() const { return ReadLittleEndian64(words_[4]); }

 private:
  void set_header_hash(uint64_t value) {
    words_[0] = WriteLittleEndian64(value);
  }
  void set_data_size(uint64_t value) { words_[1] = WriteLittleEndian64(value); }
  void set_data_hash(uint64_t value) { words_[2] = WriteLittleEndian64(value); }
  void set_num_records(uint64_t value) {
    words_[3] = WriteLittleEndian64(value);
  }
  void set_decoded_data_size(uint64_t value) {
    words_[4] = WriteLittleEndian64(value);
  }

  uint64_t words_[5];
};

struct Chunk {
  void Reset() { data.Clear(); }

  bool WriteTo(Writer* dest) const;
  bool ReadFrom(Reader* src);

  ChunkHeader header;
  Chain data;
};

}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_CHUNK_H_
