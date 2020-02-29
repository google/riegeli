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

#include "riegeli/chunk_encoding/chunk.h"

#include <stdint.h>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/chunk_encoding/constants.h"
#include "riegeli/chunk_encoding/hash.h"

namespace riegeli {

ChunkHeader::ChunkHeader(const Chain& data, ChunkType chunk_type,
                         uint64_t num_records, uint64_t decoded_data_size) {
  RIEGELI_ASSERT_LE(num_records, kMaxNumRecords)
      << "Failed precondition of ChunkHeader::ChunkHeader(): "
         "number of records out of range";
  set_data_size(data.size());
  set_data_hash(internal::Hash(data));
  set_chunk_type_and_num_records(chunk_type, num_records);
  set_decoded_data_size(decoded_data_size);
  set_header_hash(computed_header_hash());
}

uint64_t ChunkHeader::computed_header_hash() const {
  return internal::Hash(
      absl::string_view(bytes() + sizeof(uint64_t), size() - sizeof(uint64_t)));
}

bool Chunk::WriteTo(Writer* dest) const {
  if (ABSL_PREDICT_FALSE(
          !dest->Write(absl::string_view(header.bytes(), header.size())))) {
    return false;
  }
  return dest->Write(data);
}

bool Chunk::ReadFrom(Reader* src) {
  data.Clear();
  if (ABSL_PREDICT_FALSE(!src->Read(header.bytes(), header.size()))) {
    return false;
  }
  return src->Read(&data, header.data_size());
}

}  // namespace riegeli
