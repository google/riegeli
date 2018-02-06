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

#ifndef RIEGELI_CHUNK_ENCODING_CHUNK_DECODER_H_
#define RIEGELI_CHUNK_ENCODING_CHUNK_DECODER_H_

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/base/string_view.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/chunk_encoding/field_filter.h"

// Forward declarations to reduce the amount of includes going into public
// record_reader.h.
namespace google {
namespace protobuf {
class MessageLite;
}  // namespace protobuf
}  // namespace google

namespace riegeli {

// Forward declarations to reduce the amount of includes going into public
// record_reader.h.
class Chunk;
class ChunkHeader;

class ChunkDecoder : public Object {
 public:
  class Options {
   public:
    // Not defaulted because of a C++ defect:
    // https://stackoverflow.com/questions/17430377
    Options() noexcept {}

    // If true, corrupted records will be skipped. if false, corrupted records
    // will cause reading to fail.
    //
    // Default: false
    Options& set_skip_corruption(bool skip_corruption) & {
      skip_corruption_ = std::move(skip_corruption);
      return *this;
    }
    Options&& set_skip_corruption(bool skip_corruption) && {
      return std::move(set_skip_corruption(skip_corruption));
    }

    // Specifies the set of fields to be included in returned records, allowing
    // to exclude the remaining fields (but does not guarantee exclusion).
    // Excluding data makes reading faster.
    Options& set_field_filter(FieldFilter field_filter) & {
      field_filter_ = std::move(field_filter);
      return *this;
    }
    Options&& set_field_filter(FieldFilter field_filter) && {
      return std::move(set_field_filter(std::move(field_filter)));
    }

   private:
    friend class ChunkDecoder;

    bool skip_corruption_ = false;
    FieldFilter field_filter_ = FieldFilter::All();
  };

  explicit ChunkDecoder(Options options = Options());

  // The source ChunkDecoder is left cleared.
  ChunkDecoder(ChunkDecoder&& src) noexcept;
  ChunkDecoder& operator=(ChunkDecoder&& src) noexcept;

  ~ChunkDecoder();

  void Clear();
  bool Reset(const Chunk& chunk);

  // Reads the next record.
  //
  // ReadRecord(MessageLite*) parses raw bytes to a proto message after reading.
  // The remaining overloads read raw bytes (they never generate a new failure).
  // For ReadRecord(string_view*) the string_view is valid until the next
  // non-const operation on this ChunkDecoder.
  //
  // If key != nullptr, *key is set to the record index on success.
  //
  // Return values:
  //  * true                    - success (*record is set, healthy())
  //  * false (when healthy())  - chunk ends
  //  * false (when !healthy()) - failure
  bool ReadRecord(google::protobuf::MessageLite* record, uint64_t* key = nullptr);
  bool ReadRecord(string_view* record, uint64_t* key = nullptr);
  bool ReadRecord(std::string* record, uint64_t* key = nullptr);
  bool ReadRecord(Chain* record, uint64_t* key = nullptr);

  uint64_t index() const { return index_; }
  void SetIndex(uint64_t index);
  uint64_t num_records() const { return num_records_; }

 protected:
  void Done() override { Clear(); }

 private:
  bool Initialize(uint8_t chunk_type, const ChunkHeader& header,
                  ChainReader* data_reader, Chain* values);
  bool InitializeSimple(const ChunkHeader& header, ChainReader* data_reader,
                        Chain* values);
  bool InitializeTransposed(const ChunkHeader& header, ChainReader* data_reader,
                            Chain* values);

  bool skip_corruption_;
  FieldFilter field_filter_;
  // Invariants:
  //   if healthy() then boundaries_[0] == 0
  //   for each i, boundaries_[i + 1] >= boundaries_[i]
  std::vector<size_t> boundaries_;
  ChainReader values_reader_;
  // Invariant: if healthy() then num_records_ == boundaries_.size() - 1
  uint64_t num_records_;
  // Invariant: if healthy() then index_ <= num_records()
  //                         else index_ == num_records()
  uint64_t index_;
  std::string record_scratch_;
};

inline bool ChunkDecoder::ReadRecord(string_view* record, uint64_t* key) {
  if (RIEGELI_UNLIKELY(index_ == num_records())) return false;
  if (key != nullptr) *key = index_;
  ++index_;
  RIEGELI_ASSERT_GE(boundaries_[index_], boundaries_[index_ - 1]);
  if (!values_reader_.Read(record, &record_scratch_,
                           boundaries_[index_] - boundaries_[index_ - 1])) {
    RIEGELI_ASSERT_UNREACHABLE();
  }
  return true;
}

inline bool ChunkDecoder::ReadRecord(std::string* record, uint64_t* key) {
  if (RIEGELI_UNLIKELY(index_ == num_records())) return false;
  if (key != nullptr) *key = index_;
  ++index_;
  record->clear();
  RIEGELI_ASSERT_GE(boundaries_[index_], boundaries_[index_ - 1]);
  if (!values_reader_.Read(record,
                           boundaries_[index_] - boundaries_[index_ - 1])) {
    RIEGELI_ASSERT_UNREACHABLE();
  }
  return true;
}

inline bool ChunkDecoder::ReadRecord(Chain* record, uint64_t* key) {
  if (RIEGELI_UNLIKELY(index_ == num_records())) return false;
  if (key != nullptr) *key = index_;
  ++index_;
  record->Clear();
  RIEGELI_ASSERT_GE(boundaries_[index_], boundaries_[index_ - 1]);
  if (!values_reader_.Read(record,
                           boundaries_[index_] - boundaries_[index_ - 1])) {
    RIEGELI_ASSERT_UNREACHABLE();
  }
  return true;
}

inline void ChunkDecoder::SetIndex(uint64_t index) {
  index_ = UnsignedMin(index, num_records());
  if (!values_reader_.Seek(boundaries_[index_])) RIEGELI_ASSERT_UNREACHABLE();
}

}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_CHUNK_DECODER_H_
