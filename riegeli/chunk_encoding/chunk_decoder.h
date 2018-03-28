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

#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/chunk_encoding/field_filter.h"
#include "riegeli/chunk_encoding/types.h"

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
      skip_corruption_ = skip_corruption;
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

  // Creates an empty ChunkDecoder.
  explicit ChunkDecoder(Options options = Options());

  ChunkDecoder(ChunkDecoder&& src) noexcept;
  ChunkDecoder& operator=(ChunkDecoder&& src) noexcept;

  // Resets the ChunkDecoder to an empty chunk.
  void Reset();

  // Resets the ChunkDecoder and parses the chunk.
  //
  // Return values:
  //  * true  - success (healthy())
  //  * false - failure (!healthy())
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
  //  * false (when !healthy()) - failure (impossible if healthy() on entry and
  //                              skip_corruption is true)
  bool ReadRecord(google::protobuf::MessageLite* record, uint64_t* key = nullptr);
  bool ReadRecord(absl::string_view* record, uint64_t* key = nullptr);
  bool ReadRecord(std::string* record, uint64_t* key = nullptr);
  bool ReadRecord(Chain* record, uint64_t* key = nullptr);

  uint64_t index() const { return index_; }
  void SetIndex(uint64_t index);
  uint64_t num_records() const { return IntCast<uint64_t>(limits_.size()); }

 protected:
  void Done() override;

 private:
  bool Parse(ChunkType chunk_type, const ChunkHeader& header, ChainReader* src,
             Chain* dest);

  bool skip_corruption_;
  FieldFilter field_filter_;
  // Invariants:
  //   limits_ are sorted
  //   (limits_.empty() ? 0 : limits_.back()) == size of values_reader_
  //   (index_ == 0 ? 0 : limits_[index_ - 1]) == values_reader_.pos()
  std::vector<size_t> limits_;
  ChainReader values_reader_;
  // Invariants:
  //   index_ <= num_records()
  //   if !healthy() then index_ == num_records()
  uint64_t index_;
  std::string record_scratch_;
};

// Implementation details follow.

inline bool ChunkDecoder::ReadRecord(absl::string_view* record, uint64_t* key) {
  if (RIEGELI_UNLIKELY(index_ == limits_.size())) return false;
  if (key != nullptr) *key = index_;
  const size_t start = IntCast<size_t>(values_reader_.pos());
  const size_t limit = limits_[IntCast<size_t>(index_++)];
  RIEGELI_ASSERT_LE(start, limit)
      << "Failed invariant of ChunkDecoder: record end positions not sorted";
  if (!values_reader_.Read(record, &record_scratch_, limit - start)) {
    RIEGELI_ASSERT_UNREACHABLE() << "Failed reading record from values reader: "
                                 << values_reader_.Message();
  }
  return true;
}

inline bool ChunkDecoder::ReadRecord(std::string* record, uint64_t* key) {
  if (RIEGELI_UNLIKELY(index_ == num_records())) return false;
  if (key != nullptr) *key = index_;
  const size_t start = IntCast<size_t>(values_reader_.pos());
  const size_t limit = limits_[IntCast<size_t>(index_++)];
  RIEGELI_ASSERT_LE(start, limit)
      << "Failed invariant of ChunkDecoder: record end positions not sorted";
  record->clear();
  if (!values_reader_.Read(record, limit - start)) {
    RIEGELI_ASSERT_UNREACHABLE() << "Failed reading record from values reader: "
                                 << values_reader_.Message();
  }
  return true;
}

inline bool ChunkDecoder::ReadRecord(Chain* record, uint64_t* key) {
  if (RIEGELI_UNLIKELY(index_ == num_records())) return false;
  if (key != nullptr) *key = index_;
  const size_t start = IntCast<size_t>(values_reader_.pos());
  const size_t limit = limits_[IntCast<size_t>(index_++)];
  RIEGELI_ASSERT_LE(start, limit)
      << "Failed invariant of ChunkDecoder: record end positions not sorted";
  record->Clear();
  if (!values_reader_.Read(record, limit - start)) {
    RIEGELI_ASSERT_UNREACHABLE() << "Failed reading record from values reader: "
                                 << values_reader_.Message();
  }
  return true;
}

inline void ChunkDecoder::SetIndex(uint64_t index) {
  index_ = UnsignedMin(index, num_records());
  const size_t start = index_ == 0 ? 0u : limits_[IntCast<size_t>(index_ - 1)];
  if (!values_reader_.Seek(start)) {
    RIEGELI_ASSERT_UNREACHABLE()
        << "Failed seeking values reader: " << values_reader_.Message();
  }
}

}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_CHUNK_DECODER_H_
