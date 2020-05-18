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

#ifndef RIEGELI_CHUNK_ENCODING_CHUNK_ENCODER_H_
#define RIEGELI_CHUNK_ENCODING_CHUNK_ENCODER_H_

#include <stddef.h>
#include <stdint.h>

#include <string>
#include <type_traits>
#include <vector>

#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/message_serialize.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/chunk_encoding/constants.h"

namespace riegeli {

class ChunkEncoder : public Object {
 public:
  // Creates an empty `ChunkEncoder`.
  ChunkEncoder() noexcept : Object(kInitiallyOpen) {}

  ChunkEncoder(const ChunkEncoder&) = delete;
  ChunkEncoder& operator=(const ChunkEncoder&) = delete;

  // Resets the `ChunkEncoder` back to empty.
  virtual void Clear();

  // Adds the next record.
  //
  // `AddRecord(google::protobuf::MessageLite)` serializes a proto message to
  // raw bytes beforehand. The remaining overloads accept raw bytes.
  //
  // `std::string&&` is accepted with a template to avoid implicit conversions
  // to `std::string` which can be ambiguous against `std::string_view`
  // (e.g. `const char*`).
  //
  // Return values:
  //  * `true`  - success (`healthy()`)
  //  * `false` - failure (`!healthy()`)
  bool AddRecord(const google::protobuf::MessageLite& record);
  virtual bool AddRecord(const google::protobuf::MessageLite& record,
                         SerializeOptions serialize_options);
  virtual bool AddRecord(absl::string_view record) = 0;
  template <typename Src,
            std::enable_if_t<std::is_same<Src, std::string>::value, int> = 0>
  bool AddRecord(Src&& record);
  virtual bool AddRecord(const Chain& record) = 0;
  virtual bool AddRecord(Chain&& record);
  virtual bool AddRecord(const absl::Cord& record) = 0;
  virtual bool AddRecord(absl::Cord&& record);

  // Add multiple records, expressed as concatenated record values and sorted
  // record end positions.
  //
  // Preconditions:
  //   `limits` are sorted
  //   `(limits.empty() ? 0 : limits.back()) == records.size()`
  //
  // Return values:
  //  * `true`  - success (`healthy()`)
  //  * `false` - failure (`!healthy()`)
  virtual bool AddRecords(Chain records, std::vector<size_t> limits) = 0;

  // Returns the number of records added so far.
  uint64_t num_records() const { return num_records_; }

  // Returns the sum of record sizes added so far.
  uint64_t decoded_data_size() const { return decoded_data_size_; }

  // Encodes the chunk to `dest`, setting `chunk_type`, `num_records`, and
  // `decoded_data_size`. Closes the `ChunkEncoder`.
  //
  // Return values:
  //  * `true`  - success (`healthy()`)
  //  * `false` - failure (`!healthy()`);
  //              if `!dest.healthy()` then the problem was at `dest`
  virtual bool EncodeAndClose(Writer& dest, ChunkType& chunk_type,
                              uint64_t& num_records,
                              uint64_t& decoded_data_size) = 0;

 protected:
  void Done() override;

  uint64_t num_records_ = 0;
  uint64_t decoded_data_size_ = 0;
};

// Implementation details follow.

inline void ChunkEncoder::Clear() {
  Object::Reset(kInitiallyOpen);
  num_records_ = 0;
  decoded_data_size_ = 0;
}

inline bool ChunkEncoder::AddRecord(
    const google::protobuf::MessageLite& record) {
  return AddRecord(record, SerializeOptions());
}

template <typename Src,
          std::enable_if_t<std::is_same<Src, std::string>::value, int>>
inline bool ChunkEncoder::AddRecord(Src&& record) {
  if (ABSL_PREDICT_TRUE(record.size() <= kMaxBytesToCopy)) {
    return AddRecord(absl::string_view(record));
  } else {
    // `std::move(record)` is correct and `std::forward<Src>(record)` is not
    // necessary: `Src` is always `std::string`, never an lvalue reference.
    return AddRecord(Chain(std::move(record)));
  }
}

}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_CHUNK_ENCODER_H_
