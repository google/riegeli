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
#include <tuple>
#include <utility>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/base/resetter.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/chunk_encoding/field_projection.h"

namespace riegeli {

class ChunkDecoder : public Object {
 public:
  class Options {
   public:
    Options() noexcept {}

    // Specifies the set of fields to be included in returned records, allowing
    // to exclude the remaining fields (but does not guarantee exclusion).
    // Excluding data makes reading faster.
    Options& set_field_projection(const FieldProjection& field_projection) & {
      field_projection_ = field_projection;
      return *this;
    }
    Options& set_field_projection(FieldProjection&& field_projection) & {
      field_projection_ = std::move(field_projection);
      return *this;
    }
    Options&& set_field_projection(const FieldProjection& field_projection) && {
      return std::move(set_field_projection(field_projection));
    }
    Options&& set_field_projection(FieldProjection&& field_projection) && {
      return std::move(set_field_projection(std::move(field_projection)));
    }
    FieldProjection& field_projection() & { return field_projection_; }
    const FieldProjection& field_projection() const& {
      return field_projection_;
    }
    FieldProjection&& field_projection() && {
      return std::move(field_projection_);
    }
    const FieldProjection&& field_projection() const&& {
      return std::move(field_projection_);
    }

   private:
    FieldProjection field_projection_ = FieldProjection::All();
  };

  // Creates an empty `ChunkDecoder`.
  explicit ChunkDecoder(Options options = Options());

  ChunkDecoder(ChunkDecoder&& that) noexcept;
  ChunkDecoder& operator=(ChunkDecoder&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `ChunkDecoder`. This avoids
  // constructing a temporary `ChunkDecoder` and moving from it.
  void Reset(Options options = Options());

  // Resets the `ChunkDecoder` to an empty chunk. Keeps options unchanged.
  void Clear();

  // Resets the `ChunkDecoder` and parses the chunk. Keeps options unchanged.
  //
  // Return values:
  //  * `true`  - success (`healthy()`)
  //  * `false` - failure (`!healthy()`)
  bool Decode(const Chunk& chunk);

  // Reads the next record.
  //
  // `ReadRecord(google::protobuf::MessageLite&)` parses raw bytes to a proto
  // message after reading. The remaining overloads read raw bytes (they never
  // generate a new failure). For `ReadRecord(absl::string_view&)` the
  // `absl::string_view` is valid until the next non-const operation on this
  // `ChunkDecoder`.
  //
  // Return values:
  //  * `true`                      - success (`record` is set, `healthy()`)
  //  * `false` (when `healthy()`)  - chunk ends
  //  * `false` (when `!healthy()`) - failure
  bool ReadRecord(google::protobuf::MessageLite& record);
  bool ReadRecord(absl::string_view& record);
  bool ReadRecord(std::string& record);
  bool ReadRecord(Chain& record);
  bool ReadRecord(absl::Cord& record);

  // If `!healthy()` and the failure was caused by an unparsable message, then
  // `Recover()` allows reading again by skipping the unparsable message.
  //
  // If `healthy()`, or if `!healthy()` but the failure was not caused by an
  // unparsable message, then `Recover()` does nothing and returns `false`.
  //
  // Return values:
  //  * `true`  - success
  //  * `false` - failure not caused by an unparsable message
  bool Recover();

  // Returns the current record index. Unchanged by `Close()`.
  uint64_t index() const { return index_; }

  // Sets the current record index.
  //
  // If `index > num_records()`, the current index is set to `num_records()`.
  //
  // Precondition: `healthy()`
  void SetIndex(uint64_t index);

  // Returns the number of records. Unchanged by `Close()`.
  uint64_t num_records() const { return IntCast<uint64_t>(limits_.size()); }

 protected:
  void Done() override;

 private:
  bool Parse(const ChunkHeader& header, Reader& src, Chain& dest);

  FieldProjection field_projection_;
  // Invariants if `healthy()`:
  //   `limits_` are sorted
  //   `(limits_.empty() ? 0 : limits_.back())` == size of `values_reader_`
  //   `(index_ == 0 ? 0 : limits_[index_ - 1]) == values_reader_.pos()`
  std::vector<size_t> limits_;
  ChainReader<Chain> values_reader_;
  // Invariant: if `healthy()` then `index_ <= num_records()`
  uint64_t index_ = 0;
  // Whether `Recover()` is applicable.
  //
  // Invariant: if `recoverable_` then `!healthy()`
  bool recoverable_ = false;
};

// Implementation details follow.

inline ChunkDecoder::ChunkDecoder(Options options)
    : Object(kInitiallyOpen),
      field_projection_(std::move(options.field_projection())),
      values_reader_(std::forward_as_tuple()) {}

inline ChunkDecoder::ChunkDecoder(ChunkDecoder&& that) noexcept
    : Object(std::move(that)),
      field_projection_(std::move(that.field_projection_)),
      limits_(std::move(that.limits_)),
      values_reader_(std::move(that.values_reader_)),
      index_(that.index_),
      recoverable_(std::exchange(that.recoverable_, false)) {}

inline ChunkDecoder& ChunkDecoder::operator=(ChunkDecoder&& that) noexcept {
  Object::operator=(std::move(that));
  field_projection_ = std::move(that.field_projection_);
  limits_ = std::move(that.limits_);
  values_reader_ = std::move(that.values_reader_);
  index_ = that.index_;
  recoverable_ = std::exchange(that.recoverable_, false);
  return *this;
}

inline void ChunkDecoder::Reset(Options options) {
  field_projection_ = std::move(options.field_projection());
  Clear();
}

inline void ChunkDecoder::Clear() {
  Object::Reset(kInitiallyOpen);
  limits_.clear();
  values_reader_.Reset(std::forward_as_tuple());
  index_ = 0;
  recoverable_ = false;
}

inline bool ChunkDecoder::ReadRecord(absl::string_view& record) {
  if (ABSL_PREDICT_FALSE(!healthy() || index() == num_records())) {
    record = absl::string_view();
    return false;
  }
  const size_t start = IntCast<size_t>(values_reader_.pos());
  const size_t limit = limits_[IntCast<size_t>(index_)];
  RIEGELI_ASSERT_LE(start, limit)
      << "Failed invariant of ChunkDecoder: record end positions not sorted";
  if (!values_reader_.Read(limit - start, record)) {
    RIEGELI_ASSERT_UNREACHABLE() << "Failed reading record from values reader: "
                                 << values_reader_.status();
  }
  ++index_;
  return true;
}

inline bool ChunkDecoder::ReadRecord(std::string& record) {
  if (ABSL_PREDICT_FALSE(!healthy() || index() == num_records())) {
    record.clear();
    return false;
  }
  const size_t start = IntCast<size_t>(values_reader_.pos());
  const size_t limit = limits_[IntCast<size_t>(index_)];
  RIEGELI_ASSERT_LE(start, limit)
      << "Failed invariant of ChunkDecoder: record end positions not sorted";
  if (!values_reader_.Read(limit - start, record)) {
    RIEGELI_ASSERT_UNREACHABLE() << "Failed reading record from values reader: "
                                 << values_reader_.status();
  }
  ++index_;
  return true;
}

inline bool ChunkDecoder::ReadRecord(Chain& record) {
  if (ABSL_PREDICT_FALSE(!healthy() || index() == num_records())) {
    record.Clear();
    return false;
  }
  const size_t start = IntCast<size_t>(values_reader_.pos());
  const size_t limit = limits_[IntCast<size_t>(index_)];
  RIEGELI_ASSERT_LE(start, limit)
      << "Failed invariant of ChunkDecoder: record end positions not sorted";
  if (!values_reader_.Read(limit - start, record)) {
    RIEGELI_ASSERT_UNREACHABLE() << "Failed reading record from values reader: "
                                 << values_reader_.status();
  }
  ++index_;
  return true;
}

inline bool ChunkDecoder::ReadRecord(absl::Cord& record) {
  if (ABSL_PREDICT_FALSE(!healthy() || index() == num_records())) {
    record.Clear();
    return false;
  }
  const size_t start = IntCast<size_t>(values_reader_.pos());
  const size_t limit = limits_[IntCast<size_t>(index_)];
  RIEGELI_ASSERT_LE(start, limit)
      << "Failed invariant of ChunkDecoder: record end positions not sorted";
  if (!values_reader_.Read(limit - start, record)) {
    RIEGELI_ASSERT_UNREACHABLE() << "Failed reading record from values reader: "
                                 << values_reader_.status();
  }
  ++index_;
  return true;
}

inline void ChunkDecoder::SetIndex(uint64_t index) {
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of ChunkDecoder::SetIndex(): " << status();
  index_ = UnsignedMin(index, num_records());
  const size_t start =
      index_ == 0 ? size_t{0} : limits_[IntCast<size_t>(index_ - 1)];
  if (!values_reader_.Seek(start)) {
    RIEGELI_ASSERT_UNREACHABLE()
        << "Failed seeking values reader: " << values_reader_.status();
  }
}

template <>
struct Resetter<ChunkDecoder> : ResetterByReset<ChunkDecoder> {};

}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_CHUNK_DECODER_H_
