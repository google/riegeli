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

#ifndef RIEGELI_RECORDS_RECORD_POSITION_H_
#define RIEGELI_RECORDS_RECORD_POSITION_H_

#include <stdint.h>

#include <future>
#include <iosfwd>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/call_once.h"
#include "absl/strings/string_view.h"
#include "absl/types/variant.h"
#include "absl/utility/utility.h"
#include "riegeli/base/base.h"
#include "riegeli/chunk_encoding/chunk.h"

namespace riegeli {

// RecordPosition represents the position of a record in a Riegeli/records file,
// or a position between records.
//
// There are two ways of expressing positions, both strictly monotonic:
//  * RecordPosition (a class) - Faster for seeking.
//  * Position (an integer)    - Scaled between 0 and file size.
//
// RecordPosition can be converted to Position by numeric().
//
// Working with RecordPosition is recommended, unless it is needed to seek to an
// approximate position interpolated along the file, e.g. for splitting the file
// into shards, or unless the position must be expressed as an integer from the
// range [0, file_size] in order to fit into a preexisting API.
class RecordPosition {
 public:
  // Creates a RecordPosition corresponding to the first record.
  constexpr RecordPosition() noexcept {}

  // Creates a RecordPosition corresponding to the given record of the chunk
  // beginning at the given file position.
  explicit RecordPosition(uint64_t chunk_begin, uint64_t record_index);

  RecordPosition(const RecordPosition& that) noexcept;
  RecordPosition& operator=(const RecordPosition& that) noexcept;

  // File position of the beginning of the chunk containing the given record.
  uint64_t chunk_begin() const { return chunk_begin_; }
  // Index of the record within the chunk.
  uint64_t record_index() const { return record_index_; }

  // Converts RecordPosition to an integer scaled between 0 and file size.
  // Distinct RecordPositions of a valid file have distinct numeric values.
  uint64_t numeric() const { return chunk_begin_ + record_index_; }

  // Text format: "<chunk_begin>/<record_index>".
  std::string ToString() const;
  bool FromString(absl::string_view serialized);

  // Binary format: chunk_begin and record_index as BigEndian-encoded 8-byte
  // integers. Serialized strings have the same natural order as the
  // corresponding positions.
  std::string ToBytes() const;
  bool FromBytes(absl::string_view serialized);

  friend bool operator==(RecordPosition a, RecordPosition b);
  friend bool operator!=(RecordPosition a, RecordPosition b);
  friend bool operator<(RecordPosition a, RecordPosition b);
  friend bool operator>(RecordPosition a, RecordPosition b);
  friend bool operator<=(RecordPosition a, RecordPosition b);
  friend bool operator>=(RecordPosition a, RecordPosition b);

  template <typename HashState>
  friend HashState AbslHashValue(HashState hash_state, RecordPosition self);

  // Same as: out << pos.ToString()
  friend std::ostream& operator<<(std::ostream& out, RecordPosition pos);

 private:
  // Invariant: record_index_ <= numeric_limits<uint64_t>::max() - chunk_begin_
  uint64_t chunk_begin_ = 0;
  uint64_t record_index_ = 0;
};

// FutureRecordPosition is similar to shared_future<RecordPosition>.
//
// RecordWriter returns FutureRecordPosition instead of RecordPosition because
// with parallelism > 0 the actual position is not known until pending chunks
// finish encoding in background.
class FutureRecordPosition {
 public:
  struct PadToBlockBoundary {};
  using Action =
      absl::variant<std::shared_future<ChunkHeader>, PadToBlockBoundary>;

  constexpr FutureRecordPosition() noexcept {}

  explicit FutureRecordPosition(RecordPosition pos) noexcept;

  FutureRecordPosition(Position pos_before_chunks, std::vector<Action> actions,
                       uint64_t record_index);

  FutureRecordPosition(const FutureRecordPosition& that) noexcept;
  FutureRecordPosition& operator=(const FutureRecordPosition& that) noexcept;

  FutureRecordPosition(FutureRecordPosition&& that) noexcept;
  FutureRecordPosition& operator=(FutureRecordPosition&& that) noexcept;

  // May block if returned by RecordWriter with parallelism > 0.
  RecordPosition get() const;

 private:
  class FutureChunkBegin;

  std::shared_ptr<FutureChunkBegin> future_chunk_begin_;
  // If future_chunk_begin_ == nullptr, chunk_begin_ is stored here, otherwise
  // it is future_chunk_begin_->get().
  Position chunk_begin_ = 0;
  uint64_t record_index_ = 0;
};

// Implementation details follow.

inline RecordPosition::RecordPosition(uint64_t chunk_begin,
                                      uint64_t record_index)
    : chunk_begin_(chunk_begin), record_index_(record_index) {
  RIEGELI_ASSERT_LE(record_index,
                    std::numeric_limits<uint64_t>::max() - chunk_begin)
      << "RecordPosition overflow";
}

inline RecordPosition::RecordPosition(const RecordPosition& that) noexcept
    : chunk_begin_(that.chunk_begin_), record_index_(that.record_index_) {}

inline RecordPosition& RecordPosition::operator=(
    const RecordPosition& that) noexcept {
  chunk_begin_ = that.chunk_begin_;
  record_index_ = that.record_index_;
  return *this;
}

inline bool operator==(RecordPosition a, RecordPosition b) {
  return a.chunk_begin() == b.chunk_begin() &&
         a.record_index() == b.record_index();
}

inline bool operator!=(RecordPosition a, RecordPosition b) {
  return a.chunk_begin() != b.chunk_begin() ||
         a.record_index() != b.record_index();
}

inline bool operator<(RecordPosition a, RecordPosition b) {
  if (a.chunk_begin() != b.chunk_begin()) {
    return a.chunk_begin() < b.chunk_begin();
  }
  return a.record_index() < b.record_index();
}

inline bool operator>(RecordPosition a, RecordPosition b) {
  if (a.chunk_begin() != b.chunk_begin()) {
    return a.chunk_begin() > b.chunk_begin();
  }
  return a.record_index() > b.record_index();
}

inline bool operator<=(RecordPosition a, RecordPosition b) {
  if (a.chunk_begin() != b.chunk_begin()) {
    return a.chunk_begin() < b.chunk_begin();
  }
  return a.record_index() <= b.record_index();
}

inline bool operator>=(RecordPosition a, RecordPosition b) {
  if (a.chunk_begin() != b.chunk_begin()) {
    return a.chunk_begin() > b.chunk_begin();
  }
  return a.record_index() >= b.record_index();
}

template <typename HashState>
inline HashState AbslHashValue(HashState hash_state, RecordPosition self) {
  return HashState::combine(std::move(hash_state), self.chunk_begin_,
                            self.record_index_);
}

class FutureRecordPosition::FutureChunkBegin {
 public:
  explicit FutureChunkBegin(Position pos_before_chunks,
                            std::vector<Action> actions);

  FutureChunkBegin(const FutureChunkBegin&) = delete;
  FutureChunkBegin& operator=(const FutureChunkBegin&) = delete;

  Position get() const;

 private:
  void Resolve() const;

  mutable absl::once_flag flag_;
  // Position before writing chunks according to actions_.
  mutable Position pos_before_chunks_ = 0;
  // Headers of chunks to be written after pos_before_chunks_.
  mutable std::vector<Action> actions_;
};

inline Position FutureRecordPosition::FutureChunkBegin::get() const {
  absl::call_once(flag_, &FutureChunkBegin::Resolve, this);
  RIEGELI_ASSERT(actions_.empty())
      << "FutureRecordPosition::FutureChunkBegin::Resolve() "
         "did not clear actions_";
  return pos_before_chunks_;
}

inline FutureRecordPosition::FutureRecordPosition(RecordPosition pos) noexcept
    : chunk_begin_(pos.chunk_begin()), record_index_(pos.record_index()) {}

inline FutureRecordPosition::FutureRecordPosition(
    const FutureRecordPosition& that) noexcept
    : future_chunk_begin_(that.future_chunk_begin_),
      chunk_begin_(that.chunk_begin_),
      record_index_(that.record_index_) {}

inline FutureRecordPosition& FutureRecordPosition::operator=(
    const FutureRecordPosition& that) noexcept {
  future_chunk_begin_ = that.future_chunk_begin_;
  chunk_begin_ = that.chunk_begin_;
  record_index_ = that.record_index_;
  return *this;
}

inline FutureRecordPosition::FutureRecordPosition(
    FutureRecordPosition&& that) noexcept
    : future_chunk_begin_(std::move(that.future_chunk_begin_)),
      chunk_begin_(absl::exchange(that.chunk_begin_, 0)),
      record_index_(absl::exchange(that.record_index_, 0)) {}

inline FutureRecordPosition& FutureRecordPosition::operator=(
    FutureRecordPosition&& that) noexcept {
  future_chunk_begin_ = std::move(that.future_chunk_begin_);
  chunk_begin_ = absl::exchange(that.chunk_begin_, 0);
  record_index_ = absl::exchange(that.record_index_, 0);
  return *this;
}

inline RecordPosition FutureRecordPosition::get() const {
  return RecordPosition(future_chunk_begin_ == nullptr
                            ? chunk_begin_
                            : future_chunk_begin_->get(),
                        record_index_);
}

}  // namespace riegeli

#endif  // RIEGELI_RECORDS_RECORD_POSITION_H_
