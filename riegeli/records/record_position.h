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
#include <string>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/base/call_once.h"
#include "absl/strings/string_view.h"
#include "absl/types/variant.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/intrusive_ref_count.h"
#include "riegeli/base/types.h"
#include "riegeli/chunk_encoding/chunk.h"

namespace riegeli {

// `RecordPosition` represents the position of a record in a Riegeli/records
// file, or a position between records.
//
// There are two ways of expressing positions, both strictly monotonic:
//  * `RecordPosition` (a class) - Faster for seeking.
//  * `Position` (an integer)    - Scaled between 0 and file size.
//
// `RecordPosition` can be converted to `Position` by `numeric()`.
//
// Working with `RecordPosition` is recommended, unless it is needed to seek to
// an approximate position interpolated along the file, e.g. for splitting the
// file into shards, or unless the position must be expressed as an integer from
// the range [0..`file_size`] in order to fit into a preexisting API.
class RecordPosition : public WithCompare<RecordPosition> {
 public:
  // Creates a `RecordPosition` corresponding to the first record.
  constexpr RecordPosition() = default;

  // Creates a `RecordPosition` corresponding to the given record of the chunk
  // beginning at the given file position.
  explicit RecordPosition(uint64_t chunk_begin, uint64_t record_index);

  RecordPosition(const RecordPosition& that) = default;
  RecordPosition& operator=(const RecordPosition& that) = default;

  // File position of the beginning of the chunk containing the given record.
  uint64_t chunk_begin() const { return chunk_begin_; }
  // Index of the record within the chunk.
  uint64_t record_index() const { return record_index_; }

  // Converts `RecordPosition` to an integer scaled between 0 and file size.
  // Distinct `RecordPosition`s of a valid file have distinct numeric values.
  uint64_t numeric() const { return chunk_begin_ + record_index_; }

  // Text format: "<chunk_begin>/<record_index>".
  std::string ToString() const;
  bool FromString(absl::string_view serialized);

  // Binary format: `chunk_begin` and `record_index` as BigEndian-encoded 8-byte
  // integers. Serialized strings have the same natural order as the
  // corresponding positions.
  std::string ToBytes() const;
  bool FromBytes(absl::string_view serialized);

  friend bool operator==(RecordPosition a, RecordPosition b) {
    return a.chunk_begin() == b.chunk_begin() &&
           a.record_index() == b.record_index();
  }
  friend StrongOrdering RIEGELI_COMPARE(const RecordPosition& a,
                                        const RecordPosition& b) {
    {
      const StrongOrdering ordering = Compare(a.chunk_begin(), b.chunk_begin());
      if (ordering != 0) {
        return ordering;
      }
    }
    return Compare(a.record_index(), b.record_index());
  }

  template <typename HashState>
  friend HashState AbslHashValue(HashState hash_state, RecordPosition self) {
    return HashState::combine(std::move(hash_state), self.chunk_begin_,
                              self.record_index_);
  }

  // Default stringification by `absl::StrCat()` etc.
  //
  // Writes `self.ToString()` to `sink`.
  template <typename Sink>
  friend void AbslStringify(Sink& sink, RecordPosition self) {
    sink.Append(self.ToString());
  }

  // Writes `self.ToString()` to `out`.
  friend std::ostream& operator<<(std::ostream& out, RecordPosition self) {
    self.OutputImpl(out);
    return out;
  }

 private:
  void OutputImpl(std::ostream& out) const;

  // Invariant:
  //   `record_index_ <= std::numeric_limits<uint64_t>::max() - chunk_begin_`
  uint64_t chunk_begin_ = 0;
  uint64_t record_index_ = 0;
};

// `FutureChunkBegin` is similar to `std::shared_future<Position>`.
//
// It is used to implement `FutureRecordPosition` and internally in
// `RecordWriter`.

namespace records_internal {

class
#ifdef ABSL_ATTRIBUTE_TRIVIAL_ABI
    ABSL_ATTRIBUTE_TRIVIAL_ABI
#endif
        FutureChunkBegin {
 public:
  struct PadToBlockBoundary {};
  using Action =
      absl::variant<std::shared_future<ChunkHeader>, PadToBlockBoundary>;

  constexpr FutureChunkBegin() = default;

  /*implicit*/ FutureChunkBegin(Position chunk_begin) noexcept;

  explicit FutureChunkBegin(Position pos_before_chunks,
                            std::vector<Action> actions);

  FutureChunkBegin(const FutureChunkBegin& that) noexcept;
  FutureChunkBegin& operator=(const FutureChunkBegin& that) noexcept;

  FutureChunkBegin(FutureChunkBegin&& that) noexcept;
  FutureChunkBegin& operator=(FutureChunkBegin&& that) noexcept;

  // May block if returned by `RecordWriter` with `parallelism > 0`.
  Position get() const;

  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const FutureChunkBegin* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&self->unresolved_);
  }

 private:
  class Unresolved;

  // `unresolved_` is a pointer to save memory in the common case when
  // it is absent.
  //
  // The pointer uses shared ownership because `Unresolved` is not copyable,
  // which is because its contents are resolved lazily in a const method,
  // so a copy constructor would need to block.
  RefCountedPtr<const Unresolved> unresolved_;
  // If `unresolved_ == nullptr`, `chunk_begin_` is stored here,
  // otherwise it is `unresolved_->get()`.
  Position resolved_ = 0;
};

}  // namespace records_internal

// `FutureRecordPosition` is similar to `std::shared_future<RecordPosition>`.
//
// `RecordWriter` returns `FutureRecordPosition` instead of `RecordPosition`
// because with `parallelism > 0` the actual position is not known until pending
// chunks finish encoding in background.
class
#ifdef ABSL_ATTRIBUTE_TRIVIAL_ABI
    ABSL_ATTRIBUTE_TRIVIAL_ABI
#endif
        FutureRecordPosition {
 public:
  constexpr FutureRecordPosition() = default;

  /*implicit*/ FutureRecordPosition(RecordPosition pos) noexcept;

  explicit FutureRecordPosition(records_internal::FutureChunkBegin chunk_begin,
                                uint64_t record_index);

  FutureRecordPosition(const FutureRecordPosition& that) noexcept;
  FutureRecordPosition& operator=(const FutureRecordPosition& that) noexcept;

  FutureRecordPosition(FutureRecordPosition&& that) noexcept;
  FutureRecordPosition& operator=(FutureRecordPosition&& that) noexcept;

  // May block if returned by `RecordWriter` with `parallelism > 0`.
  RecordPosition get() const;

  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const FutureRecordPosition* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&self->chunk_begin_);
  }

 private:
  records_internal::FutureChunkBegin chunk_begin_;
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

namespace records_internal {

class FutureChunkBegin::Unresolved : public RefCountedBase<Unresolved> {
 public:
  explicit Unresolved(Position pos_before_chunks, std::vector<Action> actions);

  Unresolved(const Unresolved&) = delete;
  Unresolved& operator=(const Unresolved&) = delete;

  Position get() const;

  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const Unresolved* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&self->actions_);
  }

 private:
  void Resolve() const;

  mutable absl::once_flag flag_;
  // Position before writing chunks according to `actions_`.
  mutable Position pos_before_chunks_ = 0;
  // Headers of chunks to be written after `pos_before_chunks_`.
  mutable std::vector<Action> actions_;
};

inline Position FutureChunkBegin::Unresolved::get() const {
  absl::call_once(flag_, &Unresolved::Resolve, this);
  RIEGELI_ASSERT(actions_.empty()) << "FutureChunkBegin::Unresolved::Resolve() "
                                      "did not clear actions_";
  return pos_before_chunks_;
}

inline FutureChunkBegin::FutureChunkBegin(Position chunk_begin) noexcept
    : resolved_(chunk_begin) {}

inline FutureChunkBegin::FutureChunkBegin(const FutureChunkBegin& that) noexcept
    : unresolved_(that.unresolved_), resolved_(that.resolved_) {}

inline FutureChunkBegin& FutureChunkBegin::operator=(
    const FutureChunkBegin& that) noexcept {
  unresolved_ = that.unresolved_;
  resolved_ = that.resolved_;
  return *this;
}

inline FutureChunkBegin::FutureChunkBegin(FutureChunkBegin&& that) noexcept
    : unresolved_(std::move(that.unresolved_)),
      resolved_(std::exchange(that.resolved_, 0)) {}

inline FutureChunkBegin& FutureChunkBegin::operator=(
    FutureChunkBegin&& that) noexcept {
  unresolved_ = std::move(that.unresolved_);
  resolved_ = std::exchange(that.resolved_, 0);
  return *this;
}

inline Position FutureChunkBegin::get() const {
  return unresolved_ == nullptr ? resolved_ : unresolved_->get();
}

}  // namespace records_internal

inline FutureRecordPosition::FutureRecordPosition(RecordPosition pos) noexcept
    : chunk_begin_(pos.chunk_begin()), record_index_(pos.record_index()) {}

inline FutureRecordPosition::FutureRecordPosition(
    records_internal::FutureChunkBegin chunk_begin, uint64_t record_index)
    : chunk_begin_(std::move(chunk_begin)), record_index_(record_index) {}

inline FutureRecordPosition::FutureRecordPosition(
    const FutureRecordPosition& that) noexcept
    : chunk_begin_(that.chunk_begin_), record_index_(that.record_index_) {}

inline FutureRecordPosition& FutureRecordPosition::operator=(
    const FutureRecordPosition& that) noexcept {
  chunk_begin_ = that.chunk_begin_;
  record_index_ = that.record_index_;
  return *this;
}

inline FutureRecordPosition::FutureRecordPosition(
    FutureRecordPosition&& that) noexcept
    : chunk_begin_(std::move(that.chunk_begin_)),
      record_index_(std::exchange(that.record_index_, 0)) {}

inline FutureRecordPosition& FutureRecordPosition::operator=(
    FutureRecordPosition&& that) noexcept {
  chunk_begin_ = std::move(that.chunk_begin_);
  record_index_ = std::exchange(that.record_index_, 0);
  return *this;
}

inline RecordPosition FutureRecordPosition::get() const {
  return RecordPosition(chunk_begin_.get(), record_index_);
}

}  // namespace riegeli

#endif  // RIEGELI_RECORDS_RECORD_POSITION_H_
