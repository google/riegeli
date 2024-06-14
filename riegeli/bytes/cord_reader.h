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

#ifndef RIEGELI_BYTES_CORD_READER_H_
#define RIEGELI_BYTES_CORD_READER_H_

#include <stddef.h>

#include <memory>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/moving_dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/pullable_reader.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

class BackwardWriter;
class Writer;

// Template parameter independent part of `CordReader`.
class CordReaderBase : public PullableReader {
 public:
  // Returns the `absl::Cord` being read from. Unchanged by `Close()`.
  virtual const absl::Cord* SrcCord() const = 0;

  bool ToleratesReadingAhead() override { return true; }
  bool SupportsRandomAccess() override { return true; }
  bool SupportsNewReader() override { return true; }

 protected:
  using PullableReader::PullableReader;

  CordReaderBase(CordReaderBase&& that) noexcept;
  CordReaderBase& operator=(CordReaderBase&& that) noexcept;

  void Reset(Closed);
  void Reset();
  void Initialize(const absl::Cord* src);

  void Done() override;
  bool PullBehindScratch(size_t recommended_length) override;
  using PullableReader::ReadBehindScratch;
  bool ReadBehindScratch(size_t length, Chain& dest) override;
  bool ReadBehindScratch(size_t length, absl::Cord& dest) override;
  using PullableReader::CopyBehindScratch;
  bool CopyBehindScratch(Position length, Writer& dest) override;
  bool CopyBehindScratch(size_t length, BackwardWriter& dest) override;
  bool SeekBehindScratch(Position new_pos) override;
  absl::optional<Position> SizeImpl() override;
  std::unique_ptr<Reader> NewReaderImpl(Position initial_pos) override;

  // Invariant:
  //   if `!is_open()` or
  //      `*SrcCord()` is flat with size at most `kMaxBytesToCopy`
  //       then `iter_ == absl::nullopt`
  //       else `iter_ != absl::nullopt` and
  //            `*iter_` reads from `*SrcCord()`
  absl::optional<absl::Cord::CharIterator> iter_;

 private:
  // Moves `*iter_` to account for data which have been read from the buffer.
  //
  // Precondition: `iter_ != absl::nullopt`
  void SyncBuffer();

  // Sets buffer pointers to `absl::Cord::ChunkRemaining(*iter_)`,
  // or to `nullptr` if `*iter_ == src.char_end()`.
  //
  // Precondition: `iter_ != absl::nullopt`
  void MakeBuffer(const absl::Cord& src);

  // Invariants if `iter_ == absl::nullopt` and `is_open()`:
  //   scratch is not used
  //   `start() == SrcCord()->TryFlat()->data()`
  //   `start_to_limit() == SrcCord()->TryFlat()->size()`
  //   `start_pos() == 0`
  //
  // Invariants if `iter_ != absl::nullopt` and scratch is not used:
  //   `start() == (*iter_ == SrcCord()->char_end()
  //                    ? nullptr
  //                    : absl::Cord::ChunkRemaining(*iter_).data())`
  //   `start_to_limit() == (*iter_ == SrcCord()->char_end()
  //                          ? 0
  //                          : absl::Cord::ChunkRemaining(*iter_).size())`
  //   `start_pos()` is the position of `*iter_` in `*SrcCord()`
};

// A `Reader` which reads from an `absl::Cord`.
//
// It supports random access and `NewReader()`.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the `absl::Cord` being read from. `Src` must support
// `Dependency<const absl::Cord*, Src>`, e.g.
// `const absl::Cord*` (not owned, default), `absl::Cord` (owned),
// `AnyDependency<const absl::Cord*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as
// `InitializerTargetT` of the type of the first constructor argument.
// This requires C++17.
//
// The `absl::Cord` must not be changed until the `CordReader` is closed or no
// longer used.
template <typename Src = const absl::Cord*>
class CordReader : public CordReaderBase {
 public:
  // Creates a closed `CordReader`.
  explicit CordReader(Closed) noexcept : CordReaderBase(kClosed) {}

  // Will read from the `absl::Cord` provided by `src`.
  explicit CordReader(Initializer<Src> src);

  CordReader(CordReader&& that) = default;
  CordReader& operator=(CordReader&& that) = default;

  // Makes `*this` equivalent to a newly constructed `CordReader`. This avoids
  // constructing a temporary `CordReader` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Src> src);

  // Returns the object providing and possibly owning the `absl::Cord` being
  // read from. Unchanged by `Close()`.
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  const absl::Cord* SrcCord() const override { return src_.get(); }

 private:
  class Mover;

  // The object providing and possibly owning the `absl::Cord` being read from.
  MovingDependency<const absl::Cord*, Src, Mover> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit CordReader(Closed) -> CordReader<DeleteCtad<Closed>>;
template <typename Src>
explicit CordReader(Src&& src) -> CordReader<InitializerTargetT<Src>>;
#endif

// Implementation details follow.

inline CordReaderBase::CordReaderBase(CordReaderBase&& that) noexcept
    : PullableReader(static_cast<PullableReader&&>(that)),
      iter_(std::exchange(that.iter_, absl::nullopt)) {}

inline CordReaderBase& CordReaderBase::operator=(
    CordReaderBase&& that) noexcept {
  PullableReader::operator=(static_cast<PullableReader&&>(that));
  iter_ = std::exchange(that.iter_, absl::nullopt);
  return *this;
}

inline void CordReaderBase::Reset(Closed) {
  PullableReader::Reset(kClosed);
  iter_ = absl::nullopt;
}

inline void CordReaderBase::Reset() {
  PullableReader::Reset();
  iter_ = absl::nullopt;
}

inline void CordReaderBase::Initialize(const absl::Cord* src) {
  RIEGELI_ASSERT(src != nullptr)
      << "Failed precondition of CordReader: null Cord pointer";
  if (const absl::optional<absl::string_view> flat = src->TryFlat()) {
    if (flat->size() <= kMaxBytesToCopy) {
      set_buffer(flat->data(), flat->size());
      move_limit_pos(available());
      return;
    }
  }
  iter_ = src->char_begin();
  MakeBuffer(*src);
}

inline void CordReaderBase::MakeBuffer(const absl::Cord& src) {
  RIEGELI_ASSERT(iter_ != absl::nullopt)
      << "Failed precondition of CordReaderBase::MakeBuffer(): "
         "no Cord iterator";
  if (*iter_ == src.char_end()) {
    set_buffer();
    return;
  }
  const absl::string_view fragment = absl::Cord::ChunkRemaining(*iter_);
  set_buffer(fragment.data(), fragment.size());
  move_limit_pos(available());
}

template <typename Src>
class CordReader<Src>::Mover {
 public:
  static auto member() { return &CordReader::src_; }

  explicit Mover(CordReader& self, ABSL_ATTRIBUTE_UNUSED CordReader& that)
      : behind_scratch_(&self),
        uses_buffer_(self.start() != nullptr),
        position_(IntCast<size_t>(self.start_pos())),
        start_to_cursor_(self.start_to_cursor()) {
#if RIEGELI_DEBUG
    if (self.iter_ == absl::nullopt) {
      if (uses_buffer_) {
        const absl::optional<absl::string_view> flat = that.src_->TryFlat();
        RIEGELI_ASSERT(flat != absl::nullopt)
            << "CordReader source changed unexpectedly";
        RIEGELI_ASSERT(flat->data() == self.start())
            << "CordReader source changed unexpectedly";
        RIEGELI_ASSERT_EQ(flat->size(), self.start_to_limit())
            << "CordReader source changed unexpectedly";
      }
    } else {
      if (position_ == that.src_->size()) {
        RIEGELI_ASSERT(*self.iter_ == that.src_->char_end())
            << "CordReader source changed unexpectedly";
        RIEGELI_ASSERT(!uses_buffer_)
            << "CordReader source changed unexpectedly";
      } else {
        const absl::string_view fragment =
            absl::Cord::ChunkRemaining(*self.iter_);
        RIEGELI_ASSERT(fragment.data() == self.start())
            << "CordReader source changed unexpectedly";
        RIEGELI_ASSERT_EQ(fragment.size(), self.start_to_limit())
            << "CordReader source changed unexpectedly";
      }
    }
#endif
  }

  void Done(CordReader& self) {
    if (self.iter_ == absl::nullopt) {
      if (uses_buffer_) {
        const absl::optional<absl::string_view> flat = self.src_->TryFlat();
        RIEGELI_ASSERT(flat != absl::nullopt)
            << "CordReader source changed unexpectedly";
        self.set_buffer(flat->data(), flat->size(), start_to_cursor_);
      }
    } else {
      if (position_ == self.src_->size()) {
        self.iter_ = self.src_->char_end();
      } else {
        self.iter_ = self.src_->char_begin();
        absl::Cord::Advance(&*self.iter_, position_);
        const absl::string_view fragment =
            absl::Cord::ChunkRemaining(*self.iter_);
        self.set_buffer(fragment.data(), fragment.size(), start_to_cursor_);
      }
    }
  }

 private:
  BehindScratch behind_scratch_;
  bool uses_buffer_;
  size_t position_;
  size_t start_to_cursor_;
};

template <typename Src>
inline CordReader<Src>::CordReader(Initializer<Src> src)
    : src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
inline void CordReader<Src>::Reset(Closed) {
  CordReaderBase::Reset(kClosed);
  src_.Reset();
}

template <typename Src>
inline void CordReader<Src>::Reset(Initializer<Src> src) {
  CordReaderBase::Reset();
  src_.Reset(std::move(src));
  Initialize(src_.get());
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_CORD_READER_H_
