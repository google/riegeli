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

  CordReader(CordReader&& that) noexcept;
  CordReader& operator=(CordReader&& that) noexcept;

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
  // Moves `that.src_` to `src_`. Buffer pointers are already moved from `src_`
  // to `*this`; adjust them to match `src_`.
  void MoveSrc(CordReader&& that);

  // The object providing and possibly owning the `absl::Cord` being read from.
  Dependency<const absl::Cord*, Src> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit CordReader(Closed) -> CordReader<DeleteCtad<Closed>>;
template <typename Src>
explicit CordReader(Src&& src) -> CordReader<InitializerTargetT<Src>>;
#endif

// Implementation details follow.

inline CordReaderBase::CordReaderBase(CordReaderBase&& that) noexcept
    : PullableReader(static_cast<PullableReader&&>(that)) {
  // `iter_` will be moved by `CordReader<Src>::MoveSrc()`.
}

inline CordReaderBase& CordReaderBase::operator=(
    CordReaderBase&& that) noexcept {
  PullableReader::operator=(static_cast<PullableReader&&>(that));
  // `iter_` will be moved by `CordReader<Src>::MoveSrc()`.
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
inline CordReader<Src>::CordReader(Initializer<Src> src)
    : src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
inline CordReader<Src>::CordReader(CordReader&& that) noexcept
    : CordReaderBase(static_cast<CordReaderBase&&>(that)) {
  MoveSrc(std::move(that));
}

template <typename Src>
inline CordReader<Src>& CordReader<Src>::operator=(CordReader&& that) noexcept {
  CordReaderBase::operator=(static_cast<CordReaderBase&&>(that));
  MoveSrc(std::move(that));
  return *this;
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

template <typename Src>
inline void CordReader<Src>::MoveSrc(CordReader&& that) {
  if (src_.kIsStable) {
    src_ = std::move(that.src_);
    iter_ = std::exchange(that.iter_, absl::nullopt);
  } else {
    BehindScratch behind_scratch(this);
    const size_t position = IntCast<size_t>(start_pos());
    const size_t cursor_index = start_to_cursor();
    src_ = std::move(that.src_);
    if (that.iter_ == absl::nullopt) {
      iter_ = absl::nullopt;
      if (start() != nullptr) {
        const absl::optional<absl::string_view> flat = src_->TryFlat();
        RIEGELI_ASSERT(flat != absl::nullopt)
            << "Failed invariant of CordReaderBase: "
               "no Cord iterator but Cord is not flat";
        set_buffer(flat->data(), flat->size(), cursor_index);
      }
    } else {
      // Reset `that.iter_` before `iter_` to support self-assignment.
      that.iter_ = absl::nullopt;
      if (position == src_->size()) {
        iter_ = src_->char_end();
        set_buffer();
      } else {
        iter_ = src_->char_begin();
        absl::Cord::Advance(&*iter_, position);
        const absl::string_view fragment = absl::Cord::ChunkRemaining(*iter_);
        set_buffer(fragment.data(), fragment.size(), cursor_index);
      }
    }
  }
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_CORD_READER_H_
