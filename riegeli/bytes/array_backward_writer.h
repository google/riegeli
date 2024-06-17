// Copyright 2018 Google LLC
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

#ifndef RIEGELI_BYTES_ARRAY_BACKWARD_WRITER_H_
#define RIEGELI_BYTES_ARRAY_BACKWARD_WRITER_H_

#include <stddef.h>

#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/meta/type_traits.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/moving_dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/pushable_backward_writer.h"

namespace riegeli {

// Template parameter independent part of `ArrayBackwardWriter`.
class ArrayBackwardWriterBase : public PushableBackwardWriter {
 public:
  // Returns the array being written to. Unchanged by `Close()`.
  virtual absl::Span<char> DestSpan() const = 0;

  // Returns written data in a suffix of the original array. Valid only after
  // `Close()` or `Flush()`.
  absl::Span<char> written() const { return written_; }

  bool PrefersCopying() const override { return true; }
  bool SupportsTruncate() override { return true; }

 protected:
  using PushableBackwardWriter::PushableBackwardWriter;

  ArrayBackwardWriterBase(ArrayBackwardWriterBase&& that) noexcept;
  ArrayBackwardWriterBase& operator=(ArrayBackwardWriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset();
  void Initialize(absl::Span<char> dest);
  void set_written(absl::Span<char> written) { written_ = written; }

  bool PushBehindScratch(size_t recommended_length) override;
  using PushableBackwardWriter::WriteBehindScratch;
  bool WriteBehindScratch(absl::string_view src) override;
  bool FlushBehindScratch(FlushType flush_type) override;
  bool TruncateBehindScratch(Position new_size) override;

 private:
  // Written data. Valid only after `Close()` or `Flush()`.
  absl::Span<char> written_;

  // Invariants if `ok()` and scratch is not used:
  //   `limit() == DestSpan().data()`
  //   `start_to_limit() == DestSpan().size()`
  //   `start_pos() == 0`
};

// A `BackwardWriter` which writes to a preallocated array with a known size
// limit.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the array being written to. `Dest` must support
// `Dependency<absl::Span<char>, Dest>`, e.g.
// `absl::Span<char>` (not owned, default), `std::string*` (not owned),
// `std::string` (owned), `Any<absl::Span<char>>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as
// `InitializerTargetT` of the type of the first constructor argument, except
// that CTAD is deleted if the first constructor argument is a reference to a
// type that `absl::Span<char>` would be constructible from, other than
// `absl::Span<char>` itself (to avoid writing to an unintentionally separate
// copy of an existing object). This requires C++17.
//
// The array must not be destroyed until the `ArrayBackwardWriter` is closed or
// no longer used.
template <typename Dest = absl::Span<char>>
class ArrayBackwardWriter : public ArrayBackwardWriterBase {
 public:
  // Creates a closed `ArrayBackwardWriter`.
  explicit ArrayBackwardWriter(Closed) noexcept
      : ArrayBackwardWriterBase(kClosed) {}

  // Will write to the array provided by `dest`.
  explicit ArrayBackwardWriter(Initializer<Dest> dest);

  // Will write to `absl::MakeSpan(dest, size)`. This constructor is present
  // only if `Dest` is `absl::Span<char>`.
  template <typename DependentDest = Dest,
            std::enable_if_t<
                std::is_same<DependentDest, absl::Span<char>>::value, int> = 0>
  explicit ArrayBackwardWriter(char* dest, size_t size);

  ArrayBackwardWriter(ArrayBackwardWriter&& that) = default;
  ArrayBackwardWriter& operator=(ArrayBackwardWriter&& that) = default;

  // Makes `*this` equivalent to a newly constructed `ArrayBackwardWriter`. This
  // avoids constructing a temporary `ArrayBackwardWriter` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Dest> dest);
  template <typename DependentDest = Dest,
            std::enable_if_t<
                std::is_same<DependentDest, absl::Span<char>>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(char* dest, size_t size);

  // Returns the object providing and possibly owning the array being written
  // to. Unchanged by `Close()`.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  absl::Span<char> DestSpan() const override { return dest_.get(); }

 private:
  class Mover;

  // The object providing and possibly owning the array being written to.
  MovingDependency<absl::Span<char>, Dest, Mover> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit ArrayBackwardWriter(Closed) -> ArrayBackwardWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit ArrayBackwardWriter(Dest&& dest)
    -> ArrayBackwardWriter<std::conditional_t<
        absl::conjunction<
            absl::negation<std::is_same<std::decay_t<Dest>, absl::Span<char>>>,
            std::is_lvalue_reference<Dest>,
            std::is_constructible<absl::Span<char>, Dest>,
            absl::negation<std::is_pointer<std::remove_reference_t<Dest>>>>::
            value,
        DeleteCtad<Dest&&>, InitializerTargetT<Dest>>>;
explicit ArrayBackwardWriter(char* dest, size_t size)
    -> ArrayBackwardWriter<absl::Span<char>>;
#endif

// Implementation details follow.

inline ArrayBackwardWriterBase::ArrayBackwardWriterBase(
    ArrayBackwardWriterBase&& that) noexcept
    : PushableBackwardWriter(static_cast<PushableBackwardWriter&&>(that)),
      written_(that.written_) {}

inline ArrayBackwardWriterBase& ArrayBackwardWriterBase::operator=(
    ArrayBackwardWriterBase&& that) noexcept {
  PushableBackwardWriter::operator=(
      static_cast<PushableBackwardWriter&&>(that));
  written_ = that.written_;
  return *this;
}

inline void ArrayBackwardWriterBase::Reset(Closed) {
  PushableBackwardWriter::Reset(kClosed);
  written_ = absl::Span<char>();
}

inline void ArrayBackwardWriterBase::Reset() {
  PushableBackwardWriter::Reset();
  written_ = absl::Span<char>();
}

inline void ArrayBackwardWriterBase::Initialize(absl::Span<char> dest) {
  set_buffer(dest.data(), dest.size());
}

template <typename Dest>
class ArrayBackwardWriter<Dest>::Mover {
 public:
  static auto member() { return &ArrayBackwardWriter::dest_; }

  explicit Mover(ArrayBackwardWriter& self, ArrayBackwardWriter& that)
      : behind_scratch_(&self),
        uses_buffer_(self.start() != nullptr),
        start_to_cursor_(self.start_to_cursor()),
        has_written_(self.written().data() != nullptr),
        written_size_(self.written().size()) {
    if (uses_buffer_) {
      RIEGELI_ASSERT(that.dest_.get().data() == self.limit())
          << "ArrayBackwardWriter destination changed unexpectedly";
      RIEGELI_ASSERT_EQ(that.dest_.get().size(), self.start_to_limit())
          << "ArrayBackwardWriter destination changed unexpectedly";
    }
    if (has_written_) {
      RIEGELI_ASSERT(that.dest_.get().data() + that.dest_.get().size() ==
                     self.written().data() + self.written().size())
          << "ArrayBackwardWriter destination changed unexpectedly";
    }
  }

  void Done(ArrayBackwardWriter& self) {
    if (uses_buffer_) {
      const absl::Span<char> dest = self.dest_.get();
      self.set_buffer(dest.data(), dest.size(), start_to_cursor_);
    }
    if (has_written_) {
      const absl::Span<char> dest = self.dest_.get();
      self.set_written(absl::MakeSpan(dest.data() + dest.size() - written_size_,
                                      written_size_));
    }
  }

 private:
  BehindScratch behind_scratch_;
  bool uses_buffer_;
  size_t start_to_cursor_;
  bool has_written_;
  size_t written_size_;
};

template <typename Dest>
inline ArrayBackwardWriter<Dest>::ArrayBackwardWriter(Initializer<Dest> dest)
    : dest_(std::move(dest)) {
  Initialize(dest_.get());
}

template <typename Dest>
template <
    typename DependentDest,
    std::enable_if_t<std::is_same<DependentDest, absl::Span<char>>::value, int>>
inline ArrayBackwardWriter<Dest>::ArrayBackwardWriter(char* dest, size_t size)
    : ArrayBackwardWriter(absl::MakeSpan(dest, size)) {}

template <typename Dest>
inline void ArrayBackwardWriter<Dest>::Reset(Closed) {
  ArrayBackwardWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void ArrayBackwardWriter<Dest>::Reset(Initializer<Dest> dest) {
  ArrayBackwardWriterBase::Reset();
  dest_.Reset(std::move(dest));
  Initialize(dest_.get());
}

template <typename Dest>
template <
    typename DependentDest,
    std::enable_if_t<std::is_same<DependentDest, absl::Span<char>>::value, int>>
inline void ArrayBackwardWriter<Dest>::Reset(char* dest, size_t size) {
  Reset(absl::MakeSpan(dest, size));
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_ARRAY_BACKWARD_WRITER_H_
