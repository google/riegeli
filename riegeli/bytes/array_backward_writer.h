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

#include <tuple>
#include <utility>

#include "absl/types/span.h"
#include "riegeli/base/base.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/resetter.h"
#include "riegeli/bytes/pushable_backward_writer.h"
#include "riegeli/bytes/span_dependency.h"

namespace riegeli {

// Template parameter independent part of `ArrayBackwardWriter`.
class ArrayBackwardWriterBase : public PushableBackwardWriter {
 public:
  // Returns the array being written to. Unchanged by `Close()`.
  virtual absl::Span<char> dest_span() = 0;
  virtual absl::Span<const char> dest_span() const = 0;

  // Returns written data in a suffix of the original array. Valid only after
  // `Close()` or `Flush()`.
  absl::Span<char> written() { return written_; }
  absl::Span<const char> written() const { return written_; }

  bool Flush(FlushType flush_type) override;
  bool SupportsTruncate() const override { return true; }
  bool Truncate(Position new_size) override;

 protected:
  explicit ArrayBackwardWriterBase(InitiallyClosed) noexcept
      : PushableBackwardWriter(kInitiallyClosed) {}
  explicit ArrayBackwardWriterBase(InitiallyOpen) noexcept
      : PushableBackwardWriter(kInitiallyOpen) {}

  ArrayBackwardWriterBase(ArrayBackwardWriterBase&& that) noexcept;
  ArrayBackwardWriterBase& operator=(ArrayBackwardWriterBase&& that) noexcept;

  void Reset(InitiallyClosed);
  void Reset(InitiallyOpen);
  void Initialize(absl::Span<char> dest);

  void Done() override;
  bool PushSlow(size_t min_length, size_t recommended_length) override;

  // Written data. Valid only after `Close()` or `Flush()`.
  absl::Span<char> written_;

  // Invariant: `start_pos() == 0`
};

// A `BackwardWriter` which writes to a preallocated array with a known size
// limit.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the array being written to. `Dest` must support
// `Dependency<absl::Span<char>, Dest>`, e.g.
// `absl::Span<char>` (not owned, default), `std::string*` (not owned),
// `std::string` (owned).
//
// The array must not be destroyed until the `ArrayBackwardWriter` is closed or
// no longer used.
template <typename Dest = absl::Span<char>>
class ArrayBackwardWriter : public ArrayBackwardWriterBase {
 public:
  // Creates a closed `ArrayBackwardWriter`.
  ArrayBackwardWriter() noexcept : ArrayBackwardWriterBase(kInitiallyClosed) {}

  // Will write to the array provided by `dest`.
  explicit ArrayBackwardWriter(const Dest& dest);
  explicit ArrayBackwardWriter(Dest&& dest);

  ArrayBackwardWriter(ArrayBackwardWriter&& that) noexcept;
  ArrayBackwardWriter& operator=(ArrayBackwardWriter&& that) noexcept;

  // Will write to the array provided by a `Dest` constructed from elements of
  // `dest_args`. This avoids constructing a temporary `Dest` and moving from
  // it.
  template <typename... DestArgs>
  explicit ArrayBackwardWriter(std::tuple<DestArgs...> dest_args);

  // Makes `*this` equivalent to a newly constructed `ArrayBackwardWriter`. This
  // avoids constructing a temporary `ArrayBackwardWriter` and moving from it.
  void Reset();
  void Reset(const Dest& dest);
  void Reset(Dest&& dest);
  template <typename... DestArgs>
  void Reset(std::tuple<DestArgs...> dest_args);

  // Returns the object providing and possibly owning the array being written
  // to. Unchanged by `Close()`.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  absl::Span<char> dest_span() override { return dest_.get(); }
  absl::Span<const char> dest_span() const override { return dest_.get(); }

 private:
  void MoveDest(ArrayBackwardWriter&& that);

  // The object providing and possibly owning the array being written to.
  Dependency<absl::Span<char>, Dest> dest_;
};

// Implementation details follow.

inline ArrayBackwardWriterBase::ArrayBackwardWriterBase(
    ArrayBackwardWriterBase&& that) noexcept
    : PushableBackwardWriter(std::move(that)), written_(that.written_) {}

inline ArrayBackwardWriterBase& ArrayBackwardWriterBase::operator=(
    ArrayBackwardWriterBase&& that) noexcept {
  PushableBackwardWriter::operator=(std::move(that));
  written_ = that.written_;
  return *this;
}

inline void ArrayBackwardWriterBase::Reset(InitiallyClosed) {
  PushableBackwardWriter::Reset(kInitiallyClosed);
  written_ = absl::Span<char>();
}

inline void ArrayBackwardWriterBase::Reset(InitiallyOpen) {
  PushableBackwardWriter::Reset(kInitiallyOpen);
  written_ = absl::Span<char>();
}

inline void ArrayBackwardWriterBase::Initialize(absl::Span<char> dest) {
  set_buffer(dest.data(), dest.size());
}

inline void ArrayBackwardWriterBase::Done() {
  if (ABSL_PREDICT_TRUE(healthy())) {
    if (ABSL_PREDICT_TRUE(SyncScratch())) {
      written_ = absl::Span<char>(cursor(), written_to_buffer());
    }
  }
  PushableBackwardWriter::Done();
}

template <typename Dest>
inline ArrayBackwardWriter<Dest>::ArrayBackwardWriter(const Dest& dest)
    : ArrayBackwardWriterBase(kInitiallyOpen), dest_(dest) {
  Initialize(dest_.get());
}

template <typename Dest>
inline ArrayBackwardWriter<Dest>::ArrayBackwardWriter(Dest&& dest)
    : ArrayBackwardWriterBase(kInitiallyOpen), dest_(std::move(dest)) {
  Initialize(dest_.get());
}

template <typename Dest>
template <typename... DestArgs>
inline ArrayBackwardWriter<Dest>::ArrayBackwardWriter(
    std::tuple<DestArgs...> dest_args)
    : ArrayBackwardWriterBase(kInitiallyOpen), dest_(std::move(dest_args)) {
  Initialize(dest_.get());
}

template <typename Dest>
inline ArrayBackwardWriter<Dest>::ArrayBackwardWriter(
    ArrayBackwardWriter&& that) noexcept
    : ArrayBackwardWriterBase(std::move(that)) {
  MoveDest(std::move(that));
}

template <typename Dest>
inline ArrayBackwardWriter<Dest>& ArrayBackwardWriter<Dest>::operator=(
    ArrayBackwardWriter&& that) noexcept {
  ArrayBackwardWriterBase::operator=(std::move(that));
  MoveDest(std::move(that));
  return *this;
}

template <typename Dest>
inline void ArrayBackwardWriter<Dest>::Reset() {
  ArrayBackwardWriterBase::Reset(kInitiallyClosed);
  dest_.Reset();
}

template <typename Dest>
inline void ArrayBackwardWriter<Dest>::Reset(const Dest& dest) {
  ArrayBackwardWriterBase::Reset(kInitiallyOpen);
  dest_.Reset(dest);
  Initialize(dest_.get());
}

template <typename Dest>
inline void ArrayBackwardWriter<Dest>::Reset(Dest&& dest) {
  ArrayBackwardWriterBase::Reset(kInitiallyOpen);
  dest_.Reset(std::move(dest));
  Initialize(dest_.get());
}

template <typename Dest>
template <typename... DestArgs>
inline void ArrayBackwardWriter<Dest>::Reset(
    std::tuple<DestArgs...> dest_args) {
  ArrayBackwardWriterBase::Reset(kInitiallyOpen);
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get());
}

template <typename Dest>
inline void ArrayBackwardWriter<Dest>::MoveDest(ArrayBackwardWriter&& that) {
  if (dest_.kIsStable()) {
    dest_ = std::move(that.dest_);
  } else {
    BehindScratch behind_scratch(this);
    const size_t cursor_index = written_to_buffer();
    const size_t written_size = written_.size();
    dest_ = std::move(that.dest_);
    if (start() != nullptr) {
      set_buffer(dest_.get().data(), dest_.get().size(), cursor_index);
    }
    if (written_.data() != nullptr) {
      written_ = absl::Span<char>(
          dest_.get().data() + dest_.get().size() - written_size, written_size);
    }
  }
}

template <typename Dest>
struct Resetter<ArrayBackwardWriter<Dest>>
    : ResetterByReset<ArrayBackwardWriter<Dest>> {};

}  // namespace riegeli

#endif  // RIEGELI_BYTES_ARRAY_BACKWARD_WRITER_H_
