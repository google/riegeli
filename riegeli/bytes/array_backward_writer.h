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

#include <string>
#include <utility>

#include "absl/types/span.h"
#include "absl/utility/utility.h"
#include "riegeli/base/base.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/span_dependency.h"

namespace riegeli {

// Template parameter invariant part of ArrayBackwardWriter.
class ArrayBackwardWriterBase : public BackwardWriter {
 public:
  // Returns the array being written to. Unchanged by Close().
  virtual absl::Span<char> dest_span() = 0;
  virtual absl::Span<const char> dest_span() const = 0;

  // Returns written data in a suffix of the original array. Valid only after
  // Close() or Flush().
  absl::Span<char> written() { return written_; }
  absl::Span<const char> written() const { return written_; }

  bool Flush(FlushType flush_type) override;
  bool SupportsTruncate() const override { return true; }
  bool Truncate(Position new_size) override;

 protected:
  explicit ArrayBackwardWriterBase(State state) noexcept
      : BackwardWriter(state) {}

  ArrayBackwardWriterBase(ArrayBackwardWriterBase&& that) noexcept;
  ArrayBackwardWriterBase& operator=(ArrayBackwardWriterBase&& that) noexcept;

  void Done() override;
  bool PushSlow(size_t min_length, size_t recommended_length) override;

  // Written data. Valid only after Close() or Flush().
  absl::Span<char> written_;

  // Invariant: if healthy() then start_pos_ == 0
};

// A BackwardWriter which writes to a preallocated array with a known size
// limit.
//
// The Dest template parameter specifies the type of the object providing and
// possibly owning the array being written to. Dest must support
// Dependency<Span<char>, Src>, e.g. Span<char> (not owned, default),
// string* (not owned), string (owned).
//
// The array must not be destroyed until the ArrayBackwardWriter is closed or no
// longer used.
template <typename Dest = absl::Span<char>>
class ArrayBackwardWriter : public ArrayBackwardWriterBase {
 public:
  // Creates a closed ArrayBackwardWriter.
  ArrayBackwardWriter() noexcept : ArrayBackwardWriterBase(State::kClosed) {}

  // Will write to the array provided by dest.
  explicit ArrayBackwardWriter(Dest dest);

  ArrayBackwardWriter(ArrayBackwardWriter&& that) noexcept;
  ArrayBackwardWriter& operator=(ArrayBackwardWriter&& that) noexcept;

  // Returns the object providing and possibly owning the array being written
  // to. Unchanged by Close().
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
    : BackwardWriter(std::move(that)),
      written_(absl::exchange(that.written_, absl::Span<char>())) {}

inline ArrayBackwardWriterBase& ArrayBackwardWriterBase::operator=(
    ArrayBackwardWriterBase&& that) noexcept {
  BackwardWriter::operator=(std::move(that));
  written_ = absl::exchange(that.written_, absl::Span<char>());
  return *this;
}

template <typename Dest>
inline ArrayBackwardWriter<Dest>::ArrayBackwardWriter(Dest dest)
    : ArrayBackwardWriterBase(State::kOpen), dest_(std::move(dest)) {
  limit_ = dest_.get().data();
  start_ = limit_ + dest_.get().size();
  cursor_ = start_;
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
inline void ArrayBackwardWriter<Dest>::MoveDest(ArrayBackwardWriter&& that) {
  if (dest_.kIsStable()) {
    dest_ = std::move(that.dest_);
  } else {
    const size_t cursor_index = written_to_buffer();
    const size_t written_size = written_.size();
    dest_ = std::move(that.dest_);
    if (start_ != nullptr) {
      limit_ = dest_.get().data();
      start_ = limit_ + dest_.get().size();
      cursor_ = start_ - cursor_index;
    }
    if (written_.data() != nullptr) {
      written_ = absl::Span<char>(
          dest_.get().data() + dest_.get().size() - written_size, written_size);
    }
  }
}

extern template class ArrayBackwardWriter<absl::Span<char>>;
extern template class ArrayBackwardWriter<std::string*>;
extern template class ArrayBackwardWriter<std::string>;

}  // namespace riegeli

#endif  // RIEGELI_BYTES_ARRAY_BACKWARD_WRITER_H_
