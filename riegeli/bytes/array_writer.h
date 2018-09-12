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

#ifndef RIEGELI_BYTES_ARRAY_WRITER_H_
#define RIEGELI_BYTES_ARRAY_WRITER_H_

#include <stddef.h>
#include <string>
#include <utility>

#include "absl/types/span.h"
#include "riegeli/base/base.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/span_dependency.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Template parameter invariant part of ArrayWriter.
class ArrayWriterBase : public Writer {
 public:
  // Returns the array being written to. Unchanged by Close().
  virtual absl::Span<char> dest_span() = 0;
  virtual absl::Span<const char> dest_span() const = 0;

  // Returns written data in a prefix of the original array. Valid only after
  // Close() or Flush().
  absl::Span<char> written() { return written_; }
  absl::Span<const char> written() const { return written_; }

  bool Flush(FlushType flush_type) override;
  bool SupportsTruncate() const override { return true; }
  bool Truncate(Position new_size) override;

 protected:
  explicit ArrayWriterBase(State state) noexcept : Writer(state) {}

  ArrayWriterBase(ArrayWriterBase&& src) noexcept;
  ArrayWriterBase& operator=(ArrayWriterBase&& src) noexcept;

  void Done() override;
  bool PushSlow() override;

  // Written data. Valid only after Close() or Flush().
  absl::Span<char> written_;
};

// A Writer which writes to a preallocated array with a known size limit.
//
// The Dest template parameter specifies the type of the object providing and
// possibly owning the array being written to. Dest must support
// Dependency<Span<char>, Src>, e.g. Span<char> (not owned, default),
// string* (not owned), string (owned).
//
// The array must not be destroyed until the ArrayWriter is closed or no longer
// used.
template <typename Dest = absl::Span<char>>
class ArrayWriter : public ArrayWriterBase {
 public:
  // Creates a closed ArrayWriter.
  ArrayWriter() noexcept : ArrayWriterBase(State::kClosed) {}

  // Will write to the array provided by dest.
  explicit ArrayWriter(Dest dest);

  ArrayWriter(ArrayWriter&& src) noexcept;
  ArrayWriter& operator=(ArrayWriter&& src) noexcept;

  // Returns the object providing and possibly owning the array being written
  // to. Unchanged by Close().
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  absl::Span<char> dest_span() override { return dest_.ptr(); }
  absl::Span<const char> dest_span() const override { return dest_.ptr(); }

 private:
  void MoveDest(ArrayWriter&& src);

  // The object providing and possibly owning the array being written to.
  Dependency<absl::Span<char>, Dest> dest_;

  // Invariant: if healthy() then start_pos_ == 0
};

// Implementation details follow.

inline ArrayWriterBase::ArrayWriterBase(ArrayWriterBase&& src) noexcept
    : Writer(std::move(src)),
      written_(riegeli::exchange(src.written_, absl::Span<char>())) {}

inline ArrayWriterBase& ArrayWriterBase::operator=(
    ArrayWriterBase&& src) noexcept {
  Writer::operator=(std::move(src));
  written_ = riegeli::exchange(src.written_, absl::Span<char>());
  return *this;
}

template <typename Dest>
ArrayWriter<Dest>::ArrayWriter(Dest dest)
    : ArrayWriterBase(State::kOpen), dest_(std::move(dest)) {
  start_ = dest_.ptr().data();
  cursor_ = start_;
  limit_ = start_ + dest_.ptr().size();
}

template <typename Dest>
ArrayWriter<Dest>::ArrayWriter(ArrayWriter&& src) noexcept
    : ArrayWriterBase(std::move(src)) {
  MoveDest(std::move(src));
}

template <typename Dest>
ArrayWriter<Dest>& ArrayWriter<Dest>::operator=(ArrayWriter&& src) noexcept {
  ArrayWriterBase::operator=(std::move(src));
  MoveDest(std::move(src));
  return *this;
}

template <typename Dest>
void ArrayWriter<Dest>::MoveDest(ArrayWriter&& src) {
  if (dest_.kIsStable()) {
    dest_ = std::move(src.dest_);
  } else {
    const size_t cursor_index = written_to_buffer();
    const size_t written_size = written_.size();
    dest_ = std::move(src.dest_);
    if (start_ != nullptr) {
      start_ = dest_.ptr().data();
      cursor_ = start_ + cursor_index;
      limit_ = start_ + dest_.ptr().size();
    }
    if (written_.data() != nullptr) {
      written_ = absl::Span<char>(dest_.ptr().data(), written_size);
    }
  }
}

extern template class ArrayWriter<absl::Span<char>>;
extern template class ArrayWriter<std::string*>;
extern template class ArrayWriter<std::string>;

}  // namespace riegeli

#endif  // RIEGELI_BYTES_ARRAY_WRITER_H_
