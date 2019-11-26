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

#include <tuple>
#include <utility>

#include "absl/types/span.h"
#include "riegeli/base/base.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/resetter.h"
#include "riegeli/bytes/pushable_writer.h"
#include "riegeli/bytes/span_dependency.h"

namespace riegeli {

// Template parameter independent part of `ArrayWriter`.
class ArrayWriterBase : public PushableWriter {
 public:
  // Returns the array being written to. Unchanged by `Close()`.
  virtual absl::Span<char> dest_span() = 0;
  virtual absl::Span<const char> dest_span() const = 0;

  // Returns written data in a prefix of the original array. Valid only after
  // `Close()` or `Flush()`.
  absl::Span<char> written() { return written_; }
  absl::Span<const char> written() const { return written_; }

  bool Flush(FlushType flush_type) override;
  bool SupportsTruncate() const override { return true; }
  bool Truncate(Position new_size) override;

 protected:
  explicit ArrayWriterBase(InitiallyClosed) noexcept
      : PushableWriter(kInitiallyClosed) {}
  explicit ArrayWriterBase(InitiallyOpen) noexcept
      : PushableWriter(kInitiallyOpen) {}

  ArrayWriterBase(ArrayWriterBase&& that) noexcept;
  ArrayWriterBase& operator=(ArrayWriterBase&& that) noexcept;

  void Reset(InitiallyClosed);
  void Reset(InitiallyOpen);
  void Initialize(absl::Span<char> dest);

  void Done() override;
  bool PushSlow(size_t min_length, size_t recommended_length) override;

  // Written data. Valid only after `Close()` or `Flush()`.
  absl::Span<char> written_;

  // Invariant: `start_pos() == 0`
};

// A `Writer` which writes to a preallocated array with a known size limit.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the array being written to. `Dest` must support
// `Dependency<absl::Span<char>, Dest>`, e.g.
// `absl::Span<char>` (not owned, default), `std::string*` (not owned),
// `std::string` (owned).
//
// The array must not be destroyed until the `ArrayWriter` is closed or no
// longer used.
template <typename Dest = absl::Span<char>>
class ArrayWriter : public ArrayWriterBase {
 public:
  // Creates a closed `ArrayWriter`.
  ArrayWriter() noexcept : ArrayWriterBase(kInitiallyClosed) {}

  // Will write to the array provided by `dest`.
  explicit ArrayWriter(const Dest& dest);
  explicit ArrayWriter(Dest&& dest);

  // Will write to the array provided by a `Dest` constructed from elements of
  // `dest_args`. This avoids constructing a temporary `Dest` and moving from
  // it.
  template <typename... DestArgs>
  explicit ArrayWriter(std::tuple<DestArgs...> dest_args);

  ArrayWriter(ArrayWriter&& that) noexcept;
  ArrayWriter& operator=(ArrayWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `ArrayWriter`. This avoids
  // constructing a temporary `ArrayWriter` and moving from it.
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
  void MoveDest(ArrayWriter&& that);

  // The object providing and possibly owning the array being written to.
  Dependency<absl::Span<char>, Dest> dest_;
};

// Implementation details follow.

inline ArrayWriterBase::ArrayWriterBase(ArrayWriterBase&& that) noexcept
    : PushableWriter(std::move(that)), written_(that.written_) {}

inline ArrayWriterBase& ArrayWriterBase::operator=(
    ArrayWriterBase&& that) noexcept {
  PushableWriter::operator=(std::move(that));
  written_ = that.written_;
  return *this;
}

inline void ArrayWriterBase::Reset(InitiallyClosed) {
  PushableWriter::Reset(kInitiallyClosed);
  written_ = absl::Span<char>();
}

inline void ArrayWriterBase::Reset(InitiallyOpen) {
  PushableWriter::Reset(kInitiallyOpen);
  written_ = absl::Span<char>();
}

inline void ArrayWriterBase::Initialize(absl::Span<char> dest) {
  set_buffer(dest.data(), dest.size());
}

template <typename Dest>
inline ArrayWriter<Dest>::ArrayWriter(const Dest& dest)
    : ArrayWriterBase(kInitiallyOpen), dest_(dest) {
  Initialize(dest_.get());
}

template <typename Dest>
inline ArrayWriter<Dest>::ArrayWriter(Dest&& dest)
    : ArrayWriterBase(kInitiallyOpen), dest_(std::move(dest)) {
  Initialize(dest_.get());
}

template <typename Dest>
template <typename... DestArgs>
inline ArrayWriter<Dest>::ArrayWriter(std::tuple<DestArgs...> dest_args)
    : ArrayWriterBase(kInitiallyOpen), dest_(std::move(dest_args)) {
  Initialize(dest_.get());
}

template <typename Dest>
inline ArrayWriter<Dest>::ArrayWriter(ArrayWriter&& that) noexcept
    : ArrayWriterBase(std::move(that)) {
  MoveDest(std::move(that));
}

template <typename Dest>
inline ArrayWriter<Dest>& ArrayWriter<Dest>::operator=(
    ArrayWriter&& that) noexcept {
  ArrayWriterBase::operator=(std::move(that));
  MoveDest(std::move(that));
  return *this;
}

template <typename Dest>
inline void ArrayWriter<Dest>::Reset() {
  ArrayWriterBase::Reset(kInitiallyClosed);
  dest_.Reset();
}

template <typename Dest>
inline void ArrayWriter<Dest>::Reset(const Dest& dest) {
  ArrayWriterBase::Reset(kInitiallyOpen);
  dest_.Reset(dest);
  Initialize(dest_.get());
}

template <typename Dest>
inline void ArrayWriter<Dest>::Reset(Dest&& dest) {
  ArrayWriterBase::Reset(kInitiallyOpen);
  dest_.Reset(std::move(dest));
  Initialize(dest_.get());
}

template <typename Dest>
template <typename... DestArgs>
inline void ArrayWriter<Dest>::Reset(std::tuple<DestArgs...> dest_args) {
  ArrayWriterBase::Reset(kInitiallyOpen);
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get());
}

template <typename Dest>
inline void ArrayWriter<Dest>::MoveDest(ArrayWriter&& that) {
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
      written_ = absl::Span<char>(dest_.get().data(), written_size);
    }
  }
}

template <typename Dest>
struct Resetter<ArrayWriter<Dest>> : ResetterByReset<ArrayWriter<Dest>> {};

}  // namespace riegeli

#endif  // RIEGELI_BYTES_ARRAY_WRITER_H_
