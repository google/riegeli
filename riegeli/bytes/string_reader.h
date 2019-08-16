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

#ifndef RIEGELI_BYTES_STRING_READER_H_
#define RIEGELI_BYTES_STRING_READER_H_

#include <stddef.h>

#include <tuple>
#include <utility>

#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/resetter.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/string_view_dependency.h"  // IWYU pragma: keep

namespace riegeli {

// Template parameter invariant part of StringReader.
class StringReaderBase : public Reader {
 public:
  // Returns the string or array being read from. Unchanged by Close().
  virtual absl::string_view src_string_view() const = 0;

  bool SupportsRandomAccess() const override { return true; }
  bool Size(Position* size) override;

 protected:
  explicit StringReaderBase(InitiallyClosed) noexcept
      : Reader(kInitiallyClosed) {}
  explicit StringReaderBase(InitiallyOpen) noexcept : Reader(kInitiallyOpen) {}

  StringReaderBase(StringReaderBase&& that) noexcept;
  StringReaderBase& operator=(StringReaderBase&& that) noexcept;

  void Initialize(absl::string_view src);

  bool PullSlow(size_t min_length, size_t recommended_length) override;
  bool SeekSlow(Position new_pos) override;
};

// A Reader which reads from a string. It supports random access.
//
// The Src template parameter specifies the type of the object providing and
// possibly owning the string or array being read from. Src must support
// Dependency<string_view, Src>, e.g. string_view (not owned, default),
// const string* (not owned), string (owned).
//
// It might be better to use ChainReader<Chain> instead of StringReader<string>
// to allow sharing the data (Chain blocks are reference counted, string data
// have a single owner).
//
// The string or array must not be changed until the StringReader is closed or
// no longer used.
template <typename Src = absl::string_view>
class StringReader : public StringReaderBase {
 public:
  // Creates a closed StringReader.
  StringReader() noexcept : StringReaderBase(kInitiallyClosed) {}

  // Will read from the string or array provided by src.
  explicit StringReader(const Src& src);
  explicit StringReader(Src&& src);

  // Will read from the string or array provided by a Src constructed from
  // elements of src_args. This avoids constructing a temporary Src and moving
  // from it.
  template <typename... SrcArgs>
  explicit StringReader(std::tuple<SrcArgs...> src_args);

  StringReader(StringReader&& that) noexcept;
  StringReader& operator=(StringReader&& that) noexcept;

  // Makes *this equivalent to a newly constructed StringReader. This avoids
  // constructing a temporary StringReader and moving from it.
  void Reset();
  void Reset(const Src& src);
  void Reset(Src&& src);
  template <typename... SrcArgs>
  void Reset(std::tuple<SrcArgs...> src_args);

  // Returns the object providing and possibly owning the string or array being
  // read from. Unchanged by Close().
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  absl::string_view src_string_view() const override { return src_.get(); }

 private:
  void MoveSrc(StringReader&& that);

  // The object providing and possibly owning the string or array being read
  // from.
  Dependency<absl::string_view, Src> src_;
};

// Implementation details follow.

inline StringReaderBase::StringReaderBase(StringReaderBase&& that) noexcept
    : Reader(std::move(that)) {}

inline StringReaderBase& StringReaderBase::operator=(
    StringReaderBase&& that) noexcept {
  Reader::operator=(std::move(that));
  return *this;
}

inline void StringReaderBase::Initialize(absl::string_view src) {
  start_ = src.data();
  cursor_ = start_;
  limit_ = start_ + src.size();
  limit_pos_ = src.size();
}

template <typename Src>
inline StringReader<Src>::StringReader(const Src& src)
    : StringReaderBase(kInitiallyOpen), src_(src) {
  Initialize(src_.get());
}

template <typename Src>
inline StringReader<Src>::StringReader(Src&& src)
    : StringReaderBase(kInitiallyOpen), src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline StringReader<Src>::StringReader(std::tuple<SrcArgs...> src_args)
    : StringReaderBase(kInitiallyOpen), src_(std::move(src_args)) {
  Initialize(src_.get());
}

template <typename Src>
inline StringReader<Src>::StringReader(StringReader&& that) noexcept
    : StringReaderBase(std::move(that)) {
  MoveSrc(std::move(that));
}

template <typename Src>
inline StringReader<Src>& StringReader<Src>::operator=(
    StringReader&& that) noexcept {
  StringReaderBase::operator=(std::move(that));
  MoveSrc(std::move(that));
  return *this;
}

template <typename Src>
inline void StringReader<Src>::Reset() {
  StringReaderBase::Reset(kInitiallyClosed);
  src_.Reset();
}

template <typename Src>
inline void StringReader<Src>::Reset(const Src& src) {
  StringReaderBase::Reset(kInitiallyOpen);
  src_.Reset(src);
  Initialize(src_.get());
}

template <typename Src>
inline void StringReader<Src>::Reset(Src&& src) {
  StringReaderBase::Reset(kInitiallyOpen);
  src_.Reset(std::move(src));
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline void StringReader<Src>::Reset(std::tuple<SrcArgs...> src_args) {
  StringReaderBase::Reset(kInitiallyOpen);
  src_.Reset(std::move(src_args));
  Initialize(src_.get());
}

template <typename Src>
inline void StringReader<Src>::MoveSrc(StringReader&& that) {
  if (src_.kIsStable()) {
    src_ = std::move(that.src_);
  } else {
    const size_t cursor_index = read_from_buffer();
    src_ = std::move(that.src_);
    if (start_ != nullptr) {
      start_ = src_.get().data();
      cursor_ = start_ + cursor_index;
      limit_ = start_ + src_.get().size();
    }
  }
}

template <typename Src>
struct Resetter<StringReader<Src>> : ResetterByReset<StringReader<Src>> {};

}  // namespace riegeli

#endif  // RIEGELI_BYTES_STRING_READER_H_
