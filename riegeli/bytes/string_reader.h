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

#include <string>
#include <utility>

#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/string_view_dependency.h"

namespace riegeli {

// Template parameter invariant part of StringReader.
class StringReaderBase : public Reader {
 public:
  // Returns the string or array being read from. Unchanged by Close().
  virtual absl::string_view src_string_view() const = 0;

  bool SupportsRandomAccess() const override { return true; }
  bool Size(Position* size) override;

 protected:
  explicit StringReaderBase(State state) noexcept : Reader(state) {}

  StringReaderBase(StringReaderBase&& that) noexcept;
  StringReaderBase& operator=(StringReaderBase&& that) noexcept;

  bool PullSlow() override;
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
  StringReader() noexcept : StringReaderBase(State::kClosed) {}

  // Will read from the string or array provided by src.
  explicit StringReader(Src src);

  StringReader(StringReader&& that) noexcept;
  StringReader& operator=(StringReader&& that) noexcept;

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

template <typename Src>
inline StringReader<Src>::StringReader(Src src)
    : StringReaderBase(State::kOpen), src_(std::move(src)) {
  start_ = src_.get().data();
  cursor_ = start_;
  limit_ = start_ + src_.get().size();
  limit_pos_ = src_.get().size();
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

extern template class StringReader<absl::string_view>;
extern template class StringReader<const std::string*>;
extern template class StringReader<std::string>;

}  // namespace riegeli

#endif  // RIEGELI_BYTES_STRING_READER_H_
