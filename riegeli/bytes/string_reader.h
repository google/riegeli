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

#include <memory>
#include <optional>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/moving_dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

// Template parameter independent part of `StringReader`.
class StringReaderBase : public Reader {
 public:
  // Returns the `std::string` or array being read from. Unchanged by `Close()`.
  virtual absl::string_view SrcStringView() const
      ABSL_ATTRIBUTE_LIFETIME_BOUND = 0;

  bool ToleratesReadingAhead() override { return true; }
  bool SupportsRandomAccess() override { return true; }
  bool SupportsNewReader() override { return true; }

 protected:
  using Reader::Reader;

  StringReaderBase(StringReaderBase&& that) noexcept;
  StringReaderBase& operator=(StringReaderBase&& that) noexcept;

  void Initialize(absl::string_view src);

  bool PullSlow(size_t min_length, size_t recommended_length) override;
  bool SeekSlow(Position new_pos) override;
  std::optional<Position> SizeImpl() override;
  std::unique_ptr<Reader> NewReaderImpl(Position initial_pos) override;

  // Invariants if `is_open()`:
  //   `start() == SrcStringView().data()`
  //   `start_to_limit() == SrcStringView().size()`
  //   `start_pos() == 0`
};

// A `Reader` which reads from a `std::string` or array.
//
// It supports random access and `NewReader()`.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the `std::string` or array being read from. `Src` must
// support `Dependency<absl::string_view, Src>`, e.g.
// `absl::string_view` (not owned, default), `const std::string*` (not owned),
// `std::string` (owned), `Any<absl::string_view>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as
// `absl::string_view` if there are no constructor arguments or if the first
// constructor argument is an lvalue reference to a type convertible to
// `absl::string_view` (to avoid unintended string copying) or to `const char*`
// (to compute `std::strlen()` early), otherwise as `TargetT` of the type of the
// first constructor argument.
//
// It might be better to use `ChainReader<Chain>` instead of
// `StringReader<std::string>` to allow sharing the data (`Chain` blocks are
// reference counted, `std::string` data have a single owner).
//
// The `std::string` or array must not be changed until the `StringReader` is
// closed or no longer used.
template <typename Src = absl::string_view>
class StringReader : public StringReaderBase {
 public:
  // Creates a closed `StringReader`.
  explicit StringReader(Closed) noexcept : StringReaderBase(kClosed) {}

  // Will read from the `std::string` or array provided by `src`.
  explicit StringReader(Initializer<Src> src);

  // Will read from an empty `absl::string_view`. This constructor is present
  // only if `Src` is `absl::string_view`.
  template <typename DependentSrc = Src,
            std::enable_if_t<std::is_same_v<DependentSrc, absl::string_view>,
                             int> = 0>
  StringReader();

  // Will read from `absl::string_view(src, size)`. This constructor is present
  // only if `Src` is `absl::string_view`.
  template <typename DependentSrc = Src,
            std::enable_if_t<std::is_same_v<DependentSrc, absl::string_view>,
                             int> = 0>
  explicit StringReader(const char* src ABSL_ATTRIBUTE_LIFETIME_BOUND,
                        size_t size);

  StringReader(StringReader&& that) = default;
  StringReader& operator=(StringReader&& that) = default;

  // Makes `*this` equivalent to a newly constructed `StringReader`. This avoids
  // constructing a temporary `StringReader` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Src> src);
  template <typename DependentSrc = Src,
            std::enable_if_t<std::is_same_v<DependentSrc, absl::string_view>,
                             int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset();
  template <typename DependentSrc = Src,
            std::enable_if_t<std::is_same_v<DependentSrc, absl::string_view>,
                             int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(const char* src, size_t size);

  // Returns the object providing and possibly owning the `std::string` or array
  // being read from. Unchanged by `Close()`.
  Src& src() ABSL_ATTRIBUTE_LIFETIME_BOUND { return src_.manager(); }
  const Src& src() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return src_.manager();
  }
  absl::string_view SrcStringView() const
      ABSL_ATTRIBUTE_LIFETIME_BOUND override {
    return src_.get();
  }

 private:
  class Mover;

  // The object providing and possibly owning the `std::string` or array being
  // read from.
  MovingDependency<absl::string_view, Src, Mover> src_;
};

explicit StringReader(Closed) -> StringReader<DeleteCtad<Closed>>;
template <typename Src>
explicit StringReader(Src&& src) -> StringReader<std::conditional_t<
    std::disjunction_v<
        std::conjunction<std::is_lvalue_reference<Src>,
                         std::is_convertible<Src, absl::string_view>>,
        std::is_convertible<Src&&, const char*>>,
    absl::string_view, TargetT<Src>>>;
StringReader() -> StringReader<>;
explicit StringReader(const char* src, size_t size) -> StringReader<>;

// Implementation details follow.

inline StringReaderBase::StringReaderBase(StringReaderBase&& that) noexcept
    : Reader(static_cast<Reader&&>(that)) {}

inline StringReaderBase& StringReaderBase::operator=(
    StringReaderBase&& that) noexcept {
  Reader::operator=(static_cast<Reader&&>(that));
  return *this;
}

inline void StringReaderBase::Initialize(absl::string_view src) {
  set_buffer(src.data(), src.size());
  move_limit_pos(available());
}

template <typename Src>
class StringReader<Src>::Mover {
 public:
  static auto member() { return &StringReader::src_; }

  explicit Mover(StringReader& self, StringReader& that)
      : uses_buffer_(self.start() != nullptr),
        start_to_cursor_(self.start_to_cursor()) {
    if (uses_buffer_) {
      RIEGELI_ASSERT_EQ(that.src_.get().data(), self.start())
          << "StringReader source changed unexpectedly";
      RIEGELI_ASSERT_EQ(that.src_.get().size(), self.start_to_limit())
          << "StringReader source changed unexpectedly";
    }
  }

  void Done(StringReader& self) {
    if (uses_buffer_) {
      const absl::string_view src = self.src_.get();
      self.set_buffer(src.data(), src.size(), start_to_cursor_);
    }
  }

 private:
  bool uses_buffer_;
  size_t start_to_cursor_;
};

template <typename Src>
inline StringReader<Src>::StringReader(Initializer<Src> src)
    : src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
template <
    typename DependentSrc,
    std::enable_if_t<std::is_same_v<DependentSrc, absl::string_view>, int>>
inline StringReader<Src>::StringReader() : StringReader(absl::string_view()) {}

template <typename Src>
template <
    typename DependentSrc,
    std::enable_if_t<std::is_same_v<DependentSrc, absl::string_view>, int>>
inline StringReader<Src>::StringReader(
    const char* src ABSL_ATTRIBUTE_LIFETIME_BOUND, size_t size)
    : StringReader(absl::string_view(src, size)) {}

template <typename Src>
inline void StringReader<Src>::Reset(Closed) {
  StringReaderBase::Reset(kClosed);
  src_.Reset();
}

template <typename Src>
inline void StringReader<Src>::Reset(Initializer<Src> src) {
  StringReaderBase::Reset();
  src_.Reset(std::move(src));
  Initialize(src_.get());
}

template <typename Src>
template <
    typename DependentSrc,
    std::enable_if_t<std::is_same_v<DependentSrc, absl::string_view>, int>>
inline void StringReader<Src>::Reset() {
  Reset(absl::string_view());
}

template <typename Src>
template <
    typename DependentSrc,
    std::enable_if_t<std::is_same_v<DependentSrc, absl::string_view>, int>>
inline void StringReader<Src>::Reset(const char* src, size_t size) {
  Reset(absl::string_view(src, size));
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_STRING_READER_H_
