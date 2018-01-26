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

#ifndef RIEGELI_BASE_STRING_VIEW_H_
#define RIEGELI_BASE_STRING_VIEW_H_

#undef RIEGELI_INTERNAL_HAS_STD_STRING_VIEW
#ifdef __has_include
// Check __cplusplus instead of __cpp_lib_string_view because GCC 7.1 and 7.2
// fail when <string_view> is included but -std does not enable C++17, and they
// do not define __cpp_lib_string_view.
#if __has_include(<string_view>) && __cplusplus >= 201703
#define RIEGELI_INTERNAL_HAS_STD_STRING_VIEW 1
#endif
#endif

#if RIEGELI_INTERNAL_HAS_STD_STRING_VIEW

#include <string_view>

namespace riegeli {
using std::string_view;
}  // namespace riegeli

#else  // !RIEGELI_INTERNAL_HAS_STD_STRING_VIEW

#include <stddef.h>
#include <cstring>
#include <iosfwd>
#include <iterator>
#include <string>

#include "riegeli/base/assert.h"
#include "riegeli/base/base.h"

namespace riegeli {

// A subset of std::string_view from C++17.
//
// constexpr is skipped where it would be impractical to achieve before C++14.
//
// Skipped functions: at, find_first_of, find_last_of, find_first_not_of,
// find_last_not_of.
class string_view {
 public:
  using traits_type = std::char_traits<char>;
  using value_type = char;
  using pointer = char*;
  using const_pointer = const char*;
  using reference = char&;
  using const_reference = const char&;
  using const_iterator = const char*;
  using iterator = const_iterator;
  using const_reverse_iterator = std::reverse_iterator<const_iterator>;
  using reverse_iterator = const_reverse_iterator;
  using size_type = size_t;
  using difference_type = ptrdiff_t;

  static constexpr size_type npos = static_cast<size_type>(-1);

  constexpr string_view() noexcept = default;
  string_view(const char* src) noexcept
      : data_(src), size_(traits_type::length(src)) {}
  constexpr string_view(const char* data, size_type size) noexcept
      : data_(data), size_(size) {}
  string_view(const std::string& src) noexcept
      : data_(src.data()), size_(src.size()) {}

  constexpr string_view(const string_view& src) noexcept = default;
  string_view& operator=(const string_view& src) noexcept = default;

  explicit operator std::string() const {
    if (empty()) return std::string();  // std::string(nullptr, 0) is undefined.
    return std::string(data_, size_);
  }

  constexpr const_iterator begin() const noexcept { return data_; }
  constexpr const_iterator end() const noexcept { return data_ + size_; }
  constexpr const_iterator cbegin() const noexcept { return begin(); }
  constexpr const_iterator cend() const noexcept { return end(); }
  const_reverse_iterator rbegin() const noexcept {
    return const_reverse_iterator(end());
  }
  const_reverse_iterator rend() const noexcept {
    return const_reverse_iterator(begin());
  }
  const_reverse_iterator crbegin() const noexcept { return rbegin(); }
  const_reverse_iterator crend() const noexcept { return rend(); }

  constexpr size_type size() const noexcept { return size_; }
  constexpr size_type length() const noexcept { return size_; }
  constexpr size_type max_size() const noexcept { return npos / 2; }
  constexpr bool empty() const noexcept { return size_ == 0; }

  const_reference operator[](size_type pos) const {
    RIEGELI_ASSERT_LT(pos, size_);
    return data_[pos];
  }
  const_reference front() const {
    RIEGELI_ASSERT(!empty());
    return data_[0];
  }
  const_reference back() const {
    RIEGELI_ASSERT(!empty());
    return data_[size_ - 1];
  }
  constexpr const_pointer data() const noexcept { return data_; }

  void remove_prefix(size_type length) {
    RIEGELI_ASSERT_LE(length, size_);
    data_ += length;
    size_ -= length;
  }
  void remove_suffix(size_type length) {
    RIEGELI_ASSERT_LE(length, size_);
    size_ -= length;
  }
  void swap(string_view& that) noexcept {
    const string_view tmp = *this;
    *this = that;
    that = tmp;
  }

  size_type copy(char* dest, size_type length, size_type pos = 0) const {
    // Assert instead of throwing an exception.
    RIEGELI_ASSERT_LE(pos, size_);
    length = UnsignedMin(length, size_ - pos);
    if (length != 0) std::memcpy(dest, data_ + pos, length);
    return length;
  }

  string_view substr(size_type pos = 0, size_type length = npos) const {
    // Assert instead of throwing an exception.
    RIEGELI_ASSERT_LE(pos, size_);
    return string_view(data_ + pos, UnsignedMin(length, size_ - pos));
  }

  int compare(string_view that) const noexcept {
    const size_type length = UnsignedMin(size(), that.size());
    if (length > 0) {
      const int result = std::memcmp(data(), that.data(), length);
      if (result != 0) return result;
    }
    if (size() < that.size()) return -1;
    if (size() > that.size()) return 1;
    return 0;
  }
  int compare(size_type pos1, size_type length1, string_view that) const {
    return substr(pos1, length1).compare(that);
  }
  int compare(size_type pos1, size_type length1, string_view that,
              size_type pos2, size_type length2) const {
    return substr(pos1, length1).compare(that.substr(pos2, length2));
  }
  int compare(const char* that) const { return compare(string_view(that)); }
  int compare(size_type pos1, size_type length1, const char* that) const {
    return substr(pos1, length1).compare(string_view(that));
  }
  int compare(size_type pos1, size_type length1, const char* that,
              size_type size2) const {
    return substr(pos1, length1).compare(string_view(that, size2));
  }

  size_type find(string_view needle, size_type pos = 0) const noexcept;
  size_type find(char needle, size_type pos = 0) const noexcept;
  size_type find(const char* needle, size_type pos, size_type size) const {
    return find(string_view(needle, size), pos);
  }
  size_type find(const char* needle, size_type pos = 0) const {
    return find(string_view(needle), pos);
  }

  size_type rfind(string_view needle, size_type pos = npos) const noexcept;
  size_type rfind(char needle, size_type pos = npos) const noexcept;
  size_type rfind(const char* needle, size_type pos, size_type size) const {
    return rfind(string_view(needle, size), pos);
  }
  size_type rfind(const char* needle, size_type pos = npos) const {
    return rfind(string_view(needle), pos);
  }

 private:
  const_pointer data_ = nullptr;
  size_type size_ = 0;
};

inline bool operator==(string_view a, string_view b) noexcept {
  return a.size() == b.size() &&
         (a.data() == b.data() || a.empty() ||
          std::memcmp(a.data(), b.data(), a.size()) == 0);
}

inline bool operator!=(string_view a, string_view b) noexcept {
  return !(a == b);
}

inline bool operator<(string_view a, string_view b) noexcept {
  const string_view::size_type length = UnsignedMin(a.size(), b.size());
  if (length > 0) {
    const int result = std::memcmp(a.data(), b.data(), length);
    if (result < 0) return true;
    if (result > 0) return false;
  }
  return a.size() < b.size();
}

inline bool operator>(string_view a, string_view b) noexcept { return b < a; }

inline bool operator<=(string_view a, string_view b) noexcept {
  return !(b < a);
}

inline bool operator>=(string_view a, string_view b) noexcept {
  return !(a < b);
}

std::ostream& operator<<(std::ostream& out, string_view str);

}  // namespace riegeli

#endif  // !RIEGELI_INTERNAL_HAS_STD_STRING_VIEW

#endif  // RIEGELI_BASE_STRING_VIEW_H_
