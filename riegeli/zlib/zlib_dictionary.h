// Copyright 2020 Google LLC
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

#ifndef RIEGELI_ZLIB_ZLIB_DICTIONARY_H_
#define RIEGELI_ZLIB_ZLIB_DICTIONARY_H_

#include <string>
#include <type_traits>
#include <utility>

#include "absl/strings/string_view.h"
#include "riegeli/base/intrusive_ref_count.h"

namespace riegeli {

// Stores an optional Zlib dictionary for compression and decompression.
//
// An empty dictionary is equivalent to having no dictionary.
//
// A `ZlibDictionary` object can own the dictionary data, or can hold a pointer
// to unowned dictionary data which must not be changed until the last
// `ZlibReader` and `ZlibWriter` using this dictionary is closed or no longer
// used. If the same dictionary is needed for multiple compression or
// decompression sessions, the `ZlibDictionary` object can be reused.
//
// Copying a `ZlibDictionary` object is cheap, sharing the actual dictionary.
class ZlibDictionary {
 public:
  // Creates an empty `ZlibDictionary`.
  ZlibDictionary() = default;

  ZlibDictionary(const ZlibDictionary& that) = default;
  ZlibDictionary& operator=(const ZlibDictionary& that) = default;

  ZlibDictionary(ZlibDictionary&& that) = default;
  ZlibDictionary& operator=(ZlibDictionary&& that) = default;

  // Resets the `ZlibDictionary` to the empty state.
  ZlibDictionary& Reset() &;
  ZlibDictionary&& Reset() && { return std::move(Reset()); }

  // Sets a dictionary (data which should contain sequences that are commonly
  // seen in the data being compressed).
  //
  // `std::string&&` is accepted with a template to avoid implicit conversions
  // to `std::string` which can be ambiguous against `absl::string_view`
  // (e.g. `const char*`).
  ZlibDictionary& set_data(absl::string_view data) &;
  ZlibDictionary&& set_data(absl::string_view data) && {
    return std::move(set_data(data));
  }
  template <typename Src,
            std::enable_if_t<std::is_same<Src, std::string>::value, int> = 0>
  ZlibDictionary& set_data(Src&& data) &;
  template <typename Src,
            std::enable_if_t<std::is_same<Src, std::string>::value, int> = 0>
  ZlibDictionary&& set_data(Src&& data) && {
    // `std::move(data)` is correct and `std::forward<Src>(data)` is not
    // necessary: `Src` is always `std::string`, never an lvalue reference.
    return std::move(set_data(std::move(data)));
  }

  // Like `set_data()`, but does not take ownership of `data`, which must not be
  // changed until the last `ZlibReader` and `ZlibWriter` using this dictionary
  // is closed or no longer used.
  ZlibDictionary& set_data_unowned(absl::string_view data) &;
  ZlibDictionary&& set_data_unowned(absl::string_view data) && {
    return std::move(set_data_unowned(data));
  }

  // Returns `true` if no dictionary is present.
  bool empty() const { return data_.empty(); }

  // Returns the dictionary data.
  absl::string_view data() const { return data_; }

 private:
  class Owned : public RefCountedBase<Owned> {
   public:
    explicit Owned(absl::string_view data) : data_(data) {}

    template <typename Src,
              std::enable_if_t<std::is_same<Src, std::string>::value, int> = 0>
    explicit Owned(Src&& data)
        // `std::move(data)` is correct and `std::forward<Src>(data)` is not
        // necessary: `Src` is always `std::string`, never an lvalue reference.
        : data_(std::move(data)) {}

    absl::string_view data() const { return data_; }

   private:
    std::string data_;
  };

  RefCountedPtr<const Owned> owned_data_;
  absl::string_view data_;
};

// Implementation details follow.

inline ZlibDictionary& ZlibDictionary::Reset() & {
  owned_data_.reset();
  data_ = absl::string_view();
  return *this;
}

inline ZlibDictionary& ZlibDictionary::set_data(absl::string_view data) & {
  owned_data_ = MakeRefCounted<const Owned>(data);
  data_ = owned_data_->data();
  return *this;
}

template <typename Src,
          std::enable_if_t<std::is_same<Src, std::string>::value, int>>
inline ZlibDictionary& ZlibDictionary::set_data(Src&& data) & {
  // `std::move(data)` is correct and `std::forward<Src>(data)` is not
  // necessary: `Src` is always `std::string`, never an lvalue reference.
  owned_data_ = MakeRefCounted<const Owned>(std::move(data));
  data_ = owned_data_->data();
  return *this;
}

inline ZlibDictionary& ZlibDictionary::set_data_unowned(
    absl::string_view data) & {
  owned_data_.reset();
  data_ = data;
  return *this;
}

}  // namespace riegeli

#endif  // RIEGELI_ZLIB_ZLIB_DICTIONARY_H_
