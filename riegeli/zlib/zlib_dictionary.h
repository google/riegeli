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

// IWYU pragma: private, include "riegeli/zlib/zlib_reader.h"
// IWYU pragma: private, include "riegeli/zlib/zlib_writer.h"

#include <string>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/shared_ptr.h"

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
  ZlibDictionary& Reset() & ABSL_ATTRIBUTE_LIFETIME_BOUND;
  ZlibDictionary&& Reset() && ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return std::move(Reset());
  }

  // Sets a dictionary (data which should contain sequences that are commonly
  // seen in the data being compressed).
  ZlibDictionary& set_data(Initializer<std::string>::AllowingExplicit data) &
      ABSL_ATTRIBUTE_LIFETIME_BOUND;
  ZlibDictionary&& set_data(Initializer<std::string>::AllowingExplicit data) &&
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return std::move(set_data(std::move(data)));
  }

  // Like `set_data()`, but does not take ownership of `data`, which must not be
  // changed until the last `ZlibReader` and `ZlibWriter` using this dictionary
  // is closed or no longer used.
  ZlibDictionary& set_data_unowned(
      absl::string_view data ABSL_ATTRIBUTE_LIFETIME_BOUND) &
      ABSL_ATTRIBUTE_LIFETIME_BOUND;
  ZlibDictionary&& set_data_unowned(
      absl::string_view data ABSL_ATTRIBUTE_LIFETIME_BOUND) &&
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return std::move(set_data_unowned(data));
  }

  // Returns `true` if no dictionary is present.
  bool empty() const { return data_.empty(); }

  // Returns the dictionary data.
  absl::string_view data() const ABSL_ATTRIBUTE_LIFETIME_BOUND { return data_; }

 private:
  SharedPtr<const std::string> owned_data_;
  absl::string_view data_;
};

// Implementation details follow.

inline ZlibDictionary& ZlibDictionary::Reset() & ABSL_ATTRIBUTE_LIFETIME_BOUND {
  owned_data_.Reset();
  data_ = absl::string_view();
  return *this;
}

inline ZlibDictionary& ZlibDictionary::set_data(
    Initializer<std::string>::AllowingExplicit data) &
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  owned_data_.Reset(std::move(data));
  data_ = owned_data_->data();
  return *this;
}

inline ZlibDictionary& ZlibDictionary::set_data_unowned(
    absl::string_view data ABSL_ATTRIBUTE_LIFETIME_BOUND) &
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  owned_data_.Reset();
  data_ = data;
  return *this;
}

}  // namespace riegeli

#endif  // RIEGELI_ZLIB_ZLIB_DICTIONARY_H_
