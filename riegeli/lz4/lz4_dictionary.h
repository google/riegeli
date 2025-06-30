// Copyright 2022 Google LLC
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

#ifndef RIEGELI_LZ4_LZ4_DICTIONARY_H_
#define RIEGELI_LZ4_LZ4_DICTIONARY_H_

// IWYU pragma: private, include "riegeli/lz4/lz4_reader.h"
// IWYU pragma: private, include "riegeli/lz4/lz4_writer.h"

#include <stdint.h>

#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/call_once.h"
#include "absl/strings/string_view.h"
#include "lz4frame.h"
#include "riegeli/base/bytes_ref.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/shared_ptr.h"

// Copied here because the definition in `lz4frame.h` is guarded with
// `LZ4F_STATIC_LINKING_ONLY` which should not leak to the header.
typedef struct LZ4F_CDict_s LZ4F_CDict;

namespace riegeli {

// Stores an optional Lz4 dictionary for compression and decompression.
//
// An empty dictionary is equivalent to having no dictionary.
//
// A `Lz4Dictionary` object can own the dictionary data, or can hold a pointer
// to unowned dictionary data which must not be changed until the last
// `Lz4Reader` or `Lz4Writer` using this dictionary is closed or no longer used.
// A `Lz4Dictionary` object also holds prepared structures derived from
// dictionary data. If the same dictionary is needed for multiple compression
// or decompression sessions, the `Lz4Dictionary` object can be reused to avoid
// preparing them again for compression.
//
// Copying a `Lz4Dictionary` object is cheap, sharing the actual dictionary.
class Lz4Dictionary {
 public:
  // Creates an empty `Lz4Dictionary`.
  Lz4Dictionary() = default;

  Lz4Dictionary(const Lz4Dictionary& that) = default;
  Lz4Dictionary& operator=(const Lz4Dictionary& that) = default;

  Lz4Dictionary(Lz4Dictionary&& that) = default;
  Lz4Dictionary& operator=(Lz4Dictionary&& that) = default;

  // Resets the `Lz4Dictionary` to the empty state.
  Lz4Dictionary& Reset() & ABSL_ATTRIBUTE_LIFETIME_BOUND;
  Lz4Dictionary&& Reset() && ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return std::move(Reset());
  }

  // Sets a dictionary.
  //
  // Dictionary id can help to detect whether the correct dictionary is used.
  // 0 means unspecified.
  Lz4Dictionary& set_data(BytesInitializer data, uint32_t dict_id = 0) &
      ABSL_ATTRIBUTE_LIFETIME_BOUND;
  Lz4Dictionary&& set_data(BytesInitializer data, uint32_t dict_id = 0) &&
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return std::move(set_data(std::move(data), dict_id));
  }

  // Like `set_data()`, but does not take ownership of `data`, which must not
  // be changed until the last `Lz4Reader` or `Lz4Writer` using this dictionary
  // is closed or no longer used.
  Lz4Dictionary& set_data_unowned(absl::string_view data
                                      ABSL_ATTRIBUTE_LIFETIME_BOUND,
                                  uint32_t dict_id = 0) &
      ABSL_ATTRIBUTE_LIFETIME_BOUND;
  Lz4Dictionary&& set_data_unowned(absl::string_view data
                                       ABSL_ATTRIBUTE_LIFETIME_BOUND,
                                   uint32_t dict_id = 0) &&
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return std::move(set_data_unowned(data, dict_id));
  }

  // Returns `true` if no dictionary is present.
  bool empty() const;

  // Returns the dictionary data.
  absl::string_view data() const ABSL_ATTRIBUTE_LIFETIME_BOUND;

  // Returns the dictionary id.
  //
  // Dictionary id can help to detect whether the correct dictionary is used.
  // 0 means unspecified.
  uint32_t dict_id() const;

  // Returns the compression dictionary in the prepared form, or `nullptr` if
  // no dictionary is present or `LZ4F_createCDict()` failed.
  //
  // The dictionary is owned by `*this`.
  const LZ4F_CDict* PrepareCompressionDictionary() const
      ABSL_ATTRIBUTE_LIFETIME_BOUND;

 private:
  enum class Ownership { kCopied, kUnowned };

  class Repr;

  SharedPtr<const Repr> repr_;
};

// Implementation details follow.

class Lz4Dictionary::Repr {
 public:
  // Owns a copy of `data`.
  explicit Repr(BytesInitializer data,
                std::integral_constant<Ownership, Ownership::kCopied>,
                uint32_t dict_id)
      : owned_data_(std::move(data)), data_(owned_data_), dict_id_(dict_id) {}

  // Does not take ownership of `data`, which must not be changed until the
  // last `Lz4Reader` or `Lz4Writer` using this dictionary is closed or no
  // longer used.
  explicit Repr(absl::string_view data,
                std::integral_constant<Ownership, Ownership::kUnowned>,
                uint32_t dict_id)
      : data_(data), dict_id_(dict_id) {}

  Repr(const Repr&) = delete;
  Repr& operator=(const Repr&) = delete;

  // Returns the compression dictionary in the prepared form, or `nullptr` if
  // no dictionary is present or `LZ4F_createCDict()` failed.
  //
  // The dictionary is owned by `*this`.
  const LZ4F_CDict* PrepareCompressionDictionary() const;

  absl::string_view data() const { return data_; }
  uint32_t dict_id() const { return dict_id_; }

 private:
  struct LZ4F_CDictDeleter {
    void operator()(LZ4F_CDict* ptr) const;
  };

  std::string owned_data_;
  absl::string_view data_;
  uint32_t dict_id_;

  mutable absl::once_flag compression_once_;
  mutable std::unique_ptr<LZ4F_CDict, LZ4F_CDictDeleter>
      compression_dictionary_;
};

inline Lz4Dictionary& Lz4Dictionary::Reset() & ABSL_ATTRIBUTE_LIFETIME_BOUND {
  repr_.Reset();
  return *this;
}

inline Lz4Dictionary& Lz4Dictionary::set_data(BytesInitializer data,
                                              uint32_t dict_id) &
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  repr_.Reset(riegeli::Maker(
      std::move(data), std::integral_constant<Ownership, Ownership::kCopied>(),
      dict_id));
  return *this;
}

inline Lz4Dictionary& Lz4Dictionary::set_data_unowned(
    absl::string_view data ABSL_ATTRIBUTE_LIFETIME_BOUND, uint32_t dict_id) &
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  repr_.Reset(riegeli::Maker(
      data, std::integral_constant<Ownership, Ownership::kUnowned>(), dict_id));
  return *this;
}

inline bool Lz4Dictionary::empty() const {
  return repr_ == nullptr || repr_->data().empty();
}

inline absl::string_view Lz4Dictionary::data() const
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  if (repr_ == nullptr) return absl::string_view();
  return repr_->data();
}

inline uint32_t Lz4Dictionary::dict_id() const {
  if (repr_ == nullptr) return 0;
  return repr_->dict_id();
}

}  // namespace riegeli

#endif  // RIEGELI_LZ4_LZ4_DICTIONARY_H_
