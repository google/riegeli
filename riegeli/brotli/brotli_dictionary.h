// Copyright 2021 Google LLC
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

#ifndef RIEGELI_BROTLI_BROTLI_DICTIONARY_H_
#define RIEGELI_BROTLI_BROTLI_DICTIONARY_H_

#include <stddef.h>

#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/call_once.h"
#include "absl/base/thread_annotations.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "brotli/encode.h"
#include "brotli/shared_dictionary.h"
#include "riegeli/base/base.h"

namespace riegeli {

// Stores an optional Brotli dictionary for compression and decompression
// (Shared Brotli).
//
// A dictionary is empty and is equivalent to having no dictionary, or contains
// a number of raw chunks (data which should contain sequences that are commonly
// seen in the data being compressed), or contains one serialized chunk
// (prepared by shared_brotli_encode_dictionary tool).
//
// A `BrotliDictionary` object can own the dictionary data, or can hold a
// pointer to unowned dictionary data which must not be changed until the last
// `BrotliWriter` or `BrotliReader` using this dictionary is closed or no
// longer used. A `BrotliDictionary` object also holds prepared structures
// derived from dictionary data. If the same dictionary is needed for multiple
// compression or decompression sessions, the `BrotliDictionary` object can be
// reused to avoid preparing them again for compression.
//
// Copying a `BrotliDictionary` object is cheap, sharing the actual
// dictionary.
class BrotliDictionary {
 public:
  class Chunk;

  enum class Type {
    // Chunk data should contain sequences that are commonly seen in the data
    // being compressed
    kRaw = BROTLI_SHARED_DICTIONARY_RAW,
    // Chunk data prepared by shared_brotli_encode_dictionary tool.
    kSerialized = BROTLI_SHARED_DICTIONARY_SERIALIZED,
    // Chunk represented by `BrotliEncoderPreparedDictionary` pointer.
    kNative = 2,
  };

  static constexpr size_t kMaxRawChunks = SHARED_BROTLI_MAX_COMPOUND_DICTS;

  // Creates an empty `BrotliDictionary`.
  BrotliDictionary() noexcept {}

  BrotliDictionary(const BrotliDictionary& that);
  BrotliDictionary& operator=(const BrotliDictionary& that);

  BrotliDictionary(BrotliDictionary&& that) noexcept;
  BrotliDictionary& operator=(BrotliDictionary&& that) noexcept;

  // Resets the `BrotliDictionary` to the empty state.
  BrotliDictionary& Reset() &;
  BrotliDictionary&& Reset() && { return std::move(Reset()); }

  // Adds a raw chunk (data which should contain sequences that are commonly
  // seen in the data being compressed). Up to `kMaxRawChunks` can be added.
  //
  // `std::string&&` is accepted with a template to avoid implicit conversions
  // to `std::string` which can be ambiguous against `absl::string_view`
  // (e.g. `const char*`).
  BrotliDictionary& add_raw(absl::string_view data) &;
  template <typename Src,
            std::enable_if_t<std::is_same<Src, std::string>::value, int> = 0>
  BrotliDictionary& add_raw(Src&& data) &;
  BrotliDictionary&& add_raw(absl::string_view data) && {
    return std::move(add_raw(data));
  }
  template <typename Src,
            std::enable_if_t<std::is_same<Src, std::string>::value, int> = 0>
  BrotliDictionary&& add_raw(Src&& data) && {
    // `std::move(data)` is correct and `std::forward<Src>(data)` is not
    // necessary: `Src` is always `std::string`, never an lvalue reference.
    return std::move(add_raw(std::move(data)));
  }

  // Like `add_raw()`, but does not take ownership of `data`, which must not
  // be changed until the last `BrotliWriter` or `BrotliReader` using this
  // dictionary is closed or no longer used.
  BrotliDictionary& add_raw_unowned(absl::string_view data) &;
  BrotliDictionary&& add_raw_unowned(absl::string_view data) && {
    return std::move(add_raw_unowned(data));
  }

  // Sets a serialized chunk (prepared by shared_brotli_encode_dictionary tool).
  //
  // `std::string&&` is accepted with a template to avoid implicit conversions
  // to `std::string` which can be ambiguous against `absl::string_view`
  // (e.g. `const char*`).
  BrotliDictionary& set_serialized(absl::string_view data) &;
  template <typename Src,
            std::enable_if_t<std::is_same<Src, std::string>::value, int> = 0>
  BrotliDictionary& set_serialized(Src&& data) &;
  BrotliDictionary&& set_serialized(absl::string_view data) && {
    return std::move(set_serialized(data));
  }
  template <typename Src,
            std::enable_if_t<std::is_same<Src, std::string>::value, int> = 0>
  BrotliDictionary&& set_serialized(Src&& data) && {
    // `std::move(data)` is correct and `std::forward<Src>(data)` is not
    // necessary: `Src` is always `std::string`, never an lvalue reference.
    return std::move(set_serialized(std::move(data)));
  }

  // Like `set_serialized()`, but does not take ownership of `data`, which
  // must not be changed until the last `BrotliWriter` or `BrotliReader` using
  // this dictionary is closed or no longer used.
  BrotliDictionary& set_serialized_unowned(absl::string_view data) &;
  BrotliDictionary&& set_serialized_unowned(absl::string_view data) && {
    return std::move(set_serialized_unowned(data));
  }

  // Interoperability with the native Brotli engine: adds a chunk represented by
  // `BrotliEncoderPreparedDictionary` pointer. It can be used for compression
  // but not for decompression.
  BrotliDictionary& add_native(
      std::shared_ptr<const BrotliEncoderPreparedDictionary> prepared) &;
  BrotliDictionary&& add_native(
      std::shared_ptr<const BrotliEncoderPreparedDictionary> prepared) && {
    return std::move(add_native(std::move(prepared)));
  }

  // Returns `true` if no dictionary is present.
  bool empty() const { return chunks_.empty(); }

  // Returns the sequence of chunks the dictionary consists of.
  const absl::Span<const std::shared_ptr<const Chunk>> chunks() const {
    return chunks_;
  }

 private:
  enum class Ownership { kCopied, kUnowned };

  std::vector<std::shared_ptr<const Chunk>> chunks_;
};

class BrotliDictionary::Chunk {
 public:
  // Owns a copy of `data`. This constructor is public for `std::make_shared()`.
  explicit Chunk(Type type, absl::string_view data,
                 std::integral_constant<Ownership, Ownership::kCopied>)
      : type_(type), owned_data_(data), data_(owned_data_) {}

  // Owns moved `data`. This constructor is public for `std::make_shared()`.
  explicit Chunk(Type type, std::string&& data)
      : type_(type), owned_data_(std::move(data)), data_(owned_data_) {}

  // Does not take ownership of `data`, which must not be changed until the
  // last `BrotliWriter` or `BrotliReader` using this dictionary is closed or
  // no longer used. This constructor is public for `std::make_shared()`.
  explicit Chunk(Type type, absl::string_view data,
                 std::integral_constant<Ownership, Ownership::kUnowned>)
      : type_(type), data_(data) {}

  // Does not know the data. The chunk is represented by
  // `BrotliEncoderPreparedDictionary` pointer. This constructor is public for
  // `std::make_shared()`.
  explicit Chunk(
      std::shared_ptr<const BrotliEncoderPreparedDictionary> prepared)
      : type_(Type::kNative), compression_dictionary_(std::move(prepared)) {}

  Chunk(const Chunk&) = delete;
  Chunk& operator=(const Chunk&) = delete;

  Type type() const { return type_; }
  absl::string_view data() const {
    RIEGELI_ASSERT_NE(static_cast<int>(type_), static_cast<int>(Type::kNative))
        << "Original data are not available "
           "for a native Brotli dictionary chunk";
    return data_;
  }

  // Returns the compression dictionary in the prepared form, or `nullptr` if
  // `BrotliEncoderPrepareDictionary()` failed.
  std::shared_ptr<const BrotliEncoderPreparedDictionary>
  PrepareCompressionDictionary() const;

 private:
  Type type_;
  std::string owned_data_;
  absl::string_view data_;

  mutable absl::once_flag compression_once_;
  mutable std::shared_ptr<const BrotliEncoderPreparedDictionary>
      compression_dictionary_;
};

// Implementation details follow.

inline BrotliDictionary::BrotliDictionary(const BrotliDictionary& that)
    : chunks_(that.chunks_) {}

inline BrotliDictionary& BrotliDictionary::operator=(
    const BrotliDictionary& that) {
  chunks_ = that.chunks_;
  return *this;
}

inline BrotliDictionary::BrotliDictionary(BrotliDictionary&& that) noexcept
    : chunks_(std::move(that.chunks_)) {}

inline BrotliDictionary& BrotliDictionary::BrotliDictionary::operator=(
    BrotliDictionary&& that) noexcept {
  chunks_ = std::move(that.chunks_);
  return *this;
}

inline BrotliDictionary& BrotliDictionary::Reset() & {
  chunks_.clear();
  return *this;
}

inline BrotliDictionary& BrotliDictionary::add_raw(absl::string_view data) & {
  chunks_.push_back(std::make_shared<const Chunk>(
      Type::kRaw, data,
      std::integral_constant<Ownership, Ownership::kCopied>()));
  return *this;
}

template <typename Src,
          std::enable_if_t<std::is_same<Src, std::string>::value, int>>
inline BrotliDictionary& BrotliDictionary::add_raw(Src&& data) & {
  // `std::move(data)` is correct and `std::forward<Src>(data)` is not
  // necessary: `Src` is always `std::string`, never an lvalue reference.
  chunks_.push_back(std::make_shared<const Chunk>(Type::kRaw, std::move(data)));
  return *this;
}

inline BrotliDictionary& BrotliDictionary::add_raw_unowned(
    absl::string_view data) & {
  chunks_.push_back(std::make_shared<const Chunk>(
      Type::kRaw, data,
      std::integral_constant<Ownership, Ownership::kUnowned>()));
  return *this;
}

inline BrotliDictionary& BrotliDictionary::set_serialized(
    absl::string_view data) & {
  Reset();
  chunks_.push_back(std::make_shared<const Chunk>(
      Type::kSerialized, data,
      std::integral_constant<Ownership, Ownership::kCopied>()));
  return *this;
}

template <typename Src,
          std::enable_if_t<std::is_same<Src, std::string>::value, int>>
inline BrotliDictionary& BrotliDictionary::set_serialized(Src&& data) & {
  Reset();
  // `std::move(data)` is correct and `std::forward<Src>(data)` is not
  // necessary: `Src` is always `std::string`, never an lvalue reference.
  chunks_.push_back(
      std::make_shared<const Chunk>(Type::kSerialized, std::move(data)));
  return *this;
}

inline BrotliDictionary& BrotliDictionary::set_serialized_unowned(
    absl::string_view data) & {
  Reset();
  chunks_.push_back(std::make_shared<const Chunk>(
      Type::kSerialized, data,
      std::integral_constant<Ownership, Ownership::kUnowned>()));
  return *this;
}

inline BrotliDictionary& BrotliDictionary::add_native(
    std::shared_ptr<const BrotliEncoderPreparedDictionary> prepared) & {
  chunks_.push_back(std::make_shared<const Chunk>(std::move(prepared)));
  return *this;
}

}  // namespace riegeli

#endif  // RIEGELI_BROTLI_BROTLI_DICTIONARY_H_
