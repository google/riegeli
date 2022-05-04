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

#ifndef RIEGELI_ZSTD_ZSTD_DICTIONARY_H_
#define RIEGELI_ZSTD_ZSTD_DICTIONARY_H_

#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/call_once.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/intrusive_ref_count.h"
#include "zstd.h"

namespace riegeli {

// Stores an optional Zstd dictionary for compression and decompression.
//
// An empty dictionary is equivalent to having no dictionary.
//
// A `ZstdDictionary` object can own the dictionary data, or can hold a pointer
// to unowned dictionary data which must not be changed until the last
// `ZstdReader` or `ZstdWriter` using this dictionary is closed or no longer
// used. A `ZstdDictionary` object also holds prepared structures derived from
// dictionary data. If the same dictionary is needed for multiple compression
// or decompression sessions, the `ZstdDictionary` object can be reused to avoid
// preparing them again.
//
// The prepared dictionary for compression depends on the compression level. At
// most one prepared dictionary is cached, corresponding to the last compression
// level used.
//
// Copying a `ZstdDictionary` object is cheap, sharing the actual dictionary.
class ZstdDictionary {
 public:
  // Interpretation of dictionary data.
  enum class Type {
    // If dictionary data begin with `ZSTD_MAGIC_DICTIONARY`, then like
    // `kSerialized`, otherwise like `kRaw`.
    kAuto = 0,
    // Dictionary data should contain sequences that are commonly seen in the
    // data being compressed.
    kRaw = 1,
    // Shared with the dictBuilder library.
    kSerialized = 2,
  };

  // Creates an empty `ZstdDictionary`.
  ZstdDictionary() noexcept {}

  ZstdDictionary(const ZstdDictionary& that);
  ZstdDictionary& operator=(const ZstdDictionary& that);

  ZstdDictionary(ZstdDictionary&& that) noexcept;
  ZstdDictionary& operator=(ZstdDictionary&& that) noexcept;

  // Resets the `ZstdDictionary` to the empty state.
  ZstdDictionary& Reset() &;
  ZstdDictionary&& Reset() && { return std::move(Reset()); }

  // Sets a dictionary.
  //
  // `std::string&&` is accepted with a template to avoid implicit conversions
  // to `std::string` which can be ambiguous against `absl::string_view`
  // (e.g. `const char*`).
  ZstdDictionary& set_data(absl::string_view data, Type type = Type::kAuto) &;
  template <typename Src,
            std::enable_if_t<std::is_same<Src, std::string>::value, int> = 0>
  ZstdDictionary& set_data(Src&& data, Type type = Type::kAuto) &;
  ZstdDictionary&& set_data(absl::string_view data,
                            Type type = Type::kAuto) && {
    return std::move(set_data(data, type));
  }
  template <typename Src,
            std::enable_if_t<std::is_same<Src, std::string>::value, int> = 0>
  ZstdDictionary&& set_data(Src&& data, Type type = Type::kAuto) && {
    // `std::move(data)` is correct and `std::forward<Src>(data)` is not
    // necessary: `Src` is always `std::string`, never an lvalue reference.
    return std::move(set_data(std::move(data), type));
  }

  // Like `set_data()`, but does not take ownership of `data`, which must not
  // be changed until the last `ZstdReader` or `ZstdWriter` using this
  // dictionary is closed or no longer used.
  ZstdDictionary& set_data_unowned(absl::string_view data,
                                   Type type = Type::kAuto) &;
  ZstdDictionary&& set_data_unowned(absl::string_view data,
                                    Type type = Type::kAuto) && {
    return std::move(set_data_unowned(data, type));
  }

  // Returns `true` if no dictionary is present.
  bool empty() const;

  // Returns the dictionary data.
  absl::string_view data() const;

  // Returns the compression dictionary in the prepared form, or `nullptr` if
  // no dictionary is present or `ZSTD_createCDict_advanced()` failed.
  std::shared_ptr<const ZSTD_CDict> PrepareCompressionDictionary(
      int compression_level) const;

  // Returns the decompression dictionary in the prepared form, or `nullptr` if
  // no dictionary is present or `ZSTD_createDDict_advanced()` failed.
  std::shared_ptr<const ZSTD_DDict> PrepareDecompressionDictionary() const;

 private:
  enum class Ownership { kCopied, kUnowned };

  class Repr;

  RefCountedPtr<const Repr> repr_;
};

// Implementation details follow.

class ZstdDictionary::Repr : public RefCountedBase<Repr> {
 public:
  // Owns a copy of `data`.
  explicit Repr(Type type, absl::string_view data,
                std::integral_constant<Ownership, Ownership::kCopied>)
      : type_(type), owned_data_(data), data_(owned_data_) {}

  // Owns moved `data`.
  explicit Repr(Type type, std::string&& data)
      : type_(type), owned_data_(std::move(data)), data_(owned_data_) {}

  // Does not take ownership of `data`, which must not be changed until the
  // last `ZstdWriter` or `ZstdReader` using this dictionary is closed or no
  // longer used.
  explicit Repr(Type type, absl::string_view data,
                std::integral_constant<Ownership, Ownership::kUnowned>)
      : type_(type), data_(data) {}

  // Returns the compression dictionary in the prepared form, or `nullptr` if
  // no dictionary is present or `ZSTD_createCDict_advanced()` failed.
  std::shared_ptr<const ZSTD_CDict> PrepareCompressionDictionary(
      int compression_level) const;

  // Returns the decompression dictionary in the prepared form, or `nullptr`
  // if no dictionary is present or `ZSTD_createDDict_advanced()` failed.
  std::shared_ptr<const ZSTD_DDict> PrepareDecompressionDictionary() const;

  absl::string_view data() const { return data_; }

 private:
  struct CompressionCache;

  Type type_;
  std::string owned_data_;
  absl::string_view data_;

  mutable AtomicRefCountedPtr<const CompressionCache> compression_cache_;

  mutable absl::once_flag decompression_once_;
  mutable std::shared_ptr<const ZSTD_DDict> decompression_dictionary_;
};

// Holds a compression dictionary prepared for a particular compression level.
//
// If several callers of `ZstdDictionary` need a prepared dictionary with the
// same compression level at the same time, they wait for the first one to
// prepare it, and they share it.
//
// If the callers need it with different compression levels, they do not wait.
// The dictionary will be prepared again if varying compression levels later
// repeat, because the cache holds at most one entry.
struct ZstdDictionary::Repr::CompressionCache
    : RefCountedBase<CompressionCache> {
  explicit CompressionCache(int compression_level)
      : compression_level(compression_level) {}

  int compression_level;
  mutable absl::once_flag compression_once;
  mutable std::shared_ptr<const ZSTD_CDict> compression_dictionary;
};

inline ZstdDictionary::ZstdDictionary(const ZstdDictionary& that)
    : repr_(that.repr_) {}

inline ZstdDictionary& ZstdDictionary::operator=(const ZstdDictionary& that) {
  repr_ = that.repr_;
  return *this;
}

inline ZstdDictionary::ZstdDictionary(ZstdDictionary&& that) noexcept
    : repr_(std::move(that.repr_)) {}

inline ZstdDictionary& ZstdDictionary::ZstdDictionary::operator=(
    ZstdDictionary&& that) noexcept {
  repr_ = std::move(that.repr_);
  return *this;
}

inline ZstdDictionary& ZstdDictionary::Reset() & {
  repr_.reset();
  return *this;
}

inline ZstdDictionary& ZstdDictionary::set_data(absl::string_view data,
                                                Type type) & {
  repr_ = MakeRefCounted<const Repr>(
      type, data, std::integral_constant<Ownership, Ownership::kCopied>());
  return *this;
}

template <typename Src,
          std::enable_if_t<std::is_same<Src, std::string>::value, int>>
inline ZstdDictionary& ZstdDictionary::set_data(Src&& data, Type type) & {
  // `std::move(data)` is correct and `std::forward<Src>(data)` is not
  // necessary: `Src` is always `std::string`, never an lvalue reference.
  repr_ = MakeRefCounted<const Repr>(type, std::move(data));
  return *this;
}

inline ZstdDictionary& ZstdDictionary::set_data_unowned(absl::string_view data,
                                                        Type type) & {
  repr_ = MakeRefCounted<const Repr>(
      type, data, std::integral_constant<Ownership, Ownership::kUnowned>());
  return *this;
}

inline bool ZstdDictionary::empty() const {
  return repr_ == nullptr || repr_->data().empty();
}

inline absl::string_view ZstdDictionary::data() const {
  if (repr_ == nullptr) return absl::string_view();
  return repr_->data();
}

}  // namespace riegeli

#endif  // RIEGELI_ZSTD_ZSTD_DICTIONARY_H_
