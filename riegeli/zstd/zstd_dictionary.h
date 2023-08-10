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

// IWYU pragma: private, include "riegeli/zstd/zstd_reader.h"
// IWYU pragma: private, include "riegeli/zstd/zstd_writer.h"

#include <stdint.h>

#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/call_once.h"
#include "absl/base/thread_annotations.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
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
 private:
  struct ZSTD_CDictReleaser;

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

  // Owning handle to a compression dictionary in the prepared form.
  using ZSTD_CDictHandle = std::unique_ptr<ZSTD_CDict, ZSTD_CDictReleaser>;

  // Creates an empty `ZstdDictionary`.
  ZstdDictionary() = default;

  ZstdDictionary(const ZstdDictionary& that) = default;
  ZstdDictionary& operator=(const ZstdDictionary& that) = default;

  ZstdDictionary(ZstdDictionary&& that) = default;
  ZstdDictionary& operator=(ZstdDictionary&& that) = default;

  // Resets the `ZstdDictionary` to the empty state.
  ZstdDictionary& Reset() &;
  ZstdDictionary&& Reset() && { return std::move(Reset()); }

  // Sets a dictionary.
  //
  // `std::string&&` is accepted with a template to avoid implicit conversions
  // to `std::string` which can be ambiguous against `absl::string_view`
  // (e.g. `const char*`).
  ZstdDictionary& set_data(absl::string_view data, Type type = Type::kAuto) &;
  ZstdDictionary&& set_data(absl::string_view data,
                            Type type = Type::kAuto) && {
    return std::move(set_data(data, type));
  }
  template <typename Src,
            std::enable_if_t<std::is_same<Src, std::string>::value, int> = 0>
  ZstdDictionary& set_data(Src&& data, Type type = Type::kAuto) &;
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
  ZSTD_CDictHandle PrepareCompressionDictionary(int compression_level) const;

  // Returns the decompression dictionary in the prepared form, or `nullptr` if
  // no dictionary is present or `ZSTD_createDDict_advanced()` failed.
  //
  // The dictionary is owned by `*this`.
  const ZSTD_DDict* PrepareDecompressionDictionary() const;

  // Returns the dictionary ID, or 0 is no dictionary is present.
  uint32_t DictId() const;

 private:
  enum class Ownership { kCopied, kUnowned };

  struct ZSTD_CDictDeleter {
    void operator()(ZSTD_CDict* ptr) const { ZSTD_freeCDict(ptr); }
  };

  struct ZSTD_CDictCache;

  struct ZSTD_CDictReleaser {
    void operator()(ABSL_ATTRIBUTE_UNUSED ZSTD_CDict* ptr) {
      // `*ptr` is owned by `*compression_cache`.
      compression_cache.reset();
    }
    RefCountedPtr<const ZSTD_CDictCache> compression_cache;
  };

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
  ZSTD_CDictHandle PrepareCompressionDictionary(int compression_level) const;

  // Returns the decompression dictionary in the prepared form, or `nullptr`
  // if no dictionary is present or `ZSTD_createDDict_advanced()` failed.
  //
  // The dictionary is owned by `*this`.
  const ZSTD_DDict* PrepareDecompressionDictionary() const;

  absl::string_view data() const { return data_; }

 private:
  struct ZSTD_DDictDeleter {
    void operator()(ZSTD_DDict* ptr) const { ZSTD_freeDDict(ptr); }
  };

  Type type_;
  std::string owned_data_;
  absl::string_view data_;

  mutable absl::Mutex compression_cache_mutex_;
  mutable RefCountedPtr<const ZSTD_CDictCache> compression_cache_
      ABSL_GUARDED_BY(compression_cache_mutex_);

  mutable absl::once_flag decompression_once_;
  mutable std::unique_ptr<ZSTD_DDict, ZSTD_DDictDeleter>
      decompression_dictionary_;
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
struct ZstdDictionary::ZSTD_CDictCache : RefCountedBase<ZSTD_CDictCache> {
  explicit ZSTD_CDictCache(int compression_level)
      : compression_level(compression_level) {}

  int compression_level;
  mutable absl::once_flag compression_once;
  mutable std::unique_ptr<ZSTD_CDict, ZSTD_CDictDeleter> compression_dictionary;
};

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
