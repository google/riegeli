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

// IWYU pragma: private, include "riegeli/brotli/brotli_reader.h"
// IWYU pragma: private, include "riegeli/brotli/brotli_writer.h"

#include <stddef.h>

#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/base/call_once.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "brotli/encode.h"
#include "brotli/shared_dictionary.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/bytes_ref.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/shared_ptr.h"

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
// `BrotliReader` or `BrotliWriter` using this dictionary is closed or no
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
  BrotliDictionary() = default;

  BrotliDictionary(const BrotliDictionary& that) = default;
  BrotliDictionary& operator=(const BrotliDictionary& that) = default;

  BrotliDictionary(BrotliDictionary&& that) = default;
  BrotliDictionary& operator=(BrotliDictionary&& that) = default;

  // Resets the `BrotliDictionary` to the empty state.
  BrotliDictionary& Reset() & ABSL_ATTRIBUTE_LIFETIME_BOUND;
  BrotliDictionary&& Reset() && ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return std::move(Reset());
  }

  // Adds a raw chunk (data which should contain sequences that are commonly
  // seen in the data being compressed). Up to `kMaxRawChunks` can be added.
  BrotliDictionary& add_raw(BytesInitializer data) &
      ABSL_ATTRIBUTE_LIFETIME_BOUND;
  BrotliDictionary&& add_raw(BytesInitializer data) &&
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return std::move(add_raw(std::move(data)));
  }

  // Like `add_raw()`, but does not take ownership of `data`, which must not
  // be changed until the last `BrotliReader` or `BrotliWriter` using this
  // dictionary is closed or no longer used.
  BrotliDictionary& add_raw_unowned(
      absl::string_view data ABSL_ATTRIBUTE_LIFETIME_BOUND) &
      ABSL_ATTRIBUTE_LIFETIME_BOUND;
  BrotliDictionary&& add_raw_unowned(
      absl::string_view data ABSL_ATTRIBUTE_LIFETIME_BOUND) &&
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return std::move(add_raw_unowned(data));
  }

  // Sets a serialized chunk (prepared by shared_brotli_encode_dictionary tool).
  BrotliDictionary& set_serialized(BytesInitializer data) &
      ABSL_ATTRIBUTE_LIFETIME_BOUND;
  BrotliDictionary&& set_serialized(BytesInitializer data) &&
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return std::move(set_serialized(std::move(data)));
  }

  // Like `set_serialized()`, but does not take ownership of `data`, which
  // must not be changed until the last `BrotliWriter` or `BrotliReader` using
  // this dictionary is closed or no longer used.
  BrotliDictionary& set_serialized_unowned(
      absl::string_view data ABSL_ATTRIBUTE_LIFETIME_BOUND) &
      ABSL_ATTRIBUTE_LIFETIME_BOUND;
  BrotliDictionary&& set_serialized_unowned(
      absl::string_view data ABSL_ATTRIBUTE_LIFETIME_BOUND) &&
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return std::move(set_serialized_unowned(data));
  }

  // Interoperability with the native Brotli engine: adds a chunk represented by
  // `BrotliEncoderPreparedDictionary` pointer. It can be used for compression
  // but not for decompression.
  //
  // Does not take ownedship of `prepared, which must be valid until the last
  // `BrotliReader` or `BrotliWriter` using this dictionary is closed or no
  // longer used.
  BrotliDictionary& add_native_unowned(
      const BrotliEncoderPreparedDictionary* prepared
          ABSL_ATTRIBUTE_LIFETIME_BOUND) &
      ABSL_ATTRIBUTE_LIFETIME_BOUND;
  BrotliDictionary&& add_native_unowned(
      const BrotliEncoderPreparedDictionary* prepared
          ABSL_ATTRIBUTE_LIFETIME_BOUND) &&
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return std::move(add_native_unowned(prepared));
  }

  // Returns `true` if no dictionary is present.
  bool empty() const { return chunks_.empty(); }

  // Returns the sequence of chunks the dictionary consists of.
  absl::Span<const SharedPtr<const Chunk>> chunks() const
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return chunks_;
  }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const BrotliDictionary* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&self->chunks_);
  }

 private:
  enum class Ownership { kCopied, kUnowned };

  std::vector<SharedPtr<const Chunk>> chunks_;
};

class BrotliDictionary::Chunk {
 public:
  // Owns a copy of `data`.
  explicit Chunk(Type type, BytesInitializer data,
                 std::integral_constant<Ownership, Ownership::kCopied>)
      : type_(type), owned_data_(std::move(data)), data_(owned_data_) {}

  // Does not take ownership of `data`, which must not be changed until the
  // last `BrotliWriter` or `BrotliReader` using this dictionary is closed or
  // no longer used.
  explicit Chunk(Type type,
                 absl::string_view data ABSL_ATTRIBUTE_LIFETIME_BOUND,
                 std::integral_constant<Ownership, Ownership::kUnowned>)
      : type_(type), data_(data) {}

  // Does not know the data. The chunk is represented by
  // `BrotliEncoderPreparedDictionary` pointer.
  explicit Chunk(const BrotliEncoderPreparedDictionary* prepared
                     ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : type_(Type::kNative), compression_dictionary_(prepared) {}

  Chunk(const Chunk&) = delete;
  Chunk& operator=(const Chunk&) = delete;

  Type type() const { return type_; }
  absl::string_view data() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT_NE(static_cast<int>(type_), static_cast<int>(Type::kNative))
        << "Original data are not available "
           "for a native Brotli dictionary chunk";
    return data_;
  }

  // Returns the compression dictionary in the prepared form, or `nullptr` if
  // `BrotliEncoderPrepareDictionary()` failed.
  //
  // The dictionary is owned by `*this`.
  const BrotliEncoderPreparedDictionary* PrepareCompressionDictionary() const
      ABSL_ATTRIBUTE_LIFETIME_BOUND;

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const Chunk* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&self->owned_data_);
    if (const BrotliEncoderPreparedDictionary* const compression_dictionary =
            self->PrepareCompressionDictionary()) {
      memory_estimator.RegisterMemory(
          BrotliEncoderGetPreparedDictionarySize(compression_dictionary));
    }
  }

 private:
  struct BrotliEncoderDictionaryDeleter {
    void operator()(BrotliEncoderPreparedDictionary* ptr) const {
      BrotliEncoderDestroyPreparedDictionary(ptr);
    }
  };

  Type type_;
  std::string owned_data_;
  absl::string_view data_;

  mutable absl::once_flag compression_once_;
  mutable std::unique_ptr<BrotliEncoderPreparedDictionary,
                          BrotliEncoderDictionaryDeleter>
      owned_compression_dictionary_;
  mutable const BrotliEncoderPreparedDictionary* compression_dictionary_ =
      nullptr;
};

// Implementation details follow.

inline BrotliDictionary& BrotliDictionary::Reset() &
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  chunks_.clear();
  return *this;
}

inline BrotliDictionary& BrotliDictionary::add_raw(BytesInitializer data) &
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  chunks_.emplace_back(
      riegeli::Maker(Type::kRaw, std::move(data),
                     std::integral_constant<Ownership, Ownership::kCopied>()));
  return *this;
}

inline BrotliDictionary& BrotliDictionary::add_raw_unowned(
    absl::string_view data ABSL_ATTRIBUTE_LIFETIME_BOUND) &
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  chunks_.emplace_back(
      riegeli::Maker(Type::kRaw, data,
                     std::integral_constant<Ownership, Ownership::kUnowned>()));
  return *this;
}

inline BrotliDictionary& BrotliDictionary::set_serialized(
    BytesInitializer data) &
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  Reset();
  chunks_.emplace_back(
      riegeli::Maker(Type::kSerialized, std::move(data),
                     std::integral_constant<Ownership, Ownership::kCopied>()));
  return *this;
}

inline BrotliDictionary& BrotliDictionary::set_serialized_unowned(
    absl::string_view data ABSL_ATTRIBUTE_LIFETIME_BOUND) &
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  Reset();
  chunks_.emplace_back(
      riegeli::Maker(Type::kSerialized, data,
                     std::integral_constant<Ownership, Ownership::kUnowned>()));
  return *this;
}

inline BrotliDictionary& BrotliDictionary::add_native_unowned(
    const BrotliEncoderPreparedDictionary* prepared
        ABSL_ATTRIBUTE_LIFETIME_BOUND) &
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  chunks_.emplace_back(riegeli::Maker(prepared));
  return *this;
}

}  // namespace riegeli

#endif  // RIEGELI_BROTLI_BROTLI_DICTIONARY_H_
