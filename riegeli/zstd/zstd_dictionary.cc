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

// Enables the experimental zstd API:
//  * `ZSTD_createCDict_advanced()`
//  * `ZSTD_createDDict_advanced()`
//  * `ZSTD_dictLoadMethod_e`
//  * `ZSTD_dictContentType_e`
#define ZSTD_STATIC_LINKING_ONLY

#include "riegeli/zstd/zstd_dictionary.h"

#include <limits>
#include <memory>

#include "absl/base/call_once.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "riegeli/base/base.h"
#include "zstd.h"

namespace riegeli {

// Constants are defined as integer literals in zstd_dictionary.h and asserted
// here to avoid depending on `ZSTD_STATIC_LINKING_ONLY` in zstd_dictionary.h.
static_assert(
    static_cast<ZSTD_dictContentType_e>(ZstdDictionary::Type::kAuto) ==
            ZSTD_dct_auto &&
        static_cast<ZSTD_dictContentType_e>(ZstdDictionary::Type::kRaw) ==
            ZSTD_dct_rawContent &&
        static_cast<ZSTD_dictContentType_e>(
            ZstdDictionary::Type::kSerialized) == ZSTD_dct_fullDict,
    "Enum values of ZstdDictionary::Type disagree with ZSTD_dct "
    "constants");

namespace {

struct ZSTD_CDictDeleter {
  void operator()(ZSTD_CDict* ptr) const { ZSTD_freeCDict(ptr); }
};

struct ZSTD_DDictDeleter {
  void operator()(ZSTD_DDict* ptr) const { ZSTD_freeDDict(ptr); }
};

}  // namespace

// Holds a compression dictionary prepared for a particular compression level.
//
// If several callers of `ZstdDictionary` need a prepared dictionary with the
// same compression level at the same time, they wait for the first one to
// prepare it, and they share it.
//
// If the callers need it with different compression levels, they do not wait.
// The dictionary will be prepared again if varying compression levels later
// repeat, because the cache holds at most one entry.
struct ZstdDictionary::Repr::CompressionCache {
  explicit CompressionCache(int compression_level)
      : compression_level(compression_level) {}

  int compression_level;
  mutable absl::once_flag compression_once;
  mutable std::shared_ptr<const ZSTD_CDict> compression_dictionary;
};

inline std::shared_ptr<const ZSTD_CDict>
ZstdDictionary::Repr::PrepareCompressionDictionary(
    int compression_level) const {
  const std::shared_ptr<const CompressionCache> compression_cache = [&] {
    absl::MutexLock lock(&compression_mutex_);
    if (compression_cache_ == nullptr ||
        compression_cache_->compression_level != compression_level) {
      compression_cache_ =
          std::make_shared<const CompressionCache>(compression_level);
    }
    return compression_cache_;
  }();
  absl::call_once(compression_cache->compression_once, [&] {
    compression_cache->compression_dictionary =
        std::unique_ptr<ZSTD_CDict, ZSTD_CDictDeleter>(
            ZSTD_createCDict_advanced(
                data_.data(), data_.size(), ZSTD_dlm_byRef,
                static_cast<ZSTD_dictContentType_e>(type_),
                ZSTD_getCParams(compression_level, 0, data_.size()),
                ZSTD_defaultCMem));
  });
  return compression_cache->compression_dictionary;
}

inline std::shared_ptr<const ZSTD_DDict>
ZstdDictionary::Repr::PrepareDecompressionDictionary() const {
  absl::call_once(decompression_once_, [&] {
    decompression_dictionary_ = std::unique_ptr<ZSTD_DDict, ZSTD_DDictDeleter>(
        ZSTD_createDDict_advanced(data_.data(), data_.size(), ZSTD_dlm_byRef,
                                  static_cast<ZSTD_dictContentType_e>(type_),
                                  ZSTD_defaultCMem));
  });
  return decompression_dictionary_;
}

std::shared_ptr<const ZSTD_CDict> ZstdDictionary::PrepareCompressionDictionary(
    int compression_level) const {
  if (repr_ == nullptr) return nullptr;
  return repr_->PrepareCompressionDictionary(compression_level);
}

std::shared_ptr<const ZSTD_DDict>
ZstdDictionary::PrepareDecompressionDictionary() const {
  if (repr_ == nullptr) return nullptr;
  return repr_->PrepareDecompressionDictionary();
}

}  // namespace riegeli
