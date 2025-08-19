#include "absl/base/attributes.h"
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

#include <stdint.h>

#include <memory>
#include <utility>

#include "absl/base/call_once.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/shared_ptr.h"
#include "riegeli/zstd/zstd_dictionary.h"
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

inline ZstdDictionary::ZSTD_CDictHandle
ZstdDictionary::Repr::PrepareCompressionDictionary(
    int compression_level) const {
  SharedPtr<const ZSTD_CDictCache> compression_cache;
  {
    absl::MutexLock lock(compression_cache_mutex_);
    if (compression_cache_ == nullptr ||
        compression_cache_->compression_level != compression_level) {
      compression_cache_.Reset(riegeli::Maker(compression_level));
    }
    compression_cache = compression_cache_;
  }
  absl::call_once(compression_cache->compression_once, [&] {
    compression_cache->compression_dictionary.reset(ZSTD_createCDict_advanced(
        data_.data(), data_.size(), ZSTD_dlm_byRef,
        static_cast<ZSTD_dictContentType_e>(type_),
        ZSTD_getCParams(compression_level, 0, data_.size()), ZSTD_defaultCMem));
  });
  ZSTD_CDict* const ptr = compression_cache->compression_dictionary.get();
  return ZSTD_CDictHandle(ptr,
                          ZSTD_CDictReleaser{std::move(compression_cache)});
}

inline const ZSTD_DDict* ZstdDictionary::Repr::PrepareDecompressionDictionary()
    const {
  absl::call_once(decompression_once_, [&] {
    decompression_dictionary_.reset(ZSTD_createDDict_advanced(
        data_.data(), data_.size(), ZSTD_dlm_byRef,
        static_cast<ZSTD_dictContentType_e>(type_), ZSTD_defaultCMem));
  });
  return decompression_dictionary_.get();
}

ZstdDictionary::ZSTD_CDictHandle ZstdDictionary::PrepareCompressionDictionary(
    int compression_level) const {
  if (repr_ == nullptr) return nullptr;
  return repr_->PrepareCompressionDictionary(compression_level);
}

const ZSTD_DDict* ZstdDictionary::PrepareDecompressionDictionary() const
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  if (repr_ == nullptr) return nullptr;
  return repr_->PrepareDecompressionDictionary();
}

uint32_t ZstdDictionary::DictId() const {
  if (repr_ == nullptr) return 0;
  return IntCast<uint32_t>(
      ZSTD_getDictID_fromDict(repr_->data().data(), repr_->data().size()));
}

}  // namespace riegeli
