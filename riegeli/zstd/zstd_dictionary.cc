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

inline std::shared_ptr<const ZSTD_CDict>
ZstdDictionary::Repr::PrepareCompressionDictionary(
    int compression_level) const {
  RIEGELI_ASSERT_NE(compression_level, std::numeric_limits<int>::min())
      << "Failed precondition of "
         "ZstdDictionary::PrepareCompressionDictionary(): "
         "compression level out of range";
  absl::MutexLock lock(&compression_mutex_);
  if (compression_level_ != compression_level) {
    compression_dictionary_ = std::unique_ptr<ZSTD_CDict, ZSTD_CDictDeleter>(
        ZSTD_createCDict_advanced(
            data_.data(), data_.size(), ZSTD_dlm_byRef,
            static_cast<ZSTD_dictContentType_e>(type_),
            ZSTD_getCParams(compression_level, 0, data_.size()),
            ZSTD_defaultCMem));
    compression_level_ = compression_level;
  }
  return compression_dictionary_;
}

inline std::shared_ptr<const ZSTD_DDict>
ZstdDictionary::Repr::PrepareDecompressionDictionary() const {
  absl::MutexLock lock(&decompression_mutex_);
  if (!decompression_present_) {
    decompression_dictionary_ = std::unique_ptr<ZSTD_DDict, ZSTD_DDictDeleter>(
        ZSTD_createDDict_advanced(data_.data(), data_.size(), ZSTD_dlm_byRef,
                                  static_cast<ZSTD_dictContentType_e>(type_),
                                  ZSTD_defaultCMem));
    decompression_present_ = true;
  }
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
