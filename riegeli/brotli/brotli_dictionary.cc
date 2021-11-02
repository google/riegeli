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

#include "riegeli/brotli/brotli_dictionary.h"

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <string>

#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "brotli/encode.h"
#include "brotli/shared_dictionary.h"
#include "riegeli/base/base.h"

namespace riegeli {

namespace {

struct BrotliEncoderDictionaryDeleter {
  void operator()(BrotliEncoderPreparedDictionary* ptr) const {
    BrotliEncoderDestroyPreparedDictionary(ptr);
  }
};

}  // namespace

std::shared_ptr<const BrotliEncoderPreparedDictionary>
BrotliDictionary::Chunk::PrepareCompressionDictionary() const {
  absl::MutexLock lock(&compression_mutex_);
  if (!compression_present_) {
    RIEGELI_ASSERT_NE(static_cast<int>(type_), static_cast<int>(Type::kNative))
        << "Failed invariant of BrotliDictionary::Chunk: "
           "unprepared native chunk";
    // TODO: Ask the Brotli engine to avoid copying the data when it
    // supports that.
    compression_dictionary_ = std::unique_ptr<BrotliEncoderPreparedDictionary,
                                              BrotliEncoderDictionaryDeleter>(
        BrotliEncoderPrepareDictionary(
            static_cast<BrotliSharedDictionaryType>(type_), data_.size(),
            reinterpret_cast<const uint8_t*>(data_.data()), BROTLI_MAX_QUALITY,
            // `BrotliAllocator` is not supported here because the prepared
            // dictionary may easily outlive the allocator.
            nullptr, nullptr, nullptr));
    compression_present_ = true;
  }
  return compression_dictionary_;
}

void BrotliDictionary::Chunk::RemoveDecompressionSupport(
    std::shared_ptr<const Chunk>& self) const {
  if (data_.data() == owned_data_.data()) {
    self = std::make_shared<const Chunk>(PrepareCompressionDictionary());
  }
}

void BrotliDictionary::RemoveDecompressionSupport() {
  for (std::shared_ptr<const Chunk>& chunk : chunks_) {
    chunk->RemoveDecompressionSupport(chunk);
  }
}

}  // namespace riegeli
