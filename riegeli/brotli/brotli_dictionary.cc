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

#include "absl/base/call_once.h"
#include "absl/strings/string_view.h"
#include "brotli/encode.h"
#include "brotli/shared_dictionary.h"
#include "riegeli/base/base.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if __cplusplus < 201703
constexpr size_t BrotliDictionary::kMaxRawChunks;
#endif

namespace {

struct BrotliEncoderDictionaryDeleter {
  void operator()(BrotliEncoderPreparedDictionary* ptr) const {
    BrotliEncoderDestroyPreparedDictionary(ptr);
  }
};

}  // namespace

std::shared_ptr<const BrotliEncoderPreparedDictionary>
BrotliDictionary::Chunk::PrepareCompressionDictionary() const {
  absl::call_once(compression_once_, [&] {
    if (type_ == Type::kNative) {
      RIEGELI_ASSERT(compression_dictionary_ != nullptr)
          << "Failed invariant of BrotliDictionary::Chunk: "
             "unprepared native chunk";
      return;
    }
    compression_dictionary_ = std::unique_ptr<BrotliEncoderPreparedDictionary,
                                              BrotliEncoderDictionaryDeleter>(
        BrotliEncoderPrepareDictionary(
            static_cast<BrotliSharedDictionaryType>(type_), data_.size(),
            reinterpret_cast<const uint8_t*>(data_.data()), BROTLI_MAX_QUALITY,
            // `BrotliAllocator` is not supported here because the prepared
            // dictionary may easily outlive the allocator.
            nullptr, nullptr, nullptr));
  });
  return compression_dictionary_;
}

}  // namespace riegeli
