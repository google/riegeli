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

// Enables the experimental lz4 API:
//  * `LZ4F_createCDict()`
//  * `LZ4F_freeCDict()`
#define LZ4F_STATIC_LINKING_ONLY

#include "riegeli/lz4/lz4_dictionary.h"

#include <memory>

#include "absl/base/call_once.h"
#include "absl/strings/string_view.h"
#include "lz4frame.h"
#include "riegeli/base/intrusive_ref_count.h"

namespace riegeli {

namespace {

struct LZ4F_CDictDeleter {
  void operator()(LZ4F_CDict* ptr) const { LZ4F_freeCDict(ptr); }
};

}  // namespace

inline std::shared_ptr<const LZ4F_CDict>
Lz4Dictionary::Repr::PrepareCompressionDictionary() const {
  absl::call_once(compression_once_, [&] {
    compression_dictionary_ = std::unique_ptr<LZ4F_CDict, LZ4F_CDictDeleter>(
        LZ4F_createCDict(data_.data(), data_.size()));
  });
  return compression_dictionary_;
}

std::shared_ptr<const LZ4F_CDict> Lz4Dictionary::PrepareCompressionDictionary()
    const {
  if (repr_ == nullptr) return nullptr;
  return repr_->PrepareCompressionDictionary();
}

}  // namespace riegeli
