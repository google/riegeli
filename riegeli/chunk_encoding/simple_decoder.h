// Copyright 2017 Google LLC
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

#ifndef RIEGELI_CHUNK_ENCODING_SIMPLE_DECODER_H_
#define RIEGELI_CHUNK_ENCODING_SIMPLE_DECODER_H_

#include <stddef.h>
#include <stdint.h>

#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/object.h"
#include "riegeli/base/recycling_pool.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/chunk_encoding/decompressor.h"

namespace riegeli {

class SimpleDecoder : public Object {
 public:
  class Options {
   public:
    Options() noexcept {}

    // Options for a global `RecyclingPool` of decompression contexts.
    //
    // They tune the amount of memory which is kept to speed up creation of new
    // decompression sessions, and usage of a background thread to clean it.
    //
    // Default: `RecyclingPoolOptions()`.
    Options& set_recycling_pool_options(
        const RecyclingPoolOptions& recycling_pool_options) & {
      recycling_pool_options_ = recycling_pool_options;
      return *this;
    }
    Options&& set_recycling_pool_options(
        const RecyclingPoolOptions& recycling_pool_options) && {
      return std::move(set_recycling_pool_options(recycling_pool_options));
    }
    const RecyclingPoolOptions& recycling_pool_options() const {
      return recycling_pool_options_;
    }

   private:
    RecyclingPoolOptions recycling_pool_options_;
  };

  // Creates a closed `SimpleDecoder`.
  explicit SimpleDecoder(Options options = Options())
      : Object(kClosed),
        recycling_pool_options_(options.recycling_pool_options()),
        values_decompressor_(kClosed) {}

  SimpleDecoder(const SimpleDecoder&) = delete;
  SimpleDecoder& operator=(const SimpleDecoder&) = delete;

  // Resets the `SimpleDecoder` and parses the chunk.
  //
  // Makes concatenated record values available for reading from `reader()`.
  // Sets `limits` to sorted record end positions.
  //
  // `*src` is not owned by this `SimpleDecoder` and must be kept alive but not
  // accessed until closing the `SimpleDecoder`.
  //
  // Return values:
  //  * `true`  - success (`ok()`)
  //  * `false` - failure (`!ok()`)
  bool Decode(Reader* src, uint64_t num_records, uint64_t decoded_data_size,
              std::vector<size_t>& limits);

  // Returns the `Reader` from which concatenated record values should be read.
  //
  // Precondition: `ok()`
  Reader& reader();

  // Verifies that the concatenated record values end at the current position,
  // failing the `SimpleDecoder` if not. Closes the `SimpleDecoder`.
  //
  // Return values:
  //  * `true`  - success (concatenated messages end at the former current
  //              position)
  //  * `false` - failure (concatenated messages do not end at the former
  //              current position or the `SimpleDecoder` was not OK before
  //              closing)
  bool VerifyEndAndClose();

 protected:
  void Done() override;

 private:
  RecyclingPoolOptions recycling_pool_options_;
  chunk_encoding_internal::Decompressor<> values_decompressor_;
};

// Implementation details follow.

inline Reader& SimpleDecoder::reader() {
  RIEGELI_ASSERT(ok()) << "Failed precondition of SimpleDecoder::reader(): "
                       << status();
  return values_decompressor_.reader();
}

}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_SIMPLE_DECODER_H_
