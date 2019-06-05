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

#ifndef RIEGELI_CHUNK_ENCODING_COMPRESSOR_H_
#define RIEGELI_CHUNK_ENCODING_COMPRESSOR_H_

#include <utility>

#include "absl/types/optional.h"
#include "absl/types/variant.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/brotli_writer.h"
#include "riegeli/bytes/chain_writer.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/bytes/zstd_writer.h"
#include "riegeli/chunk_encoding/compressor_options.h"

namespace riegeli {
namespace internal {

class Compressor : public Object {
 public:
  class TuningOptions {
   public:
    TuningOptions() noexcept {}

    // Exact uncompressed size. This may improve compression density and
    // performance, and may cause the size to be stored in the compressed stream
    // header.
    //
    // If the size hint turns out to not match reality, compression may fail.
    TuningOptions& set_final_size(absl::optional<Position> final_size) & {
      final_size_ = final_size;
      return *this;
    }
    TuningOptions&& set_final_size(absl::optional<Position> final_size) && {
      return std::move(set_final_size(final_size));
    }

    // Expected uncompressed size, or 0 if unknown. This may improve compression
    // density and performance.
    //
    // If the size hint turns out to not match reality, nothing breaks.
    //
    // set_final_size() overrides set_size_hint().
    TuningOptions& set_size_hint(Position size_hint) & {
      size_hint_ = size_hint;
      return *this;
    }
    TuningOptions&& set_size_hint(Position size_hint) && {
      return std::move(set_size_hint(size_hint));
    }

   private:
    friend class Compressor;

    absl::optional<Position> final_size_;
    Position size_hint_ = 0;
  };

  // Creates a closed Compressor.
  Compressor() noexcept : Object(kInitiallyClosed) {}

  // Creates an empty Compressor.
  explicit Compressor(CompressorOptions compressor_options,
                      TuningOptions tuning_options = TuningOptions());

  Compressor(const Compressor&) = delete;
  Compressor& operator=(const Compressor&) = delete;

  // Resets the Compressor back to empty. Changes tuning options.
  void Reset(TuningOptions tuning_options);

  // Resets the Compressor back to empty. Keeps tuning options unchanged.
  void Reset();

  // Returns the Writer to which uncompressed data should be written.
  //
  // Precondition: healthy()
  Writer* writer();

  // Writes compressed data to *dest. Closes the Compressor.
  //
  // If options.compression_type() is not kNone, writes uncompressed size as a
  // varint before the data.
  //
  // Return values:
  //  * true  - success (healthy())
  //  * false - failure (!healthy())
  bool EncodeAndClose(Writer* dest);

 private:
  CompressorOptions compressor_options_;
  TuningOptions tuning_options_;
  Chain compressed_;
  // Invariant:
  //   options_.compression_type() is consistent with
  //       the active member of writer_
  absl::variant<ChainWriter<>, BrotliWriter<ChainWriter<>>,
                ZstdWriter<ChainWriter<>>>
      writer_;
};

// Implementation details follow.

inline Writer* Compressor::writer() {
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of Compressor::writer(): " << status();
  return absl::visit([](Writer& writer) { return &writer; }, writer_);
}

}  // namespace internal
}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_COMPRESSOR_H_
