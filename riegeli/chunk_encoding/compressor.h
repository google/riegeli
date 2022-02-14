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

#include <memory>
#include <utility>

#include "absl/status/status.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/chunk_encoding/compressor_options.h"

namespace riegeli {
namespace chunk_encoding_internal {

class Compressor : public Object {
 public:
  class TuningOptions {
   public:
    TuningOptions() noexcept {}

    // Exact uncompressed size, or `absl::nullopt` if unknown. This may improve
    // compression density and performance, and may cause the size to be stored
    // in the compressed stream header.
    //
    // If the pledged size turns out to not match reality, compression may fail.
    //
    // Default: `absl::nullopt`.
    TuningOptions& set_pledged_size(absl::optional<Position> pledged_size) & {
      pledged_size_ = pledged_size;
      return *this;
    }
    TuningOptions&& set_pledged_size(absl::optional<Position> pledged_size) && {
      return std::move(set_pledged_size(pledged_size));
    }
    absl::optional<Position> pledged_size() const { return pledged_size_; }

    // Expected uncompressed size, or `absl::nullopt` if unknown. This may
    // improve compression density and performance.
    //
    // If the size hint turns out to not match reality, nothing breaks.
    //
    // `pledged_size()`, if not `absl::nullopt`, overrides `size_hint()`.
    //
    // Default: `absl::nullopt`.
    TuningOptions& set_size_hint(absl::optional<Position> size_hint) & {
      size_hint_ = size_hint;
      return *this;
    }
    TuningOptions&& set_size_hint(absl::optional<Position> size_hint) && {
      return std::move(set_size_hint(size_hint));
    }
    absl::optional<Position> size_hint() const { return size_hint_; }

   private:
    absl::optional<Position> pledged_size_;
    absl::optional<Position> size_hint_;
  };

  // Creates a closed `Compressor`.
  explicit Compressor(Closed) noexcept : Object(kClosed) {}

  // Creates an empty `Compressor`.
  explicit Compressor(CompressorOptions compressor_options,
                      TuningOptions tuning_options = TuningOptions());

  Compressor(const Compressor&) = delete;
  Compressor& operator=(const Compressor&) = delete;

  // Resets the `Compressor` back to empty. Keeps compressor options unchanged.
  // Changes tuning options.
  void Clear(TuningOptions tuning_options);

  // Resets the `Compressor` back to empty. Keeps compressor options and tuning
  // options unchanged.
  void Clear();

  // Returns the `Writer` to which uncompressed data should be written.
  //
  // Precondition: `ok()`
  Writer& writer();

  // Writes compressed data to `dest`. Closes the `Compressor` on success.
  //
  // If `compressor_options.compression_type()` is not `kNone`, writes
  // uncompressed size as a varint before the data.
  //
  // Return values:
  //  * `true`  - success (`ok()`)
  //  * `false` - failure (`!ok()`)
  bool EncodeAndClose(Writer& dest);

  // Like `EncodeAndClose()`, but writes the compressed size as a varint before
  // anything else. The compressed size includes the length of the uncompressed
  // size.
  bool LengthPrefixedEncodeAndClose(Writer& dest);

 private:
  void Initialize();

  CompressorOptions compressor_options_;
  TuningOptions tuning_options_;
  Chain compressed_;
  std::unique_ptr<Writer> writer_;
};

// Implementation details follow.

inline Writer& Compressor::writer() {
  RIEGELI_ASSERT(ok()) << "Failed precondition of Compressor::writer(): "
                       << status();
  return *writer_;
}

}  // namespace chunk_encoding_internal
}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_COMPRESSOR_H_
