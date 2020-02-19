// Copyright 2019 Google LLC
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

#include "riegeli/bytes/snappy_reader.h"

#include <stddef.h>

#include <tuple>
#include <utility>

#include "absl/base/optimization.h"
#include "riegeli/base/base.h"
#include "riegeli/base/canonical_errors.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/chain_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/snappy_streams.h"
#include "snappy.h"

namespace riegeli {

void SnappyReaderBase::Initialize(Reader* src) {
  RIEGELI_ASSERT(src != nullptr)
      << "Failed precondition of SnappyReader: null Reader pointer";
  if (ABSL_PREDICT_FALSE(!src->healthy()) && src->available() == 0) {
    Fail(*src);
    return;
  }
  size_t decompressed_size;
  if (ABSL_PREDICT_FALSE(!SnappyDecompressedSize(src, &decompressed_size))) {
    decompressed_size = 0;
  }
  Chain decompressed;
  {
    Status status = SnappyDecompress<Reader*, ChainWriter<>>(
        src, std::forward_as_tuple(
                 &decompressed,
                 ChainWriterBase::Options().set_size_hint(decompressed_size)));
    if (ABSL_PREDICT_FALSE(!status.ok())) {
      Fail(std::move(status));
      return;
    }
  }
  // `SnappyReaderBase` derives from `ChainReader<Chain>` but the `Chain` to
  // read from was not known in `SnappyReaderBase` constructor. This sets the
  // `Chain` and updates the `ChainReader` to read from it.
  ChainReader::Reset(std::move(decompressed));
}

namespace internal {

Status SnappyDecompressImpl(Reader* src, Writer* dest) {
  ReaderSnappySource source(src);
  WriterSnappySink sink(dest);
  const bool ok = snappy::Uncompress(&source, &sink);
  if (ABSL_PREDICT_FALSE(!dest->healthy())) return dest->status();
  if (ABSL_PREDICT_FALSE(!src->healthy())) return src->status();
  if (ABSL_PREDICT_FALSE(!ok)) {
    return DataLossError("Invalid snappy-compressed stream");
  }
  return OkStatus();
}

}  // namespace internal

bool SnappyDecompressedSize(Reader* src, size_t* size) {
  // Decompressed size is stored in up to 5 initial bytes.
  src->Pull(5);
  return snappy::GetUncompressedLength(src->cursor(), src->available(), size);
}

}  // namespace riegeli
