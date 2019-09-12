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
  if (src->available() == 0 && ABSL_PREDICT_FALSE(!src->healthy())) {
    Fail(*src);
    return;
  }
  // Uncompressed size is stored in up to 5 initial bytes.
  src->Pull(5);
  size_t uncompressed_size;
  if (ABSL_PREDICT_FALSE(!snappy::GetUncompressedLength(
          src->cursor(), src->available(), &uncompressed_size))) {
    uncompressed_size = 0;
  }
  ChainWriter<Chain> writer(
      std::forward_as_tuple(),
      ChainWriterBase::Options().set_size_hint(uncompressed_size));
  internal::ReaderSnappySource source(src);
  internal::WriterSnappySink sink(&writer);
  const bool ok = snappy::Uncompress(&source, &sink);
  if (ABSL_PREDICT_FALSE(!writer.Close())) {
    Fail(writer);
    return;
  }
  if (ABSL_PREDICT_FALSE(!src->healthy())) {
    Fail(*src);
    return;
  }
  if (ABSL_PREDICT_FALSE(!ok)) {
    Fail(DataLossError("Invalid snappy-compressed stream"));
    return;
  }
  // SnappyReaderBase derives from ChainReader<Chain> but the Chain to read from
  // was not known in SnappyReaderBase constructor. This sets the Chain and
  // updates the ChainReader to read from it.
  ChainReader::Reset(std::move(writer.dest()));
}

}  // namespace riegeli
