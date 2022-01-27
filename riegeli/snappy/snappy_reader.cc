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

#include "riegeli/snappy/snappy_reader.h"

#include <stddef.h>

#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/chain_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/snappy/snappy_streams.h"
#include "snappy.h"

namespace riegeli {

void SnappyReaderBase::Initialize(Reader* src,
                                  absl::optional<Position> assumed_size) {
  RIEGELI_ASSERT(src != nullptr)
      << "Failed precondition of SnappyReader: null Reader pointer";
  if (ABSL_PREDICT_FALSE(!src->healthy()) && src->available() == 0) {
    FailWithoutAnnotation(AnnotateOverSrc(src->status()));
    return;
  }
  const absl::optional<size_t> uncompressed_size = SnappyUncompressedSize(*src);
  Chain decompressed;
  {
    absl::Status status = SnappyDecompress(
        *src,
        ChainWriter<>(&decompressed, ChainWriterBase::Options().set_size_hint(
                                         uncompressed_size)),
        SnappyDecompressOptions().set_assumed_size(assumed_size));
    if (ABSL_PREDICT_FALSE(!status.ok())) {
      FailWithoutAnnotation(std::move(status));
      return;
    }
  }
  // `SnappyReaderBase` derives from `ChainReader<Chain>` but the `Chain` to
  // read from was not known in `SnappyReaderBase` constructor. This sets the
  // `Chain` and updates the `ChainReader` to read from it.
  ChainReader::Reset(std::move(decompressed));
}

void SnappyReaderBase::Done() {
  ChainReader::Done();
  ChainReader::src() = Chain();
}

absl::Status SnappyReaderBase::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) {
    Reader& src = *src_reader();
    status = src.AnnotateStatus(std::move(status));
  }
  // The status might have been annotated by `*src->reader()` with the
  // compressed position. Clarify that the current position is the uncompressed
  // position instead of delegating to `ChainReader::AnnotateStatusImpl()`.
  return AnnotateOverSrc(std::move(status));
}

absl::Status SnappyReaderBase::AnnotateOverSrc(absl::Status status) {
  if (is_open()) {
    return Annotate(status, absl::StrCat("at uncompressed byte ", pos()));
  }
  return status;
}

namespace snappy_internal {

absl::Status SnappyDecompressImpl(Reader& src, Writer& dest,
                                  SnappyDecompressOptions options) {
  ReaderSnappySource source(&src, options.assumed_size());
  WriterSnappySink sink(&dest);
  const bool ok = snappy::Uncompress(&source, &sink);
  if (ABSL_PREDICT_FALSE(!dest.healthy())) return dest.status();
  if (ABSL_PREDICT_FALSE(!src.healthy())) return src.status();
  if (ABSL_PREDICT_FALSE(!ok)) {
    return Annotate(src.AnnotateStatus(absl::InvalidArgumentError(
                        "Invalid snappy-compressed stream")),
                    absl::StrCat("at uncompressed byte ", dest.pos()));
  }
  return absl::OkStatus();
}

}  // namespace snappy_internal

absl::optional<size_t> SnappyUncompressedSize(Reader& src) {
  // Uncompressed size is stored in up to 5 initial bytes.
  src.Pull(5);
  size_t size;
  if (ABSL_PREDICT_FALSE(!snappy::GetUncompressedLength(
          src.cursor(), src.available(), &size))) {
    return absl::nullopt;
  }
  return size;
}

}  // namespace riegeli
