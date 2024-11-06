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
#include <stdint.h>

#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/types/optional.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/chain_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/snappy/snappy_streams.h"
#include "riegeli/varint/varint_reading.h"
#include "snappy.h"

namespace riegeli {

void SnappyReaderBase::Initialize(Reader* src) {
  RIEGELI_ASSERT_NE(src, nullptr)
      << "Failed precondition of SnappyReader: null Reader pointer";
  if (ABSL_PREDICT_FALSE(!src->ok()) && src->available() == 0) {
    FailWithoutAnnotation(AnnotateOverSrc(src->status()));
    return;
  }
  Chain decompressed;
  {
    absl::Status status = SnappyDecompress(*src, ChainWriter<>(&decompressed));
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
    Reader& src = *SrcReader();
    status = src.AnnotateStatus(std::move(status));
  }
  // The status might have been annotated by `src` with the compressed position.
  // Clarify that the current position is the uncompressed position instead of
  // delegating to `ChainReader::AnnotateStatusImpl()`.
  return AnnotateOverSrc(std::move(status));
}

absl::Status SnappyReaderBase::AnnotateOverSrc(absl::Status status) {
  if (is_open()) {
    return Annotate(status, absl::StrCat("at uncompressed byte ", pos()));
  }
  return status;
}

namespace snappy_internal {

absl::Status SnappyDecompressImpl(Reader& src, Writer& dest) {
  const absl::optional<Position> size = src.Size();
  if (ABSL_PREDICT_FALSE(size == absl::nullopt)) return src.status();
  ReaderSnappySource source(&src, *size);
  WriterSnappySink sink(&dest);
  const bool uncompress_ok = snappy::Uncompress(&source, &sink);
  if (ABSL_PREDICT_FALSE(!dest.ok())) return dest.status();
  if (ABSL_PREDICT_FALSE(!src.ok())) return src.status();
  if (ABSL_PREDICT_FALSE(!uncompress_ok)) {
    return Annotate(src.AnnotateStatus(absl::InvalidArgumentError(
                        "Invalid snappy-compressed stream")),
                    absl::StrCat("at uncompressed byte ", dest.pos()));
  }
  return absl::OkStatus();
}

}  // namespace snappy_internal

absl::optional<size_t> SnappyUncompressedSize(Reader& src) {
  if (ABSL_PREDICT_FALSE(!src.Pull(1, kMaxLengthVarint32))) {
    return absl::nullopt;
  }
  if (ABSL_PREDICT_FALSE(src.available() < kMaxLengthVarint32)) {
    size_t length = 1;
    while (length < kMaxLengthVarint32 &&
           static_cast<uint8_t>(src.cursor()[length - 1]) >= 0x80) {
      ++length;
      if (ABSL_PREDICT_FALSE(!src.Pull(length, kMaxLengthVarint32))) {
        return absl::nullopt;
      }
    }
  }
  uint32_t size;
  const absl::optional<const char*> cursor =
      ReadVarint32(src.cursor(), src.limit(), size);
  if (ABSL_PREDICT_FALSE(cursor == absl::nullopt)) return absl::nullopt;
  return size;
}

}  // namespace riegeli
