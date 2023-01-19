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

#include "riegeli/chunk_encoding/chunk_decoder.h"

#include <stddef.h>
#include <stdint.h>

#include <string>
#include <utility>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/chain_backward_writer.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/chunk_encoding/constants.h"
#include "riegeli/chunk_encoding/field_projection.h"
#include "riegeli/chunk_encoding/simple_decoder.h"
#include "riegeli/chunk_encoding/transpose_decoder.h"
#include "riegeli/messages/message_parse.h"

namespace riegeli {

void ChunkDecoder::Done() { recoverable_ = false; }

bool ChunkDecoder::Decode(const Chunk& chunk) {
  Clear();
  ChainReader<> data_reader(&chunk.data);
  if (ABSL_PREDICT_FALSE(chunk.header.num_records() > limits_.max_size())) {
    return Fail(absl::ResourceExhaustedError("Too many records"));
  }
  Chain values;
  if (ABSL_PREDICT_FALSE(!Parse(chunk.header, data_reader, values))) {
    limits_.clear();  // Ensure that `index() == num_records()`.
    return false;
  }
  RIEGELI_ASSERT_EQ(limits_.size(), chunk.header.num_records())
      << "Wrong number of record end positions";
  RIEGELI_ASSERT_EQ(limits_.empty() ? size_t{0} : limits_.back(), values.size())
      << "Wrong last record end position";
  if (chunk.header.num_records() == 0) {
    RIEGELI_ASSERT_EQ(values.size(), 0u) << "Wrong decoded data size";
  } else if (field_projection_.includes_all()) {
    RIEGELI_ASSERT_EQ(values.size(), chunk.header.decoded_data_size())
        << "Wrong decoded data size";
  } else {
    RIEGELI_ASSERT_LE(values.size(), chunk.header.decoded_data_size())
        << "Wrong decoded data size";
  }
  values_reader_.Reset(std::move(values));
  return true;
}

inline bool ChunkDecoder::Parse(const ChunkHeader& header, Reader& src,
                                Chain& dest) {
  switch (header.chunk_type()) {
    case ChunkType::kFileSignature:
      if (ABSL_PREDICT_FALSE(header.data_size() != 0)) {
        return Fail(absl::InvalidArgumentError(absl::StrCat(
            "Invalid file signature chunk: data size is not zero: ",
            header.data_size())));
      }
      if (ABSL_PREDICT_FALSE(header.num_records() != 0)) {
        return Fail(absl::InvalidArgumentError(absl::StrCat(
            "Invalid file signature chunk: number of records is not zero: ",
            header.num_records())));
      }
      if (ABSL_PREDICT_FALSE(header.decoded_data_size() != 0)) {
        return Fail(absl::InvalidArgumentError(absl::StrCat(
            "Invalid file signature chunk: decoded data size is not zero: ",
            header.decoded_data_size())));
      }
      return true;
    case ChunkType::kFileMetadata:
      if (ABSL_PREDICT_FALSE(header.num_records() != 0)) {
        return Fail(absl::InvalidArgumentError(absl::StrCat(
            "Invalid file metadata chunk: number of records is not zero: ",
            header.num_records())));
      }
      return true;
    case ChunkType::kPadding:
      if (ABSL_PREDICT_FALSE(header.num_records() != 0)) {
        return Fail(absl::InvalidArgumentError(absl::StrCat(
            "Invalid padding chunk: number of records is not zero: ",
            header.num_records())));
      }
      if (ABSL_PREDICT_FALSE(header.decoded_data_size() != 0)) {
        return Fail(absl::InvalidArgumentError(absl::StrCat(
            "Invalid padding chunk: decoded data size is not zero: ",
            header.decoded_data_size())));
      }
      return true;
    case ChunkType::kSimple: {
      SimpleDecoder simple_decoder;
      if (ABSL_PREDICT_FALSE(!simple_decoder.Decode(&src, header.num_records(),
                                                    header.decoded_data_size(),
                                                    limits_))) {
        return Fail(simple_decoder.status());
      }
      if (ABSL_PREDICT_FALSE(!simple_decoder.reader().Read(
              IntCast<size_t>(header.decoded_data_size()), dest))) {
        return Fail(simple_decoder.reader().StatusOrAnnotate(
            absl::InvalidArgumentError("Reading record values failed")));
      }
      if (ABSL_PREDICT_FALSE(!simple_decoder.VerifyEndAndClose())) {
        return Fail(simple_decoder.status());
      }
      if (ABSL_PREDICT_FALSE(!src.VerifyEndAndClose())) {
        return Fail(src.status());
      }
      return true;
    }
    case ChunkType::kTransposed: {
      TransposeDecoder transpose_decoder;
      ChainBackwardWriter<> dest_writer(&dest);
      if (field_projection_.includes_all()) {
        dest_writer.SetWriteSizeHint(header.decoded_data_size());
      }
      const bool decode_ok = transpose_decoder.Decode(
          header.num_records(), header.decoded_data_size(), field_projection_,
          src, dest_writer, limits_);
      if (ABSL_PREDICT_FALSE(!dest_writer.Close())) {
        return Fail(dest_writer.status());
      }
      if (ABSL_PREDICT_FALSE(!decode_ok)) {
        return Fail(transpose_decoder.status());
      }
      if (ABSL_PREDICT_FALSE(!src.VerifyEndAndClose())) {
        return Fail(src.status());
      }
      return true;
    }
  }
  if (header.num_records() == 0) {
    // Ignore chunks with no records, even if the type is unknown.
    return true;
  }
  return Fail(absl::UnimplementedError(absl::StrCat(
      "Unknown chunk type: ", static_cast<uint64_t>(header.chunk_type()))));
}

bool ChunkDecoder::ReadRecord(google::protobuf::MessageLite& record) {
  if (ABSL_PREDICT_FALSE(!ok() || index() == num_records())) return false;
  const size_t start = IntCast<size_t>(values_reader_.pos());
  const size_t limit = limits_[IntCast<size_t>(index_)];
  RIEGELI_ASSERT_LE(start, limit)
      << "Failed invariant of ChunkDecoder: record end positions not sorted";
  if (absl::Status status =
          ParseFromReaderWithLength(values_reader_, limit - start, record);
      ABSL_PREDICT_FALSE(!status.ok())) {
    if (!values_reader_.Seek(limit)) {
      RIEGELI_ASSERT_UNREACHABLE()
          << "Seeking record values failed: " << values_reader_.status();
    }
    recoverable_ = true;
    return Fail(std::move(status));
  }
  ++index_;
  return true;
}

bool ChunkDecoder::Recover() {
  if (!recoverable_) return false;
  RIEGELI_ASSERT(!ok()) << "Failed invariant of ChunkDecoder: "
                           "recovery applicable but ChunkDecoder OK";
  recoverable_ = false;
  MarkNotFailed();
  ++index_;
  return true;
}

}  // namespace riegeli
