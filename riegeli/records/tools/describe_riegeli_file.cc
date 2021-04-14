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

#include <fcntl.h>
#include <stddef.h>
#include <stdint.h>

#include <iostream>
#include <limits>
#include <string>
#include <tuple>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/flags/usage.h"
#include "absl/status/status.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "google/protobuf/io/zero_copy_stream_impl.h"
#include "google/protobuf/text_format.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/chain_backward_writer.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/fd_reader.h"
#include "riegeli/bytes/limiting_reader.h"
#include "riegeli/bytes/null_backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/chunk_encoding/constants.h"
#include "riegeli/chunk_encoding/decompressor.h"
#include "riegeli/chunk_encoding/field_projection.h"
#include "riegeli/chunk_encoding/transpose_decoder.h"
#include "riegeli/messages/message_parse.h"
#include "riegeli/records/chunk_reader.h"
#include "riegeli/records/records_metadata.pb.h"
#include "riegeli/records/skipped_region.h"
#include "riegeli/records/tools/riegeli_summary.pb.h"
#include "riegeli/varint/varint_reading.h"

ABSL_FLAG(bool, show_records_metadata, true,
          "If true, show parsed file metadata.");
ABSL_FLAG(bool, show_record_sizes, false,
          "If true, show the list of record sizes in each chunk.");

namespace riegeli {
namespace tools {
namespace {

absl::Status DescribeFileMetadataChunk(const Chunk& chunk,
                                       RecordsMetadata& records_metadata) {
  // Based on `RecordReaderBase::ParseMetadata()`.
  if (ABSL_PREDICT_FALSE(chunk.header.num_records() != 0)) {
    return absl::DataLossError(absl::StrCat(
        "Invalid file metadata chunk: number of records is not zero: ",
        chunk.header.num_records()));
  }
  ChainReader<> data_reader(&chunk.data);
  TransposeDecoder transpose_decoder;
  ChainBackwardWriter<Chain> serialized_metadata_writer(
      std::forward_as_tuple(), ChainBackwardWriterBase::Options().set_size_hint(
                                   chunk.header.decoded_data_size()));
  std::vector<size_t> limits;
  const bool ok = transpose_decoder.Decode(1, chunk.header.decoded_data_size(),
                                           FieldProjection::All(), data_reader,
                                           serialized_metadata_writer, limits);
  if (ABSL_PREDICT_FALSE(!serialized_metadata_writer.Close())) {
    return serialized_metadata_writer.status();
  }
  if (ABSL_PREDICT_FALSE(!ok)) return transpose_decoder.status();
  if (ABSL_PREDICT_FALSE(!data_reader.VerifyEndAndClose())) {
    return data_reader.status();
  }
  const Chain& serialized_metadata = serialized_metadata_writer.dest();
  RIEGELI_ASSERT_EQ(limits.size(), 1u)
      << "Metadata chunk has unexpected record limits";
  RIEGELI_ASSERT_EQ(limits.back(), serialized_metadata.size())
      << "Metadata chunk has unexpected record limits";
  return ParseFromChain(serialized_metadata, records_metadata);
}

absl::Status DescribeSimpleChunk(const Chunk& chunk,
                                 summary::SimpleChunk& simple_chunk) {
  // Based on `SimpleDecoder::Decode()`.
  ChainReader<> chunk_reader(&chunk.data);

  const absl::optional<uint8_t> compression_type_byte = chunk_reader.ReadByte();
  if (ABSL_PREDICT_FALSE(compression_type_byte == absl::nullopt)) {
    return absl::DataLossError("Reading compression type failed");
  }
  const CompressionType compression_type =
      static_cast<CompressionType>(*compression_type_byte);
  simple_chunk.set_compression_type(
      static_cast<summary::CompressionType>(compression_type));

  if (absl::GetFlag(FLAGS_show_record_sizes)) {
    const absl::optional<uint64_t> sizes_size = ReadVarint64(chunk_reader);
    if (ABSL_PREDICT_FALSE(sizes_size == absl::nullopt)) {
      return absl::DataLossError("Reading size of sizes failed");
    }

    if (ABSL_PREDICT_FALSE(*sizes_size > std::numeric_limits<Position>::max() -
                                             chunk_reader.pos())) {
      return absl::ResourceExhaustedError("Size of sizes too large");
    }
    internal::Decompressor<LimitingReader<>> sizes_decompressor(
        std::forward_as_tuple(&chunk_reader, chunk_reader.pos() + *sizes_size),
        compression_type);
    if (ABSL_PREDICT_FALSE(!sizes_decompressor.healthy())) {
      return sizes_decompressor.status();
    }
    while (IntCast<size_t>(simple_chunk.record_sizes_size()) <
           chunk.header.num_records()) {
      const absl::optional<uint64_t> size =
          ReadVarint64(sizes_decompressor.reader());
      if (ABSL_PREDICT_FALSE(size == absl::nullopt)) {
        sizes_decompressor.reader().Fail(
            absl::DataLossError("Reading record size failed"));
        return sizes_decompressor.reader().status();
      }
      simple_chunk.add_record_sizes(*size);
    }
    if (ABSL_PREDICT_FALSE(!sizes_decompressor.VerifyEndAndClose())) {
      return sizes_decompressor.status();
    }
  }
  return absl::OkStatus();
}

absl::Status DescribeTransposedChunk(
    const Chunk& chunk, summary::TransposedChunk& transposed_chunk) {
  // Based on `TransposeDecoder::Decode()`.
  ChainReader<> chunk_reader(&chunk.data);

  const absl::optional<uint8_t> compression_type_byte = chunk_reader.ReadByte();
  if (ABSL_PREDICT_FALSE(compression_type_byte == absl::nullopt)) {
    return absl::DataLossError("Reading compression type failed");
  }
  transposed_chunk.set_compression_type(
      static_cast<summary::CompressionType>(*compression_type_byte));

  if (absl::GetFlag(FLAGS_show_record_sizes)) {
    // Based on `ChunkDecoder::Parse()`.
    chunk_reader.Seek(0);
    TransposeDecoder transpose_decoder;
    NullBackwardWriter dest_writer(NullBackwardWriter::kInitiallyOpen);
    std::vector<size_t> limits;
    const bool ok = transpose_decoder.Decode(
        chunk.header.num_records(), chunk.header.decoded_data_size(),
        FieldProjection::All(), chunk_reader, dest_writer, limits);
    if (ABSL_PREDICT_FALSE(!dest_writer.Close())) return dest_writer.status();
    if (ABSL_PREDICT_FALSE(!ok)) return transpose_decoder.status();
    if (ABSL_PREDICT_FALSE(!chunk_reader.VerifyEndAndClose())) {
      return chunk_reader.status();
    }
    size_t prev_limit = 0;
    for (const size_t next_limit : limits) {
      RIEGELI_ASSERT_LE(prev_limit, next_limit)
          << "Failed postcondition of TransposeDecoder: "
             "record end positions not sorted";
      transposed_chunk.add_record_sizes(next_limit - prev_limit);
      prev_limit = next_limit;
    }
  }
  return absl::OkStatus();
}

void DescribeFile(absl::string_view filename, std::ostream& report) {
  absl::Format(&report,
               "file {\n"
               "  filename: \"%s\"\n",
               absl::Utf8SafeCEscape(filename));
  DefaultChunkReader<FdReader<>> chunk_reader(
      std::forward_as_tuple(filename, O_RDONLY));
  const absl::optional<Position> size = chunk_reader.Size();
  if (size != absl::nullopt) {
    absl::Format(&report, "  file_size: %u\n", *size);
  }
  google::protobuf::TextFormat::Printer printer;
  printer.SetInitialIndentLevel(2);
  printer.SetUseShortRepeatedPrimitives(true);
  printer.SetUseUtf8StringEscaping(true);
  for (;;) {
    report.flush();
    const Position chunk_begin = chunk_reader.pos();
    Chunk chunk;
    if (ABSL_PREDICT_FALSE(!chunk_reader.ReadChunk(chunk))) {
      SkippedRegion skipped_region;
      if (chunk_reader.Recover(&skipped_region)) {
        absl::Format(&std::cerr, "%s\n", skipped_region.message());
        continue;
      }
      break;
    }
    summary::Chunk chunk_summary;
    chunk_summary.set_chunk_begin(chunk_begin);
    chunk_summary.set_chunk_type(
        static_cast<summary::ChunkType>(chunk.header.chunk_type()));
    chunk_summary.set_data_size(chunk.header.data_size());
    chunk_summary.set_num_records(chunk.header.num_records());
    chunk_summary.set_decoded_data_size(chunk.header.decoded_data_size());
    {
      absl::Status status;
      switch (chunk.header.chunk_type()) {
        case ChunkType::kFileMetadata:
          if (absl::GetFlag(FLAGS_show_records_metadata)) {
            status = DescribeFileMetadataChunk(
                chunk, *chunk_summary.mutable_file_metadata_chunk());
          }
          break;
        case ChunkType::kSimple:
          status =
              DescribeSimpleChunk(chunk, *chunk_summary.mutable_simple_chunk());
          break;
        case ChunkType::kTransposed:
          status = DescribeTransposedChunk(
              chunk, *chunk_summary.mutable_transposed_chunk());
          break;
        default:
          break;
      }
      if (ABSL_PREDICT_FALSE(!status.ok())) {
        absl::Format(&std::cerr, "%s\n", status.message());
      }
    }
    absl::Format(&report, "  chunk {\n");
    google::protobuf::io::OstreamOutputStream proto_out(&report);
    printer.Print(chunk_summary, &proto_out);
    absl::Format(&report, "  }\n");
  }
  absl::Format(&report, "}\n");
  report.flush();
  if (!chunk_reader.Close()) {
    absl::Format(&std::cerr, "%s\n", chunk_reader.status().message());
  }
}

const char kUsage[] =
    "Usage: describe_riegeli_file (OPTION|FILE)...\n"
    "\n"
    "Shows summary of Riegeli/records file contents.\n";

}  // namespace
}  // namespace tools
}  // namespace riegeli

int main(int argc, char** argv) {
  absl::SetProgramUsageMessage(riegeli::tools::kUsage);
  const std::vector<char*> args = absl::ParseCommandLine(argc, argv);
  for (size_t i = 1; i < args.size(); ++i) {
    riegeli::tools::DescribeFile(args[i], std::cout);
  }
}
