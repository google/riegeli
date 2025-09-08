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

#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <optional>
#include <string>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/cleanup/cleanup.h"
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/flags/usage.h"
#include "absl/status/status.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "google/protobuf/repeated_ptr_field.h"
#include "riegeli/base/any.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/chain_backward_writer.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/fd_reader.h"
#include "riegeli/bytes/limiting_reader.h"
#include "riegeli/bytes/null_backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/std_io.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/chunk_encoding/constants.h"
#include "riegeli/chunk_encoding/decompressor.h"
#include "riegeli/chunk_encoding/field_projection.h"
#include "riegeli/chunk_encoding/transpose_decoder.h"
#include "riegeli/lines/line_writing.h"
#include "riegeli/lines/text_writer.h"
#include "riegeli/messages/parse_message.h"
#include "riegeli/messages/text_print_message.h"
#include "riegeli/records/chunk_reader.h"
#include "riegeli/records/records_metadata.pb.h"
#include "riegeli/records/skipped_region.h"
#include "riegeli/records/tools/riegeli_summary.pb.h"
#include "riegeli/varint/varint_reading.h"

ABSL_FLAG(bool, show_records_metadata, false,
          "If true, show parsed file metadata.");
ABSL_FLAG(bool, show_record_sizes, false,
          "If true, show the list of record sizes in each chunk.");
ABSL_FLAG(bool, show_records, false,
          "If true, show contents of records in each chunk.");

namespace riegeli::tools {
namespace {

absl::Status DescribeFileMetadataChunk(const Chunk& chunk,
                                       RecordsMetadata& records_metadata) {
  // Based on `RecordReaderBase::ParseMetadata()`.
  if (ABSL_PREDICT_FALSE(chunk.header.num_records() != 0)) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Invalid file metadata chunk: number of records is not zero: ",
        chunk.header.num_records()));
  }
  ChainReader data_reader(&chunk.data);
  TransposeDecoder transpose_decoder;
  ChainBackwardWriter serialized_metadata_writer;
  serialized_metadata_writer.SetWriteSizeHint(chunk.header.decoded_data_size());
  std::vector<size_t> limits;
  const bool decode_ok = transpose_decoder.Decode(
      1, chunk.header.decoded_data_size(), FieldProjection::All(), data_reader,
      serialized_metadata_writer, limits);
  if (ABSL_PREDICT_FALSE(!serialized_metadata_writer.Close())) {
    return serialized_metadata_writer.status();
  }
  if (ABSL_PREDICT_FALSE(!decode_ok)) return transpose_decoder.status();
  if (ABSL_PREDICT_FALSE(!data_reader.VerifyEndAndClose())) {
    return data_reader.status();
  }
  const Chain& serialized_metadata = serialized_metadata_writer.dest();
  RIEGELI_ASSERT_EQ(limits.size(), 1u)
      << "Metadata chunk has unexpected record limits";
  RIEGELI_ASSERT_EQ(limits.back(), serialized_metadata.size())
      << "Metadata chunk has unexpected record limits";
  return ParseMessage(serialized_metadata, records_metadata);
}

absl::Status ReadRecords(
    Reader& src, const std::vector<size_t>& limits,
    google::protobuf::RepeatedPtrField<std::string>& dest) {
  // Based on `ChunkDecoder::ReadRecord()`.
  const Position initial_pos = src.pos();
  for (const size_t limit : limits) {
    const size_t start = IntCast<size_t>(src.pos() - initial_pos);
    RIEGELI_ASSERT_LE(start, limit) << "Record end positions not sorted";
    if (ABSL_PREDICT_FALSE(!src.Read(limit - start, *dest.Add()))) {
      return src.StatusOrAnnotate(
          absl::InvalidArgumentError("Reading record failed"));
    }
  }
  return absl::OkStatus();
}

absl::Status DescribeSimpleChunk(const Chunk& chunk,
                                 summary::SimpleChunk& simple_chunk) {
  ChainReader src(&chunk.data);
  const bool show_record_sizes = absl::GetFlag(FLAGS_show_record_sizes);
  const bool show_records = absl::GetFlag(FLAGS_show_records);

  // Based on `SimpleDecoder::Decode()`.
  uint8_t compression_type_byte;
  if (ABSL_PREDICT_FALSE(!src.ReadByte(compression_type_byte))) {
    return absl::InvalidArgumentError("Reading compression type failed");
  }
  const CompressionType compression_type =
      static_cast<CompressionType>(compression_type_byte);
  simple_chunk.set_compression_type(
      static_cast<summary::CompressionType>(compression_type));

  if (show_record_sizes || show_records) {
    uint64_t sizes_size;
    if (ABSL_PREDICT_FALSE(!ReadVarint64(src, sizes_size))) {
      return absl::InvalidArgumentError("Reading size of sizes failed");
    }

    chunk_encoding_internal::Decompressor<LimitingReader<>> sizes_decompressor(
        riegeli::Maker(
            &src, LimitingReaderBase::Options().set_exact_length(sizes_size)),
        compression_type);
    if (ABSL_PREDICT_FALSE(!sizes_decompressor.ok())) {
      return sizes_decompressor.status();
    }
    if (show_record_sizes) {
      if (ABSL_PREDICT_FALSE(chunk.header.num_records() >
                             unsigned{std::numeric_limits<int>::max()})) {
        return absl::ResourceExhaustedError("Too many records");
      }
      simple_chunk.mutable_record_sizes()->Reserve(
          IntCast<int>(chunk.header.num_records()));
    }
    std::vector<size_t> limits;
    if (show_records) {
      if (ABSL_PREDICT_FALSE(chunk.header.num_records() > limits.max_size())) {
        return absl::ResourceExhaustedError("Too many records");
      }
      limits.reserve(IntCast<size_t>(chunk.header.num_records()));
    }
    size_t limit = 0;
    for (uint64_t i = 0; i < chunk.header.num_records(); ++i) {
      uint64_t size;
      if (ABSL_PREDICT_FALSE(
              !ReadVarint64(sizes_decompressor.reader(), size))) {
        return sizes_decompressor.reader().StatusOrAnnotate(
            absl::InvalidArgumentError("Reading record size failed"));
      }
      if (ABSL_PREDICT_FALSE(size > chunk.header.decoded_data_size() - limit)) {
        return absl::InvalidArgumentError(
            "Decoded data size larger than expected");
      }
      limit += IntCast<size_t>(size);
      if (show_record_sizes) simple_chunk.add_record_sizes(size);
      if (show_records) limits.push_back(limit);
    }
    if (ABSL_PREDICT_FALSE(!sizes_decompressor.VerifyEndAndClose())) {
      return sizes_decompressor.status();
    }
    if (ABSL_PREDICT_FALSE(limit != chunk.header.decoded_data_size())) {
      return absl::InvalidArgumentError(
          "Decoded data size smaller than expected");
    }

    if (show_records) {
      chunk_encoding_internal::Decompressor<> records_decompressor(
          &src, compression_type);
      if (ABSL_PREDICT_FALSE(!records_decompressor.ok())) {
        return records_decompressor.status();
      }
      if (absl::Status status =
              ReadRecords(records_decompressor.reader(), limits,
                          *simple_chunk.mutable_records());
          !status.ok()) {
        return status;
      }
      if (ABSL_PREDICT_FALSE(!records_decompressor.VerifyEndAndClose())) {
        return records_decompressor.status();
      }
      if (ABSL_PREDICT_FALSE(!src.VerifyEndAndClose())) return src.status();
    }
  }
  return absl::OkStatus();
}

absl::Status DescribeTransposedChunk(
    const Chunk& chunk, summary::TransposedChunk& transposed_chunk) {
  ChainReader src(&chunk.data);
  const bool show_record_sizes = absl::GetFlag(FLAGS_show_record_sizes);
  const bool show_records = absl::GetFlag(FLAGS_show_records);

  // Based on `TransposeDecoder::Decode()`.
  uint8_t compression_type_byte;
  if (ABSL_PREDICT_FALSE(!src.ReadByte(compression_type_byte))) {
    return absl::InvalidArgumentError("Reading compression type failed");
  }
  transposed_chunk.set_compression_type(
      static_cast<summary::CompressionType>(compression_type_byte));

  if (show_record_sizes || show_records) {
    // Based on `ChunkDecoder::Parse()`.
    src.Seek(0);
    TransposeDecoder transpose_decoder;
    Chain dest;
    Any<BackwardWriter*>::Inlining<ChainBackwardWriter<>, NullBackwardWriter>
        dest_writer;
    if (show_records) {
      dest_writer = riegeli::Maker<ChainBackwardWriter>(&dest);
    } else {
      dest_writer = riegeli::Maker<NullBackwardWriter>();
    }
    dest_writer->SetWriteSizeHint(chunk.header.decoded_data_size());
    std::vector<size_t> limits;
    const bool decode_ok = transpose_decoder.Decode(
        chunk.header.num_records(), chunk.header.decoded_data_size(),
        FieldProjection::All(), src, *dest_writer, limits);
    if (ABSL_PREDICT_FALSE(!dest_writer->Close())) return dest_writer->status();
    if (ABSL_PREDICT_FALSE(!decode_ok)) return transpose_decoder.status();
    if (show_record_sizes) {
      if (ABSL_PREDICT_FALSE(limits.size() >
                             unsigned{std::numeric_limits<int>::max()})) {
        return absl::ResourceExhaustedError("Too many records");
      }
      transposed_chunk.mutable_record_sizes()->Reserve(
          IntCast<int>(limits.size()));
      size_t prev_limit = 0;
      for (const size_t next_limit : limits) {
        RIEGELI_ASSERT_LE(prev_limit, next_limit)
            << "Failed postcondition of TransposeDecoder: "
               "record end positions not sorted";
        transposed_chunk.add_record_sizes(next_limit - prev_limit);
        prev_limit = next_limit;
      }
    }

    if (show_records) {
      ChainReader records_reader(&dest);
      if (absl::Status status = ReadRecords(
              records_reader, limits, *transposed_chunk.mutable_records());
          !status.ok()) {
        return status;
      }
      if (ABSL_PREDICT_FALSE(!records_reader.VerifyEndAndClose())) {
        return records_reader.status();
      }
    }
    if (ABSL_PREDICT_FALSE(!src.VerifyEndAndClose())) return src.status();
  }
  return absl::OkStatus();
}

void DescribeFile(absl::string_view filename, Writer& report) {
  WriteLine("file {", report);
  WriteLine("  filename: \"", absl::Utf8SafeCEscape(filename), '"', report);
  DefaultChunkReader chunk_reader(riegeli::Maker<FdReader>(filename));
  if (chunk_reader.SupportsRandomAccess()) {
    const std::optional<Position> size = chunk_reader.Size();
    if (size != std::nullopt) WriteLine("  file_size: ", *size, report);
  }
  TextPrintMessageOptions print_options;
  print_options.printer().SetInitialIndentLevel(2);
  print_options.printer().SetUseShortRepeatedPrimitives(true);
  print_options.printer().SetUseUtf8StringEscaping(true);
  for (;;) {
    report.Flush();

    const ChunkHeader* chunk_header;
    if (ABSL_PREDICT_FALSE(!chunk_reader.PullChunkHeader(&chunk_header))) {
      SkippedRegion skipped_region;
      if (chunk_reader.Recover(&skipped_region)) {
        WriteLine("  # FILE CORRUPTED: ", skipped_region.ToString(), report);
        continue;
      }
      break;
    }

    summary::Chunk chunk_summary;
    chunk_summary.set_chunk_begin(chunk_reader.pos());
    chunk_summary.set_chunk_type(
        static_cast<summary::ChunkType>(chunk_header->chunk_type()));
    chunk_summary.set_data_size(chunk_header->data_size());
    chunk_summary.set_num_records(chunk_header->num_records());
    chunk_summary.set_decoded_data_size(chunk_header->decoded_data_size());

    WriteLine("  chunk {", report);
    absl::Cleanup report_chunk = [&] {
      TextPrintMessage(chunk_summary, TextWriter(&report), print_options)
          .IgnoreError();
      WriteLine("  }", report);
    };

    Chunk chunk;
    if (ABSL_PREDICT_FALSE(!chunk_reader.ReadChunk(chunk))) {
      SkippedRegion skipped_region;
      if (chunk_reader.Recover(&skipped_region)) {
        WriteLine("    # FILE CORRUPTED: ", skipped_region.ToString(), report);
        continue;
      }
      break;
    }

    absl::Status status;
    switch (chunk_summary.chunk_type()) {
      case summary::FILE_METADATA:
        if (absl::GetFlag(FLAGS_show_records_metadata)) {
          status = DescribeFileMetadataChunk(
              chunk, *chunk_summary.mutable_file_metadata_chunk());
        }
        break;
      case summary::SIMPLE:
        status =
            DescribeSimpleChunk(chunk, *chunk_summary.mutable_simple_chunk());
        break;
      case summary::TRANSPOSED:
        status = DescribeTransposedChunk(
            chunk, *chunk_summary.mutable_transposed_chunk());
        break;
      default:
        break;
    }
    if (ABSL_PREDICT_FALSE(!status.ok())) {
      WriteLine("    # FILE CORRUPTED: ",
                Annotate(chunk_reader.AnnotateStatus(status),
                         absl::StrCat("at record ", chunk_summary.chunk_begin(),
                                      "/0"))
                    .message(),
                report);
    }
  }
  if (!chunk_reader.Close()) {
    WriteLine("  # FILE READ ERROR: ", chunk_reader.status().message(), report);
  }
  WriteLine('}', report);
  report.Flush();
}

const char kUsage[] =
    "Usage: describe_riegeli_file (OPTION|FILE)...\n"
    "\n"
    "Shows summary of Riegeli/records file contents.\n";

}  // namespace
}  // namespace riegeli::tools

int main(int argc, char** argv) {
  absl::SetProgramUsageMessage(riegeli::tools::kUsage);
  const std::vector<char*> args = absl::ParseCommandLine(argc, argv);
  riegeli::StdOut std_out;
  for (size_t i = 1; i < args.size(); ++i) {
    riegeli::tools::DescribeFile(args[i], std_out);
  }
  std_out.Close();
}
