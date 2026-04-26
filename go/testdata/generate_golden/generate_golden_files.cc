// Generates golden .riegeli files for Go compatibility tests.
//
// Usage: generate_golden_files <output_directory>

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "riegeli/bytes/fd_writer.h"
#include "riegeli/records/record_writer.h"

namespace {

using riegeli::RecordWriter;
using riegeli::RecordWriterBase;

bool WriteFile(absl::string_view path, absl::string_view options_text,
               const std::vector<std::string>& records) {
  RecordWriterBase::Options options;
  absl::Status status = options.FromString(options_text);
  if (!status.ok()) {
    std::cerr << "Invalid options '" << options_text << "': " << status
              << std::endl;
    return false;
  }
  RecordWriter<riegeli::FdWriter<>> writer(
      riegeli::FdWriter<>(std::string(path)), std::move(options));
  for (const auto& rec : records) {
    if (!writer.WriteRecord(rec)) {
      std::cerr << "WriteRecord failed: " << writer.status() << std::endl;
      return false;
    }
  }
  if (!writer.Close()) {
    std::cerr << "Close failed for " << path << ": " << writer.status()
              << std::endl;
    return false;
  }
  return true;
}

}  // namespace

int main(int argc, char* argv[]) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <output_directory>" << std::endl;
    return 1;
  }
  std::string dir = argv[1];

  // 1. Empty file (signature only).
  if (!WriteFile(absl::StrCat(dir, "/empty.riegeli"), "uncompressed", {})) {
    return 1;
  }
  std::cerr << "Generated empty.riegeli" << std::endl;

  // 2. Single string, uncompressed.
  if (!WriteFile(absl::StrCat(dir, "/single_string_uncompressed.riegeli"),
                 "uncompressed", {"Hello"})) {
    return 1;
  }
  std::cerr << "Generated single_string_uncompressed.riegeli" << std::endl;

  // 3. Three strings, uncompressed.
  if (!WriteFile(absl::StrCat(dir, "/three_strings_uncompressed.riegeli"),
                 "uncompressed", {"a", "bc", "def"})) {
    return 1;
  }
  std::cerr << "Generated three_strings_uncompressed.riegeli" << std::endl;

  // 4-6. Multi-records with different compression.
  std::vector<std::string> multi_records;
  for (int i = 0; i < 23; i++) {
    std::string rec;
    for (int j = 0; j < 100; j++) {
      rec += "record_data_";
    }
    rec += std::string(1, 'A' + (i % 26));
    multi_records.push_back(rec);
  }

  if (!WriteFile(absl::StrCat(dir, "/multi_records_brotli.riegeli"), "brotli",
                 multi_records)) {
    return 1;
  }
  std::cerr << "Generated multi_records_brotli.riegeli" << std::endl;

  if (!WriteFile(absl::StrCat(dir, "/multi_records_zstd.riegeli"), "zstd",
                 multi_records)) {
    return 1;
  }
  std::cerr << "Generated multi_records_zstd.riegeli" << std::endl;

  if (!WriteFile(absl::StrCat(dir, "/multi_records_snappy.riegeli"), "snappy",
                 multi_records)) {
    return 1;
  }
  std::cerr << "Generated multi_records_snappy.riegeli" << std::endl;

  // 7. Block-spanning records (50KB each).
  std::vector<std::string> block_spanning;
  for (int i = 0; i < 5; i++) {
    std::string rec(50000, '\0');
    for (int j = 0; j < 50000; j++) {
      rec[j] = static_cast<char>((i * 50000 + j) % 256);
    }
    block_spanning.push_back(rec);
  }
  if (!WriteFile(absl::StrCat(dir, "/block_spanning.riegeli"), "uncompressed",
                 block_spanning)) {
    return 1;
  }
  std::cerr << "Generated block_spanning.riegeli" << std::endl;

  // 8. Empty records.
  std::vector<std::string> empty_records(10, "");
  if (!WriteFile(absl::StrCat(dir, "/empty_records.riegeli"), "uncompressed",
                 empty_records)) {
    return 1;
  }
  std::cerr << "Generated empty_records.riegeli" << std::endl;

  // 9. Large record (1MB).
  std::string large(1 << 20, '\0');
  for (int i = 0; i < (1 << 20); i++) {
    large[i] = static_cast<char>(i % 256);
  }
  if (!WriteFile(absl::StrCat(dir, "/large_record.riegeli"), "uncompressed",
                 {large})) {
    return 1;
  }
  std::cerr << "Generated large_record.riegeli" << std::endl;

  // 10. Many small records with small chunk size.
  {
    std::string path = absl::StrCat(dir, "/many_records.riegeli");
    RecordWriterBase::Options options;
    absl::Status status =
        options.FromString("uncompressed,chunk_size:4096");
    if (!status.ok()) {
      std::cerr << "Invalid options: " << status << std::endl;
      return 1;
    }
    RecordWriter<riegeli::FdWriter<>> writer(
        riegeli::FdWriter<>(path), std::move(options));
    for (int i = 0; i < 1000; i++) {
      std::string rec(i % 100, 'x');
      if (!writer.WriteRecord(rec)) {
        std::cerr << "WriteRecord failed: " << writer.status() << std::endl;
        return 1;
      }
    }
    if (!writer.Close()) {
      std::cerr << "Close failed: " << writer.status() << std::endl;
      return 1;
    }
    std::cerr << "Generated many_records.riegeli" << std::endl;
  }

  // 11. Multi-records uncompressed (same data as brotli version).
  if (!WriteFile(absl::StrCat(dir, "/multi_records_uncompressed.riegeli"),
                 "uncompressed", multi_records)) {
    return 1;
  }
  std::cerr << "Generated multi_records_uncompressed.riegeli" << std::endl;

  // 12. Transposed non-proto records with brotli compression.
  // When transpose:true is set, non-proto records use the kNonProto path.
  if (!WriteFile(absl::StrCat(dir, "/transposed_nonproto_brotli.riegeli"),
                 "transpose,brotli", {"alpha", "beta", "gamma", "delta"})) {
    return 1;
  }
  std::cerr << "Generated transposed_nonproto_brotli.riegeli" << std::endl;

  // 13. Transposed non-proto records with zstd.
  if (!WriteFile(absl::StrCat(dir, "/transposed_nonproto_zstd.riegeli"),
                 "transpose,zstd", {"one", "two", "three"})) {
    return 1;
  }
  std::cerr << "Generated transposed_nonproto_zstd.riegeli" << std::endl;

  // 14. Transposed non-proto records uncompressed.
  if (!WriteFile(absl::StrCat(dir, "/transposed_nonproto_uncompressed.riegeli"),
                 "transpose,uncompressed",
                 {"Hello", "World", "from", "transposed", "chunk"})) {
    return 1;
  }
  std::cerr << "Generated transposed_nonproto_uncompressed.riegeli"
            << std::endl;

  // 15. Transposed with multi_records data (non-proto) + brotli.
  if (!WriteFile(absl::StrCat(dir, "/transposed_multi_brotli.riegeli"),
                 "transpose,brotli", multi_records)) {
    return 1;
  }
  std::cerr << "Generated transposed_multi_brotli.riegeli" << std::endl;

  // 16. Transposed proto-like records.
  // Hand-encode simple proto messages:
  //   message SimpleMessage { int32 id = 1; bytes payload = 2; }
  // Proto encoding:
  //   field 1 (varint): tag=0x08, value
  //   field 2 (length-delimited): tag=0x12, length, data
  {
    std::vector<std::string> proto_records;
    for (int i = 0; i < 10; i++) {
      std::string rec;
      // Field 1: id = i (varint)
      rec += '\x08';  // tag: field 1, wire type 0 (varint)
      rec += static_cast<char>(i);  // varint value (small enough for 1 byte)
      // Field 2: payload = "payload_XX"
      std::string payload = absl::StrCat("payload_", i);
      rec += '\x12';  // tag: field 2, wire type 2 (length-delimited)
      rec += static_cast<char>(payload.size());  // length varint
      rec += payload;
      proto_records.push_back(rec);
    }
    if (!WriteFile(absl::StrCat(dir, "/transposed_proto_brotli.riegeli"),
                   "transpose,brotli", proto_records)) {
      return 1;
    }
    std::cerr << "Generated transposed_proto_brotli.riegeli" << std::endl;
  }

  std::cerr << "All golden files generated successfully." << std::endl;
  return 0;
}
