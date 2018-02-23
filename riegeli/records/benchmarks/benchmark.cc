// Copyright 2018 Google LLC
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

// Make file offsets 64-bit even on 32-bit systems.
#undef _FILE_OFFSET_BITS
#define _FILE_OFFSET_BITS 64

#include <getopt.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <algorithm>
#include <array>
#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "riegeli/base/base.h"
#include "riegeli/base/options_parser.h"
#include "riegeli/base/str_cat.h"
#include "riegeli/base/str_error.h"
#include "riegeli/bytes/fd_reader.h"
#include "riegeli/bytes/fd_writer.h"
#include "riegeli/bytes/writer_utils.h"
#include "riegeli/records/benchmarks/tfrecord_recognizer.h"
#include "riegeli/records/chunk_reader.h"
#include "riegeli/records/record_reader.h"
#include "riegeli/records/record_writer.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/status.h"
#include "tensorflow/core/lib/io/compression.h"
#include "tensorflow/core/lib/io/record_reader.h"
#include "tensorflow/core/lib/io/record_writer.h"
#include "tensorflow/core/platform/env.h"
#include "tensorflow/core/platform/file_system.h"

namespace {

uint64_t FileSize(const std::string& filename) {
  struct stat stat_info;
  const int result = stat(filename.c_str(), &stat_info);
  RIEGELI_CHECK_EQ(result, 0) << "stat() failed: " << riegeli::StrError(errno);
  return riegeli::IntCast<uint64_t>(stat_info.st_size);
}

uint64_t CpuTimeNow_ns() {
  struct timespec time_info;
  RIEGELI_CHECK_EQ(clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &time_info), 0);
  return riegeli::IntCast<uint64_t>(time_info.tv_sec) * uint64_t{1000000000} +
         riegeli::IntCast<uint64_t>(time_info.tv_nsec);
}

uint64_t RealTimeNow_ns() {
  struct timespec time_info;
  RIEGELI_CHECK_EQ(clock_gettime(CLOCK_MONOTONIC, &time_info), 0);
  return riegeli::IntCast<uint64_t>(time_info.tv_sec) * uint64_t{1000000000} +
         riegeli::IntCast<uint64_t>(time_info.tv_nsec);
}

class Stats {
 public:
  void Add(double value);

  double Median();

 private:
  std::vector<double> samples_;
};

void Stats::Add(double value) { samples_.push_back(value); }

double Stats::Median() {
  RIEGELI_CHECK(!samples_.empty()) << "No data";
  const size_t middle = samples_.size() / 2;
  std::nth_element(samples_.begin(), samples_.begin() + middle, samples_.end());
  return samples_[middle];
}

class Benchmarks {
 public:
  static bool ReadFile(const std::string& filename, std::vector<std::string>* records,
                       size_t* max_size);

  Benchmarks(std::vector<std::string> records, std::string output_dir, int repetitions);

  void RegisterTFRecord(std::string tfrecord_options);
  void RegisterRiegeli(std::string riegeli_options);

  void RunAll();

 private:
  static void WriteTFRecord(
      const std::string& filename,
      const tensorflow::io::RecordWriterOptions& record_writer_options,
      const std::vector<std::string>& records);
  static bool ReadTFRecord(
      const std::string& filename,
      const tensorflow::io::RecordReaderOptions& record_reader_options,
      std::vector<std::string>* records, size_t* max_size = nullptr);

  static void WriteRiegeli(const std::string& filename,
                           riegeli::RecordWriter::Options record_writer_options,
                           const std::vector<std::string>& records);
  static bool ReadRiegeli(const std::string& filename,
                          riegeli::RecordReader::Options record_reader_options,
                          std::vector<std::string>* records,
                          size_t* max_size = nullptr);

  void RunOne(
      const std::string& name,
      std::function<void(const std::string&, const std::vector<std::string>&)>
          write_records,
      std::function<void(const std::string&, std::vector<std::string>*)> read_records);

  static std::string Filename(std::string name);

  std::vector<std::string> records_;
  size_t original_size_;
  std::string output_dir_;
  int repetitions_;
  std::vector<std::pair<std::string, const char*>> tfrecord_benchmarks_;
  std::vector<std::pair<std::string, riegeli::RecordWriter::Options>>
      riegeli_benchmarks_;
  int max_name_width_ = 0;
};

bool Benchmarks::ReadFile(const std::string& filename, std::vector<std::string>* records,
                          size_t* max_size) {
  riegeli::FdReader file_reader(filename, O_RDONLY);
  {
    riegeli::TFRecordDetector tfrecord_recognizer(&file_reader);
    tensorflow::io::RecordReaderOptions record_reader_options;
    if (tfrecord_recognizer.CheckFileFormat(&record_reader_options)) {
      RIEGELI_CHECK(tfrecord_recognizer.Close())
          << tfrecord_recognizer.Message();
      RIEGELI_CHECK(file_reader.Close()) << file_reader.Message();
      std::cout << "Reading TFRecord: " << filename << std::endl;
      return ReadTFRecord(filename, record_reader_options, records, max_size);
    }
  }
  RIEGELI_CHECK(file_reader.Seek(0)) << file_reader.Message();
  {
    riegeli::ChunkReader chunk_reader(&file_reader);
    if (chunk_reader.CheckFileFormat()) {
      RIEGELI_CHECK(chunk_reader.Close()) << chunk_reader.Message();
      RIEGELI_CHECK(file_reader.Close()) << file_reader.Message();
      std::cout << "Reading Riegeli/records: " << filename << std::endl;
      return ReadRiegeli(filename, riegeli::RecordReader::Options(), records,
                         max_size);
    }
  }
  std::cerr << "Unknown file format: " << filename << std::endl;
  std::exit(1);
}

void Benchmarks::WriteTFRecord(
    const std::string& filename,
    const tensorflow::io::RecordWriterOptions& record_writer_options,
    const std::vector<std::string>& records) {
  tensorflow::Env* const env = tensorflow::Env::Default();
  std::unique_ptr<tensorflow::WritableFile> file_writer;
  {
    const tensorflow::Status status =
        env->NewWritableFile(filename, &file_writer);
    RIEGELI_CHECK(status.ok()) << status;
  }
  tensorflow::io::RecordWriter record_writer(file_writer.get(),
                                             record_writer_options);
  for (const auto& record : records) {
    const tensorflow::Status status = record_writer.WriteRecord(record);
    RIEGELI_CHECK(status.ok()) << status;
  }
  const tensorflow::Status status = record_writer.Close();
  RIEGELI_CHECK(status.ok()) << status;
}

bool Benchmarks::ReadTFRecord(
    const std::string& filename,
    const tensorflow::io::RecordReaderOptions& record_reader_options,
    std::vector<std::string>* records, size_t* max_size) {
  size_t max_size_storage = std::numeric_limits<size_t>::max();
  if (max_size == nullptr) max_size = &max_size_storage;
  tensorflow::Env* const env = tensorflow::Env::Default();
  std::unique_ptr<tensorflow::RandomAccessFile> file_reader;
  {
    const tensorflow::Status status =
        env->NewRandomAccessFile(filename, &file_reader);
    RIEGELI_CHECK(status.ok()) << status;
  }
  tensorflow::io::SequentialRecordReader record_reader(file_reader.get(),
                                                       record_reader_options);
  std::string record;
  for (;;) {
    const tensorflow::Status status = record_reader.ReadRecord(&record);
    if (!status.ok()) {
      RIEGELI_CHECK(tensorflow::errors::IsOutOfRange(status)) << status;
      break;
    }
    const size_t memory =
        riegeli::LengthVarint64(record.size()) + record.size();
    if (RIEGELI_UNLIKELY(*max_size < memory)) return false;
    *max_size -= memory;
    records->push_back(std::move(record));
  }
  return true;
}

void Benchmarks::WriteRiegeli(
    const std::string& filename,
    riegeli::RecordWriter::Options record_writer_options,
    const std::vector<std::string>& records) {
  riegeli::FdWriter file_writer(filename, O_WRONLY | O_CREAT | O_TRUNC);
  riegeli::RecordWriter record_writer(&file_writer, record_writer_options);
  for (const auto& record : records) {
    RIEGELI_CHECK(record_writer.WriteRecord(record)) << record_writer.Message();
  }
  RIEGELI_CHECK(record_writer.Close()) << record_writer.Message();
  RIEGELI_CHECK(file_writer.Close()) << file_writer.Message();
}

bool Benchmarks::ReadRiegeli(
    const std::string& filename,
    riegeli::RecordReader::Options record_reader_options,
    std::vector<std::string>* records, size_t* max_size) {
  size_t max_size_storage = std::numeric_limits<size_t>::max();
  if (max_size == nullptr) max_size = &max_size_storage;
  riegeli::FdReader file_reader(filename, O_RDONLY);
  riegeli::RecordReader record_reader(&file_reader, record_reader_options);
  std::string record;
  while (record_reader.ReadRecord(&record)) {
    const size_t memory =
        riegeli::LengthVarint64(record.size()) + record.size();
    if (RIEGELI_UNLIKELY(*max_size < memory)) return false;
    *max_size -= memory;
    records->push_back(std::move(record));
  }
  RIEGELI_CHECK(record_reader.Close()) << record_reader.Message();
  RIEGELI_CHECK(file_reader.Close()) << file_reader.Message();
  return true;
}

std::string Benchmarks::Filename(std::string name) {
  for (char ch : name) {
    if (!(ch == '-' || ch == '.' || (ch >= '0' && ch <= '9') ||
          (ch >= 'A' && ch <= 'Z') || ch == '_' || (ch >= 'a' && ch <= 'z'))) {
      ch = '_';
    }
  }
  return name;
}

Benchmarks::Benchmarks(std::vector<std::string> records, std::string output_dir,
                       int repetitions)
    : records_(std::move(records)),
      original_size_(0),
      output_dir_(std::move(output_dir)),
      repetitions_(repetitions) {
  for (const auto& record : records_) {
    original_size_ += riegeli::LengthVarint64(record.size()) + record.size();
  }
}

void Benchmarks::RegisterTFRecord(std::string tfrecord_options) {
  max_name_width_ =
      std::max(max_name_width_,
               riegeli::IntCast<int>(
                   riegeli::StrCat("tfrecord ", tfrecord_options).size()));
  const char* compression = tensorflow::io::compression::kNone;
  std::string message;
  RIEGELI_CHECK(riegeli::ParseOptions(
      {
          {"uncompressed",
           riegeli::EnumOption(&compression,
                               {{"", tensorflow::io::compression::kNone}})},
          {"gzip",
           riegeli::EnumOption(&compression,
                               {{"", tensorflow::io::compression::kGzip}})},
      },
      tfrecord_options, &message))
      << message;
  tfrecord_benchmarks_.emplace_back(std::move(tfrecord_options), compression);
}

void Benchmarks::RegisterRiegeli(std::string riegeli_options) {
  max_name_width_ =
      std::max(max_name_width_,
               riegeli::IntCast<int>(
                   riegeli::StrCat("riegeli ", riegeli_options).size()));
  riegeli::RecordWriter::Options options;
  std::string message;
  RIEGELI_CHECK(options.Parse(riegeli_options, &message)) << message;
  riegeli_benchmarks_.emplace_back(std::move(riegeli_options),
                                   std::move(options));
}

void Benchmarks::RunAll() {
  std::cout << "Original size: " << std::fixed << std::setprecision(3)
            << (static_cast<double>(original_size_) / 1000000.0) << " MB"
            << std::endl;
  std::cout << std::left << std::setw(max_name_width_) << ""
            << " Compr.    Write       Read" << std::endl;
  std::cout << std::left << std::setw(max_name_width_) << ""
            << " ratio    CPU Real   CPU Real" << std::endl;
  std::cout << std::setw(max_name_width_) << "Format"
            << "   %     MB/s MB/s  MB/s MB/s" << std::endl;
  std::cout << std::setfill('-') << std::setw(max_name_width_ + 30) << ""
            << std::setfill(' ') << std::endl;

  for (const auto& tfrecord_options : tfrecord_benchmarks_) {
    RunOne(riegeli::StrCat("tfrecord ", tfrecord_options.first),
           [&](const std::string& filename, const std::vector<std::string>& records) {
             WriteTFRecord(
                 filename,
                 tensorflow::io::RecordWriterOptions::CreateRecordWriterOptions(
                     tfrecord_options.second),
                 records);
           },
           [&](const std::string& filename, std::vector<std::string>* records) {
             return ReadTFRecord(
                 filename,
                 tensorflow::io::RecordReaderOptions::CreateRecordReaderOptions(
                     tfrecord_options.second),
                 records);
           });
  }
  for (const auto& riegeli_options : riegeli_benchmarks_) {
    RunOne(riegeli::StrCat("riegeli ", riegeli_options.first),
           [&](const std::string& filename, const std::vector<std::string>& records) {
             WriteRiegeli(filename, riegeli_options.second, records);
           },
           [&](const std::string& filename, std::vector<std::string>* records) {
             return ReadRiegeli(filename, riegeli::RecordReader::Options(),
                                records);
           });
  }
}

void Benchmarks::RunOne(
    const std::string& name,
    std::function<void(const std::string&, const std::vector<std::string>&)>
        write_records,
    std::function<void(const std::string&, std::vector<std::string>*)> read_records) {
  const std::string filename =
      riegeli::StrCat(output_dir_, "/record_benchmark_", Filename(name));

  Stats compression;
  Stats writing_cpu_speed;
  Stats writing_real_speed;
  Stats reading_cpu_speed;
  Stats reading_real_speed;
  for (int i = 0; i < repetitions_ + 1; ++i) {
    const uint64_t cpu_time_before_ns = CpuTimeNow_ns();
    const uint64_t real_time_before_ns = RealTimeNow_ns();
    write_records(filename, records_);
    const uint64_t cpu_time_after_ns = CpuTimeNow_ns();
    const uint64_t real_time_after_ns = RealTimeNow_ns();
    if (i == 0) {
      // Warm-up.
    } else {
      compression.Add(static_cast<double>(FileSize(filename)) /
                      static_cast<double>(original_size_) * 100.0);
      writing_cpu_speed.Add(
          static_cast<double>(original_size_) /
          static_cast<double>(cpu_time_after_ns - cpu_time_before_ns) * 1000.0);
      writing_real_speed.Add(
          static_cast<double>(original_size_) /
          static_cast<double>(real_time_after_ns - real_time_before_ns) *
          1000.0);
    }
  }
  for (int i = 0; i < repetitions_ + 1; ++i) {
    std::vector<std::string> decoded_records;
    const uint64_t cpu_time_before_ns = CpuTimeNow_ns();
    const uint64_t real_time_before_ns = RealTimeNow_ns();
    read_records(filename, &decoded_records);
    const uint64_t cpu_time_after_ns = CpuTimeNow_ns();
    const uint64_t real_time_after_ns = RealTimeNow_ns();
    if (i == 0) {
      // Warm-up and correctness check.
      RIEGELI_CHECK(decoded_records == records_)
          << "Decoded records do not match for " << name;
    } else {
      reading_cpu_speed.Add(
          static_cast<double>(original_size_) /
          static_cast<double>(cpu_time_after_ns - cpu_time_before_ns) * 1000.0);
      reading_real_speed.Add(
          static_cast<double>(original_size_) /
          static_cast<double>(real_time_after_ns - real_time_before_ns) *
          1000.0);
    }
  }

  std::cout << std::left << std::setw(max_name_width_) << name << ' '
            << std::right << std::setw(6) << std::fixed << std::setprecision(2)
            << compression.Median();
  for (const auto& stats_cpu_real :
       {std::array<Stats*, 2>{&writing_cpu_speed, &writing_real_speed},
        std::array<Stats*, 2>{&reading_cpu_speed, &reading_real_speed}}) {
    std::cout << ' ';
    for (const auto& stats : stats_cpu_real) {
      std::cout << ' ' << std::right << std::setw(4) << std::setprecision(0)
                << stats->Median();
    }
  }
  std::cout << std::endl;
}

const char kUsage[] =
    "Usage: benchmark (OPTION|FILE)...\n"
    "\n"
    "FILEs may be TFRecord or Riegeli/records files.\n"
    "\n"
    "OPTIONs:\n"
    "  --tfrecord_benchmarks=BENCHMARKS\n"
    "      Whitespace-separated TFRecord RecordWriter/RecordReader options\n"
    "  --riegeli_benchmarks=BENCHMARKS\n"
    "      Whitespace-separated Riegeli RecordWriter options\n"
    "  --max_size=BYTES\n"
    "      Maximum size of records to read, in bytes, default 10000000\n"
    "  --output_dir=DIR\n"
    "      Directory to write files to (files are named record_benchmark_*),\n"
    "      default /tmp\n"
    "  --repetitions=N\n"
    "      Number of times to repeat each benchmark, default 5";

const struct option kOptions[] = {
    {"help", no_argument, nullptr, 0},
    {"tfrecord_benchmarks", required_argument, nullptr, 1},
    {"riegeli_benchmarks", required_argument, nullptr, 2},
    {"max_size", required_argument, nullptr, 3},
    {"output_dir", required_argument, nullptr, 4},
    {"repetitions", required_argument, nullptr, 5},
    {nullptr, 0, nullptr, 0}};

template <typename Function>
void ForEachWord(const std::string& words, Function f) {
  std::stringstream in(words);
  std::string word;
  while (in >> word) f(std::move(word));
}

}  // namespace

int main(int argc, char** argv) {
  std::string tfrecord_benchmarks = "uncompressed gzip";
  std::string riegeli_benchmarks =
      "uncompressed "
      "brotli:6 "
      "brotli:6,parallelism:10 "
      "zstd:9 "
      "transpose,uncompressed "
      "transpose,brotli:6 "
      "transpose,brotli:6,parallelism:10 "
      "transpose,zstd:9";
  size_t max_size = size_t{100} * 1000 * 1000;
  std::string output_dir = "/tmp";
  int repetitions = 5;
  for (;;) {
    int option_index;
    const int option =
        getopt_long_only(argc, argv, "", kOptions, &option_index);
    if (option == -1) break;
    switch (option) {
      case 0:  // --help
        std::cout << kUsage << std::endl;
        return 0;
      case 1:  // --tfrecord_benchmarks
        tfrecord_benchmarks = optarg;
        break;
      case 2:  // --riegeli_benchmarks
        riegeli_benchmarks = optarg;
        break;
      case 3:  // --max_size
        if (RIEGELI_LIKELY(*optarg != '\0')) {
          errno = 0;
          char* end;
          max_size = riegeli::IntCast<size_t>(std::strtoul(optarg, &end, 10));
          if (RIEGELI_LIKELY(errno == 0 && *end == '\0')) break;
        }
        std::cerr << argv[0]
                  << ": option '--max_size' requires an integer argument\n";
        return 1;
      case 4:  // --output_dir
        output_dir = std::string(optarg);
        break;
      case 5:  // --repetitions
        if (RIEGELI_LIKELY(*optarg != '\0')) {
          errno = 0;
          char* end;
          repetitions = riegeli::IntCast<int>(std::strtol(optarg, &end, 10));
          if (RIEGELI_LIKELY(errno == 0 && *end == '\0')) break;
        }
        std::cerr << argv[0]
                  << ": option '--repetitions' requires an integer argument\n";
        return 1;
      case '?':
        return 1;
      default:
        RIEGELI_ASSERT_UNREACHABLE()
            << "getopt_long_only() returned " << option;
    }
  }
  argc -= optind - 1;
  argv += optind - 1;
  std::vector<std::string> records;
  if (argc == 1) {
    std::cerr << kUsage << std::endl;
    return 1;
  }
  for (int i = 1; i < argc; ++i) {
    if (!Benchmarks::ReadFile(argv[i], &records, &max_size)) break;
  }
  Benchmarks benchmarks(std::move(records), std::move(output_dir), repetitions);
  ForEachWord(tfrecord_benchmarks, [&](std::string tfrecord_options) {
    benchmarks.RegisterTFRecord(std::move(tfrecord_options));
  });
  ForEachWord(riegeli_benchmarks, [&](std::string riegeli_options) {
    benchmarks.RegisterRiegeli(std::move(riegeli_options));
  });
  benchmarks.RunAll();
}
