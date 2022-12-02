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

#include <stddef.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdlib>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/flags/usage.h"
#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/options_parser.h"
#include "riegeli/bytes/fd_reader.h"
#include "riegeli/bytes/fd_writer.h"
#include "riegeli/bytes/std_io.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/lines/line_writing.h"
#include "riegeli/lines/text_writer.h"
#include "riegeli/records/chunk_reader.h"
#include "riegeli/records/record_reader.h"
#include "riegeli/records/record_writer.h"
#include "riegeli/records/tools/tfrecord_recognizer.h"
#include "riegeli/varint/varint_writing.h"
#include "tensorflow/core/lib/io/compression.h"
#include "tensorflow/core/lib/io/record_reader.h"
#include "tensorflow/core/lib/io/record_writer.h"
#include "tensorflow/core/platform/env.h"
#include "tensorflow/core/platform/errors.h"
#include "tensorflow/core/platform/file_system.h"
#include "tensorflow/core/platform/status.h"
#include "tensorflow/core/platform/tstring.h"

ABSL_FLAG(std::string, tfrecord_benchmarks, "uncompressed gzip",
          "Whitespace-separated TFRecord RecordWriter/RecordReader options");
ABSL_FLAG(std::string, riegeli_benchmarks,
          "uncompressed "
          "brotli:0 "
          "brotli:6 "
          "brotli:6,parallelism:10 "
          "brotli:9 "
          "zstd:1 "
          "zstd:3 "
          "zstd:15 "
          "snappy "
          "transpose,uncompressed "
          "transpose,brotli:6 "
          "transpose,brotli:6,parallelism:10 "
          "transpose,zstd:3 "
          "transpose,snappy",
          "Whitespace-separated Riegeli RecordWriter options");
ABSL_FLAG(uint64_t, max_size, uint64_t{100} * 1000 * 1000,
          "Maximum size of records to read, in bytes");
ABSL_FLAG(std::string, output_dir, "/tmp",
          "Directory to write files to (files are named record_benchmark_*)");
ABSL_FLAG(int32_t, repetitions, 5, "Number of times to repeat each benchmark");

namespace {

class SizeLimiter {
 public:
  explicit SizeLimiter(size_t limit);

  bool Accept(size_t size);

 private:
  size_t limit_;
  size_t remaining_;
};

SizeLimiter::SizeLimiter(size_t limit) : limit_(limit), remaining_(limit) {}

bool SizeLimiter::Accept(size_t size) {
  if (ABSL_PREDICT_TRUE(size < remaining_)) {
    remaining_ -= size;
    return true;
  }
  if (remaining_ == limit_) {
    // Return at least one item.
    remaining_ = 0;
    return true;
  }
  return false;
}

uint64_t FileSize(const std::string& filename) {
  struct stat stat_info;
  RIEGELI_CHECK_EQ(stat(filename.c_str(), &stat_info), 0)
      << absl::ErrnoToStatus(errno, "stat() failed").message();
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
  std::nth_element(samples_.begin(),
                   samples_.begin() + riegeli::IntCast<ptrdiff_t>(middle),
                   samples_.end());
  return samples_[middle];
}

class Benchmarks {
 public:
  static bool ReadFile(absl::string_view filename,
                       std::vector<std::string>* records,
                       SizeLimiter* size_limiter, riegeli::Writer& report);

  explicit Benchmarks(std::vector<std::string> records, std::string output_dir,
                      int repetitions);

  void RegisterTFRecord(absl::string_view tfrecord_options);
  void RegisterRiegeli(absl::string_view riegeli_options);

  void RunAll(riegeli::Writer& report);

 private:
  static void WriteTFRecord(
      absl::string_view filename,
      const tensorflow::io::RecordWriterOptions& record_writer_options,
      absl::Span<const std::string> records);
  static bool ReadTFRecord(
      absl::string_view filename,
      const tensorflow::io::RecordReaderOptions& record_reader_options,
      std::vector<std::string>* records, SizeLimiter* size_limiter = nullptr);

  static void WriteRiegeli(
      absl::string_view filename,
      riegeli::RecordWriterBase::Options record_writer_options,
      absl::Span<const std::string> records);
  static bool ReadRiegeli(
      absl::string_view filename,
      riegeli::RecordReaderBase::Options record_reader_options,
      std::vector<std::string>* records, SizeLimiter* size_limiter = nullptr);

  void RunOne(
      absl::string_view name,
      absl::FunctionRef<void(absl::string_view, absl::Span<const std::string>)>
          write_records,
      absl::FunctionRef<void(absl::string_view, std::vector<std::string>*)>
          read_records,
      riegeli::Writer& report);

  static std::string Filename(absl::string_view name);

  std::vector<std::string> records_;
  size_t original_size_;
  std::string output_dir_;
  int repetitions_;
  std::vector<std::pair<std::string, const char*>> tfrecord_benchmarks_;
  std::vector<std::pair<std::string, riegeli::RecordWriterBase::Options>>
      riegeli_benchmarks_;
  int max_name_width_ = 0;
};

bool Benchmarks::ReadFile(absl::string_view filename,
                          std::vector<std::string>* records,
                          SizeLimiter* size_limiter, riegeli::Writer& report) {
  riegeli::FdReader<> file_reader(filename);
  if (ABSL_PREDICT_FALSE(!file_reader.ok())) {
    riegeli::StdErr errors;
    riegeli::WriteLine("Could not open file: ", file_reader.status().ToString(),
                       errors);
    errors.Close();
    std::exit(1);
  }
  {
    riegeli::TFRecordRecognizer tfrecord_recognizer(&file_reader);
    tensorflow::io::RecordReaderOptions record_reader_options;
    if (tfrecord_recognizer.CheckFileFormat(record_reader_options)) {
      RIEGELI_CHECK(tfrecord_recognizer.Close())
          << tfrecord_recognizer.status();
      RIEGELI_CHECK(file_reader.Close()) << file_reader.status();
      riegeli::WriteLine("Reading TFRecord: ", filename, report);
      report.Flush();
      return ReadTFRecord(filename, record_reader_options, records,
                          size_limiter);
    }
  }
  RIEGELI_CHECK(file_reader.Seek(0)) << file_reader.status();
  {
    riegeli::DefaultChunkReader<> chunk_reader(&file_reader);
    if (chunk_reader.CheckFileFormat()) {
      RIEGELI_CHECK(chunk_reader.Close()) << chunk_reader.status();
      RIEGELI_CHECK(file_reader.Close()) << file_reader.status();
      riegeli::WriteLine("Reading Riegeli/records: ", filename, report);
      report.Flush();
      return ReadRiegeli(filename, riegeli::RecordReaderBase::Options(),
                         records, size_limiter);
    }
  }
  riegeli::StdErr errors;
  riegeli::WriteLine("Unknown file format: ", filename, errors);
  errors.Close();
  std::exit(1);
}

void Benchmarks::WriteTFRecord(
    absl::string_view filename,
    const tensorflow::io::RecordWriterOptions& record_writer_options,
    absl::Span<const std::string> records) {
  tensorflow::Env* const env = tensorflow::Env::Default();
  std::unique_ptr<tensorflow::WritableFile> file_writer;
  {
    const tensorflow::Status status =
        env->NewWritableFile(std::string(filename), &file_writer);
    RIEGELI_CHECK(status.ok()) << status;
  }
  tensorflow::io::RecordWriter record_writer(file_writer.get(),
                                             record_writer_options);
  for (const absl::string_view record : records) {
    const tensorflow::Status status = record_writer.WriteRecord(record);
    RIEGELI_CHECK(status.ok()) << status;
  }
  const tensorflow::Status status = record_writer.Close();
  RIEGELI_CHECK(status.ok()) << status;
}

bool Benchmarks::ReadTFRecord(
    absl::string_view filename,
    const tensorflow::io::RecordReaderOptions& record_reader_options,
    std::vector<std::string>* records, SizeLimiter* size_limiter) {
  tensorflow::Env* const env = tensorflow::Env::Default();
  std::unique_ptr<tensorflow::RandomAccessFile> file_reader;
  {
    const tensorflow::Status status =
        env->NewRandomAccessFile(std::string(filename), &file_reader);
    RIEGELI_CHECK(status.ok()) << status;
  }
  tensorflow::io::SequentialRecordReader record_reader(file_reader.get(),
                                                       record_reader_options);
  tensorflow::tstring record;
  for (;;) {
    const tensorflow::Status status = record_reader.ReadRecord(&record);
    if (!status.ok()) {
      RIEGELI_CHECK(tensorflow::errors::IsOutOfRange(status)) << status;
      break;
    }
    if (size_limiter != nullptr &&
        ABSL_PREDICT_FALSE(!size_limiter->Accept(
            riegeli::LengthVarint64(record.size()) + record.size()))) {
      return false;
    }
    records->push_back(std::move(record));
  }
  return true;
}

void Benchmarks::WriteRiegeli(
    absl::string_view filename,
    riegeli::RecordWriterBase::Options record_writer_options,
    absl::Span<const std::string> records) {
  riegeli::RecordWriter<riegeli::FdWriter<>> record_writer(
      std::forward_as_tuple(filename), std::move(record_writer_options));
  for (const absl::string_view record : records) {
    RIEGELI_CHECK(record_writer.WriteRecord(record)) << record_writer.status();
  }
  RIEGELI_CHECK(record_writer.Close()) << record_writer.status();
}

bool Benchmarks::ReadRiegeli(
    absl::string_view filename,
    riegeli::RecordReaderBase::Options record_reader_options,
    std::vector<std::string>* records, SizeLimiter* size_limiter) {
  riegeli::RecordReader<riegeli::FdReader<>> record_reader(
      std::forward_as_tuple(filename), std::move(record_reader_options));
  std::string record;
  while (record_reader.ReadRecord(record)) {
    if (size_limiter != nullptr &&
        ABSL_PREDICT_FALSE(!size_limiter->Accept(
            riegeli::LengthVarint64(record.size()) + record.size()))) {
      return false;
    }
    records->push_back(std::move(record));
  }
  RIEGELI_CHECK(record_reader.Close()) << record_reader.status();
  return true;
}

std::string Benchmarks::Filename(absl::string_view name) {
  std::string filename(name);
  for (char& ch : filename) {
    if (!(ch == '-' || ch == '.' || (ch >= '0' && ch <= '9') ||
          (ch >= 'A' && ch <= 'Z') || ch == '_' || (ch >= 'a' && ch <= 'z'))) {
      ch = '_';
    }
  }
  return filename;
}

Benchmarks::Benchmarks(std::vector<std::string> records, std::string output_dir,
                       int repetitions)
    : records_(std::move(records)),
      original_size_(0),
      output_dir_(std::move(output_dir)),
      repetitions_(repetitions) {
  for (const absl::string_view record : records_) {
    original_size_ += riegeli::LengthVarint64(record.size()) + record.size();
  }
}

void Benchmarks::RegisterTFRecord(absl::string_view tfrecord_options) {
  max_name_width_ =
      std::max(max_name_width_,
               riegeli::IntCast<int>(absl::string_view("tfrecord ").size() +
                                     tfrecord_options.size()));
  const char* compression = tensorflow::io::compression::kNone;
  riegeli::OptionsParser options_parser;
  options_parser.AddOption(
      "uncompressed",
      riegeli::ValueParser::And(
          riegeli::ValueParser::FailIfSeen("gzip"),
          riegeli::ValueParser::Empty(tensorflow::io::compression::kNone,
                                      &compression)));
  options_parser.AddOption(
      "gzip", riegeli::ValueParser::And(
                  riegeli::ValueParser::FailIfSeen("uncompressed"),
                  riegeli::ValueParser::Empty(
                      tensorflow::io::compression::kGzip, &compression)));
  RIEGELI_CHECK(options_parser.FromString(tfrecord_options))
      << options_parser.status();
  tfrecord_benchmarks_.emplace_back(tfrecord_options, compression);
}

void Benchmarks::RegisterRiegeli(absl::string_view riegeli_options) {
  max_name_width_ =
      std::max(max_name_width_,
               riegeli::IntCast<int>(absl::string_view("riegeli ").size() +
                                     riegeli_options.size()));
  riegeli::RecordWriterBase::Options options;
  RIEGELI_CHECK_EQ(options.FromString(riegeli_options), absl::OkStatus());
  riegeli_benchmarks_.emplace_back(riegeli_options, std::move(options));
}

void Benchmarks::RunAll(riegeli::Writer& report) {
  absl::Format(&report, "Original uncompressed size: %.3f MB",
               static_cast<double>(original_size_) / 1000000.0);
  WriteLine(report);
  WriteLine("Creating files ", output_dir_, "/record_benchmark_*", report);
  absl::Format(&report, "%-*s", max_name_width_, "");
  WriteLine("  Compr.    Write       Read", report);
  absl::Format(&report, "%-*s", max_name_width_, "");
  WriteLine("  ratio    CPU Real   CPU Real", report);
  absl::Format(&report, "%-*s", max_name_width_, "Format");
  WriteLine("    %     MB/s MB/s  MB/s MB/s", report);
  report.WriteChars(riegeli::IntCast<size_t>(max_name_width_ + 30), '-');
  WriteLine(report);

  for (const std::pair<std::string, const char*>& tfrecord_options :
       tfrecord_benchmarks_) {
    RunOne(
        absl::StrCat("tfrecord ", tfrecord_options.first),
        [&](absl::string_view filename, absl::Span<const std::string> records) {
          WriteTFRecord(
              filename,
              tensorflow::io::RecordWriterOptions::CreateRecordWriterOptions(
                  tfrecord_options.second),
              records);
        },
        [&](absl::string_view filename, std::vector<std::string>* records) {
          return ReadTFRecord(
              filename,
              tensorflow::io::RecordReaderOptions::CreateRecordReaderOptions(
                  tfrecord_options.second),
              records);
        },
        report);
  }
  for (const std::pair<std::string, riegeli::RecordWriterBase::Options>&
           riegeli_options : riegeli_benchmarks_) {
    RunOne(
        absl::StrCat("riegeli ", riegeli_options.first),
        [&](absl::string_view filename, absl::Span<const std::string> records) {
          WriteRiegeli(filename, riegeli_options.second, records);
        },
        [&](absl::string_view filename, std::vector<std::string>* records) {
          return ReadRiegeli(filename, riegeli::RecordReaderBase::Options(),
                             records);
        },
        report);
  }
}

void Benchmarks::RunOne(
    absl::string_view name,
    absl::FunctionRef<void(absl::string_view, absl::Span<const std::string>)>
        write_records,
    absl::FunctionRef<void(absl::string_view, std::vector<std::string>*)>
        read_records,
    riegeli::Writer& report) {
  absl::Format(&report, "%-*s ", max_name_width_, name);
  report.Flush();
  const std::string filename =
      absl::StrCat(output_dir_, "/record_benchmark_", Filename(name));

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

  absl::Format(&report, "%7.3f", compression.Median());
  for (const std::array<Stats*, 2>& stats_cpu_real :
       {std::array<Stats*, 2>{{&writing_cpu_speed, &writing_real_speed}},
        std::array<Stats*, 2>{{&reading_cpu_speed, &reading_real_speed}}}) {
    report.Write(' ');
    for (Stats* const stats : stats_cpu_real) {
      absl::Format(&report, " %4.0f", stats->Median());
    }
  }
  riegeli::WriteLine(report);
}

const char kUsage[] =
    "Usage: records_benchmark (OPTION|FILE)...\n"
    "\n"
    "FILEs may be TFRecord or Riegeli/records files.\n";

template <typename Function>
void ForEachWord(absl::string_view words, Function&& f) {
  for (const absl::string_view word :
       absl::StrSplit(words, absl::ByAnyChar("\t\n "), absl::SkipEmpty())) {
    f(word);
  }
}

}  // namespace

int main(int argc, char** argv) {
  absl::SetProgramUsageMessage(kUsage);
  const std::vector<char*> args = absl::ParseCommandLine(argc, argv);
  std::vector<std::string> records;
  if (args.size() <= 1) {
    riegeli::TextWriter<riegeli::WriteNewline::kNative, riegeli::StdErr>
        std_err(std::forward_as_tuple());
    std_err.Write(kUsage, '\n');
    std_err.Close();
    return 1;
  }
  riegeli::StdOut std_out;
  SizeLimiter size_limiter(
      riegeli::IntCast<size_t>(absl::GetFlag(FLAGS_max_size)));
  for (size_t i = 1; i < args.size(); ++i) {
    if (!Benchmarks::ReadFile(args[i], &records, &size_limiter, std_out)) break;
  }
  Benchmarks benchmarks(std::move(records), absl::GetFlag(FLAGS_output_dir),
                        absl::GetFlag(FLAGS_repetitions));
  ForEachWord(absl::GetFlag(FLAGS_tfrecord_benchmarks),
              [&](absl::string_view tfrecord_options) {
                benchmarks.RegisterTFRecord(tfrecord_options);
              });
  ForEachWord(absl::GetFlag(FLAGS_riegeli_benchmarks),
              [&](absl::string_view riegeli_options) {
                benchmarks.RegisterRiegeli(riegeli_options);
              });
  benchmarks.RunAll(std_out);
  std_out.Close();
}
