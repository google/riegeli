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

#ifndef RIEGELI_RECORDS_RECORD_WRITER_H_
#define RIEGELI_RECORDS_RECORD_WRITER_H_

#include <stdint.h>
#include <future>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/call_once.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/chunk_encoding/compressor_options.h"
#include "riegeli/records/record_position.h"

namespace google {
namespace protobuf {
class MessageLite;
}  // namespace protobuf
}  // namespace google

namespace riegeli {

class ChunkEncoder;
class ChunkWriter;

// FutureRecordPosition is similar to shared_future<RecordPosition>.
//
// RecordWriter returns FutureRecordPosition instead of RecordPosition because
// with parallelism > 0 the actual position is not known until pending chunks
// finish encoding in background.
class FutureRecordPosition {
 public:
  constexpr FutureRecordPosition() noexcept {}

  explicit FutureRecordPosition(RecordPosition pos) noexcept;

  FutureRecordPosition(FutureRecordPosition&& src) noexcept;
  FutureRecordPosition& operator=(FutureRecordPosition&& src) noexcept;

  FutureRecordPosition(const FutureRecordPosition& src);
  FutureRecordPosition& operator=(const FutureRecordPosition& src);

  // May block if returned by RecordWriter with parallelism > 0.
  RecordPosition get() const;

 private:
  friend class RecordWriter;

  class FutureChunkBegin;

  explicit FutureRecordPosition(Position chunk_begin);

  FutureRecordPosition(
      Position pos_before_chunks,
      std::vector<std::shared_future<ChunkHeader>> chunk_headers);

  std::shared_ptr<FutureChunkBegin> future_chunk_begin_;
  // If future_chunk_begin_ == nullptr, chunk_begin_ is stored here, otherwise
  // it is future_chunk_begin_->get().
  Position chunk_begin_ = 0;
  uint64_t record_index_ = 0;
};

// RecordWriter writes records to a Riegeli/records file. A record is
// conceptually a binary string; usually it is a serialized proto message.
//
// For writing records, this kind of loop can be used:
//
//   SomeProto record;
//   while (more records to write) {
//     ... Compute record.
//     if (!record_writer_.Write(record)) {
//       // record_writer_.Close() will fail below.
//       break;
//     }
//   }
//   if (!record_writer_.Close()) {
//     ... Failed with reason: record_writer_.message()
//   }
class RecordWriter final : public Object {
 public:
  class Options {
   public:
    Options() noexcept {}

    // Parses options from text:
    //
    //   options ::= option? ("," option?)*
    //   option ::=
    //     "default" |
    //     "transpose" (":" ("true" | "false"))? |
    //     "uncompressed" |
    //     "brotli" (":" brotli_level)? |
    //     "zstd" (":" zstd_level)? |
    //     "window_log" ":" window_log |
    //     "chunk_size" ":" chunk_size |
    //     "bucket_fraction" ":" bucket_fraction |
    //     "parallelism" ":" parallelism
    //   brotli_level ::= integer 0..11 (default 9)
    //   zstd_level ::= integer 1..22 (default 9)
    //   window_log ::= "auto" or integer 10..31
    //   chunk_size ::=
    //     integer expressed as real with optional suffix [BkKMGTPE], 1..
    //   bucket_fraction ::= real 0..1
    //   parallelism ::= integer 0..
    //
    // Return values:
    //  * true  - success
    //  * false - failure (*message is set)
    bool Parse(absl::string_view text, std::string* message);

    // If true, records should be serialized proto messages (but nothing will
    // break if they are not). A chunk of records will be processed in a way
    // which allows for better compression.
    //
    // If false, a chunk of records will be stored in a simpler format, directly
    // or with compression.
    //
    // Default: false.
    Options& set_transpose(bool transpose) & {
      transpose_ = transpose;
      return *this;
    }
    Options&& set_transpose(bool transpose) && {
      return std::move(set_transpose(transpose));
    }

    // Changes compression algorithm to none.
    Options& set_uncompressed() & {
      compressor_options_.set_uncompressed();
      return *this;
    }
    Options&& set_uncompressed() && { return std::move(set_uncompressed()); }

    // Changes compression algorithm to Brotli. Sets compression level which
    // tunes the tradeoff between compression density and compression speed
    // (higher = better density but slower).
    //
    // compression_level must be between kMinBrotli() (0) and kMaxBrotli() (11).
    // Default: kDefaultBrotli() (9).
    //
    // This is the default compression algorithm.
    static constexpr int kMinBrotli() {
      return CompressorOptions::kMinBrotli();
    }
    static constexpr int kMaxBrotli() {
      return CompressorOptions::kMaxBrotli();
    }
    static constexpr int kDefaultBrotli() {
      return CompressorOptions::kDefaultBrotli();
    }
    Options& set_brotli(int compression_level = kDefaultBrotli()) & {
      compressor_options_.set_brotli(compression_level);
      return *this;
    }
    Options&& set_brotli(int compression_level = kDefaultBrotli()) && {
      return std::move(set_brotli(compression_level));
    }

    // Changes compression algorithm to Zstd. Sets compression level which tunes
    // the tradeoff between compression density and compression speed (higher =
    // better density but slower).
    //
    // compression_level must be between kMinZstd() (1) and kMaxZstd() (22).
    // Default: kDefaultZstd() (9).
    static constexpr int kMinZstd() { return CompressorOptions::kMinZstd(); }
    static int kMaxZstd() { return CompressorOptions::kMaxZstd(); }
    static constexpr int kDefaultZstd() {
      return CompressorOptions::kDefaultZstd();
    }
    Options& set_zstd(int compression_level = kDefaultZstd()) & {
      compressor_options_.set_zstd(compression_level);
      return *this;
    }
    Options&& set_zstd(int compression_level = kDefaultZstd()) && {
      return std::move(set_zstd(compression_level));
    }

    // Logarithm of the LZ77 sliding window size. This tunes the tradeoff
    // between compression density and memory usage (higher = better density but
    // more memory).
    //
    // Special value kDefaultWindowLog() (-1) means to keep the default
    // (brotli: 22, zstd: derived from compression level and chunk size).
    //
    // For uncompressed, window_log must be kDefaultWindowLog() (-1).
    //
    // For brotli, window_log must be kDefaultWindowLog() (-1) or between
    // BrotliWriter::Options::kMinWindowLog() (10) and
    // BrotliWriter::Options::kMaxWindowLog() (30).
    //
    // For zstd, window_log must be kDefaultWindowLog() (-1) or between
    // ZstdWriter::Options::kMinWindowLog() (10) and
    // ZstdWriter::Options::kMaxWindowLog() (30 in 32-bit build, 31 in 64-bit
    // build).
    //
    // Default: kDefaultWindowLog() (-1).
    static int kMinWindowLog() { return CompressorOptions::kMinWindowLog(); }
    static int kMaxWindowLog() { return CompressorOptions::kMaxWindowLog(); }
    static constexpr int kDefaultWindowLog() {
      return CompressorOptions::kDefaultWindowLog();
    }
    Options& set_window_log(int window_log) & {
      compressor_options_.set_window_log(window_log);
      return *this;
    }
    Options&& set_window_log(int window_log) && {
      return std::move(set_window_log(window_log));
    }

    // Sets the desired uncompressed size of a chunk which groups messages to be
    // transposed, compressed, and written together.
    //
    // A larger chunk size improves compression density; a smaller chunk size
    // allows to read pieces of the file independently with finer granularity,
    // and reduces memory usage of both writer and reader.
    //
    // Default: 1 << 20
    Options& set_chunk_size(uint64_t size) & {
      RIEGELI_ASSERT_GT(size, 0u)
          << "Failed precondition of RecordWriter::Options::set_chunk_size(): "
             "zero chunk size";
      chunk_size_ = size;
      return *this;
    }
    Options&& set_chunk_size(uint64_t size) && {
      return std::move(set_chunk_size(size));
    }

    // Sets the desired uncompressed size of a bucket which groups values of
    // several fields of the given wire type to be compressed together,
    // relatively to the desired chunk size, on the scale between 0.0 (compress
    // each field separately) to 1.0 (put all fields of the same wire type in
    // the same bucket).
    //
    // This is meaningful if transpose and compression are enabled. A larger
    // bucket size improves compression density; a smaller bucket size makes
    // reading with filtering faster, allowing to skip decompression of values
    // of fields which are filtered out.
    //
    // Default: 1.0
    Options& set_bucket_fraction(double fraction) & {
      RIEGELI_ASSERT_GE(fraction, 0.0)
          << "Failed precondition of "
             "RecordWriter::Options::set_bucket_fraction(): "
             "negative bucket fraction";
      RIEGELI_ASSERT_LE(fraction, 1.0)
          << "Failed precondition of "
             "RecordWriter::Options::set_bucket_fraction(): "
             "fraction larger than 1";
      bucket_fraction_ = fraction;
      return *this;
    }
    Options&& set_bucket_fraction(double fraction) && {
      return std::move(set_bucket_fraction(fraction));
    }

    // Sets the maximum number of chunks being encoded in parallel in
    // background. Larger parallelism can increase throughput, up to a point
    // where it no longer matters; smaller parallelism reduces memory usage.
    //
    // If parallelism > 0, chunks are written to the byte Writer in background
    // and reporting writing errors is delayed.
    //
    // Default: 0
    Options& set_parallelism(int parallelism) & {
      RIEGELI_ASSERT_GE(parallelism, 0)
          << "Failed precondition of RecordWriter::Options::set_parallelism(): "
             "negative parallelism";
      parallelism_ = parallelism;
      return *this;
    }
    Options&& set_parallelism(int parallelism) && {
      return std::move(set_parallelism(parallelism));
    }

   private:
    friend class RecordWriter;

    bool transpose_ = false;
    CompressorOptions compressor_options_;
    uint64_t chunk_size_ = uint64_t{1} << 20;
    double bucket_fraction_ = 1.0;
    int parallelism_ = 0;
  };

  // Creates a closed RecordWriter.
  RecordWriter() noexcept;

  // Will write records to the byte Writer which is owned by this RecordWriter
  // and will be closed and deleted when the RecordWriter is closed.
  explicit RecordWriter(std::unique_ptr<Writer> byte_writer,
                        Options options = Options());

  // Will write records to the byte Writer which is not owned by this
  // RecordWriter and must be kept alive but not accessed until closing the
  // RecordWriter.
  explicit RecordWriter(Writer* byte_writer, Options options = Options());

  // Will write records to the ChunkWriter which is owned by this RecordWriter
  // and will be closed and deleted when the RecordWriter is closed.
  //
  // Specifying a ChunkWriter instead of a byte Writer allows to customize how
  // chunks are stored, e.g. by forwarding them to another ChunkWriter running
  // elsewhere.
  explicit RecordWriter(std::unique_ptr<ChunkWriter> chunk_writer,
                        Options options = Options());

  // Will write records to the ChunkWriter which is not owned by this
  // RecordWriter and must be kept alive but not accessed until closing the
  // RecordWriter.
  //
  // Specifying a ChunkWriter instead of a byte Writer allows to customize how
  // chunks are stored, e.g. by forwarding them to another ChunkWriter running
  // elsewhere.
  explicit RecordWriter(ChunkWriter* chunk_writer, Options options = Options());

  RecordWriter(RecordWriter&& src) noexcept;
  RecordWriter& operator=(RecordWriter&& src) noexcept;

  ~RecordWriter();

  // Writes the next record.
  //
  // WriteRecord(MessageLite) serializes a proto message to raw bytes
  // beforehand. The remaining overloads accept raw bytes.
  //
  // If key != nullptr, *key is set to the canonical record position on success.
  //
  // Return values:
  //  * true  - success (healthy())
  //  * false - failure (!healthy())
  bool WriteRecord(const google::protobuf::MessageLite& record,
                   FutureRecordPosition* key = nullptr);
  bool WriteRecord(absl::string_view record,
                   FutureRecordPosition* key = nullptr);
  bool WriteRecord(std::string&& record, FutureRecordPosition* key = nullptr);
  bool WriteRecord(const char* record, FutureRecordPosition* key = nullptr);
  bool WriteRecord(const Chain& record, FutureRecordPosition* key = nullptr);
  bool WriteRecord(Chain&& record, FutureRecordPosition* key = nullptr);

  // Finalizes any open chunk and pushes buffered data to the Writer.
  // If Options::set_parallelism() was used, waits for any background writing to
  // complete.
  //
  // This degrades compression density if used too often.
  //
  // Additionally, attempts to ensure the following, depending on flush_type:
  //  * FlushType::kFromObject  - data is written to the Writer's destination
  //  * FlushType::kFromProcess - data survives process crash
  //  * FlushType::kFromMachine - data survives operating system crash
  //
  // Return values:
  //  * true                    - success (pushed and synced, healthy())
  //  * false (when healthy())  - failure to sync
  //  * false (when !healthy()) - failure to push
  bool Flush(FlushType flush_type);

  // Returns the current position.
  //
  // Pos().get().numeric() returns the position as an integer of type Position.
  //
  // A position returned by Pos() before writing a record is not greater than
  // the canonical position returned by WriteRecord() in *key for that record,
  // but seeking to either position will read the same record.
  //
  // After Flush(), Pos() is equal to the canonical position returned by the
  // following WriteRecord() in *key.
  FutureRecordPosition Pos() const;

 protected:
  void Done() override;

 private:
  class Impl;
  class SerialImpl;
  class ParallelImpl;
  class DummyImpl;

  static std::unique_ptr<ChunkEncoder> MakeChunkEncoder(const Options& options);

  template <typename Record>
  bool WriteRecordImpl(Record&& record, FutureRecordPosition* key);

  uint64_t desired_chunk_size_ = 0;
  uint64_t chunk_size_so_far_ = 0;
  std::unique_ptr<ChunkWriter> owned_chunk_writer_;
  // impl_ must be defined after owned_chunk_writer_ so that it is destroyed
  // before owned_chunk_writer_, because background work of impl_ may need
  // owned_chunk_writer_
  //
  // Invariant: if healthy() them impl_ != nullptr
  std::unique_ptr<Impl> impl_;
};

// Implementation details follow.

class FutureRecordPosition::FutureChunkBegin {
 public:
  FutureChunkBegin(
      Position pos_before_chunks,
      std::vector<std::shared_future<ChunkHeader>> chunk_headers);

  FutureChunkBegin(const FutureChunkBegin&) = delete;
  FutureChunkBegin& operator=(const FutureChunkBegin&) = delete;

  Position get() const;

 private:
  void Resolve() const;

  mutable absl::once_flag flag;
  // Position before writing chunks having chunk_headers_.
  mutable Position pos_before_chunks_ = 0;
  // Headers of chunks to be written after pos_before_chunks_.
  mutable std::vector<std::shared_future<ChunkHeader>> chunk_headers_;
};

inline Position FutureRecordPosition::FutureChunkBegin::get() const {
  absl::call_once(flag, &FutureChunkBegin::Resolve, this);
  RIEGELI_ASSERT(chunk_headers_.empty())
      << "FutureRecordPosition::FutureChunkBegin::Resolve() "
         "did not clear chunk_headers_";
  return pos_before_chunks_;
}

inline FutureRecordPosition::FutureRecordPosition(RecordPosition pos) noexcept
    : chunk_begin_(pos.chunk_begin()), record_index_(pos.record_index()) {}

inline FutureRecordPosition::FutureRecordPosition(
    FutureRecordPosition&& src) noexcept
    : future_chunk_begin_(std::move(src.future_chunk_begin_)),
      chunk_begin_(riegeli::exchange(src.chunk_begin_, 0)),
      record_index_(riegeli::exchange(src.record_index_, 0)) {}

inline FutureRecordPosition& FutureRecordPosition::operator=(
    FutureRecordPosition&& src) noexcept {
  future_chunk_begin_ = std::move(src.future_chunk_begin_);
  chunk_begin_ = riegeli::exchange(src.chunk_begin_, 0);
  record_index_ = riegeli::exchange(src.record_index_, 0);
  return *this;
}

inline FutureRecordPosition::FutureRecordPosition(
    const FutureRecordPosition& src)
    : future_chunk_begin_(src.future_chunk_begin_),
      chunk_begin_(src.chunk_begin_),
      record_index_(src.record_index_) {}

inline FutureRecordPosition& FutureRecordPosition::operator=(
    const FutureRecordPosition& src) {
  future_chunk_begin_ = src.future_chunk_begin_;
  chunk_begin_ = src.chunk_begin_;
  record_index_ = src.record_index_;
  return *this;
}

inline RecordPosition FutureRecordPosition::get() const {
  return RecordPosition(future_chunk_begin_ == nullptr
                            ? chunk_begin_
                            : future_chunk_begin_->get(),
                        record_index_);
}

inline bool RecordWriter::WriteRecord(const google::protobuf::MessageLite& record,
                                      FutureRecordPosition* key) {
  return WriteRecordImpl(record, key);
}

inline bool RecordWriter::WriteRecord(absl::string_view record,
                                      FutureRecordPosition* key) {
  return WriteRecordImpl<const absl::string_view&>(record, key);
}

inline bool RecordWriter::WriteRecord(std::string&& record,
                                      FutureRecordPosition* key) {
  return WriteRecordImpl(std::move(record), key);
}

inline bool RecordWriter::WriteRecord(const char* record,
                                      FutureRecordPosition* key) {
  return WriteRecordImpl<const absl::string_view&>(record, key);
}

inline bool RecordWriter::WriteRecord(const Chain& record,
                                      FutureRecordPosition* key) {
  return WriteRecordImpl(record, key);
}

inline bool RecordWriter::WriteRecord(Chain&& record,
                                      FutureRecordPosition* key) {
  return WriteRecordImpl(std::move(record), key);
}

}  // namespace riegeli

#endif  // RIEGELI_RECORDS_RECORD_WRITER_H_
