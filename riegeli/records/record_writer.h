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
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/meta/type_traits.h"
#include "absl/strings/string_view.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/base/stable_dependency.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/chunk_encoding/compressor_options.h"
#include "riegeli/records/chunk_writer.h"
#include "riegeli/records/chunk_writer_dependency.h"
#include "riegeli/records/record_position.h"
#include "riegeli/records/records_metadata.pb.h"

namespace riegeli {

// Sets record_type_name and file_descriptor in metadata, based on the message
// descriptor of the type of records.
//
// TODO: This currently includes whole file descriptors. It would be
// better to prune them to keep only what is needed for the message descriptor.
void SetRecordType(RecordsMetadata* metadata,
                   const google::protobuf::Descriptor* descriptor);

class RecordWriterBase : public Object {
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
    //   zstd_level ::= integer -32..22 (default 9)
    //   window_log ::= "auto" or integer 10..31
    //   chunk_size ::=
    //     integer expressed as real with optional suffix [BkKMGTPE], 1..
    //   bucket_fraction ::= real 0..1
    //   parallelism ::= integer 0..
    //
    // Return values:
    //  * true  - success
    //  * false - failure (*error_message is set)
    bool Parse(absl::string_view text, std::string* error_message = nullptr);

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
    // compression_level must be between kMinZstd() (-32) and kMaxZstd() (22).
    // Level 0 is currently equivalent to 3. Default: kDefaultZstd() (9).
    static int kMinZstd() { return CompressorOptions::kMinZstd(); }
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
    // BrotliWriterBase::Options::kMinWindowLog() (10) and
    // BrotliWriterBase::Options::kMaxWindowLog() (30).
    //
    // For zstd, window_log must be kDefaultWindowLog() (-1) or between
    // ZstdWriterBase::Options::kMinWindowLog() (10) and
    // ZstdWriterBase::Options::kMaxWindowLog() (30 in 32-bit build, 31 in
    // 64-bit build).
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
          << "Failed precondition of "
             "RecordWriterBase::Options::set_chunk_size(): "
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
             "RecordWriterBase::Options::set_bucket_fraction(): "
             "negative bucket fraction";
      RIEGELI_ASSERT_LE(fraction, 1.0)
          << "Failed precondition of "
             "RecordWriterBase::Options::set_bucket_fraction(): "
             "fraction larger than 1";
      bucket_fraction_ = fraction;
      return *this;
    }
    Options&& set_bucket_fraction(double fraction) && {
      return std::move(set_bucket_fraction(fraction));
    }

    // Sets file metadata to be written at the beginning (if metadata has any
    // fields set).
    //
    // Metadata are written only when the file is written from the beginning,
    // not when it is appended to.
    //
    // Record type in metadata can be conveniently set by SetRecordType().
    //
    // Default: no fields set
    Options& set_metadata(RecordsMetadata metadata) & {
      metadata_ = std::move(metadata);
      return *this;
    }
    Options&& set_metadata(RecordsMetadata metadata) && {
      return std::move(set_metadata(std::move(metadata)));
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
          << "Failed precondition of "
             "RecordWriterBase::Options::set_parallelism(): "
             "negative parallelism";
      parallelism_ = parallelism;
      return *this;
    }
    Options&& set_parallelism(int parallelism) && {
      return std::move(set_parallelism(parallelism));
    }

   private:
    friend class RecordWriterBase;

    bool transpose_ = false;
    CompressorOptions compressor_options_;
    uint64_t chunk_size_ = uint64_t{1} << 20;
    double bucket_fraction_ = 1.0;
    RecordsMetadata metadata_;
    int parallelism_ = 0;
  };

  ~RecordWriterBase();

  // Returns the Riegeli/records file being written to. Unchanged by Close().
  virtual ChunkWriter* dest_chunk_writer() = 0;
  virtual const ChunkWriter* dest_chunk_writer() const = 0;

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
  template <typename Record>
  absl::enable_if_t<std::is_convertible<Record, absl::string_view>::value, bool>
  WriteRecord(const Record& record, FutureRecordPosition* key = nullptr) {
    return WriteRecord(absl::string_view(record), key);
  };
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
  // After Close() or Flush(), Pos() is equal to the canonical position returned
  // by the following WriteRecord() in *key (after reopening the file for
  // appending in the case of Close()).
  FutureRecordPosition Pos() const;

 protected:
  explicit RecordWriterBase(State state) noexcept;

  RecordWriterBase(RecordWriterBase&& that) noexcept;
  RecordWriterBase& operator=(RecordWriterBase&& that) noexcept;

  void Initialize(ChunkWriter* chunk_writer, Options&& options);

  void Done() override;

  void DoneBackground();

 private:
  class Worker;
  class SerialWorker;
  class ParallelWorker;

  template <typename Record>
  bool WriteRecordImpl(Record&& record, FutureRecordPosition* key);

  uint64_t desired_chunk_size_ = 0;
  uint64_t chunk_size_so_far_ = 0;
  std::unique_ptr<Worker> worker_;
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
//
// The Dest template parameter specifies the type of the object providing and
// possibly owning the byte Writer. Dest must support Dependency<Writer*, Dest>,
// e.g. Writer* (not owned, default), unique_ptr<Writer> (owned),
// ChainWriter<> (owned).
//
// Dest can also specify a ChunkWriter instead of a byte Writer. In this case
// Dest must support Dependency<ChunkWriter*, Dest>, e.g.
// ChunkWriter* (not owned), unique_ptr<ChunkWriter> (owned),
// DefaultChunkWriter<> (owned).
//
// The byte Writer or ChunkWriter must not be accessed until the RecordWriter is
// closed or (when options.set_parallelism(true) is not used) no longer used.
template <typename Dest = Writer*>
class RecordWriter : public RecordWriterBase {
 public:
  // Creates a closed RecordWriter.
  RecordWriter() noexcept : RecordWriterBase(State::kClosed) {}

  // Will write to the byte Writer or ChunkWriter provided by dest.
  explicit RecordWriter(Dest dest, Options options = Options());

  RecordWriter(RecordWriter&& that) noexcept;
  RecordWriter& operator=(RecordWriter&& that) noexcept;

  ~RecordWriter() { DoneBackground(); }

  // Returns the object providing and possibly owning the byte Writer or
  // ChunkWriter. Unchanged by Close().
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  ChunkWriter* dest_chunk_writer() override { return dest_.ptr(); }
  const ChunkWriter* dest_chunk_writer() const override { return dest_.ptr(); }

 protected:
  void Done() override;

 private:
  // The object providing and possibly owning the byte Writer or ChunkWriter.
  StableDependency<ChunkWriter*, Dest> dest_;
};

// Implementation details follow.

inline bool RecordWriterBase::WriteRecord(
    const google::protobuf::MessageLite& record, FutureRecordPosition* key) {
  return WriteRecordImpl(record, key);
}

inline bool RecordWriterBase::WriteRecord(absl::string_view record,
                                          FutureRecordPosition* key) {
  return WriteRecordImpl<const absl::string_view&>(record, key);
}

inline bool RecordWriterBase::WriteRecord(std::string&& record,
                                          FutureRecordPosition* key) {
  return WriteRecordImpl(std::move(record), key);
}

inline bool RecordWriterBase::WriteRecord(const Chain& record,
                                          FutureRecordPosition* key) {
  return WriteRecordImpl(record, key);
}

inline bool RecordWriterBase::WriteRecord(Chain&& record,
                                          FutureRecordPosition* key) {
  return WriteRecordImpl(std::move(record), key);
}

template <typename Dest>
RecordWriter<Dest>::RecordWriter(Dest dest, Options options)
    : RecordWriterBase(State::kOpen), dest_(std::move(dest)) {
  RIEGELI_ASSERT(dest_.ptr() != nullptr)
      << "Failed precondition of RecordWriter<Dest>::RecordWriter(Dest): "
         "null ChunkWriter pointer";
  Initialize(dest_.ptr(), std::move(options));
}

template <typename Dest>
inline RecordWriter<Dest>::RecordWriter(RecordWriter&& that) noexcept
    : RecordWriterBase(std::move(that)), dest_(std::move(that.dest_)) {}

template <typename Dest>
inline RecordWriter<Dest>& RecordWriter<Dest>::operator=(
    RecordWriter&& that) noexcept {
  DoneBackground();
  RecordWriterBase::operator=(std::move(that));
  dest_ = std::move(that.dest_);
  return *this;
}

template <typename Dest>
void RecordWriter<Dest>::Done() {
  RecordWriterBase::Done();
  if (dest_.kIsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) Fail(*dest_);
  }
}

extern template class RecordWriter<Writer*>;
extern template class RecordWriter<std::unique_ptr<Writer>>;
extern template class RecordWriter<ChunkWriter*>;
extern template class RecordWriter<std::unique_ptr<ChunkWriter>>;
extern template class RecordWriter<DefaultChunkWriter<Writer*>>;
extern template class RecordWriter<DefaultChunkWriter<std::unique_ptr<Writer>>>;

}  // namespace riegeli

#endif  // RIEGELI_RECORDS_RECORD_WRITER_H_
