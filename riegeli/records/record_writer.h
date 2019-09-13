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
#include <tuple>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/base/resetter.h"
#include "riegeli/base/stable_dependency.h"
#include "riegeli/base/status.h"
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

// Template parameter invariant part of RecordWriter.
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
    //     "snappy" |
    //     "window_log" ":" window_log |
    //     "chunk_size" ":" chunk_size |
    //     "bucket_fraction" ":" bucket_fraction |
    //     "pad_to_block_boundary" (":" ("true" | "false"))? |
    //     "parallelism" ":" parallelism
    //   brotli_level ::= integer 0..11 (default 9)
    //   zstd_level ::= integer -131072..22 (default 9)
    //   window_log ::= "auto" or integer 10..31
    //   chunk_size ::=
    //     integer expressed as real with optional suffix [BkKMGTPE], 1..
    //   bucket_fraction ::= real 0..1
    //   parallelism ::= integer 0..
    //
    // An empty string is the same as "default".
    //
    // Options are documented below, and also at
    // https://github.com/google/riegeli/blob/master/doc/record_writer_options.md
    //
    // Returns status:
    //  * status.ok()  - success
    //  * !status.ok() - failure
    Status FromString(absl::string_view text);

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
    // compression_level must be between kMinBrotli (0) and kMaxBrotli (11).
    // Default: kDefaultBrotli (9).
    //
    // This is the default compression algorithm.
    static constexpr int kMinBrotli = CompressorOptions::kMinBrotli;
    static constexpr int kMaxBrotli = CompressorOptions::kMaxBrotli;
    static constexpr int kDefaultBrotli = CompressorOptions::kDefaultBrotli;
    Options& set_brotli(int compression_level = kDefaultBrotli) & {
      compressor_options_.set_brotli(compression_level);
      return *this;
    }
    Options&& set_brotli(int compression_level = kDefaultBrotli) && {
      return std::move(set_brotli(compression_level));
    }

    // Changes compression algorithm to Zstd. Sets compression level which tunes
    // the tradeoff between compression density and compression speed (higher =
    // better density but slower).
    //
    // compression_level must be between kMinZstd (-131072) and kMaxZstd (22).
    // Level 0 is currently equivalent to 3. Default: kDefaultZstd (9).
    static constexpr int kMinZstd = CompressorOptions::kMinZstd;
    static constexpr int kMaxZstd = CompressorOptions::kMaxZstd;
    static constexpr int kDefaultZstd = CompressorOptions::kDefaultZstd;
    Options& set_zstd(int compression_level = kDefaultZstd) & {
      compressor_options_.set_zstd(compression_level);
      return *this;
    }
    Options&& set_zstd(int compression_level = kDefaultZstd) && {
      return std::move(set_zstd(compression_level));
    }

    // Changes compression algorithm to Snappy.
    //
    // There are no Snappy compression levels to tune.
    Options& set_snappy() & {
      compressor_options_.set_snappy();
      return *this;
    }
    Options&& set_snappy() && { return std::move(set_snappy()); }

    // Logarithm of the LZ77 sliding window size. This tunes the tradeoff
    // between compression density and memory usage (higher = better density but
    // more memory).
    //
    // Special value kDefaultWindowLog (-1) means to keep the default
    // (brotli: 22, zstd: derived from compression level and chunk size).
    //
    // For uncompressed and snappy, window_log must be kDefaultWindowLog (-1).
    //
    // For brotli, window_log must be kDefaultWindowLog (-1) or between
    // BrotliWriterBase::Options::kMinWindowLog (10) and
    // BrotliWriterBase::Options::kMaxWindowLog (30).
    //
    // For zstd, window_log must be kDefaultWindowLog (-1) or between
    // ZstdWriterBase::Options::kMinWindowLog (10) and
    // ZstdWriterBase::Options::kMaxWindowLog (30 in 32-bit build, 31 in 64-bit
    // build).
    //
    // Default: kDefaultWindowLog (-1).
    static constexpr int kMinWindowLog = CompressorOptions::kMinWindowLog;
    static constexpr int kMaxWindowLog = CompressorOptions::kMaxWindowLog;
    static constexpr int kDefaultWindowLog =
        CompressorOptions::kDefaultWindowLog;
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
    static constexpr uint64_t kDefaultChunkSize = uint64_t{1} << 20;
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
    // relative to the desired chunk size, on the scale between 0.0 (compress
    // each field separately) to 1.0 (put all fields of the same wire type in
    // the same bucket).
    //
    // This is meaningful if transpose and compression are enabled. A larger
    // bucket size improves compression density; a smaller bucket size makes
    // reading with projection faster, allowing to skip decompression of values
    // of fields which are not included.
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
      serialized_metadata_.Clear();
      return *this;
    }
    Options&& set_metadata(RecordsMetadata metadata) && {
      return std::move(set_metadata(std::move(metadata)));
    }

    // Like set_metadata(), but metadata are passed in the serialized form.
    //
    // This is faster if the caller has metadata already serialized.
    Options& set_serialized_metadata(Chain metadata) & {
      metadata_.Clear();
      serialized_metadata_ = std::move(metadata);
      return *this;
    }
    Options&& set_serialized_metadata(Chain metadata) && {
      return std::move(set_serialized_metadata(std::move(metadata)));
    }

    // If true, padding is written to reach a 64KB block boundary when the
    // RecordWriter is created, before Close(), and before Flush().
    //
    // Consequences:
    //
    //  * Even if the existing file was corrupted or truncated, data appended to
    //    it will be readable.
    //
    //  * Physical concatenation of separately written files yields a valid file
    //    (setting metadata in subsequent files is wasteful but harmless).
    //
    //  * Up to 64KB is wasted when padding is written.
    //
    // Default: false
    Options& set_pad_to_block_boundary(bool pad_to_block_boundary) & {
      pad_to_block_boundary_ = pad_to_block_boundary;
      return *this;
    }
    Options&& set_pad_to_block_boundary(bool pad_to_block_boundary) && {
      return std::move(set_pad_to_block_boundary(pad_to_block_boundary));
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
    uint64_t chunk_size_ = kDefaultChunkSize;
    double bucket_fraction_ = 1.0;
    RecordsMetadata metadata_;
    Chain serialized_metadata_;
    bool pad_to_block_boundary_ = false;
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
  //  * true  - success (healthy())
  //  * false - failure (!healthy())
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
  explicit RecordWriterBase(InitiallyClosed) noexcept;
  explicit RecordWriterBase(InitiallyOpen) noexcept;

  RecordWriterBase(RecordWriterBase&& that) noexcept;
  RecordWriterBase& operator=(RecordWriterBase&& that) noexcept;

  void Reset(InitiallyClosed);
  void Reset(InitiallyOpen);
  void Initialize(ChunkWriter* dest, Options&& options);

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
  // Invariant: if !closed() then worker_ != nullptr.
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
//     ... Failed with reason: record_writer_.status()
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
  RecordWriter() noexcept : RecordWriterBase(kInitiallyClosed) {}

  // Will write to the byte Writer or ChunkWriter provided by dest.
  explicit RecordWriter(const Dest& dest, Options options = Options());
  explicit RecordWriter(Dest&& dest, Options options = Options());

  // Will write to the byte Writer or ChunkWriter provided by a Dest constructed
  // from elements of dest_args. This avoids constructing a temporary Dest and
  // moving from it.
  template <typename... DestArgs>
  explicit RecordWriter(std::tuple<DestArgs...> dest_args,
                        Options options = Options());

  RecordWriter(RecordWriter&& that) noexcept;
  RecordWriter& operator=(RecordWriter&& that) noexcept;

  ~RecordWriter() { DoneBackground(); }

  // Makes *this equivalent to a newly constructed RecordWriter. This avoids
  // constructing a temporary RecordWriter and moving from it.
  void Reset();
  void Reset(const Dest& dest, Options options = Options());
  void Reset(Dest&& dest, Options options = Options());
  template <typename... DestArgs>
  void Reset(std::tuple<DestArgs...> dest_args, Options options = Options());

  // Returns the object providing and possibly owning the byte Writer or
  // ChunkWriter. Unchanged by Close().
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  ChunkWriter* dest_chunk_writer() override { return dest_.get(); }
  const ChunkWriter* dest_chunk_writer() const override { return dest_.get(); }

 protected:
  void Done() override;

 private:
  // The object providing and possibly owning the byte Writer or ChunkWriter.
  StableDependency<ChunkWriter*, Dest> dest_;
};

// Implementation details follow.

extern template bool RecordWriterBase::WriteRecordImpl(
    const google::protobuf::MessageLite& record, FutureRecordPosition* key);
extern template bool RecordWriterBase::WriteRecordImpl(
    const absl::string_view& record, FutureRecordPosition* key);
extern template bool RecordWriterBase::WriteRecordImpl(
    std::string&& record, FutureRecordPosition* key);
extern template bool RecordWriterBase::WriteRecordImpl(
    const Chain& record, FutureRecordPosition* key);
extern template bool RecordWriterBase::WriteRecordImpl(
    Chain&& record, FutureRecordPosition* key);

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

inline bool RecordWriterBase::WriteRecord(const char* record,
                                          FutureRecordPosition* key) {
  return WriteRecordImpl<const absl::string_view&>(record, key);
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
inline RecordWriter<Dest>::RecordWriter(const Dest& dest, Options options)
    : RecordWriterBase(kInitiallyOpen), dest_(dest) {
  Initialize(dest_.get(), std::move(options));
}

template <typename Dest>
inline RecordWriter<Dest>::RecordWriter(Dest&& dest, Options options)
    : RecordWriterBase(kInitiallyOpen), dest_(std::move(dest)) {
  Initialize(dest_.get(), std::move(options));
}

template <typename Dest>
template <typename... DestArgs>
inline RecordWriter<Dest>::RecordWriter(std::tuple<DestArgs...> dest_args,
                                        Options options)
    : RecordWriterBase(kInitiallyOpen), dest_(std::move(dest_args)) {
  Initialize(dest_.get(), std::move(options));
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
inline void RecordWriter<Dest>::Reset() {
  RecordWriterBase::Reset(kInitiallyClosed);
  dest_.Reset();
}

template <typename Dest>
inline void RecordWriter<Dest>::Reset(const Dest& dest, Options options) {
  RecordWriterBase::Reset(kInitiallyOpen);
  dest_.Reset(dest);
  Initialize(dest_.get(), std::move(options));
}

template <typename Dest>
inline void RecordWriter<Dest>::Reset(Dest&& dest, Options options) {
  RecordWriterBase::Reset(kInitiallyOpen);
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), std::move(options));
}

template <typename Dest>
template <typename... DestArgs>
inline void RecordWriter<Dest>::Reset(std::tuple<DestArgs...> dest_args,
                                      Options options) {
  RecordWriterBase::Reset(kInitiallyOpen);
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get(), std::move(options));
}

template <typename Dest>
void RecordWriter<Dest>::Done() {
  RecordWriterBase::Done();
  if (dest_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) Fail(*dest_);
  }
}

template <typename Dest>
struct Resetter<RecordWriter<Dest>> : ResetterByReset<RecordWriter<Dest>> {};

}  // namespace riegeli

#endif  // RIEGELI_RECORDS_RECORD_WRITER_H_
