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
#include <type_traits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/base/stable_dependency.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/chunk_encoding/compressor_options.h"
#include "riegeli/chunk_encoding/constants.h"
#include "riegeli/messages/message_serialize.h"
#include "riegeli/records/chunk_writer.h"
#include "riegeli/records/chunk_writer_dependency.h"
#include "riegeli/records/record_position.h"
#include "riegeli/records/records_metadata.pb.h"

namespace riegeli {

// Sets `record_type_name` and `file_descriptor` in metadata, based on the
// message descriptor of the type of records.
//
// TODO: This currently includes whole file descriptors. It would be
// better to prune them to keep only what is needed for the message descriptor.
void SetRecordType(const google::protobuf::Descriptor& descriptor,
                   RecordsMetadata& metadata);

// Template parameter independent part of `RecordWriter`.
class RecordWriterBase : public Object {
 public:
  class Options {
   public:
    Options() noexcept {}

    // Parses options from text:
    // ```
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
    //   brotli_level ::= integer 0..11 (default 6)
    //   zstd_level ::= integer -131072..22 (default 3)
    //   window_log ::= "auto" or integer 10..31
    //   chunk_size ::= "auto" or integer expressed as real with optional suffix
    //     [BkKMGTPE], 1..
    //   bucket_fraction ::= real 0..1
    //   parallelism ::= integer 0..
    // ```
    //
    // An empty string is the same as "default".
    //
    // Options are documented below, and also at
    // https://github.com/google/riegeli/blob/master/doc/record_writer_options.md
    //
    // Returns status:
    //  * `status.ok()`  - success
    //  * `!status.ok()` - failure
    absl::Status FromString(absl::string_view text);

    // If `true`, records should be serialized proto messages (but nothing will
    // break if they are not). A chunk of records will be processed in a way
    // which allows for better compression.
    //
    // If `false`, a chunk of records will be stored in a simpler format,
    // directly or with compression.
    //
    // Default: `false`.
    Options& set_transpose(bool transpose) & {
      transpose_ = transpose;
      return *this;
    }
    Options&& set_transpose(bool transpose) && {
      return std::move(set_transpose(transpose));
    }
    bool transpose() const { return transpose_; }

    // Changes compression algorithm to Uncompressed (turns compression off).
    Options& set_uncompressed() & {
      compressor_options_.set_uncompressed();
      return *this;
    }
    Options&& set_uncompressed() && { return std::move(set_uncompressed()); }

    // Changes compression algorithm to Brotli. Sets compression level which
    // tunes the tradeoff between compression density and compression speed
    // (higher = better density but slower).
    //
    // `compression_level` must be between `kMinBrotli` (0) and
    // `kMaxBrotli` (11). Default: `kDefaultBrotli` (6).
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
    // `compression_level` must be between `kMinZstd` (-131072) and
    // `kMaxZstd` (22). Level 0 is currently equivalent to 3.
    // Default: `kDefaultZstd` (3).
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

    CompressionType compression_type() const {
      return compressor_options_.compression_type();
    }

    int compression_level() const {
      return compressor_options_.compression_level();
    }

    // Logarithm of the LZ77 sliding window size. This tunes the tradeoff
    // between compression density and memory usage (higher = better density but
    // more memory).
    //
    // Special value `absl::nullopt` means to keep the default (Brotli: 22,
    // Zstd: derived from compression level and chunk size).
    //
    // For Uncompressed and Snappy, `window_log` must be `absl::nullopt`.
    //
    // For Brotli, `window_log` must be `absl::nullopt` or between
    // `BrotliWriterBase::Options::kMinWindowLog` (10) and
    // `BrotliWriterBase::Options::kMaxWindowLog` (30).
    //
    // For Zstd, `window_log` must be `absl::nullopt` or between
    // `ZstdWriterBase::Options::kMinWindowLog` (10) and
    // `ZstdWriterBase::Options::kMaxWindowLog` (30 in 32-bit build,
    // 31 in 64-bit build).
    //
    // Default: `absl::nullopt`.
    static constexpr int kMinWindowLog = CompressorOptions::kMinWindowLog;
    static constexpr int kMaxWindowLog = CompressorOptions::kMaxWindowLog;
    Options& set_window_log(absl::optional<int> window_log) & {
      compressor_options_.set_window_log(window_log);
      return *this;
    }
    Options&& set_window_log(absl::optional<int> window_log) && {
      return std::move(set_window_log(window_log));
    }
    absl::optional<int> window_log() const {
      return compressor_options_.window_log();
    }

    // Returns grouped compression options.
    CompressorOptions& compressor_options() { return compressor_options_; }
    const CompressorOptions& compressor_options() const {
      return compressor_options_;
    }

    // Sets the desired uncompressed size of a chunk which groups messages to be
    // transposed, compressed, and written together.
    //
    // A larger chunk size improves compression density; a smaller chunk size
    // allows to read pieces of the file independently with finer granularity,
    // and reduces memory usage of both writer and reader.
    //
    // Special value `absl::nullopt` means to keep the default
    // (compressed: 1M, uncompressed: 4k).
    //
    // Default: `absl::nullopt`.
    Options& set_chunk_size(absl::optional<uint64_t> chunk_size) & {
      if (chunk_size != absl::nullopt) {
        RIEGELI_ASSERT_GT(*chunk_size, 0u)
            << "Failed precondition of "
               "RecordWriterBase::Options::set_chunk_size(): "
               "zero chunk size";
      }
      chunk_size_ = chunk_size;
      return *this;
    }
    Options&& set_chunk_size(absl::optional<uint64_t> chunk_size) && {
      return std::move(set_chunk_size(chunk_size));
    }
    absl::optional<uint64_t> chunk_size() const { return chunk_size_; }
    uint64_t effective_chunk_size() const {
      if (chunk_size_ == absl::nullopt) {
        return compression_type() == CompressionType::kNone ? uint64_t{4} << 10
                                                            : uint64_t{1} << 20;
      }
      return *chunk_size_;
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
    // Default: 1.0.
    Options& set_bucket_fraction(double bucket_fraction) & {
      RIEGELI_ASSERT_GE(bucket_fraction, 0.0)
          << "Failed precondition of "
             "RecordWriterBase::Options::set_bucket_fraction(): "
             "negative bucket fraction";
      RIEGELI_ASSERT_LE(bucket_fraction, 1.0)
          << "Failed precondition of "
             "RecordWriterBase::Options::set_bucket_fraction(): "
             "fraction larger than 1";
      bucket_fraction_ = bucket_fraction;
      return *this;
    }
    Options&& set_bucket_fraction(double bucket_fraction) && {
      return std::move(set_bucket_fraction(bucket_fraction));
    }
    double bucket_fraction() const { return bucket_fraction_; }

    // Sets file metadata to be written at the beginning (unless
    // `absl::nullopt`).
    //
    // Metadata are written only when the file is written from the beginning,
    // not when it is appended to.
    //
    // Record type in metadata can be conveniently set by `SetRecordType()`.
    //
    // Default: no fields set.
    Options& set_metadata(const absl::optional<RecordsMetadata>& metadata) & {
      metadata_ = metadata;
      serialized_metadata_ = absl::nullopt;
      return *this;
    }
    Options& set_metadata(absl::optional<RecordsMetadata>&& metadata) & {
      metadata_ = std::move(metadata);
      serialized_metadata_ = absl::nullopt;
      return *this;
    }
    Options& set_metadata(const RecordsMetadata& metadata) & {
      metadata_ = metadata;
      serialized_metadata_ = absl::nullopt;
      return *this;
    }
    Options& set_metadata(RecordsMetadata&& metadata) & {
      metadata_ = std::move(metadata);
      serialized_metadata_ = absl::nullopt;
      return *this;
    }
    Options&& set_metadata(const absl::optional<RecordsMetadata>& metadata) && {
      return std::move(set_metadata(metadata));
    }
    Options&& set_metadata(absl::optional<RecordsMetadata>&& metadata) && {
      return std::move(set_metadata(std::move(metadata)));
    }
    Options&& set_metadata(const RecordsMetadata& metadata) && {
      return std::move(set_metadata(metadata));
    }
    Options&& set_metadata(RecordsMetadata&& metadata) && {
      return std::move(set_metadata(std::move(metadata)));
    }
    absl::optional<RecordsMetadata>& metadata() { return metadata_; }
    const absl::optional<RecordsMetadata>& metadata() const {
      return metadata_;
    }

    // Like `set_metadata()`, but metadata are passed in the serialized form.
    //
    // This is faster if the caller has metadata already serialized.
    Options& set_serialized_metadata(
        const absl::optional<Chain>& serialized_metadata) & {
      metadata_ = absl::nullopt;
      serialized_metadata_ = serialized_metadata;
      return *this;
    }
    Options& set_serialized_metadata(
        absl::optional<Chain>&& serialized_metadata) & {
      metadata_ = absl::nullopt;
      serialized_metadata_ = std::move(serialized_metadata);
      return *this;
    }
    Options& set_serialized_metadata(const Chain& serialized_metadata) & {
      metadata_ = absl::nullopt;
      serialized_metadata_ = serialized_metadata;
      return *this;
    }
    Options& set_serialized_metadata(Chain&& serialized_metadata) & {
      metadata_ = absl::nullopt;
      serialized_metadata_ = std::move(serialized_metadata);
      return *this;
    }
    Options&& set_serialized_metadata(
        const absl::optional<Chain>& serialized_metadata) && {
      return std::move(set_serialized_metadata(serialized_metadata));
    }
    Options&& set_serialized_metadata(
        absl::optional<Chain>&& serialized_metadata) && {
      return std::move(set_serialized_metadata(std::move(serialized_metadata)));
    }
    Options&& set_serialized_metadata(const Chain& serialized_metadata) && {
      return std::move(set_serialized_metadata(serialized_metadata));
    }
    Options&& set_serialized_metadata(Chain&& serialized_metadata) && {
      return std::move(set_serialized_metadata(std::move(serialized_metadata)));
    }
    absl::optional<Chain>& serialized_metadata() {
      return serialized_metadata_;
    }
    const absl::optional<Chain>& serialized_metadata() const {
      return serialized_metadata_;
    }

    // If `true`, padding is written to reach a 64KB block boundary when the
    // `RecordWriter` is created, before `Close()`, and before `Flush()`.
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
    // Default: `false`.
    Options& set_pad_to_block_boundary(bool pad_to_block_boundary) & {
      pad_to_block_boundary_ = pad_to_block_boundary;
      return *this;
    }
    Options&& set_pad_to_block_boundary(bool pad_to_block_boundary) && {
      return std::move(set_pad_to_block_boundary(pad_to_block_boundary));
    }
    bool pad_to_block_boundary() const { return pad_to_block_boundary_; }

    // Sets the maximum number of chunks being encoded in parallel in
    // background. Larger parallelism can increase throughput, up to a point
    // where it no longer matters; smaller parallelism reduces memory usage.
    //
    // If `parallelism > 0`, chunks are written to the byte `Writer` in
    // background and reporting writing errors is delayed.
    //
    // Default: 0.
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
    int parallelism() const { return parallelism_; }

   private:
    bool transpose_ = false;
    CompressorOptions compressor_options_;
    absl::optional<uint64_t> chunk_size_;
    double bucket_fraction_ = 1.0;
    absl::optional<RecordsMetadata> metadata_;
    absl::optional<Chain> serialized_metadata_;
    bool pad_to_block_boundary_ = false;
    int parallelism_ = 0;
  };

  // `get()` returns the resolved value. Can block.
  using FutureBool = std::shared_future<bool>;

  ~RecordWriterBase();

  // Returns the Riegeli/records file being written to. Unchanged by `Close()`.
  virtual ChunkWriter* dest_chunk_writer() = 0;
  virtual const ChunkWriter* dest_chunk_writer() const = 0;

  // Writes the next record.
  //
  // `WriteRecord(google::protobuf::MessageLite)` serializes a proto message to
  // raw bytes beforehand. The remaining overloads accept raw bytes.
  //
  // `std::string&&` is accepted with a template to avoid implicit conversions
  // to `std::string` which can be ambiguous against `absl::string_view`
  // (e.g. `const char*`).
  //
  // If `key != nullptr`, `*key` is set to the canonical record position on
  // success. This parameter is deprecated: use `LastPos()` instead.
  //
  // Return values:
  //  * `true`  - success (`healthy()`)
  //  * `false` - failure (`!healthy()`)
  bool WriteRecord(const google::protobuf::MessageLite& record,
                   FutureRecordPosition* key = nullptr);
  bool WriteRecord(const google::protobuf::MessageLite& record,
                   SerializeOptions serialize_options,
                   FutureRecordPosition* key = nullptr);
  bool WriteRecord(absl::string_view record,
                   FutureRecordPosition* key = nullptr);
  template <typename Src,
            std::enable_if_t<std::is_same<Src, std::string>::value, int> = 0>
  bool WriteRecord(Src&& record, FutureRecordPosition* key = nullptr);
  bool WriteRecord(const Chain& record, FutureRecordPosition* key = nullptr);
  bool WriteRecord(Chain&& record, FutureRecordPosition* key = nullptr);
  bool WriteRecord(const absl::Cord& record,
                   FutureRecordPosition* key = nullptr);
  bool WriteRecord(absl::Cord&& record, FutureRecordPosition* key = nullptr);

  // Finalizes any open chunk and pushes buffered data to the destination.
  // If `Options::parallelism() > 0`, waits for any background writing to
  // complete.
  //
  // This makes data written so far visible, but in contrast to `Close()`,
  // keeps the possibility to write more data later. What exactly does it mean
  // for data to be visible depends on the destination.
  //
  // This degrades compression density if used too often.
  //
  // The scope of objects to flush and the intended data durability (without a
  // guarantee) are specified by `flush_type`:
  //  * `FlushType::kFromObject`  - Makes data written so far visible in other
  //                                objects, propagating flushing through owned
  //                                dependencies of the given writer.
  //  * `FlushType::kFromProcess` - Makes data written so far visible outside
  //                                the process, propagating flushing through
  //                                dependencies of the given writer.
  //                                This is the default.
  //  * `FlushType::kFromMachine` - Makes data written so far visible outside
  //                                the process and durable in case of operating
  //                                system crash, propagating flushing through
  //                                dependencies of the given writer.
  //
  // Return values:
  //  * `true`  - success (`healthy()`)
  //  * `false` - failure (`!healthy()`)
  bool Flush(FlushType flush_type = FlushType::kFromProcess);

  // Like `Flush()`, but if `Options::parallelism() > 0`, does not wait for
  // background writing to complete. Returns a `FutureBool` which can be used to
  // wait for background writing to complete.
  //
  // Like any member function, `FutureFlush()` must not be called concurrently
  // with other member functions, but there are no concurrency restrictions on
  // calling `get()` on the result.
  //
  // `Flush()` is equivalent to `FutureFlush().get()`.
  FutureBool FutureFlush(FlushType flush_type = FlushType::kFromProcess);

  // Returns the canonical position of the last record written.
  //
  // The canonical position is the largest among all equivalent positions.
  // Seeking to any equivalent position leads to reading the same record.
  //
  // `LastPos().get().numeric()` returns the position as an integer of type
  // `Position`.
  //
  // Precondition: a record was successfully written and there was no
  // intervening call to `Close()`, `Flush()` or `FutureFlush()` (this can be
  // checked with `last_record_is_valid()`).
  FutureRecordPosition LastPos() const;

  // Returns `true` if calling `LastPos()` is valid.
  bool last_record_is_valid() const { return last_record_is_valid_; }

  // Returns a position of the next record (or the end of file if there is no
  // next record).
  //
  // A position which is not canonical can be smaller than the equivalent
  // canonical position. Seeking to any equivalent position leads to reading the
  // same record.
  //
  // `Pos().get().numeric()` returns the position as an integer of type
  // `Position`.
  //
  // After opening the file, `Close()`, or `Flush()`, `Pos()` is the canonical
  // position of the next record, and `Pos().get().record_index() == 0`.
  FutureRecordPosition Pos() const;

  // Returns an estimation of the file size if no more data is written, without
  // affecting data representation (i.e. without closing the current chunk) and
  // without blocking.
  //
  // This is an underestimation because pending work is not taken into account:
  //  * The currently open chunk.
  //  * If `Options::parallelism() > 0`, chunks being encoded in background.
  //
  // The exact file size can be found by `FutureFlush(FlushType::kFromObject)`
  // which closes the currently open chunk, and `Pos().get().chunk_begin()`
  // (`record_index() == 0` after flushing) which might need to wait for some
  // background work to complete.
  Position EstimatedSize() const;

 protected:
  explicit RecordWriterBase(InitiallyClosed) noexcept;
  explicit RecordWriterBase(InitiallyOpen) noexcept;

  RecordWriterBase(RecordWriterBase&& that) noexcept;
  RecordWriterBase& operator=(RecordWriterBase&& that) noexcept;

  void Reset(InitiallyClosed);
  void Reset(InitiallyOpen);

  virtual bool is_owning() const = 0;

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
  bool last_record_is_valid_ = false;
  // Invariant: if `is_open()` then `worker_ != nullptr`.
  std::unique_ptr<Worker> worker_;
};

// `RecordWriter` writes records to a Riegeli/records file. A record is
// conceptually a binary string; usually it is a serialized proto message.
//
// For writing records, this kind of loop can be used:
// ```
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
// ```
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the byte `Writer`. `Dest` must support
// `Dependency<Writer*, Dest>`, e.g. `Writer*` (not owned, default),
// `std::unique_ptr<Writer>` (owned), `ChainWriter<>` (owned).
//
// `Dest` can also specify a `ChunkWriter` instead of a byte `Writer`. In this
// case `Dest` must support `Dependency<ChunkWriter*, Dest>`, e.g.
// `ChunkWriter*` (not owned), `std::unique_ptr<ChunkWriter>` (owned),
// `DefaultChunkWriter<>` (owned).
//
// By relying on CTAD the template argument can be deduced as the value type of
// the first constructor argument. This requires C++17.
//
// The byte `Writer` or `ChunkWriter` must not be accessed until the
// `RecordWriter` is closed or (when parallelism in options is 0) no longer
// used.
template <typename Dest = Writer*>
class RecordWriter : public RecordWriterBase {
 public:
  // Creates a closed `RecordWriter`.
  RecordWriter() noexcept : RecordWriterBase(kInitiallyClosed) {}

  // Will write to the byte `Writer` or `ChunkWriter` provided by `dest`.
  explicit RecordWriter(const Dest& dest, Options options = Options());
  explicit RecordWriter(Dest&& dest, Options options = Options());

  // Will write to the byte `Writer` or `ChunkWriter` provided by a `Dest`
  // constructed from elements of `dest_args`. This avoids constructing a
  // temporary `Dest` and moving from it.
  template <typename... DestArgs>
  explicit RecordWriter(std::tuple<DestArgs...> dest_args,
                        Options options = Options());

  RecordWriter(RecordWriter&& that) noexcept;
  RecordWriter& operator=(RecordWriter&& that) noexcept;

  ~RecordWriter() { DoneBackground(); }

  // Makes `*this` equivalent to a newly constructed `RecordWriter`. This avoids
  // constructing a temporary `RecordWriter` and moving from it.
  void Reset();
  void Reset(const Dest& dest, Options options = Options());
  void Reset(Dest&& dest, Options options = Options());
  template <typename... DestArgs>
  void Reset(std::tuple<DestArgs...> dest_args, Options options = Options());

  // Returns the object providing and possibly owning the byte `Writer` or
  // `ChunkWriter`. Unchanged by `Close()`.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  ChunkWriter* dest_chunk_writer() override { return dest_.get(); }
  const ChunkWriter* dest_chunk_writer() const override { return dest_.get(); }

 protected:
  void Done() override;

  bool is_owning() const override { return dest_.is_owning(); }

 private:
  // The object providing and possibly owning the byte `Writer` or
  // `ChunkWriter`.
  StableDependency<ChunkWriter*, Dest> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
RecordWriter()->RecordWriter<DeleteCtad<>>;
template <typename Dest>
explicit RecordWriter(const Dest& dest, RecordWriterBase::Options options =
                                            RecordWriterBase::Options())
    -> RecordWriter<std::decay_t<Dest>>;
template <typename Dest>
explicit RecordWriter(Dest&& dest, RecordWriterBase::Options options =
                                       RecordWriterBase::Options())
    -> RecordWriter<std::decay_t<Dest>>;
template <typename... DestArgs>
explicit RecordWriter(
    std::tuple<DestArgs...> dest_args,
    RecordWriterBase::Options options = RecordWriterBase::Options())
    -> RecordWriter<DeleteCtad<std::tuple<DestArgs...>>>;
#endif

// Implementation details follow.

extern template bool RecordWriterBase::WriteRecord(std::string&& record,
                                                   FutureRecordPosition* key);

inline bool RecordWriterBase::WriteRecord(
    const google::protobuf::MessageLite& record, FutureRecordPosition* key) {
  return WriteRecord(record, SerializeOptions(), key);
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
    : RecordWriterBase(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      dest_(std::move(that.dest_)) {}

template <typename Dest>
inline RecordWriter<Dest>& RecordWriter<Dest>::operator=(
    RecordWriter&& that) noexcept {
  DoneBackground();
  RecordWriterBase::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
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

}  // namespace riegeli

#endif  // RIEGELI_RECORDS_RECORD_WRITER_H_
