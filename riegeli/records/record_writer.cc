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

#include "riegeli/records/record_writer.h"

#include <stddef.h>
#include <stdint.h>

#include <cmath>
#include <deque>
#include <future>
#include <limits>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/optional.h"
#include "absl/types/variant.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/descriptor.pb.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/base/options_parser.h"
#include "riegeli/base/parallelism.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/chain_writer.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/chunk_encoding/chunk_encoder.h"
#include "riegeli/chunk_encoding/compressor_options.h"
#include "riegeli/chunk_encoding/constants.h"
#include "riegeli/chunk_encoding/deferred_encoder.h"
#include "riegeli/chunk_encoding/simple_encoder.h"
#include "riegeli/chunk_encoding/transpose_encoder.h"
#include "riegeli/messages/message_serialize.h"
#include "riegeli/records/chunk_writer.h"
#include "riegeli/records/record_position.h"
#include "riegeli/records/records_metadata.pb.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if !__cpp_inline_variables
constexpr int RecordWriterBase::Options::kMinBrotli;
constexpr int RecordWriterBase::Options::kMaxBrotli;
constexpr int RecordWriterBase::Options::kDefaultBrotli;
constexpr int RecordWriterBase::Options::kMinZstd;
constexpr int RecordWriterBase::Options::kMaxZstd;
constexpr int RecordWriterBase::Options::kDefaultZstd;
constexpr int RecordWriterBase::Options::kMinSnappy;
constexpr int RecordWriterBase::Options::kMaxSnappy;
constexpr int RecordWriterBase::Options::kDefaultSnappy;
constexpr int RecordWriterBase::Options::kMinWindowLog;
constexpr int RecordWriterBase::Options::kMaxWindowLog;
#endif

namespace {

class FileDescriptorCollector {
 public:
  explicit FileDescriptorCollector(
      google::protobuf::RepeatedPtrField<google::protobuf::FileDescriptorProto>*
          file_descriptors)
      : file_descriptors_(RIEGELI_ASSERT_NOTNULL(file_descriptors)) {}

  void AddFile(const google::protobuf::FileDescriptor* file_descriptor) {
    if (!files_seen_.insert(file_descriptor->name()).second) return;
    for (int i = 0; i < file_descriptor->dependency_count(); ++i) {
      AddFile(file_descriptor->dependency(i));
    }
    file_descriptor->CopyTo(file_descriptors_->Add());
  }

 private:
  google::protobuf::RepeatedPtrField<google::protobuf::FileDescriptorProto>*
      file_descriptors_;
  absl::flat_hash_set<std::string> files_seen_;
};

}  // namespace

void SetRecordType(const google::protobuf::Descriptor& descriptor,
                   RecordsMetadata& metadata) {
  metadata.set_record_type_name(descriptor.full_name());
  metadata.clear_file_descriptor();
  FileDescriptorCollector collector(metadata.mutable_file_descriptor());
  collector.AddFile(descriptor.file());
}

absl::Status RecordWriterBase::Options::FromString(absl::string_view text) {
  std::string compressor_text;
  uint64_t chunk_size;
  OptionsParser options_parser;
  options_parser.AddOption("default", ValueParser::FailIfAnySeen());
  options_parser.AddOption(
      "transpose",
      ValueParser::Enum({{"", true}, {"true", true}, {"false", false}},
                        &transpose_));
  options_parser.AddOption("uncompressed",
                           ValueParser::CopyTo(&compressor_text));
  options_parser.AddOption("brotli", ValueParser::CopyTo(&compressor_text));
  options_parser.AddOption("zstd", ValueParser::CopyTo(&compressor_text));
  options_parser.AddOption("snappy", ValueParser::CopyTo(&compressor_text));
  options_parser.AddOption("window_log", ValueParser::CopyTo(&compressor_text));
  options_parser.AddOption(
      "chunk_size",
      ValueParser::Or(
          ValueParser::Enum({{"auto", absl::nullopt}}, &chunk_size_),
          ValueParser::And(
              ValueParser::Bytes(1, std::numeric_limits<uint64_t>::max(),
                                 &chunk_size),
              [this, &chunk_size](ValueParser& value_parser) {
                chunk_size_ = chunk_size;
                return true;
              })));
  options_parser.AddOption("bucket_fraction",
                           ValueParser::Real(0.0, 1.0, &bucket_fraction_));
  options_parser.AddOption(
      "pad_to_block_boundary",
      ValueParser::Enum({{"", Padding::kTrue},
                         {"true", Padding::kTrue},
                         {"false", Padding::kFalse},
                         {"initially", Padding::kInitially}},
                        &pad_to_block_boundary_));
  options_parser.AddOption(
      "parallelism",
      ValueParser::Int(0, std::numeric_limits<int>::max(), &parallelism_));
  if (ABSL_PREDICT_FALSE(!options_parser.FromString(text))) {
    return options_parser.status();
  }
  return compressor_options_.FromString(compressor_text);
}

class RecordWriterBase::Worker {
 public:
  explicit Worker(ChunkWriter* chunk_writer, Options&& options);

  virtual ~Worker();

  bool Close();
  virtual absl::Status status() const = 0;
  virtual absl::Status AnnotateStatus(absl::Status status) = 0;

  // Precondition for `Close()`: chunk is not open.

  // Precondition: chunk is not open.
  virtual void OpenChunk() = 0;

  // Precondition: chunk is open.
  template <typename Record>
  bool AddRecord(Record&& record);
  bool AddRecord(const google::protobuf::MessageLite& record,
                 SerializeOptions serialize_options);

  // Precondition: chunk is open.
  //
  // If the result is `false` then `!ok()`.
  virtual bool CloseChunk() = 0;

  bool MaybePadToBlockBoundary();

  // Precondition: chunk is not open.
  virtual bool Flush(FlushType flush_type) = 0;

  // Precondition: chunk is not open.
  virtual FutureStatus FutureFlush(FlushType flush_type) = 0;

  virtual FutureRecordPosition LastPos() const = 0;

  virtual FutureRecordPosition Pos() const = 0;

  virtual Position EstimatedSize() const = 0;

 protected:
  void Initialize(Position initial_pos);

  virtual void Done() {}
  virtual bool ok() const = 0;
  ABSL_ATTRIBUTE_COLD bool Fail(absl::Status status);
  virtual bool FailWithoutAnnotation(absl::Status status) = 0;

  virtual bool WriteSignature() = 0;
  virtual bool WriteMetadata() = 0;
  virtual bool PadToBlockBoundary() = 0;

  std::unique_ptr<ChunkEncoder> MakeChunkEncoder();
  void EncodeSignature(Chunk& chunk);
  bool EncodeMetadata(Chunk& chunk);
  bool EncodeChunk(ChunkEncoder& chunk_encoder, Chunk& chunk);

  ObjectState state_;
  Options options_;
  // Invariant: `chunk_writer_ != nullptr`
  ChunkWriter* chunk_writer_;
  // Invariant: if chunk is open then `chunk_encoder_ != nullptr`
  std::unique_ptr<ChunkEncoder> chunk_encoder_;
};

inline RecordWriterBase::Worker::Worker(ChunkWriter* chunk_writer,
                                        Options&& options)
    : options_(std::move(options)),
      chunk_writer_(RIEGELI_ASSERT_NOTNULL(chunk_writer)),
      chunk_encoder_(MakeChunkEncoder()) {
  if (ABSL_PREDICT_FALSE(!chunk_writer_->ok())) {
    // `FailWithoutAnnotation()` is pure virtual and must not be called from the
    // constructor.
    state_.Fail(chunk_writer_->status());
  }
}

RecordWriterBase::Worker::~Worker() {}

bool RecordWriterBase::Worker::Close() {
  if (ABSL_PREDICT_FALSE(!state_.is_open())) return state_.not_failed();
  Done();
  return state_.MarkClosed();
}

inline bool RecordWriterBase::Worker::Fail(absl::Status status) {
  return FailWithoutAnnotation(AnnotateStatus(std::move(status)));
}

inline void RecordWriterBase::Worker::Initialize(Position initial_pos) {
  if (initial_pos == 0) {
    if (ABSL_PREDICT_FALSE(!WriteSignature())) return;
    if (ABSL_PREDICT_FALSE(!WriteMetadata())) return;
  } else if (options_.pad_to_block_boundary() != Padding::kFalse) {
    PadToBlockBoundary();
  }
}

inline bool RecordWriterBase::Worker::MaybePadToBlockBoundary() {
  if (options_.pad_to_block_boundary() == Padding::kTrue) {
    return PadToBlockBoundary();
  } else {
    return true;
  }
}

inline std::unique_ptr<ChunkEncoder>
RecordWriterBase::Worker::MakeChunkEncoder() {
  std::unique_ptr<ChunkEncoder> chunk_encoder;
  if (options_.transpose()) {
    const long double long_double_bucket_size =
        std::round(static_cast<long double>(options_.effective_chunk_size()) *
                   static_cast<long double>(options_.bucket_fraction()));
    const uint64_t bucket_size =
        ABSL_PREDICT_FALSE(
            long_double_bucket_size >=
            static_cast<long double>(std::numeric_limits<uint64_t>::max()))
            ? std::numeric_limits<uint64_t>::max()
        : ABSL_PREDICT_TRUE(long_double_bucket_size >= 1.0L)
            ? static_cast<uint64_t>(long_double_bucket_size)
            : uint64_t{1};
    chunk_encoder = std::make_unique<TransposeEncoder>(
        options_.compressor_options(),
        TransposeEncoder::TuningOptions()
            .set_bucket_size(bucket_size)
            .set_recycling_pool_options(options_.recycling_pool_options()));
  } else {
    chunk_encoder = std::make_unique<SimpleEncoder>(
        options_.compressor_options(),
        SimpleEncoder::TuningOptions()
            .set_size_hint(options_.effective_chunk_size())
            .set_recycling_pool_options(options_.recycling_pool_options()));
  }
  if (options_.parallelism() == 0) {
    return chunk_encoder;
  } else {
    return std::make_unique<DeferredEncoder>(std::move(chunk_encoder));
  }
}

inline void RecordWriterBase::Worker::EncodeSignature(Chunk& chunk) {
  chunk.header = ChunkHeader(chunk.data, ChunkType::kFileSignature, 0, 0);
}

inline bool RecordWriterBase::Worker::EncodeMetadata(Chunk& chunk) {
  TransposeEncoder transpose_encoder(
      options_.compressor_options(),
      TransposeEncoder::TuningOptions().set_recycling_pool_options(
          options_.recycling_pool_options()));
  if (ABSL_PREDICT_FALSE(
          options_.metadata() != absl::nullopt
              ? !transpose_encoder.AddRecord(*options_.metadata())
              : !transpose_encoder.AddRecord(
                    *options_.serialized_metadata()))) {
    return Fail(transpose_encoder.status());
  }
  ChainWriter<> data_writer(&chunk.data);
  ChunkType chunk_type;
  uint64_t num_records;
  uint64_t decoded_data_size;
  if (ABSL_PREDICT_FALSE(!transpose_encoder.EncodeAndClose(
          data_writer, chunk_type, num_records, decoded_data_size))) {
    return Fail(transpose_encoder.status());
  }
  if (ABSL_PREDICT_FALSE(!data_writer.Close())) {
    return Fail(data_writer.status());
  }
  chunk.header =
      ChunkHeader(chunk.data, ChunkType::kFileMetadata, 0, decoded_data_size);
  return true;
}

template <typename Record>
inline bool RecordWriterBase::Worker::AddRecord(Record&& record) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (ABSL_PREDICT_FALSE(
          !chunk_encoder_->AddRecord(std::forward<Record>(record)))) {
    return Fail(chunk_encoder_->status());
  }
  return true;
}

inline bool RecordWriterBase::Worker::AddRecord(
    const google::protobuf::MessageLite& record,
    SerializeOptions serialize_options) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (ABSL_PREDICT_FALSE(
          !chunk_encoder_->AddRecord(record, std::move(serialize_options)))) {
    return Fail(chunk_encoder_->status());
  }
  return true;
}

inline bool RecordWriterBase::Worker::EncodeChunk(ChunkEncoder& chunk_encoder,
                                                  Chunk& chunk) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  ChunkType chunk_type;
  uint64_t num_records;
  uint64_t decoded_data_size;
  ChainWriter<> data_writer(&chunk.data);
  if (ABSL_PREDICT_FALSE(!chunk_encoder.EncodeAndClose(
          data_writer, chunk_type, num_records, decoded_data_size))) {
    return Fail(chunk_encoder.status());
  }
  if (ABSL_PREDICT_FALSE(!data_writer.Close())) {
    return Fail(data_writer.status());
  }
  chunk.header =
      ChunkHeader(chunk.data, chunk_type, num_records, decoded_data_size);
  return true;
}

class RecordWriterBase::SerialWorker : public Worker {
 public:
  explicit SerialWorker(ChunkWriter* chunk_writer, Options&& options);

  absl::Status status() const override;
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatus(absl::Status status) override;

  void OpenChunk() override { chunk_encoder_->Clear(); }
  bool CloseChunk() override;
  bool Flush(FlushType flush_type) override;
  FutureStatus FutureFlush(FlushType flush_type) override;
  FutureRecordPosition LastPos() const override;
  FutureRecordPosition Pos() const override;
  Position EstimatedSize() const override;

 protected:
  bool ok() const override;
  ABSL_ATTRIBUTE_COLD bool FailWithoutAnnotation(absl::Status status) override;

  bool WriteSignature() override;
  bool WriteMetadata() override;
  bool PadToBlockBoundary() override;
};

inline RecordWriterBase::SerialWorker::SerialWorker(ChunkWriter* chunk_writer,
                                                    Options&& options)
    : Worker(chunk_writer, std::move(options)) {
  Initialize(chunk_writer_->pos());
}

inline bool RecordWriterBase::SerialWorker::ok() const { return state_.ok(); }

inline absl::Status RecordWriterBase::SerialWorker::status() const {
  return state_.status();
}

bool RecordWriterBase::SerialWorker::FailWithoutAnnotation(
    absl::Status status) {
  return state_.Fail(std::move(status));
}

absl::Status RecordWriterBase::SerialWorker::AnnotateStatus(
    absl::Status status) {
  return chunk_writer_->AnnotateStatus(std::move(status));
}

bool RecordWriterBase::SerialWorker::WriteSignature() {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Chunk chunk;
  EncodeSignature(chunk);
  if (ABSL_PREDICT_FALSE(!chunk_writer_->WriteChunk(chunk))) {
    return FailWithoutAnnotation(chunk_writer_->status());
  }
  return true;
}

bool RecordWriterBase::SerialWorker::WriteMetadata() {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (options_.metadata() == absl::nullopt &&
      options_.serialized_metadata() == absl::nullopt) {
    return true;
  }
  Chunk chunk;
  if (ABSL_PREDICT_FALSE(!EncodeMetadata(chunk))) return false;
  if (ABSL_PREDICT_FALSE(!chunk_writer_->WriteChunk(chunk))) {
    return FailWithoutAnnotation(chunk_writer_->status());
  }
  return true;
}

bool RecordWriterBase::SerialWorker::CloseChunk() {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Chunk chunk;
  if (ABSL_PREDICT_FALSE(!EncodeChunk(*chunk_encoder_, chunk))) return false;
  if (ABSL_PREDICT_FALSE(!chunk_writer_->WriteChunk(chunk))) {
    return FailWithoutAnnotation(chunk_writer_->status());
  }
  return true;
}

bool RecordWriterBase::SerialWorker::PadToBlockBoundary() {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (ABSL_PREDICT_FALSE(!chunk_writer_->PadToBlockBoundary())) {
    return FailWithoutAnnotation(chunk_writer_->status());
  }
  return true;
}

bool RecordWriterBase::SerialWorker::Flush(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (ABSL_PREDICT_FALSE(!chunk_writer_->Flush(flush_type))) {
    return FailWithoutAnnotation(chunk_writer_->status());
  }
  return true;
}

RecordWriterBase::FutureStatus RecordWriterBase::SerialWorker::FutureFlush(
    FlushType flush_type) {
  std::promise<absl::Status> promise;
  promise.set_value(ABSL_PREDICT_TRUE(Flush(flush_type)) ? absl::OkStatus()
                                                         : status());
  return promise.get_future();
}

FutureRecordPosition RecordWriterBase::SerialWorker::LastPos() const {
  RIEGELI_ASSERT_GT(chunk_encoder_->num_records(), 0u)
      << "Failed invariant of RecordWriterBase::SerialWorker: "
         "last position should be valid but no record was encoded";
  return RecordPosition(chunk_writer_->pos(),
                        chunk_encoder_->num_records() - 1);
}

FutureRecordPosition RecordWriterBase::SerialWorker::Pos() const {
  return RecordPosition(chunk_writer_->pos(), chunk_encoder_->num_records());
}

Position RecordWriterBase::SerialWorker::EstimatedSize() const {
  return chunk_writer_->pos();
}

// `ParallelWorker` uses parallelism internally, but the class is still only
// thread-compatible, not thread-safe.
class RecordWriterBase::ParallelWorker : public Worker {
 public:
  explicit ParallelWorker(ChunkWriter* chunk_writer, Options&& options);

  ~ParallelWorker();

  absl::Status status() const override;
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatus(absl::Status status) override;

  void OpenChunk() override { chunk_encoder_ = MakeChunkEncoder(); }
  bool CloseChunk() override;
  bool Flush(FlushType flush_type) override;
  FutureStatus FutureFlush(FlushType flush_type) override;
  FutureRecordPosition LastPos() const override;
  FutureRecordPosition Pos() const override;
  Position EstimatedSize() const override;

 protected:
  void Done() override;
  bool ok() const override;
  ABSL_ATTRIBUTE_COLD bool FailWithoutAnnotation(absl::Status status) override;

  bool WriteSignature() override;
  bool WriteMetadata() override;
  bool PadToBlockBoundary() override;

 private:
  struct ChunkPromises {
    std::promise<ChunkHeader> chunk_header;
    std::promise<Chunk> chunk;
  };

  // A request to the chunk writer thread.
  struct DoneRequest {
    std::promise<void> done;
  };
  struct AnnotateStatusRequest {
    absl::Status status;
    std::promise<absl::Status> done;
  };
  struct WriteChunkRequest {
    std::shared_future<ChunkHeader> chunk_header;
    std::future<Chunk> chunk;
  };
  struct PadToBlockBoundaryRequest {};
  struct FlushRequest {
    FlushType flush_type;
    std::promise<absl::Status> done;
  };
  using ChunkWriterRequest =
      absl::variant<DoneRequest, AnnotateStatusRequest, WriteChunkRequest,
                    PadToBlockBoundaryRequest, FlushRequest>;

  bool HasCapacityForRequest() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  records_internal::FutureChunkBegin ChunkBegin() const;

  mutable absl::Mutex mutex_;
  std::deque<ChunkWriterRequest> chunk_writer_requests_ ABSL_GUARDED_BY(mutex_);
  // Position before handling `chunk_writer_requests_`.
  Position pos_before_chunks_ ABSL_GUARDED_BY(mutex_);
};

inline RecordWriterBase::ParallelWorker::ParallelWorker(
    ChunkWriter* chunk_writer, Options&& options)
    : Worker(chunk_writer, std::move(options)),
      pos_before_chunks_(chunk_writer_->pos()) {
  internal::ThreadPool::global().Schedule([this] {
    struct Visitor {
      bool operator()(DoneRequest& request) const {
        request.done.set_value();
        return false;
      }

      bool operator()(AnnotateStatusRequest& request) const {
        request.done.set_value(
            self->chunk_writer_->AnnotateStatus(request.status));
        return true;
      }

      bool operator()(WriteChunkRequest& request) const {
        // If `!ok()`, the chunk must still be waited for, to ensure that the
        // chunk encoder thread exits before the chunk writer thread responds to
        // `DoneRequest`.
        const Chunk chunk = request.chunk.get();
        if (ABSL_PREDICT_FALSE(!self->ok())) return true;
        if (ABSL_PREDICT_FALSE(!self->chunk_writer_->WriteChunk(chunk))) {
          self->FailWithoutAnnotation(self->chunk_writer_->status());
        }
        return true;
      }

      bool operator()(PadToBlockBoundaryRequest& request) const {
        if (ABSL_PREDICT_FALSE(!self->ok())) return true;
        if (ABSL_PREDICT_FALSE(!self->chunk_writer_->PadToBlockBoundary())) {
          self->FailWithoutAnnotation(self->chunk_writer_->status());
        }
        return true;
      }

      bool operator()(FlushRequest& request) const {
        if (ABSL_PREDICT_FALSE(!self->ok())) {
          request.done.set_value(self->status());
          return true;
        }
        if (ABSL_PREDICT_FALSE(
                !self->chunk_writer_->Flush(request.flush_type))) {
          self->FailWithoutAnnotation(self->chunk_writer_->status());
          request.done.set_value(self->status());
          return true;
        }
        request.done.set_value(absl::OkStatus());
        return true;
      }

      ParallelWorker* self;
    };

    mutex_.Lock();
    for (;;) {
      mutex_.Await(absl::Condition(
          +[](std::deque<ChunkWriterRequest>* chunk_writer_requests) {
            return !chunk_writer_requests->empty();
          },
          &chunk_writer_requests_));
      ChunkWriterRequest& request = chunk_writer_requests_.front();
      mutex_.Unlock();
      if (ABSL_PREDICT_FALSE(!absl::visit(Visitor{this}, request))) return;
      mutex_.Lock();
      chunk_writer_requests_.pop_front();
      pos_before_chunks_ = chunk_writer_->pos();
    }
  });
  Initialize(pos_before_chunks_);
}

RecordWriterBase::ParallelWorker::~ParallelWorker() {
  if (ABSL_PREDICT_FALSE(state_.is_open())) Done();
}

void RecordWriterBase::ParallelWorker::Done() {
  std::promise<void> done_promise;
  std::future<void> done_future = done_promise.get_future();
  {
    absl::MutexLock lock(&mutex_);
    chunk_writer_requests_.emplace_back(DoneRequest{std::move(done_promise)});
  }
  done_future.get();
}

inline bool RecordWriterBase::ParallelWorker::ok() const {
  absl::MutexLock lock(&mutex_);
  return state_.ok();
}

inline absl::Status RecordWriterBase::ParallelWorker::status() const {
  absl::MutexLock lock(&mutex_);
  return state_.status();
}

bool RecordWriterBase::ParallelWorker::FailWithoutAnnotation(
    absl::Status status) {
  absl::MutexLock lock(&mutex_);
  return state_.Fail(std::move(status));
}

absl::Status RecordWriterBase::ParallelWorker::AnnotateStatus(
    absl::Status status) {
  std::promise<absl::Status> done_promise;
  std::future<absl::Status> done_future = done_promise.get_future();
  {
    absl::MutexLock lock(
        &mutex_, absl::Condition(this, &ParallelWorker::HasCapacityForRequest));
    chunk_writer_requests_.emplace_back(
        AnnotateStatusRequest{std::move(status), std::move(done_promise)});
  }
  return done_future.get();
}

bool RecordWriterBase::ParallelWorker::HasCapacityForRequest() const {
  return chunk_writer_requests_.size() <
         IntCast<size_t>(options_.parallelism());
}

bool RecordWriterBase::ParallelWorker::WriteSignature() {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Chunk chunk;
  EncodeSignature(chunk);
  ChunkPromises chunk_promises;
  chunk_promises.chunk_header.set_value(chunk.header);
  chunk_promises.chunk.set_value(std::move(chunk));
  {
    absl::MutexLock lock(
        &mutex_, absl::Condition(this, &ParallelWorker::HasCapacityForRequest));
    chunk_writer_requests_.emplace_back(
        WriteChunkRequest{chunk_promises.chunk_header.get_future(),
                          chunk_promises.chunk.get_future()});
  }
  return true;
}

bool RecordWriterBase::ParallelWorker::WriteMetadata() {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (options_.metadata() == absl::nullopt &&
      options_.serialized_metadata() == absl::nullopt) {
    return true;
  }
  ChunkPromises* const chunk_promises = new ChunkPromises();
  {
    absl::MutexLock lock(
        &mutex_, absl::Condition(this, &ParallelWorker::HasCapacityForRequest));
    chunk_writer_requests_.emplace_back(
        WriteChunkRequest{chunk_promises->chunk_header.get_future(),
                          chunk_promises->chunk.get_future()});
  }
  internal::ThreadPool::global().Schedule([this, chunk_promises] {
    Chunk chunk;
    EncodeMetadata(chunk);
    chunk_promises->chunk_header.set_value(chunk.header);
    chunk_promises->chunk.set_value(std::move(chunk));
    delete chunk_promises;
  });
  return true;
}

bool RecordWriterBase::ParallelWorker::CloseChunk() {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  ChunkEncoder* const chunk_encoder = chunk_encoder_.release();
  ChunkPromises* const chunk_promises = new ChunkPromises();
  {
    absl::MutexLock lock(
        &mutex_, absl::Condition(this, &ParallelWorker::HasCapacityForRequest));
    chunk_writer_requests_.emplace_back(
        WriteChunkRequest{chunk_promises->chunk_header.get_future(),
                          chunk_promises->chunk.get_future()});
  }
  internal::ThreadPool::global().Schedule(
      [this, chunk_encoder, chunk_promises] {
        Chunk chunk;
        EncodeChunk(*chunk_encoder, chunk);
        delete chunk_encoder;
        chunk_promises->chunk_header.set_value(chunk.header);
        chunk_promises->chunk.set_value(std::move(chunk));
        delete chunk_promises;
      });
  return true;
}

bool RecordWriterBase::ParallelWorker::PadToBlockBoundary() {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  {
    absl::MutexLock lock(
        &mutex_, absl::Condition(this, &ParallelWorker::HasCapacityForRequest));
    chunk_writer_requests_.emplace_back(PadToBlockBoundaryRequest());
  }
  return true;
}

bool RecordWriterBase::ParallelWorker::Flush(FlushType flush_type) {
  return FutureFlush(flush_type).get().ok();
}

RecordWriterBase::FutureStatus RecordWriterBase::ParallelWorker::FutureFlush(
    FlushType flush_type) {
  std::promise<absl::Status> done_promise;
  FutureStatus done_future = done_promise.get_future();
  {
    absl::MutexLock lock(
        &mutex_, absl::Condition(this, &ParallelWorker::HasCapacityForRequest));
    chunk_writer_requests_.emplace_back(
        FlushRequest{flush_type, std::move(done_promise)});
  }
  return done_future;
}

records_internal::FutureChunkBegin
RecordWriterBase::ParallelWorker::ChunkBegin() const {
  struct Visitor {
    void operator()(const DoneRequest&) {}
    void operator()(const AnnotateStatusRequest&) {}
    void operator()(const WriteChunkRequest& request) {
      actions.emplace_back(request.chunk_header);
    }
    void operator()(const PadToBlockBoundaryRequest&) {
      actions.emplace_back(
          records_internal::FutureChunkBegin::PadToBlockBoundary());
    }
    void operator()(const FlushRequest&) {}

    std::vector<records_internal::FutureChunkBegin::Action> actions;
  };
  Visitor visitor;
  absl::MutexLock lock(&mutex_);
  visitor.actions.reserve(chunk_writer_requests_.size());
  for (const ChunkWriterRequest& request : chunk_writer_requests_) {
    absl::visit(visitor, request);
  }
  return records_internal::FutureChunkBegin(pos_before_chunks_,
                                            std::move(visitor.actions));
}

FutureRecordPosition RecordWriterBase::ParallelWorker::LastPos() const {
  RIEGELI_ASSERT(chunk_encoder_ != nullptr)
      << "Failed invariant of RecordWriterBase::ParallelWorker: "
         "last position should be valid but chunk is closed";
  RIEGELI_ASSERT_GT(chunk_encoder_->num_records(), 0u)
      << "Failed invariant of RecordWriterBase::ParallelWorker: "
         "last position should be valid but no record was encoded";
  return FutureRecordPosition(ChunkBegin(), chunk_encoder_->num_records() - 1);
}

FutureRecordPosition RecordWriterBase::ParallelWorker::Pos() const {
  // `chunk_encoder_` is `nullptr` when the current chunk is closed, e.g. when
  // `RecordWriter` is closed or if `RecordWriter::Flush()` failed.
  return FutureRecordPosition(ChunkBegin(),
                              ABSL_PREDICT_FALSE(chunk_encoder_ == nullptr)
                                  ? uint64_t{0}
                                  : chunk_encoder_->num_records());
}

Position RecordWriterBase::ParallelWorker::EstimatedSize() const {
  absl::MutexLock lock(&mutex_);
  return pos_before_chunks_;
}

RecordWriterBase::RecordWriterBase(Closed) noexcept : Object(kClosed) {}

RecordWriterBase::RecordWriterBase() noexcept {}

void RecordWriterBase::Reset(Closed) {
  DoneBackground();
  Object::Reset(kClosed);
  desired_chunk_size_ = 0;
  chunk_size_so_far_ = 0;
  last_record_ = LastRecordIsInvalid();
}

void RecordWriterBase::Reset() {
  DoneBackground();
  Object::Reset();
  desired_chunk_size_ = 0;
  chunk_size_so_far_ = 0;
  last_record_ = LastRecordIsInvalid();
}

RecordWriterBase::RecordWriterBase(RecordWriterBase&& that) noexcept
    : Object(static_cast<Object&&>(that)),
      desired_chunk_size_(that.desired_chunk_size_),
      chunk_size_so_far_(that.chunk_size_so_far_),
      last_record_(std::exchange(that.last_record_, LastRecordIsInvalid())),
      worker_(std::move(that.worker_)) {}

RecordWriterBase& RecordWriterBase::operator=(
    RecordWriterBase&& that) noexcept {
  if (ABSL_PREDICT_TRUE(&that != this)) {
    DoneBackground();
    Object::operator=(static_cast<Object&&>(that));
    desired_chunk_size_ = that.desired_chunk_size_;
    chunk_size_so_far_ = that.chunk_size_so_far_;
    last_record_ = std::exchange(that.last_record_, LastRecordIsInvalid());
    worker_ = std::move(that.worker_);
  }
  return *this;
}

RecordWriterBase::~RecordWriterBase() {}

void RecordWriterBase::Initialize(ChunkWriter* dest, Options&& options) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of RecordWriter: null ChunkWriter pointer";
  // Ensure that `num_records` does not overflow when `WriteRecordImpl()` keeps
  // `num_records * sizeof(uint64_t)` under `desired_chunk_size_`.
  desired_chunk_size_ = UnsignedMin(options.effective_chunk_size(),
                                    kMaxNumRecords * sizeof(uint64_t));
  if (options.parallelism() == 0) {
    worker_ = std::make_unique<SerialWorker>(dest, std::move(options));
  } else {
    worker_ = std::make_unique<ParallelWorker>(dest, std::move(options));
  }
  {
    absl::Status status = worker_->status();
    if (ABSL_PREDICT_FALSE(!status.ok())) {
      FailWithoutAnnotation(std::move(status));
    }
  }
}

void RecordWriterBase::Done() {
  if (ABSL_PREDICT_FALSE(worker_ == nullptr)) {
    RIEGELI_ASSERT(!is_open()) << "Failed invariant of RecordWriterBase: "
                                  "null worker_ but RecordWriterBase is_open()";
    return;
  }
  if (chunk_size_so_far_ > 0) {
    if (absl::holds_alternative<LastRecordIsValid>(last_record_)) {
      last_record_ = LastRecordIsValidAt{worker_->LastPos()};
    }
    if (ABSL_PREDICT_FALSE(!worker_->CloseChunk())) {
      FailWithoutAnnotation(worker_->status());
    }
    chunk_size_so_far_ = 0;
  }
  if (ABSL_PREDICT_FALSE(!worker_->MaybePadToBlockBoundary())) {
    FailWithoutAnnotation(worker_->status());
  }
  if (ABSL_PREDICT_FALSE(!worker_->Close())) {
    FailWithoutAnnotation(worker_->status());
  }
}

void RecordWriterBase::DoneBackground() { worker_.reset(); }

absl::Status RecordWriterBase::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) {
    RIEGELI_ASSERT(worker_ != nullptr)
        << "Failed invariant of RecordWriterBase: "
           "null worker_ but RecordWriterBase is_open()";
    status = worker_->AnnotateStatus(std::move(status));
  }
  return AnnotateOverDest(std::move(status));
}

absl::Status RecordWriterBase::AnnotateOverDest(absl::Status status) {
  return Annotate(status, absl::StrCat("at record ", Pos().get().ToString()));
}

bool RecordWriterBase::WriteRecord(const google::protobuf::MessageLite& record,
                                   SerializeOptions serialize_options) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  const size_t size = serialize_options.GetByteSize(record);
  return WriteRecordImpl(size, record, std::move(serialize_options));
}

bool RecordWriterBase::WriteRecord(absl::string_view record) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  return WriteRecordImpl(record.size(), record);
}

template <typename Src,
          std::enable_if_t<std::is_same<Src, std::string>::value, int>>
bool RecordWriterBase::WriteRecord(Src&& record) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  const size_t size = record.size();
  // `std::move(record)` is correct and `std::forward<Src>(record)` is not
  // necessary: `Src` is always `std::string`, never an lvalue reference.
  return WriteRecordImpl(size, std::move(record));
}

template bool RecordWriterBase::WriteRecord(std::string&& record);

bool RecordWriterBase::WriteRecord(const Chain& record) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  return WriteRecordImpl(record.size(), record);
}

bool RecordWriterBase::WriteRecord(Chain&& record) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  const size_t size = record.size();
  return WriteRecordImpl(size, std::move(record));
}

bool RecordWriterBase::WriteRecord(const absl::Cord& record) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  return WriteRecordImpl(record.size(), record);
}

bool RecordWriterBase::WriteRecord(absl::Cord&& record) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  const size_t size = record.size();
  return WriteRecordImpl(size, std::move(record));
}

template <typename... Args>
inline bool RecordWriterBase::WriteRecordImpl(size_t size, Args&&... args) {
  last_record_ = LastRecordIsInvalid();
  // Decoding a chunk writes records to one array, and their positions to
  // another array. We limit the size of both arrays together, to include
  // attempts to accumulate an unbounded number of empty records.
  const uint64_t added_size = uint64_t{size} + uint64_t{sizeof(uint64_t)};
  if (ABSL_PREDICT_FALSE(chunk_size_so_far_ + added_size >
                         desired_chunk_size_) &&
      chunk_size_so_far_ > 0) {
    if (ABSL_PREDICT_FALSE(!worker_->CloseChunk())) {
      return FailWithoutAnnotation(worker_->status());
    }
    worker_->OpenChunk();
    chunk_size_so_far_ = 0;
  }
  chunk_size_so_far_ += added_size;
  if (ABSL_PREDICT_FALSE(!worker_->AddRecord(std::forward<Args>(args)...))) {
    return FailWithoutAnnotation(worker_->status());
  }
  if (ABSL_PREDICT_FALSE(chunk_size_so_far_ + uint64_t{sizeof(uint64_t)} >
                         desired_chunk_size_)) {
    // No more records will fit in this chunk, most likely a single record
    // exceeds the desired chunk size. Write the chunk now to avoid keeping a
    // large chunk in memory.
    last_record_ = LastRecordIsValidAt{worker_->LastPos()};
    if (ABSL_PREDICT_FALSE(!worker_->CloseChunk())) {
      return FailWithoutAnnotation(worker_->status());
    }
    worker_->OpenChunk();
    chunk_size_so_far_ = 0;
    return true;
  }
  last_record_ = LastRecordIsValid();
  return true;
}

bool RecordWriterBase::Flush(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (chunk_size_so_far_ > 0) {
    if (absl::holds_alternative<LastRecordIsValid>(last_record_)) {
      last_record_ = LastRecordIsValidAt{worker_->LastPos()};
    }
    if (ABSL_PREDICT_FALSE(!worker_->CloseChunk())) {
      return FailWithoutAnnotation(worker_->status());
    }
  }
  if (ABSL_PREDICT_FALSE(!worker_->MaybePadToBlockBoundary())) {
    return FailWithoutAnnotation(worker_->status());
  }
  if (flush_type != FlushType::kFromObject || IsOwning()) {
    if (ABSL_PREDICT_FALSE(!worker_->Flush(flush_type))) {
      return FailWithoutAnnotation(worker_->status());
    }
  }
  if (chunk_size_so_far_ > 0) {
    worker_->OpenChunk();
    chunk_size_so_far_ = 0;
  }
  return true;
}

RecordWriterBase::FutureStatus RecordWriterBase::FutureFlush(
    FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!ok())) {
    std::promise<absl::Status> promise;
    promise.set_value(status());
    return promise.get_future();
  }
  if (chunk_size_so_far_ > 0) {
    if (absl::holds_alternative<LastRecordIsValid>(last_record_)) {
      last_record_ = LastRecordIsValidAt{worker_->LastPos()};
    }
    if (ABSL_PREDICT_FALSE(!worker_->CloseChunk())) {
      FailWithoutAnnotation(worker_->status());
      std::promise<absl::Status> promise;
      promise.set_value(status());
      return promise.get_future();
    }
  }
  if (ABSL_PREDICT_FALSE(!worker_->MaybePadToBlockBoundary())) {
    FailWithoutAnnotation(worker_->status());
    std::promise<absl::Status> promise;
    promise.set_value(status());
    return promise.get_future();
  }
  FutureStatus result;
  if (flush_type == FlushType::kFromObject && !IsOwning()) {
    std::promise<absl::Status> promise;
    promise.set_value(absl::OkStatus());
    result = promise.get_future();
  } else {
    result = worker_->FutureFlush(flush_type);
  }
  if (chunk_size_so_far_ > 0) {
    worker_->OpenChunk();
    chunk_size_so_far_ = 0;
  }
  return result;
}

FutureRecordPosition RecordWriterBase::LastPos() const {
  RIEGELI_ASSERT(last_record_is_valid())
      << "Failed precondition of RecordWriterBase::LastPos(): "
         "no record was recently written";
  {
    const LastRecordIsValidAt* const last_record_at_pos =
        absl::get_if<LastRecordIsValidAt>(&last_record_);
    if (last_record_at_pos != nullptr) {
      return last_record_at_pos->pos;
    }
  }
  RIEGELI_ASSERT(worker_ != nullptr)
      << "Failed invariant of RecordWriterBase: "
         "last position should be valid but worker is null";
  return worker_->LastPos();
}

FutureRecordPosition RecordWriterBase::Pos() const {
  if (ABSL_PREDICT_FALSE(worker_ == nullptr)) return FutureRecordPosition();
  return worker_->Pos();
}

Position RecordWriterBase::EstimatedSize() const {
  if (ABSL_PREDICT_FALSE(worker_ == nullptr)) return 0;
  return worker_->EstimatedSize();
}

}  // namespace riegeli
