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
#include <utility>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_set.h"
#include "absl/memory/memory.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/variant.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/descriptor.pb.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/base.h"
#include "riegeli/base/canonical_errors.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/base/options_parser.h"
#include "riegeli/base/parallelism.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/chain_writer.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/chunk_encoding/chunk_encoder.h"
#include "riegeli/chunk_encoding/compressor_options.h"
#include "riegeli/chunk_encoding/constants.h"
#include "riegeli/chunk_encoding/deferred_encoder.h"
#include "riegeli/chunk_encoding/simple_encoder.h"
#include "riegeli/chunk_encoding/transpose_encoder.h"
#include "riegeli/records/chunk_writer.h"
#include "riegeli/records/record_position.h"
#include "riegeli/records/records_metadata.pb.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if __cplusplus < 201703
constexpr int RecordWriterBase::Options::kMinBrotli;
constexpr int RecordWriterBase::Options::kMaxBrotli;
constexpr int RecordWriterBase::Options::kDefaultBrotli;
constexpr int RecordWriterBase::Options::kMinZstd;
constexpr int RecordWriterBase::Options::kMaxZstd;
constexpr int RecordWriterBase::Options::kDefaultZstd;
constexpr int RecordWriterBase::Options::kMinWindowLog;
constexpr int RecordWriterBase::Options::kMaxWindowLog;
constexpr int RecordWriterBase::Options::kDefaultWindowLog;
constexpr uint64_t RecordWriterBase::Options::kDefaultChunkSize;
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

template <typename Record>
inline size_t RecordSize(const Record& record) {
  return record.size();
}

inline size_t RecordSize(const google::protobuf::MessageLite& record) {
  return record.ByteSizeLong();
}

}  // namespace

void SetRecordType(RecordsMetadata* metadata,
                   const google::protobuf::Descriptor* descriptor) {
  metadata->set_record_type_name(descriptor->full_name());
  metadata->clear_file_descriptor();
  FileDescriptorCollector collector(metadata->mutable_file_descriptor());
  collector.AddFile(descriptor->file());
}

Status RecordWriterBase::Options::FromString(absl::string_view text) {
  std::string compressor_text;
  OptionsParser options_parser;
  options_parser.AddOption("default", ValueParser::FailIfAnySeen());
  options_parser.AddOption(
      "transpose",
      ValueParser::Enum(&transpose_,
                        {{"", true}, {"true", true}, {"false", false}}));
  options_parser.AddOption("uncompressed",
                           ValueParser::CopyTo(&compressor_text));
  options_parser.AddOption("brotli", ValueParser::CopyTo(&compressor_text));
  options_parser.AddOption("zstd", ValueParser::CopyTo(&compressor_text));
  options_parser.AddOption("window_log", ValueParser::CopyTo(&compressor_text));
  options_parser.AddOption(
      "chunk_size", ValueParser::Bytes(&chunk_size_, 1,
                                       std::numeric_limits<uint64_t>::max()));
  options_parser.AddOption("bucket_fraction",
                           ValueParser::Real(&bucket_fraction_, 0.0, 1.0));
  options_parser.AddOption(
      "pad_to_block_boundary",
      ValueParser::Enum(&pad_to_block_boundary_,
                        {{"", true}, {"true", true}, {"false", false}}));
  options_parser.AddOption(
      "parallelism",
      ValueParser::Int(&parallelism_, 0, std::numeric_limits<int>::max()));
  if (ABSL_PREDICT_FALSE(!options_parser.FromString(text))) {
    return options_parser.status();
  }
  return compressor_options_.FromString(compressor_text);
}

class RecordWriterBase::Worker : public Object {
 public:
  explicit Worker(ChunkWriter* chunk_writer, Options&& options)
      : Object(kInitiallyOpen),
        options_(std::move(options)),
        chunk_writer_(RIEGELI_ASSERT_NOTNULL(chunk_writer)),
        chunk_encoder_(MakeChunkEncoder()) {
    if (ABSL_PREDICT_FALSE(!chunk_writer_->healthy())) Fail(*chunk_writer_);
  }

  ~Worker();

  // Precondition for Close(): chunk is not open.

  // Precondition: chunk is not open.
  virtual void OpenChunk() = 0;

  // Precondition: chunk is open.
  template <typename Record>
  bool AddRecord(Record&& record);

  // Precondition: chunk is open.
  //
  // If the result is false then !healthy().
  virtual bool CloseChunk() = 0;

  bool MaybePadToBlockBoundary();

  // Precondition: chunk is not open.
  virtual bool Flush(FlushType flush_type) = 0;

  virtual FutureRecordPosition Pos() const = 0;

 protected:
  void Initialize(Position initial_pos);
  virtual bool WriteSignature() = 0;
  virtual bool WriteMetadata() = 0;
  virtual bool PadToBlockBoundary() = 0;

  std::unique_ptr<ChunkEncoder> MakeChunkEncoder();
  void EncodeSignature(Chunk* chunk);
  bool EncodeMetadata(Chunk* chunk);
  bool EncodeChunk(ChunkEncoder* chunk_encoder, Chunk* chunk);

  Options options_;
  // Invariant: chunk_writer_ != nullptr
  ChunkWriter* chunk_writer_;
  // Invariant: if chunk is open then chunk_encoder_ != nullptr
  std::unique_ptr<ChunkEncoder> chunk_encoder_;
};

RecordWriterBase::Worker::~Worker() {}

inline void RecordWriterBase::Worker::Initialize(Position initial_pos) {
  if (initial_pos == 0) {
    if (ABSL_PREDICT_FALSE(!WriteSignature())) return;
    if (ABSL_PREDICT_FALSE(!WriteMetadata())) return;
  } else {
    MaybePadToBlockBoundary();
  }
}

inline bool RecordWriterBase::Worker::MaybePadToBlockBoundary() {
  if (options_.pad_to_block_boundary_) {
    return PadToBlockBoundary();
  } else {
    return true;
  }
}

inline std::unique_ptr<ChunkEncoder>
RecordWriterBase::Worker::MakeChunkEncoder() {
  std::unique_ptr<ChunkEncoder> chunk_encoder;
  if (options_.transpose_) {
    const long double long_double_bucket_size =
        std::round(static_cast<long double>(options_.chunk_size_) *
                   static_cast<long double>(options_.bucket_fraction_));
    const uint64_t bucket_size =
        ABSL_PREDICT_FALSE(
            long_double_bucket_size >=
            static_cast<long double>(std::numeric_limits<uint64_t>::max()))
            ? std::numeric_limits<uint64_t>::max()
            : ABSL_PREDICT_TRUE(long_double_bucket_size >= 1.0L)
                  ? static_cast<uint64_t>(long_double_bucket_size)
                  : uint64_t{1};
    chunk_encoder = absl::make_unique<TransposeEncoder>(
        options_.compressor_options_, bucket_size);
  } else {
    chunk_encoder = absl::make_unique<SimpleEncoder>(
        options_.compressor_options_, options_.chunk_size_);
  }
  if (options_.parallelism_ == 0) {
    return chunk_encoder;
  } else {
    return absl::make_unique<DeferredEncoder>(std::move(chunk_encoder));
  }
}

inline void RecordWriterBase::Worker::EncodeSignature(Chunk* chunk) {
  chunk->header = ChunkHeader(chunk->data, ChunkType::kFileSignature, 0, 0);
}

inline bool RecordWriterBase::Worker::EncodeMetadata(Chunk* chunk) {
  TransposeEncoder transpose_encoder(options_.compressor_options_,
                                     std::numeric_limits<uint64_t>::max());
  if (ABSL_PREDICT_FALSE(
          options_.serialized_metadata_.empty()
              ? !transpose_encoder.AddRecord(options_.metadata_)
              : !transpose_encoder.AddRecord(options_.serialized_metadata_))) {
    return Fail(transpose_encoder);
  }
  ChainWriter<> data_writer(&chunk->data);
  ChunkType chunk_type;
  uint64_t num_records;
  uint64_t decoded_data_size;
  if (ABSL_PREDICT_FALSE(!transpose_encoder.EncodeAndClose(
          &data_writer, &chunk_type, &num_records, &decoded_data_size))) {
    return Fail(transpose_encoder);
  }
  if (ABSL_PREDICT_FALSE(!data_writer.Close())) return Fail(data_writer);
  chunk->header =
      ChunkHeader(chunk->data, ChunkType::kFileMetadata, 0, decoded_data_size);
  return true;
}

template <typename Record>
inline bool RecordWriterBase::Worker::AddRecord(Record&& record) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(
          !chunk_encoder_->AddRecord(std::forward<Record>(record)))) {
    return Fail(*chunk_encoder_);
  }
  return true;
}

inline bool RecordWriterBase::Worker::EncodeChunk(ChunkEncoder* chunk_encoder,
                                                  Chunk* chunk) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  ChunkType chunk_type;
  uint64_t num_records;
  uint64_t decoded_data_size;
  chunk->data.Clear();
  ChainWriter<> data_writer(&chunk->data);
  if (ABSL_PREDICT_FALSE(!chunk_encoder->EncodeAndClose(
          &data_writer, &chunk_type, &num_records, &decoded_data_size))) {
    return Fail(*chunk_encoder);
  }
  if (ABSL_PREDICT_FALSE(!data_writer.Close())) return Fail(data_writer);
  chunk->header =
      ChunkHeader(chunk->data, chunk_type, num_records, decoded_data_size);
  return true;
}

class RecordWriterBase::SerialWorker : public Worker {
 public:
  explicit SerialWorker(ChunkWriter* chunk_writer, Options&& options);

  void OpenChunk() override { chunk_encoder_->Clear(); }
  bool CloseChunk() override;
  bool Flush(FlushType flush_type) override;
  FutureRecordPosition Pos() const override;

 protected:
  bool WriteSignature() override;
  bool WriteMetadata() override;
  bool PadToBlockBoundary() override;
};

inline RecordWriterBase::SerialWorker::SerialWorker(ChunkWriter* chunk_writer,
                                                    Options&& options)
    : Worker(chunk_writer, std::move(options)) {
  Initialize(chunk_writer_->pos());
}

bool RecordWriterBase::SerialWorker::WriteSignature() {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Chunk chunk;
  EncodeSignature(&chunk);
  if (ABSL_PREDICT_FALSE(!chunk_writer_->WriteChunk(chunk))) {
    return Fail(*chunk_writer_);
  }
  return true;
}

bool RecordWriterBase::SerialWorker::WriteMetadata() {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (options_.metadata_.ByteSizeLong() == 0 &&
      options_.serialized_metadata_.empty()) {
    return true;
  }
  Chunk chunk;
  if (ABSL_PREDICT_FALSE(!EncodeMetadata(&chunk))) return false;
  if (ABSL_PREDICT_FALSE(!chunk_writer_->WriteChunk(chunk))) {
    return Fail(*chunk_writer_);
  }
  return true;
}

bool RecordWriterBase::SerialWorker::CloseChunk() {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Chunk chunk;
  if (ABSL_PREDICT_FALSE(!EncodeChunk(chunk_encoder_.get(), &chunk))) {
    return false;
  }
  if (ABSL_PREDICT_FALSE(!chunk_writer_->WriteChunk(chunk))) {
    return Fail(*chunk_writer_);
  }
  return true;
}

bool RecordWriterBase::SerialWorker::PadToBlockBoundary() {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(!chunk_writer_->PadToBlockBoundary())) {
    return Fail(*chunk_writer_);
  }
  return true;
}

bool RecordWriterBase::SerialWorker::Flush(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(!chunk_writer_->Flush(flush_type))) {
    return Fail(*chunk_writer_);
  }
  return true;
}

FutureRecordPosition RecordWriterBase::SerialWorker::Pos() const {
  return FutureRecordPosition(
      RecordPosition(chunk_writer_->pos(), chunk_encoder_->num_records()));
}

// ParallelWorker uses parallelism internally, but the class is still only
// thread-compatible, not thread-safe.
class RecordWriterBase::ParallelWorker : public Worker {
 public:
  explicit ParallelWorker(ChunkWriter* chunk_writer, Options&& options);

  ~ParallelWorker();

  void OpenChunk() override { chunk_encoder_ = MakeChunkEncoder(); }
  bool CloseChunk() override;
  bool Flush(FlushType flush_type) override;
  FutureRecordPosition Pos() const override;

 protected:
  void Done() override;
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
  struct WriteChunkRequest {
    std::shared_future<ChunkHeader> chunk_header;
    std::future<Chunk> chunk;
  };
  struct PadToBlockBoundaryRequest {};
  struct FlushRequest {
    FlushType flush_type;
    std::promise<bool> done;
  };
  using ChunkWriterRequest =
      absl::variant<DoneRequest, WriteChunkRequest, PadToBlockBoundaryRequest,
                    FlushRequest>;

  bool HasCapacityForRequest() const;

  mutable absl::Mutex mutex_;
  std::deque<ChunkWriterRequest> chunk_writer_requests_ GUARDED_BY(mutex_);
  // Position before handling chunk_writer_requests_.
  Position pos_before_chunks_ GUARDED_BY(mutex_);
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

      bool operator()(WriteChunkRequest& request) const {
        // If !healthy(), the chunk must still be waited for, to ensure that
        // the chunk encoder thread exits before the chunk writer thread
        // responds to DoneRequest.
        const Chunk chunk = request.chunk.get();
        if (ABSL_PREDICT_FALSE(!self->healthy())) return true;
        if (ABSL_PREDICT_FALSE(!self->chunk_writer_->WriteChunk(chunk))) {
          self->Fail(*self->chunk_writer_);
        }
        return true;
      }

      bool operator()(PadToBlockBoundaryRequest& request) const {
        if (ABSL_PREDICT_FALSE(!self->healthy())) return true;
        if (ABSL_PREDICT_FALSE(!self->chunk_writer_->PadToBlockBoundary())) {
          self->Fail(*self->chunk_writer_);
        }
        return true;
      }

      bool operator()(FlushRequest& request) const {
        if (ABSL_PREDICT_FALSE(!self->healthy())) {
          request.done.set_value(false);
          return true;
        }
        if (ABSL_PREDICT_FALSE(
                !self->chunk_writer_->Flush(request.flush_type))) {
          self->Fail(*self->chunk_writer_);
          request.done.set_value(false);
          return true;
        }
        request.done.set_value(true);
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
  if (ABSL_PREDICT_FALSE(!closed())) {
    // Ask the chunk writer thread to stop working and exit.
    Fail(CancelledError());
    Done();
  }
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

bool RecordWriterBase::ParallelWorker::HasCapacityForRequest() const
    EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
  return chunk_writer_requests_.size() < IntCast<size_t>(options_.parallelism_);
}

bool RecordWriterBase::ParallelWorker::WriteSignature() {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Chunk chunk;
  EncodeSignature(&chunk);
  ChunkPromises chunk_promises;
  chunk_promises.chunk_header.set_value(chunk.header);
  chunk_promises.chunk.set_value(std::move(chunk));
  mutex_.LockWhen(
      absl::Condition(this, &ParallelWorker::HasCapacityForRequest));
  chunk_writer_requests_.emplace_back(
      WriteChunkRequest{chunk_promises.chunk_header.get_future(),
                        chunk_promises.chunk.get_future()});
  mutex_.Unlock();
  return true;
}

bool RecordWriterBase::ParallelWorker::WriteMetadata() {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (options_.metadata_.ByteSizeLong() == 0 &&
      options_.serialized_metadata_.empty()) {
    return true;
  }
  ChunkPromises* const chunk_promises = new ChunkPromises();
  mutex_.LockWhen(
      absl::Condition(this, &ParallelWorker::HasCapacityForRequest));
  chunk_writer_requests_.emplace_back(
      WriteChunkRequest{chunk_promises->chunk_header.get_future(),
                        chunk_promises->chunk.get_future()});
  mutex_.Unlock();
  internal::ThreadPool::global().Schedule([this, chunk_promises] {
    Chunk chunk;
    EncodeMetadata(&chunk);
    chunk_promises->chunk_header.set_value(chunk.header);
    chunk_promises->chunk.set_value(std::move(chunk));
    delete chunk_promises;
  });
  return true;
}

bool RecordWriterBase::ParallelWorker::CloseChunk() {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  ChunkEncoder* const chunk_encoder = chunk_encoder_.release();
  ChunkPromises* const chunk_promises = new ChunkPromises();
  mutex_.LockWhen(
      absl::Condition(this, &ParallelWorker::HasCapacityForRequest));
  chunk_writer_requests_.emplace_back(
      WriteChunkRequest{chunk_promises->chunk_header.get_future(),
                        chunk_promises->chunk.get_future()});
  mutex_.Unlock();
  internal::ThreadPool::global().Schedule(
      [this, chunk_encoder, chunk_promises] {
        Chunk chunk;
        EncodeChunk(chunk_encoder, &chunk);
        delete chunk_encoder;
        chunk_promises->chunk_header.set_value(chunk.header);
        chunk_promises->chunk.set_value(std::move(chunk));
        delete chunk_promises;
      });
  return true;
}

bool RecordWriterBase::ParallelWorker::PadToBlockBoundary() {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  mutex_.LockWhen(
      absl::Condition(this, &ParallelWorker::HasCapacityForRequest));
  chunk_writer_requests_.emplace_back(PadToBlockBoundaryRequest());
  mutex_.Unlock();
  return true;
}

bool RecordWriterBase::ParallelWorker::Flush(FlushType flush_type) {
  std::promise<bool> done_promise;
  std::future<bool> done_future = done_promise.get_future();
  mutex_.LockWhen(
      absl::Condition(this, &ParallelWorker::HasCapacityForRequest));
  chunk_writer_requests_.emplace_back(
      FlushRequest{flush_type, std::move(done_promise)});
  mutex_.Unlock();
  return done_future.get();
}

FutureRecordPosition RecordWriterBase::ParallelWorker::Pos() const {
  struct Visitor {
    void operator()(const DoneRequest&) {}
    void operator()(const WriteChunkRequest& request) {
      actions.emplace_back(request.chunk_header);
    }
    void operator()(const PadToBlockBoundaryRequest&) {
      actions.emplace_back(FutureRecordPosition::PadToBlockBoundary());
    }
    void operator()(const FlushRequest&) {}

    std::vector<FutureRecordPosition::Action> actions;
  };
  Visitor visitor;
  absl::MutexLock lock(&mutex_);
  visitor.actions.reserve(chunk_writer_requests_.size());
  for (const ChunkWriterRequest& request : chunk_writer_requests_) {
    absl::visit(visitor, request);
  }
  // chunk_encoder_ is nullptr when the current chunk is closed, e.g. when
  // RecordWriter is closed or if RecordWriter::Flush() failed.
  return FutureRecordPosition(
      pos_before_chunks_, std::move(visitor.actions),
      chunk_encoder_ == nullptr ? uint64_t{0} : chunk_encoder_->num_records());
}

RecordWriterBase::RecordWriterBase(InitiallyClosed) noexcept
    : Object(kInitiallyClosed) {}

RecordWriterBase::RecordWriterBase(InitiallyOpen) noexcept
    : Object(kInitiallyOpen) {}

void RecordWriterBase::Reset(InitiallyClosed) {
  Object::Reset(kInitiallyClosed);
  desired_chunk_size_ = 0;
  chunk_size_so_far_ = 0;
  worker_.reset();
}

void RecordWriterBase::Reset(InitiallyOpen) {
  Object::Reset(kInitiallyOpen);
  desired_chunk_size_ = 0;
  chunk_size_so_far_ = 0;
  worker_.reset();
}

RecordWriterBase::RecordWriterBase(RecordWriterBase&& that) noexcept
    : Object(std::move(that)),
      desired_chunk_size_(that.desired_chunk_size_),
      chunk_size_so_far_(that.chunk_size_so_far_),
      worker_(std::move(that.worker_)) {}

RecordWriterBase& RecordWriterBase::operator=(
    RecordWriterBase&& that) noexcept {
  Object::operator=(std::move(that));
  desired_chunk_size_ = that.desired_chunk_size_;
  chunk_size_so_far_ = that.chunk_size_so_far_;
  worker_ = std::move(that.worker_);
  return *this;
}

RecordWriterBase::~RecordWriterBase() {}

void RecordWriterBase::Initialize(ChunkWriter* dest, Options&& options) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of RecordWriter: null ChunkWriter pointer";
  if (ABSL_PREDICT_FALSE(!dest->healthy())) {
    Fail(*dest);
    return;
  }
  // Ensure that num_records does not overflow when WriteRecordImpl() keeps
  // num_records * sizeof(uint64_t) under desired_chunk_size_.
  desired_chunk_size_ =
      UnsignedMin(options.chunk_size_, kMaxNumRecords * sizeof(uint64_t));
  if (options.parallelism_ == 0) {
    worker_ = absl::make_unique<SerialWorker>(dest, std::move(options));
  } else {
    worker_ = absl::make_unique<ParallelWorker>(dest, std::move(options));
  }
  if (ABSL_PREDICT_FALSE(!worker_->healthy())) Fail(*worker_);
}

void RecordWriterBase::Done() {
  if (ABSL_PREDICT_FALSE(worker_ == nullptr)) {
    RIEGELI_ASSERT(!healthy()) << "Failed invariant of RecordWriterBase: "
                                  "null worker_ but RecordWriterBase healthy()";
    return;
  }
  if (chunk_size_so_far_ != 0) {
    if (ABSL_PREDICT_FALSE(!worker_->CloseChunk())) Fail(*worker_);
    chunk_size_so_far_ = 0;
  }
  if (ABSL_PREDICT_FALSE(!worker_->MaybePadToBlockBoundary())) Fail(*worker_);
  if (ABSL_PREDICT_FALSE(!worker_->Close())) Fail(*worker_);
}

void RecordWriterBase::DoneBackground() { worker_.reset(); }

template <typename Record>
bool RecordWriterBase::WriteRecordImpl(Record&& record,
                                       FutureRecordPosition* key) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  // Decoding a chunk writes records to one array, and their positions to
  // another array. We limit the size of both arrays together, to include
  // attempts to accumulate an unbounded number of empty records.
  const uint64_t added_size = SaturatingAdd(
      IntCast<uint64_t>(RecordSize(record)), uint64_t{sizeof(uint64_t)});
  if (ABSL_PREDICT_FALSE(chunk_size_so_far_ > desired_chunk_size_ ||
                         added_size >
                             desired_chunk_size_ - chunk_size_so_far_) &&
      chunk_size_so_far_ > 0) {
    if (ABSL_PREDICT_FALSE(!worker_->CloseChunk())) return Fail(*worker_);
    worker_->OpenChunk();
    chunk_size_so_far_ = 0;
  }
  chunk_size_so_far_ += added_size;
  if (key != nullptr) *key = worker_->Pos();
  if (ABSL_PREDICT_FALSE(!worker_->AddRecord(std::forward<Record>(record)))) {
    return Fail(*worker_);
  }
  return true;
}

template bool RecordWriterBase::WriteRecordImpl(
    const google::protobuf::MessageLite& record, FutureRecordPosition* key);
template bool RecordWriterBase::WriteRecordImpl(const absl::string_view& record,
                                                FutureRecordPosition* key);
template bool RecordWriterBase::WriteRecordImpl(std::string&& record,
                                                FutureRecordPosition* key);
template bool RecordWriterBase::WriteRecordImpl(const Chain& record,
                                                FutureRecordPosition* key);
template bool RecordWriterBase::WriteRecordImpl(Chain&& record,
                                                FutureRecordPosition* key);

bool RecordWriterBase::Flush(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (chunk_size_so_far_ != 0) {
    if (ABSL_PREDICT_FALSE(!worker_->CloseChunk())) return Fail(*worker_);
  }
  if (ABSL_PREDICT_FALSE(!worker_->MaybePadToBlockBoundary())) {
    return Fail(*worker_);
  }
  if (ABSL_PREDICT_FALSE(!worker_->Flush(flush_type))) return Fail(*worker_);
  if (chunk_size_so_far_ != 0) {
    worker_->OpenChunk();
    chunk_size_so_far_ = 0;
  }
  return true;
}

FutureRecordPosition RecordWriterBase::Pos() const {
  if (ABSL_PREDICT_FALSE(worker_ == nullptr)) return FutureRecordPosition();
  return worker_->Pos();
}

}  // namespace riegeli
