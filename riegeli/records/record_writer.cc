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
#include <cmath>
#include <deque>
#include <future>
#include <limits>
#include <memory>
#include <new>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "google/protobuf/message_lite.h"
#include "absl/base/optimization.h"
#include "absl/memory/memory.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/base/options_parser.h"
#include "riegeli/base/parallelism.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/chunk_encoding/chunk_encoder.h"
#include "riegeli/chunk_encoding/deferred_encoder.h"
#include "riegeli/chunk_encoding/simple_encoder.h"
#include "riegeli/chunk_encoding/transpose_encoder.h"
#include "riegeli/records/block.h"
#include "riegeli/records/chunk_writer.h"
#include "riegeli/records/record_position.h"

namespace riegeli {

namespace {

template <typename Record>
size_t RecordSize(const Record& record) {
  return record.size();
}

size_t RecordSize(const google::protobuf::MessageLite& record) {
  return record.ByteSizeLong();
}

}  // namespace

inline FutureRecordPosition::FutureChunkBegin::FutureChunkBegin(
    Position pos_before_chunks,
    std::vector<std::shared_future<ChunkHeader>> chunk_headers)
    : pos_before_chunks_(pos_before_chunks),
      chunk_headers_(std::move(chunk_headers)) {}

void FutureRecordPosition::FutureChunkBegin::Resolve() const {
  Position pos = pos_before_chunks_;
  for (const auto& chunk_header : chunk_headers_) {
    pos = internal::ChunkEnd(chunk_header.get(), pos);
  }
  pos_before_chunks_ = pos;
  chunk_headers_ = std::vector<std::shared_future<ChunkHeader>>();
}

inline FutureRecordPosition::FutureRecordPosition(Position chunk_begin)
    : chunk_begin_(chunk_begin) {}

inline FutureRecordPosition::FutureRecordPosition(
    Position pos_before_chunks,
    std::vector<std::shared_future<ChunkHeader>> chunk_headers)
    : future_chunk_begin_(
          chunk_headers.empty()
              ? nullptr
              : absl::make_unique<FutureChunkBegin>(pos_before_chunks,
                                                    std::move(chunk_headers))),
      chunk_begin_(pos_before_chunks) {}

bool RecordWriter::Options::Parse(absl::string_view text, std::string* message) {
  std::string compressor_text;
  OptionsParser parser;
  parser.AddOption("default",
                   parser.Empty([&parser] { return parser.FailIfAnySeen(); }));
  parser.AddOption(
      "transpose",
      parser.Enum(&transpose_, {{"", true}, {"true", true}, {"false", false}}));
  parser.AddOption("uncompressed", parser.CopyTo(&compressor_text));
  parser.AddOption("brotli", parser.CopyTo(&compressor_text));
  parser.AddOption("zstd", parser.CopyTo(&compressor_text));
  parser.AddOption("window_log", parser.CopyTo(&compressor_text));
  parser.AddOption(
      "chunk_size",
      parser.Bytes(&chunk_size_, 1, std::numeric_limits<uint64_t>::max()));
  parser.AddOption("bucket_fraction", parser.Real(&bucket_fraction_, 0.0, 1.0));
  parser.AddOption("parallelism", parser.Int(&parallelism_, 0,
                                             std::numeric_limits<int>::max()));
  if (ABSL_PREDICT_FALSE(!parser.Parse(text))) {
    *message = std::string(parser.message());
    return false;
  }
  return compressor_options_.Parse(compressor_text, message);
}

inline std::unique_ptr<ChunkEncoder> RecordWriter::MakeChunkEncoder(
    const Options& options) {
  std::unique_ptr<ChunkEncoder> chunk_encoder;
  if (options.transpose_) {
    const long double long_double_bucket_size =
        std::round(static_cast<long double>(options.chunk_size_) *
                   static_cast<long double>(options.bucket_fraction_));
    const uint64_t bucket_size =
        ABSL_PREDICT_FALSE(
            long_double_bucket_size >=
            static_cast<long double>(std::numeric_limits<uint64_t>::max()))
            ? std::numeric_limits<uint64_t>::max()
            : ABSL_PREDICT_TRUE(long_double_bucket_size >= 1.0L)
                  ? static_cast<uint64_t>(long_double_bucket_size)
                  : uint64_t{1};
    chunk_encoder = absl::make_unique<TransposeEncoder>(
        options.compressor_options_, bucket_size);
  } else {
    chunk_encoder = absl::make_unique<SimpleEncoder>(
        options.compressor_options_, options.chunk_size_);
  }
  if (options.parallelism_ == 0) {
    return chunk_encoder;
  } else {
    return absl::make_unique<DeferredEncoder>(std::move(chunk_encoder));
  }
}

class RecordWriter::Impl : public Object {
 public:
  Impl() : Object(State::kOpen) {}

  explicit Impl(std::unique_ptr<ChunkEncoder> chunk_encoder)
      : Object(State::kOpen), chunk_encoder_(std::move(chunk_encoder)) {}

  ~Impl();

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

  // Precondition: chunk is not open.
  virtual bool Flush(FlushType flush_type) = 0;

  FutureRecordPosition Pos();

 protected:
  virtual FutureRecordPosition ChunkBegin() = 0;

  std::unique_ptr<ChunkEncoder> chunk_encoder_;
};

RecordWriter::Impl::~Impl() {}

template <typename Record>
bool RecordWriter::Impl::AddRecord(Record&& record) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(
          !chunk_encoder_->AddRecord(std::forward<Record>(record)))) {
    return Fail(*chunk_encoder_);
  }
  return true;
}

inline FutureRecordPosition RecordWriter::Impl::Pos() {
  FutureRecordPosition pos = ChunkBegin();
  pos.record_index_ = chunk_encoder_->num_records();
  return pos;
}

class RecordWriter::SerialImpl final : public Impl {
 public:
  SerialImpl(ChunkWriter* chunk_writer, const Options& options)
      : Impl(MakeChunkEncoder(options)), chunk_writer_(chunk_writer) {}

  void OpenChunk() override { chunk_encoder_->Reset(); }
  bool CloseChunk() override;
  bool Flush(FlushType flush_type) override;

 protected:
  void Done() override {}
  FutureRecordPosition ChunkBegin() override;

 private:
  ChunkWriter* chunk_writer_;
};

bool RecordWriter::SerialImpl::CloseChunk() {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Chunk chunk;
  if (ABSL_PREDICT_FALSE(!chunk_encoder_->EncodeAndClose(&chunk))) {
    return Fail("Encoding chunk failed", *chunk_encoder_);
  }
  if (ABSL_PREDICT_FALSE(!chunk_writer_->WriteChunk(chunk))) {
    return Fail(*chunk_writer_);
  }
  return true;
}

bool RecordWriter::SerialImpl::Flush(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(!chunk_writer_->Flush(flush_type))) {
    if (chunk_writer_->healthy()) return false;
    return Fail(*chunk_writer_);
  }
  return true;
}

FutureRecordPosition RecordWriter::SerialImpl::ChunkBegin() {
  return FutureRecordPosition(chunk_writer_->pos());
}

// ParallelImpl uses parallelism internally, but the class is still only
// thread-compatible, not thread-safe.
class RecordWriter::ParallelImpl final : public Impl {
 public:
  ParallelImpl(ChunkWriter* chunk_writer, const Options& options);

  ~ParallelImpl();

  void OpenChunk() override { chunk_encoder_ = MakeChunkEncoder(options_); }
  bool CloseChunk() override;
  bool Flush(FlushType flush_type) override;

 protected:
  void Done() override;
  FutureRecordPosition ChunkBegin() override;

 private:
  struct ChunkPromises {
    std::promise<ChunkHeader> chunk_header;
    std::promise<Chunk> chunk;
  };

  // A request to the chunk writer thread.
  enum class RequestType {
    kWriteChunkRequest,
    kFlushRequest,
    kDoneRequest,
  };
  struct WriteChunkRequest {
    std::shared_future<ChunkHeader> chunk_header;
    std::future<Chunk> chunk;
  };
  struct FlushRequest {
    FlushType flush_type;
    std::promise<bool> done;
  };
  struct DoneRequest {
    std::promise<void> done;
  };
  struct ChunkWriterRequest {
    explicit ChunkWriterRequest(WriteChunkRequest write_chunk_request);
    explicit ChunkWriterRequest(FlushRequest flush_request);
    explicit ChunkWriterRequest(DoneRequest done_request);

    ChunkWriterRequest(ChunkWriterRequest&& src) noexcept;
    ChunkWriterRequest& operator=(ChunkWriterRequest&& src) noexcept;

    ~ChunkWriterRequest();

    RequestType request_type;
    union {
      WriteChunkRequest write_chunk_request;
      FlushRequest flush_request;
      DoneRequest done_request;
    };
  };

  Options options_;
  ChunkWriter* chunk_writer_;
  absl::Mutex mutex_;
  std::deque<ChunkWriterRequest> chunk_writer_requests_ GUARDED_BY(mutex_);
  // Position before handling chunk_writer_requests_.
  Position pos_before_chunks_ GUARDED_BY(mutex_);
};

inline RecordWriter::ParallelImpl::ChunkWriterRequest::ChunkWriterRequest(
    WriteChunkRequest write_chunk_request)
    : request_type(RequestType::kWriteChunkRequest),
      write_chunk_request(std::move(write_chunk_request)) {}

inline RecordWriter::ParallelImpl::ChunkWriterRequest::ChunkWriterRequest(
    FlushRequest flush_request)
    : request_type(RequestType::kFlushRequest),
      flush_request(std::move(flush_request)) {}

inline RecordWriter::ParallelImpl::ChunkWriterRequest::ChunkWriterRequest(
    DoneRequest done_request)
    : request_type(RequestType::kDoneRequest),
      done_request(std::move(done_request)) {}

inline RecordWriter::ParallelImpl::ChunkWriterRequest::~ChunkWriterRequest() {
  switch (request_type) {
    case RequestType::kWriteChunkRequest:
      write_chunk_request.~WriteChunkRequest();
      return;
    case RequestType::kFlushRequest:
      flush_request.~FlushRequest();
      return;
    case RequestType::kDoneRequest:
      done_request.~DoneRequest();
      return;
  }
  RIEGELI_ASSERT_UNREACHABLE()
      << "Unknown request type: " << static_cast<int>(request_type);
}

inline RecordWriter::ParallelImpl::ChunkWriterRequest::ChunkWriterRequest(
    ChunkWriterRequest&& src) noexcept
    : request_type(src.request_type) {
  switch (request_type) {
    case RequestType::kWriteChunkRequest:
      new (&write_chunk_request)
          WriteChunkRequest(std::move(src.write_chunk_request));
      return;
    case RequestType::kFlushRequest:
      new (&flush_request) FlushRequest(std::move(src.flush_request));
      return;
    case RequestType::kDoneRequest:
      new (&done_request) DoneRequest(std::move(src.done_request));
      return;
  }
  RIEGELI_ASSERT_UNREACHABLE()
      << "Unknown request type: " << static_cast<int>(request_type);
}

inline RecordWriter::ParallelImpl::ChunkWriterRequest&
RecordWriter::ParallelImpl::ChunkWriterRequest::operator=(
    ChunkWriterRequest&& src) noexcept {
  if (&src != this) {
    this->~ChunkWriterRequest();
    new (this) ChunkWriterRequest(std::move(src));
  }
  return *this;
}

inline RecordWriter::ParallelImpl::ParallelImpl(ChunkWriter* chunk_writer,
                                                const Options& options)
    : options_(options),
      chunk_writer_(chunk_writer),
      pos_before_chunks_(chunk_writer_->pos()) {
  internal::DefaultThreadPool().Schedule([this] {
    mutex_.Lock();
    for (;;) {
      mutex_.Await(absl::Condition(
          +[](std::deque<ChunkWriterRequest>* chunk_writer_requests) {
            return !chunk_writer_requests->empty();
          },
          &chunk_writer_requests_));
      ChunkWriterRequest& request = chunk_writer_requests_.front();
      mutex_.Unlock();
      switch (request.request_type) {
        case RequestType::kWriteChunkRequest: {
          // If !healthy(), the chunk must still be waited for, to ensure that
          // the chunk encoder thread exits before the chunk writer thread
          // responds to DoneRequest.
          const Chunk chunk = request.write_chunk_request.chunk.get();
          if (ABSL_PREDICT_FALSE(!healthy())) goto handled;
          if (ABSL_PREDICT_FALSE(!chunk_writer_->WriteChunk(chunk))) {
            Fail(*chunk_writer_);
          }
          goto handled;
        }
        case RequestType::kFlushRequest: {
          if (ABSL_PREDICT_FALSE(!healthy())) {
            request.flush_request.done.set_value(false);
            goto handled;
          }
          if (ABSL_PREDICT_FALSE(
                  !chunk_writer_->Flush(request.flush_request.flush_type))) {
            if (!chunk_writer_->healthy()) Fail(*chunk_writer_);
            request.flush_request.done.set_value(false);
            goto handled;
          }
          request.flush_request.done.set_value(true);
          goto handled;
        }
        case RequestType::kDoneRequest: {
          request.done_request.done.set_value();
          return;
        }
      }
      RIEGELI_ASSERT_UNREACHABLE()
          << "Unknown request type: " << static_cast<int>(request.request_type);
    handled:
      mutex_.Lock();
      chunk_writer_requests_.pop_front();
      pos_before_chunks_ = chunk_writer_->pos();
    }
  });
}

RecordWriter::ParallelImpl::~ParallelImpl() {
  if (ABSL_PREDICT_FALSE(!closed())) {
    // Ask the chunk writer thread to stop working and exit.
    Fail("Cancelled");
    Done();
  }
}

void RecordWriter::ParallelImpl::Done() {
  std::promise<void> done_promise;
  std::future<void> done_future = done_promise.get_future();
  {
    absl::MutexLock lock(&mutex_);
    chunk_writer_requests_.emplace_back(DoneRequest{std::move(done_promise)});
  }
  done_future.get();
}

bool RecordWriter::ParallelImpl::CloseChunk() {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  ChunkEncoder* const chunk_encoder = chunk_encoder_.release();
  ChunkPromises* const chunk_promises = new ChunkPromises();
  {
    mutex_.LockWhen(absl::Condition(
        +[](ParallelImpl* self) {
          self->mutex_.AssertHeld();
          return self->chunk_writer_requests_.size() <
                 IntCast<size_t>(self->options_.parallelism_);
        },
        this));
    chunk_writer_requests_.emplace_back(
        WriteChunkRequest{chunk_promises->chunk_header.get_future(),
                          chunk_promises->chunk.get_future()});
    mutex_.Unlock();
  }
  internal::DefaultThreadPool().Schedule([this, chunk_encoder, chunk_promises] {
    Chunk chunk;
    if (ABSL_PREDICT_FALSE(!chunk_encoder->EncodeAndClose(&chunk))) {
      Fail("Encoding chunk failed", *chunk_encoder);
    }
    delete chunk_encoder;
    chunk_promises->chunk_header.set_value(chunk.header);
    chunk_promises->chunk.set_value(std::move(chunk));
    delete chunk_promises;
  });
  return true;
}

bool RecordWriter::ParallelImpl::Flush(FlushType flush_type) {
  std::promise<bool> done_promise;
  std::future<bool> done_future = done_promise.get_future();
  {
    absl::MutexLock lock(&mutex_);
    chunk_writer_requests_.emplace_back(
        FlushRequest{flush_type, std::move(done_promise)});
  }
  return done_future.get();
}

FutureRecordPosition RecordWriter::ParallelImpl::ChunkBegin() {
  absl::MutexLock lock(&mutex_);
  std::vector<std::shared_future<ChunkHeader>> chunk_headers;
  chunk_headers.reserve(chunk_writer_requests_.size());
  for (const auto& pending_request : chunk_writer_requests_) {
    RIEGELI_ASSERT(pending_request.request_type ==
                   RequestType::kWriteChunkRequest)
        << "Unexpected type of request in the queue: "
        << static_cast<int>(pending_request.request_type);
    chunk_headers.push_back(pending_request.write_chunk_request.chunk_header);
  }
  return FutureRecordPosition(pos_before_chunks_, std::move(chunk_headers));
}

RecordWriter::RecordWriter() noexcept : Object(State::kClosed) {}

RecordWriter::RecordWriter(std::unique_ptr<Writer> chunk_writer,
                           Options options)
    : RecordWriter(
          absl::make_unique<DefaultChunkWriter>(std::move(chunk_writer)),
          options) {}

RecordWriter::RecordWriter(Writer* chunk_writer, Options options)
    : RecordWriter(absl::make_unique<DefaultChunkWriter>(chunk_writer),
                   options) {}

RecordWriter::RecordWriter(std::unique_ptr<ChunkWriter> chunk_writer,
                           Options options)
    : RecordWriter(chunk_writer.get(), options) {
  owned_chunk_writer_ = std::move(chunk_writer);
}

RecordWriter::RecordWriter(ChunkWriter* chunk_writer, Options options)
    : Object(State::kOpen), desired_chunk_size_(options.chunk_size_) {
  RIEGELI_ASSERT_NOTNULL(chunk_writer);
  if (chunk_writer->pos() == 0) {
    // Write file signature.
    Chunk signature;
    signature.header = ChunkHeader(signature.data, 0, 0);
    if (ABSL_PREDICT_FALSE(!chunk_writer->WriteChunk(signature))) {
      Fail(*chunk_writer);
      return;
    }
  }
  if (options.parallelism_ == 0) {
    impl_ = absl::make_unique<SerialImpl>(chunk_writer, options);
  } else {
    impl_ = absl::make_unique<ParallelImpl>(chunk_writer, options);
  }
  impl_->OpenChunk();
}

RecordWriter::RecordWriter(RecordWriter&& src) noexcept
    : Object(std::move(src)),
      desired_chunk_size_(riegeli::exchange(src.desired_chunk_size_, 0)),
      chunk_size_so_far_(riegeli::exchange(src.chunk_size_so_far_, 0)),
      owned_chunk_writer_(std::move(src.owned_chunk_writer_)),
      impl_(std::move(src.impl_)) {}

RecordWriter& RecordWriter::operator=(RecordWriter&& src) noexcept {
  Object::operator=(std::move(src));
  desired_chunk_size_ = riegeli::exchange(src.desired_chunk_size_, 0);
  chunk_size_so_far_ = riegeli::exchange(src.chunk_size_so_far_, 0);
  // impl_ must be assigned before owned_chunk_writer_ because background work
  // of impl_ may need owned_chunk_writer_.
  impl_ = std::move(src.impl_);
  owned_chunk_writer_ = std::move(src.owned_chunk_writer_);
  return *this;
}

RecordWriter::~RecordWriter() {}

void RecordWriter::Done() {
  if (ABSL_PREDICT_TRUE(healthy()) && chunk_size_so_far_ != 0) {
    if (ABSL_PREDICT_FALSE(!impl_->CloseChunk())) Fail(*impl_);
  }
  if (ABSL_PREDICT_TRUE(impl_ != nullptr)) {
    if (ABSL_PREDICT_TRUE(healthy())) {
      if (ABSL_PREDICT_FALSE(!impl_->Close())) Fail(*impl_);
    }
    impl_.reset();
  }
  if (owned_chunk_writer_ != nullptr) {
    if (ABSL_PREDICT_TRUE(healthy())) {
      if (ABSL_PREDICT_FALSE(!owned_chunk_writer_->Close())) {
        Fail(*owned_chunk_writer_);
      }
    }
    owned_chunk_writer_.reset();
  }
  desired_chunk_size_ = 0;
  chunk_size_so_far_ = 0;
}

template <typename Record>
bool RecordWriter::WriteRecordImpl(Record&& record, FutureRecordPosition* key) {
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
    if (ABSL_PREDICT_FALSE(!impl_->CloseChunk())) return Fail(*impl_);
    impl_->OpenChunk();
    chunk_size_so_far_ = 0;
  }
  chunk_size_so_far_ += added_size;
  if (key != nullptr) *key = impl_->Pos();
  if (ABSL_PREDICT_FALSE(!impl_->AddRecord(std::forward<Record>(record)))) {
    return Fail(*impl_);
  }
  return true;
}

template bool RecordWriter::WriteRecordImpl(const google::protobuf::MessageLite& record,
                                            FutureRecordPosition* key);
template bool RecordWriter::WriteRecordImpl(const absl::string_view& record,
                                            FutureRecordPosition* key);
template bool RecordWriter::WriteRecordImpl(std::string&& record,
                                            FutureRecordPosition* key);
template bool RecordWriter::WriteRecordImpl(const Chain& record,
                                            FutureRecordPosition* key);
template bool RecordWriter::WriteRecordImpl(Chain&& record,
                                            FutureRecordPosition* key);

bool RecordWriter::Flush(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (chunk_size_so_far_ != 0) {
    if (ABSL_PREDICT_FALSE(!impl_->CloseChunk())) return Fail(*impl_);
  }
  if (ABSL_PREDICT_FALSE(!impl_->Flush(flush_type))) {
    if (impl_->healthy()) return false;
    return Fail(*impl_);
  }
  if (chunk_size_so_far_ != 0) {
    impl_->OpenChunk();
    chunk_size_so_far_ = 0;
  }
  return true;
}

FutureRecordPosition RecordWriter::Pos() const {
  if (ABSL_PREDICT_FALSE(impl_ == nullptr)) return FutureRecordPosition();
  return impl_->Pos();
}

}  // namespace riegeli
