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
#include <condition_variable>
#include <deque>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <string>
#include <utility>

#include "google/protobuf/message_lite.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/memory.h"
#include "riegeli/base/object.h"
#include "riegeli/base/parallelism.h"
#include "riegeli/base/string_view.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/chunk_encoding/chunk_encoder.h"
#include "riegeli/chunk_encoding/simple_encoder.h"
#include "riegeli/chunk_encoding/transpose_encoder.h"
#include "riegeli/records/chunk_writer.h"

namespace riegeli {

inline std::unique_ptr<ChunkEncoder> RecordWriter::MakeChunkEncoder(
    const Options& options) {
  if (options.transpose_) {
    const float desired_bucket_size_as_float =
        static_cast<float>(options.desired_chunk_size_) *
        options.desired_bucket_fraction_;
    const uint64_t desired_bucket_size =
        desired_bucket_size_as_float >=
                static_cast<float>(std::numeric_limits<uint64_t>::max())
            ? std::numeric_limits<uint64_t>::max()
            : desired_bucket_size_as_float >= 1.0f
                  ? static_cast<uint64_t>(desired_bucket_size_as_float)
                  : uint64_t{1};
    if (options.parallelism_ == 0) {
      return riegeli::make_unique<TransposeEncoder>(options.compression_type_,
                                                    options.compression_level_,
                                                    desired_bucket_size);
    } else {
      return riegeli::make_unique<DeferredTransposeEncoder>(
          options.compression_type_, options.compression_level_,
          desired_bucket_size);
    }
  } else {
    return riegeli::make_unique<SimpleEncoder>(options.compression_type_,
                                               options.compression_level_);
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
  bool AddRecord(const google::protobuf::MessageLite& record) {
    if (RIEGELI_UNLIKELY(!healthy())) return false;
    if (RIEGELI_UNLIKELY(!chunk_encoder_->AddRecord(record))) {
      return Fail(*chunk_encoder_);
    }
    return true;
  }

  // Precondition: chunk is open.
  bool AddRecord(string_view record) {
    if (RIEGELI_UNLIKELY(!healthy())) return false;
    if (RIEGELI_UNLIKELY(!chunk_encoder_->AddRecord(record))) {
      return Fail(*chunk_encoder_);
    }
    return true;
  }

  // Precondition: chunk is open.
  bool AddRecord(std::string&& record) {
    if (RIEGELI_UNLIKELY(!healthy())) return false;
    if (RIEGELI_UNLIKELY(!chunk_encoder_->AddRecord(std::move(record)))) {
      return Fail(*chunk_encoder_);
    }
    return true;
  }

  // Precondition: chunk is open.
  bool AddRecord(const Chain& record) {
    if (RIEGELI_UNLIKELY(!healthy())) return false;
    if (RIEGELI_UNLIKELY(!chunk_encoder_->AddRecord(record))) {
      return Fail(*chunk_encoder_);
    }
    return true;
  }

  // Precondition: chunk is open.
  bool AddRecord(Chain&& record) {
    if (RIEGELI_UNLIKELY(!healthy())) return false;
    if (RIEGELI_UNLIKELY(!chunk_encoder_->AddRecord(std::move(record)))) {
      return Fail(*chunk_encoder_);
    }
    return true;
  }

  // Precondition: chunk is open.
  //
  // If the result is false then !healthy().
  virtual bool CloseChunk() = 0;

  // Precondition: chunk is not open.
  virtual bool Flush(FlushType flush_type) = 0;

 protected:
  std::unique_ptr<ChunkEncoder> chunk_encoder_;
};

RecordWriter::Impl::~Impl() {}

class RecordWriter::SerialImpl final : public Impl {
 public:
  SerialImpl(ChunkWriter* chunk_writer, const Options& options)
      : Impl(MakeChunkEncoder(options)), chunk_writer_(chunk_writer) {}

  void OpenChunk() override { chunk_encoder_->Reset(); }
  bool CloseChunk() override;
  bool Flush(FlushType flush_type) override;

 protected:
  void Done() override {}

 private:
  ChunkWriter* chunk_writer_;
};

bool RecordWriter::SerialImpl::CloseChunk() {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  Chunk chunk;
  if (RIEGELI_UNLIKELY(!chunk_encoder_->Encode(&chunk))) {
    return Fail("Encoding chunk failed", *chunk_encoder_);
  }
  if (RIEGELI_UNLIKELY(!chunk_writer_->WriteChunk(chunk))) {
    return Fail(*chunk_writer_);
  }
  return true;
}

bool RecordWriter::SerialImpl::Flush(FlushType flush_type) {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  if (RIEGELI_UNLIKELY(!chunk_writer_->Flush(flush_type))) {
    if (chunk_writer_->healthy()) return false;
    return Fail(*chunk_writer_);
  }
  return true;
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

 private:
  // A request to the chunk writer thread.
  enum class RequestType {
    kWriteChunkRequest,
    kFlushRequest,
    kDoneRequest,
  };
  struct WriteChunkRequest {
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
  std::mutex mutex_;
  // All variables below are guarded by mutex_.
  std::deque<ChunkWriterRequest> chunk_writer_requests_;
  std::condition_variable has_chunk_writer_request_;
  std::condition_variable has_space_for_chunk_;
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
    : options_(options), chunk_writer_(chunk_writer) {
  internal::DefaultThreadPool().Schedule([this] {
    for (;;) {
      std::unique_lock<std::mutex> lock(mutex_);
      while (chunk_writer_requests_.empty()) {
        has_chunk_writer_request_.wait(lock);
      }
      ChunkWriterRequest request = std::move(chunk_writer_requests_.front());
      chunk_writer_requests_.pop_front();
      has_space_for_chunk_.notify_one();
      lock.unlock();
      switch (request.request_type) {
        case RequestType::kWriteChunkRequest: {
          // If !healthy(), the chunk must still be waited for, to ensure that
          // the chunk encoder thread exits before the chunk writer thread
          // responds to DoneRequest.
          const Chunk chunk = request.write_chunk_request.chunk.get();
          if (RIEGELI_UNLIKELY(!healthy())) continue;
          if (RIEGELI_UNLIKELY(!chunk_writer_->WriteChunk(chunk))) {
            Fail(*chunk_writer_);
          }
          continue;
        }
        case RequestType::kFlushRequest: {
          if (RIEGELI_UNLIKELY(!healthy())) {
            request.flush_request.done.set_value(false);
            continue;
          }
          if (RIEGELI_UNLIKELY(
                  !chunk_writer_->Flush(request.flush_request.flush_type))) {
            if (!chunk_writer_->healthy()) Fail(*chunk_writer_);
            request.flush_request.done.set_value(false);
            continue;
          }
          request.flush_request.done.set_value(true);
          continue;
        }
        case RequestType::kDoneRequest: {
          request.done_request.done.set_value();
          return;
        }
      }
      RIEGELI_ASSERT_UNREACHABLE()
          << "Unknown request type: " << static_cast<int>(request.request_type);
    }
  });
}

RecordWriter::ParallelImpl::~ParallelImpl() {
  if (RIEGELI_UNLIKELY(!closed())) {
    // Ask the chunk writer thread to stop working and exit.
    Fail("Cancelled");
    Done();
  }
}

void RecordWriter::ParallelImpl::Done() {
  std::promise<void> done_promise;
  std::future<void> done_future = done_promise.get_future();
  {
    std::lock_guard<std::mutex> lock(mutex_);
    chunk_writer_requests_.emplace_back(DoneRequest{std::move(done_promise)});
    has_chunk_writer_request_.notify_one();
  }
  done_future.get();
}

bool RecordWriter::ParallelImpl::CloseChunk() {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  ChunkEncoder* const chunk_encoder = chunk_encoder_.release();
  std::promise<Chunk>* const chunk_promise =
      new std::promise<Chunk>();
  {
    std::unique_lock<std::mutex> lock(mutex_);
    while (chunk_writer_requests_.size() >=
           IntCast<size_t>(options_.parallelism_)) {
      has_space_for_chunk_.wait(lock);
    }
    chunk_writer_requests_.emplace_back(
        WriteChunkRequest{chunk_promise->get_future()});
    has_chunk_writer_request_.notify_one();
  }
  internal::DefaultThreadPool().Schedule([this, chunk_encoder, chunk_promise] {
    Chunk chunk;
    if (RIEGELI_UNLIKELY(!chunk_encoder->Encode(&chunk))) {
      Fail("Encoding chunk failed", *chunk_encoder);
    }
    delete chunk_encoder;
    chunk_promise->set_value(std::move(chunk));
    delete chunk_promise;
  });
  return true;
}

bool RecordWriter::ParallelImpl::Flush(FlushType flush_type) {
  std::promise<bool> done_promise;
  std::future<bool> done_future = done_promise.get_future();
  {
    std::lock_guard<std::mutex> lock(mutex_);
    chunk_writer_requests_.emplace_back(
        FlushRequest{flush_type, std::move(done_promise)});
    has_chunk_writer_request_.notify_one();
  }
  return done_future.get();
}

RecordWriter::RecordWriter() noexcept : Object(State::kClosed) {}

RecordWriter::RecordWriter(std::unique_ptr<Writer> chunk_writer,
                           Options options)
    : RecordWriter(
          riegeli::make_unique<DefaultChunkWriter>(std::move(chunk_writer)),
          options) {}

RecordWriter::RecordWriter(Writer* chunk_writer, Options options)
    : RecordWriter(riegeli::make_unique<DefaultChunkWriter>(chunk_writer),
                   options) {}

RecordWriter::RecordWriter(std::unique_ptr<ChunkWriter> chunk_writer,
                           Options options)
    : RecordWriter(chunk_writer.get(), options) {
  owned_chunk_writer_ = std::move(chunk_writer);
}

RecordWriter::RecordWriter(ChunkWriter* chunk_writer, Options options)
    : Object(State::kOpen), desired_chunk_size_(options.desired_chunk_size_) {
  RIEGELI_ASSERT_NOTNULL(chunk_writer);
  if (chunk_writer->pos() == 0) {
    // Write file signature.
    Chunk signature;
    signature.header = ChunkHeader(signature.data, 0, 0);
    if (RIEGELI_UNLIKELY(!chunk_writer->WriteChunk(signature))) {
      Fail(*chunk_writer);
      return;
    }
  }
  if (options.parallelism_ == 0) {
    impl_ = riegeli::make_unique<SerialImpl>(chunk_writer, options);
  } else {
    impl_ = riegeli::make_unique<ParallelImpl>(chunk_writer, options);
  }
  impl_->OpenChunk();
}

RecordWriter::RecordWriter(RecordWriter&& src) noexcept
    : Object(std::move(src)),
      desired_chunk_size_(riegeli::exchange(src.desired_chunk_size_, 0)),
      chunk_size_(riegeli::exchange(src.chunk_size_, 0)),
      owned_chunk_writer_(std::move(src.owned_chunk_writer_)),
      impl_(std::move(src.impl_)) {}

RecordWriter& RecordWriter::operator=(RecordWriter&& src) noexcept {
  Object::operator=(std::move(src));
  desired_chunk_size_ = riegeli::exchange(src.desired_chunk_size_, 0);
  chunk_size_ = riegeli::exchange(src.chunk_size_, 0);
  // impl_ must be assigned before owned_chunk_writer_ because background work
  // of impl_ may need owned_chunk_writer_.
  impl_ = std::move(src.impl_);
  owned_chunk_writer_ = std::move(src.owned_chunk_writer_);
  return *this;
}

RecordWriter::~RecordWriter() {}

void RecordWriter::Done() {
  if (RIEGELI_LIKELY(healthy()) && chunk_size_ != 0) {
    if (RIEGELI_UNLIKELY(!impl_->CloseChunk())) Fail(*impl_);
  }
  if (RIEGELI_LIKELY(impl_ != nullptr)) {
    if (RIEGELI_LIKELY(healthy())) {
      if (RIEGELI_UNLIKELY(!impl_->Close())) Fail(*impl_);
    }
    impl_.reset();
  }
  if (owned_chunk_writer_ != nullptr) {
    if (RIEGELI_LIKELY(healthy())) {
      if (RIEGELI_UNLIKELY(!owned_chunk_writer_->Close())) {
        Fail(*owned_chunk_writer_);
      }
    }
    owned_chunk_writer_.reset();
  }
  desired_chunk_size_ = 0;
  chunk_size_ = 0;
}

bool RecordWriter::WriteRecord(const google::protobuf::MessageLite& record) {
  if (RIEGELI_UNLIKELY(!EnsureRoomForRecord(record.ByteSizeLong()))) {
    return false;
  }
  if (RIEGELI_UNLIKELY(!impl_->AddRecord(record))) {
    return Fail(*impl_);
  }
  return true;
}

bool RecordWriter::WriteRecord(string_view record) {
  if (RIEGELI_UNLIKELY(!EnsureRoomForRecord(record.size()))) return false;
  if (RIEGELI_UNLIKELY(!impl_->AddRecord(record))) {
    return Fail(*impl_);
  }
  return true;
}

bool RecordWriter::WriteRecord(std::string&& record) {
  if (RIEGELI_UNLIKELY(!EnsureRoomForRecord(record.size()))) return false;
  if (RIEGELI_UNLIKELY(!impl_->AddRecord(std::move(record)))) {
    return Fail(*impl_);
  }
  return true;
}

bool RecordWriter::WriteRecord(const Chain& record) {
  if (RIEGELI_UNLIKELY(!EnsureRoomForRecord(record.size()))) return false;
  if (RIEGELI_UNLIKELY(!impl_->AddRecord(record))) {
    return Fail(*impl_);
  }
  return true;
}

bool RecordWriter::WriteRecord(Chain&& record) {
  if (RIEGELI_UNLIKELY(!EnsureRoomForRecord(record.size()))) return false;
  if (RIEGELI_UNLIKELY(!impl_->AddRecord(std::move(record)))) {
    return Fail(*impl_);
  }
  return true;
}

inline bool RecordWriter::EnsureRoomForRecord(size_t record_size) {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  // Decoding a chunk writes records to one array, and their positions to
  // another array. We limit the size of both arrays together, to include
  // attempts to accumulate an unbounded number of empty records.
  const uint64_t added_size =
      RIEGELI_UNLIKELY(record_size >
                       std::numeric_limits<uint64_t>::max() - sizeof(uint64_t))
          ? std::numeric_limits<uint64_t>::max()
          : IntCast<uint64_t>(record_size) + sizeof(uint64_t);
  if (RIEGELI_UNLIKELY(chunk_size_ != 0 &&
                       (chunk_size_ > desired_chunk_size_ ||
                        added_size > desired_chunk_size_ - chunk_size_))) {
    if (RIEGELI_UNLIKELY(!impl_->CloseChunk())) return Fail(*impl_);
    impl_->OpenChunk();
    chunk_size_ = 0;
  }
  chunk_size_ += added_size;
  return true;
}

bool RecordWriter::Flush(FlushType flush_type) {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  if (chunk_size_ != 0) {
    if (RIEGELI_UNLIKELY(!impl_->CloseChunk())) return Fail(*impl_);
  }
  if (RIEGELI_UNLIKELY(!impl_->Flush(flush_type))) {
    if (impl_->healthy()) return false;
    return Fail(*impl_);
  }
  if (chunk_size_ != 0) {
    impl_->OpenChunk();
    chunk_size_ = 0;
  }
  return true;
}

}  // namespace riegeli
