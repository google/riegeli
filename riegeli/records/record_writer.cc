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

#include <atomic>
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
#include "riegeli/base/assert.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/memory.h"
#include "riegeli/base/object.h"
#include "riegeli/base/parallelism.h"
#include "riegeli/base/string_view.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/chunk_encoding/chunk_encoder.h"
#include "riegeli/records/chunk_writer.h"

namespace riegeli {

inline std::unique_ptr<ChunkEncoder> RecordWriter::MakeChunkEncoder(
    const Options& options) {
  if (options.transpose_) {
    const float desired_bucket_size_as_float =
        static_cast<float>(options.desired_chunk_size_) *
        options.desired_bucket_fraction_;
    const size_t desired_bucket_size =
        desired_bucket_size_as_float >=
                static_cast<float>(std::numeric_limits<size_t>::max())
            ? std::numeric_limits<size_t>::max()
            : desired_bucket_size_as_float >= 1.0f
                  ? static_cast<size_t>(desired_bucket_size_as_float)
                  : size_t{1};
    if (options.parallelism_ == 0) {
      return riegeli::make_unique<EagerTransposedChunkEncoder>(
          options.compression_type_, options.compression_level_,
          desired_bucket_size);
    } else {
      return riegeli::make_unique<DeferredTransposedChunkEncoder>(
          options.compression_type_, options.compression_level_,
          desired_bucket_size);
    }
  } else {
    return riegeli::make_unique<SimpleChunkEncoder>(options.compression_type_,
                                                    options.compression_level_);
  }
}

class RecordWriter::Impl {
 public:
  Impl() = default;

  explicit Impl(std::unique_ptr<ChunkEncoder> chunk_encoder)
      : chunk_encoder_(std::move(chunk_encoder)) {}

  virtual ~Impl();

  // Either Close() or Cancel() must be called once before destruction.
  //
  // Precondition: chunk is not open.
  virtual bool Close() = 0;
  virtual void Cancel() = 0;

  bool healthy() const { return healthy_.load(std::memory_order_acquire); }

  // Precondition: !healthy()
  const std::string& message() const {
    RIEGELI_ASSERT(!healthy());
    return message_;
  }

  // Precondition: chunk is not open.
  virtual void OpenChunk() = 0;

  // Precondition: chunk is open.
  void AddRecord(const google::protobuf::MessageLite& record) {
    chunk_encoder_->AddRecord(record);
  }

  // Precondition: chunk is open.
  void AddRecord(string_view record) { chunk_encoder_->AddRecord(record); }

  // Precondition: chunk is open.
  void AddRecord(std::string&& record) {
    chunk_encoder_->AddRecord(std::move(record));
  }

  // Precondition: chunk is open.
  void AddRecord(const Chain& record) { chunk_encoder_->AddRecord(record); }

  // Precondition: chunk is open.
  void AddRecord(Chain&& record) {
    chunk_encoder_->AddRecord(std::move(record));
  }

  // Precondition: chunk is open.
  //
  // If the result is false then !healthy().
  virtual bool CloseChunk() = 0;

  // Precondition: chunk is not open.
  virtual bool Flush(FlushType flush_type) = 0;

 protected:
  RIEGELI_ATTRIBUTE_COLD bool Fail(std::string message);

  std::unique_ptr<ChunkEncoder> chunk_encoder_;

 private:
  std::atomic<bool> healthy_{true};
  // message_ can be changed only while healthy_ is true.
  std::string message_;
};

RecordWriter::Impl::~Impl() = default;

inline bool RecordWriter::Impl::Fail(std::string message) {
  RIEGELI_ASSERT(healthy());
  message_ = std::move(message);
  healthy_.store(std::memory_order_release);
  return false;
}

class RecordWriter::SerialImpl final : public Impl {
 public:
  SerialImpl(ChunkWriter* chunk_writer, const Options& options)
      : Impl(MakeChunkEncoder(options)), chunk_writer_(chunk_writer) {}

  bool Close() override { return chunk_writer_->Close(); }
  void Cancel() override { chunk_writer_->Cancel(); }
  void OpenChunk() override { chunk_encoder_->Reset(); }
  bool CloseChunk() override;
  bool Flush(FlushType flush_type) override;

 private:
  ChunkWriter* chunk_writer_;
};

bool RecordWriter::SerialImpl::CloseChunk() {
  Chunk chunk;
  if (RIEGELI_UNLIKELY(!chunk_encoder_->Encode(&chunk))) {
    return Fail("Failed to encode chunk");
  }
  if (RIEGELI_UNLIKELY(!chunk_writer_->WriteChunk(chunk))) {
    RIEGELI_ASSERT(!chunk_writer_->healthy());
    return Fail(chunk_writer_->Message());
  }
  return true;
}

bool RecordWriter::SerialImpl::Flush(FlushType flush_type) {
  if (RIEGELI_UNLIKELY(!chunk_writer_->Flush(flush_type))) {
    if (chunk_writer_->healthy()) return false;
    return Fail(chunk_writer_->Message());
  }
  return true;
}

// ParallelImpl uses parallelism internally, but the class is still only
// thread-compatible, not thread-safe.
class RecordWriter::ParallelImpl final : public Impl {
 public:
  ParallelImpl(ChunkWriter* chunk_writer, const Options& options);

  bool Close() override;
  void Cancel() override;
  void OpenChunk() override { chunk_encoder_ = MakeChunkEncoder(options_); }
  bool CloseChunk() override;
  bool Flush(FlushType flush_type) override;

 private:
  // A request to the chunk writer thread.
  enum class RequestType {
    kWriteChunkRequest,
    kFlushRequest,
    kCloseRequest,
  };
  struct WriteChunkRequest {
    // chunk.get().data.empty() when chunk encoder failed.
    std::future<Chunk> chunk;
  };
  struct FlushRequest {
    FlushType flush_type;
    std::promise<bool> done;
  };
  struct CloseRequest {
    std::promise<bool> done;
  };
  struct ChunkWriterRequest {
    explicit ChunkWriterRequest(WriteChunkRequest write_chunk_request);
    explicit ChunkWriterRequest(FlushRequest flush_request);
    explicit ChunkWriterRequest(CloseRequest close_request);

    ChunkWriterRequest(ChunkWriterRequest&& src) noexcept;
    ChunkWriterRequest& operator=(ChunkWriterRequest&& src) noexcept;

    ~ChunkWriterRequest();

    RequestType request_type;
    union {
      WriteChunkRequest write_chunk_request;
      FlushRequest flush_request;
      CloseRequest close_request;
    };
  };

  // If cancelled() is true, a CloseRequest is interpreted as a request to
  // cancel writing. Also, further expensive work can be skipped.
  bool cancelled() const { return cancelled_.load(std::memory_order_relaxed); }

  Options options_;
  std::atomic<bool> cancelled_{false};
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
    CloseRequest close_request)
    : request_type(RequestType::kCloseRequest),
      close_request(std::move(close_request)) {}

inline RecordWriter::ParallelImpl::ChunkWriterRequest::~ChunkWriterRequest() {
  switch (request_type) {
    case RequestType::kWriteChunkRequest:
      write_chunk_request.~WriteChunkRequest();
      return;
    case RequestType::kFlushRequest:
      flush_request.~FlushRequest();
      return;
    case RequestType::kCloseRequest:
      close_request.~CloseRequest();
      return;
  }
  RIEGELI_UNREACHABLE() << "Unknown request type: "
                        << static_cast<int>(request_type);
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
    case RequestType::kCloseRequest:
      new (&close_request) CloseRequest(std::move(src.close_request));
      return;
  }
  RIEGELI_UNREACHABLE() << "Unknown request type: "
                        << static_cast<int>(request_type);
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
          if (RIEGELI_UNLIKELY(!healthy() || cancelled())) continue;
          const Chunk chunk = request.write_chunk_request.chunk.get();
          if (RIEGELI_UNLIKELY(cancelled())) continue;
          if (RIEGELI_UNLIKELY(chunk.data.empty())) {
            Fail("Failed to encode chunk");
            continue;
          }
          if (RIEGELI_UNLIKELY(!chunk_writer_->WriteChunk(chunk))) {
            RIEGELI_ASSERT(!chunk_writer_->healthy());
            Fail(chunk_writer_->Message());
          }
          continue;
        }
        case RequestType::kFlushRequest:
          if (RIEGELI_UNLIKELY(!healthy() || cancelled())) {
            request.flush_request.done.set_value(false);
            continue;
          }
          if (RIEGELI_UNLIKELY(
                  !chunk_writer_->Flush(request.flush_request.flush_type))) {
            if (!chunk_writer_->healthy()) {
              Fail(chunk_writer_->Message());
            }
            request.flush_request.done.set_value(false);
            continue;
          }
          request.flush_request.done.set_value(true);
          continue;
        case RequestType::kCloseRequest:
          if (RIEGELI_UNLIKELY(!healthy() || cancelled())) {
            chunk_writer_->Cancel();
            request.close_request.done.set_value(false);
            return;
          }
          request.close_request.done.set_value(true);
          return;
      }
      RIEGELI_UNREACHABLE()
          << "Unknown request type: " << static_cast<int>(request.request_type);
    }
  });
}

bool RecordWriter::ParallelImpl::Close() {
  std::promise<bool> done_promise;
  std::future<bool> done_future = done_promise.get_future();
  {
    std::lock_guard<std::mutex> lock(mutex_);
    chunk_writer_requests_.emplace_back(CloseRequest{std::move(done_promise)});
    has_chunk_writer_request_.notify_one();
  }
  return done_future.get();
}

void RecordWriter::ParallelImpl::Cancel() {
  cancelled_.store(true, std::memory_order_relaxed);
  Close();
}

bool RecordWriter::ParallelImpl::CloseChunk() {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  ChunkEncoder* const chunk_encoder = chunk_encoder_.release();
  std::promise<Chunk>* const chunk_promise =
      new std::promise<Chunk>();
  {
    std::unique_lock<std::mutex> lock(mutex_);
    while (chunk_writer_requests_.size() >=
           static_cast<size_t>(options_.parallelism_)) {
      has_space_for_chunk_.wait(lock);
    }
    chunk_writer_requests_.emplace_back(
        WriteChunkRequest{chunk_promise->get_future()});
    has_chunk_writer_request_.notify_one();
  }
  internal::DefaultThreadPool().Schedule([this, chunk_encoder, chunk_promise] {
    Chunk chunk;
    if (RIEGELI_UNLIKELY(!chunk_encoder->Encode(&chunk))) chunk.data.Clear();
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

RecordWriter::RecordWriter() : desired_chunk_size_(0) { MarkCancelled(); }

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
    : desired_chunk_size_(options.desired_chunk_size_) {
  RIEGELI_ASSERT_NOTNULL(chunk_writer);
  if (chunk_writer->pos() == 0) {
    // Write file signature.
    Chunk signature;
    signature.header = ChunkHeader(signature.data, 0, 0);
    if (RIEGELI_UNLIKELY(!chunk_writer->WriteChunk(signature))) {
      RIEGELI_ASSERT(!chunk_writer->healthy());
      Fail(chunk_writer->Message());
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
  if (&src != this) {
    Object::operator=(std::move(src));
    desired_chunk_size_ = riegeli::exchange(src.desired_chunk_size_, 0);
    chunk_size_ = riegeli::exchange(src.chunk_size_, 0);
    owned_chunk_writer_ = std::move(src.owned_chunk_writer_);
    impl_ = std::move(src.impl_);
  }
  return *this;
}

RecordWriter::~RecordWriter() { Cancel(); }

void RecordWriter::Done() {
  if (RIEGELI_LIKELY(healthy()) && chunk_size_ != 0) {
    if (RIEGELI_UNLIKELY(!impl_->CloseChunk())) {
      RIEGELI_ASSERT(!impl_->healthy());
      Fail(impl_->message());
    }
  }
  if (RIEGELI_LIKELY(healthy())) {
    if (RIEGELI_UNLIKELY(!impl_->Close())) {
      RIEGELI_ASSERT(!impl_->healthy());
      Fail(impl_->message());
    } else if (owned_chunk_writer_ != nullptr) {
      if (RIEGELI_UNLIKELY(!owned_chunk_writer_->Close())) {
        RIEGELI_ASSERT(owned_chunk_writer_->healthy());
        Fail(owned_chunk_writer_->Message());
      }
    }
  } else if (impl_ != nullptr) {
    impl_->Cancel();
  }
  desired_chunk_size_ = 0;
  chunk_size_ = 0;
  owned_chunk_writer_.reset();
  impl_.reset();
}

bool RecordWriter::WriteRecord(const google::protobuf::MessageLite& record) {
  const size_t size = record.ByteSizeLong();
  if (RIEGELI_UNLIKELY(size > std::numeric_limits<int>::max())) {
    return Fail("Failed to serialize message of type " + record.GetTypeName() +
                " (exceeded maximum protobuf size of 2GB: " +
                std::to_string(size) + ")");
  }
  // The only remaining possibility for SerializeToZeroCopyStream() to fail is
  // when the stream itself reports failure, which should not happen because
  // ChunkEncoder writes to a Chain or string, hence we do not need to propagate
  // potential failures from AddRecord() here.
  if (RIEGELI_UNLIKELY(!EnsureRoomForRecord(size))) return false;
  impl_->AddRecord(record);
  return true;
}

bool RecordWriter::WriteRecord(string_view record) {
  if (RIEGELI_UNLIKELY(!EnsureRoomForRecord(record.size()))) return false;
  impl_->AddRecord(record);
  return true;
}

bool RecordWriter::WriteRecord(std::string&& record) {
  if (RIEGELI_UNLIKELY(!EnsureRoomForRecord(record.size()))) return false;
  impl_->AddRecord(std::move(record));
  return true;
}

bool RecordWriter::WriteRecord(const Chain& record) {
  if (RIEGELI_UNLIKELY(!EnsureRoomForRecord(record.size()))) return false;
  impl_->AddRecord(record);
  return true;
}

bool RecordWriter::WriteRecord(Chain&& record) {
  if (RIEGELI_UNLIKELY(!EnsureRoomForRecord(record.size()))) return false;
  impl_->AddRecord(std::move(record));
  return true;
}

inline bool RecordWriter::EnsureRoomForRecord(size_t record_size) {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  // Decoding a chunk allocates records in one array, and pointers to them in
  // another array. We limit the size of both arrays (restricting only the first
  // array might force accumulating an unbounded number of empty records).
  // Since the decoder architecture is not known, and for deterministic output,
  // the pointer size is conservatively assumed to be 8 bytes.
  const size_t kAssumedPointerSize = 8;
  if (chunk_size_ + record_size + kAssumedPointerSize > desired_chunk_size_ &&
      chunk_size_ != 0) {
    if (RIEGELI_UNLIKELY(!impl_->CloseChunk())) {
      RIEGELI_ASSERT(!impl_->healthy());
      return Fail(impl_->message());
    }
    impl_->OpenChunk();
    chunk_size_ = 0;
  }
  chunk_size_ += record_size + kAssumedPointerSize;
  return true;
}

bool RecordWriter::Flush(FlushType flush_type) {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  if (chunk_size_ != 0) {
    if (RIEGELI_UNLIKELY(!impl_->CloseChunk())) {
      RIEGELI_ASSERT(!impl_->healthy());
      return Fail(impl_->message());
    }
  }
  if (RIEGELI_UNLIKELY(!impl_->Flush(flush_type))) {
    if (impl_->healthy()) return false;
    return Fail(impl_->message());
  }
  if (chunk_size_ != 0) {
    impl_->OpenChunk();
    chunk_size_ = 0;
  }
  return true;
}

}  // namespace riegeli
