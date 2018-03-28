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
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/memory.h"
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
  if (RIEGELI_UNLIKELY(!parser.Parse(text))) {
    *message = std::string(parser.Message());
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
        RIEGELI_UNLIKELY(
            long_double_bucket_size >=
            static_cast<long double>(std::numeric_limits<uint64_t>::max()))
            ? std::numeric_limits<uint64_t>::max()
            : RIEGELI_LIKELY(long_double_bucket_size >= 1.0L)
                  ? static_cast<uint64_t>(long_double_bucket_size)
                  : uint64_t{1};
    chunk_encoder = riegeli::make_unique<TransposeEncoder>(
        options.compressor_options_, bucket_size);
  } else {
    chunk_encoder = riegeli::make_unique<SimpleEncoder>(
        options.compressor_options_, options.chunk_size_);
  }
  if (options.parallelism_ == 0) {
    return chunk_encoder;
  } else {
    return riegeli::make_unique<DeferredEncoder>(std::move(chunk_encoder));
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

  RecordPosition Pos();

 protected:
  virtual Position ChunkBegin() = 0;

  std::unique_ptr<ChunkEncoder> chunk_encoder_;
};

RecordWriter::Impl::~Impl() {}

template <typename Record>
bool RecordWriter::Impl::AddRecord(Record&& record) {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  if (RIEGELI_UNLIKELY(
          !chunk_encoder_->AddRecord(std::forward<Record>(record)))) {
    return Fail(*chunk_encoder_);
  }
  return true;
}

inline RecordPosition RecordWriter::Impl::Pos() {
  return RecordPosition(ChunkBegin(), chunk_encoder_->num_records());
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
  Position ChunkBegin() override { return chunk_writer_->pos(); }

 private:
  ChunkWriter* chunk_writer_;
};

bool RecordWriter::SerialImpl::CloseChunk() {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  Chunk chunk;
  if (RIEGELI_UNLIKELY(!chunk_encoder_->EncodeAndClose(&chunk))) {
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
  Position ChunkBegin() override;

 private:
  // A request to the chunk writer thread.
  enum class RequestType {
    kWriteChunkRequest,
    kFlushRequest,
    kDoneRequest,
  };
  struct WriteChunkRequest {
    std::shared_future<Chunk> chunk;
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
  // The value of chunk_writer_->pos() before handling chunk_writer_requests_.
  Position chunk_writer_pos_before_requests_;
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
      chunk_writer_pos_before_requests_(chunk_writer_->pos()) {
  internal::DefaultThreadPool().Schedule([this] {
    std::unique_lock<std::mutex> lock(mutex_);
    for (;;) {
      while (chunk_writer_requests_.empty()) {
        has_chunk_writer_request_.wait(lock);
      }
      ChunkWriterRequest& request = chunk_writer_requests_.front();
      lock.unlock();
      switch (request.request_type) {
        case RequestType::kWriteChunkRequest: {
          // If !healthy(), the chunk must still be waited for, to ensure that
          // the chunk encoder thread exits before the chunk writer thread
          // responds to DoneRequest.
          const Chunk& chunk = request.write_chunk_request.chunk.get();
          if (RIEGELI_UNLIKELY(!healthy())) goto handled;
          if (RIEGELI_UNLIKELY(!chunk_writer_->WriteChunk(chunk))) {
            Fail(*chunk_writer_);
          }
          goto handled;
        }
        case RequestType::kFlushRequest: {
          if (RIEGELI_UNLIKELY(!healthy())) {
            request.flush_request.done.set_value(false);
            goto handled;
          }
          if (RIEGELI_UNLIKELY(
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
      lock.lock();
      chunk_writer_requests_.pop_front();
      has_space_for_chunk_.notify_all();
      chunk_writer_pos_before_requests_ = chunk_writer_->pos();
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
    has_chunk_writer_request_.notify_all();
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
    has_chunk_writer_request_.notify_all();
  }
  internal::DefaultThreadPool().Schedule([this, chunk_encoder, chunk_promise] {
    Chunk chunk;
    if (RIEGELI_UNLIKELY(!chunk_encoder->EncodeAndClose(&chunk))) {
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
    has_chunk_writer_request_.notify_all();
  }
  return done_future.get();
}

Position RecordWriter::ParallelImpl::ChunkBegin() {
  Position pos;
  std::vector<std::shared_future<Chunk>> chunks;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    pos = chunk_writer_pos_before_requests_;
    chunks.reserve(chunk_writer_requests_.size());
    for (const auto& pending_request : chunk_writer_requests_) {
      RIEGELI_ASSERT(pending_request.request_type ==
                     RequestType::kWriteChunkRequest)
          << "Unexpected type of request in the queue: "
          << static_cast<int>(pending_request.request_type);
      chunks.push_back(pending_request.write_chunk_request.chunk);
    }
  }
  for (const auto& chunk_future : chunks) {
    const Chunk& chunk = chunk_future.get();
    pos = internal::ChunkEnd(chunk.header, pos);
  }
  return pos;
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
    : Object(State::kOpen), desired_chunk_size_(options.chunk_size_) {
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
  if (RIEGELI_LIKELY(healthy()) && chunk_size_so_far_ != 0) {
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
  chunk_size_so_far_ = 0;
}

template <typename Record>
bool RecordWriter::WriteRecordImpl(Record&& record, RecordPosition* key) {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  // Decoding a chunk writes records to one array, and their positions to
  // another array. We limit the size of both arrays together, to include
  // attempts to accumulate an unbounded number of empty records.
  const uint64_t added_size = SaturatingAdd(
      IntCast<uint64_t>(RecordSize(record)), uint64_t{sizeof(uint64_t)});
  if (RIEGELI_UNLIKELY(chunk_size_so_far_ > desired_chunk_size_ ||
                       added_size > desired_chunk_size_ - chunk_size_so_far_) &&
      chunk_size_so_far_ > 0) {
    if (RIEGELI_UNLIKELY(!impl_->CloseChunk())) return Fail(*impl_);
    impl_->OpenChunk();
    chunk_size_so_far_ = 0;
  }
  chunk_size_so_far_ += added_size;
  if (key != nullptr) *key = impl_->Pos();
  if (RIEGELI_UNLIKELY(!impl_->AddRecord(std::forward<Record>(record)))) {
    return Fail(*impl_);
  }
  return true;
}

template bool RecordWriter::WriteRecordImpl(const google::protobuf::MessageLite& record,
                                            RecordPosition* key);
template bool RecordWriter::WriteRecordImpl(const absl::string_view& record,
                                            RecordPosition* key);
template bool RecordWriter::WriteRecordImpl(std::string&& record,
                                            RecordPosition* key);
template bool RecordWriter::WriteRecordImpl(const Chain& record,
                                            RecordPosition* key);
template bool RecordWriter::WriteRecordImpl(Chain&& record,
                                            RecordPosition* key);

bool RecordWriter::Flush(FlushType flush_type) {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  if (chunk_size_so_far_ != 0) {
    if (RIEGELI_UNLIKELY(!impl_->CloseChunk())) return Fail(*impl_);
  }
  if (RIEGELI_UNLIKELY(!impl_->Flush(flush_type))) {
    if (impl_->healthy()) return false;
    return Fail(*impl_);
  }
  if (chunk_size_so_far_ != 0) {
    impl_->OpenChunk();
    chunk_size_so_far_ = 0;
  }
  return true;
}

RecordPosition RecordWriter::Pos() const {
  if (RIEGELI_UNLIKELY(impl_ == nullptr)) return RecordPosition();
  return impl_->Pos();
}

}  // namespace riegeli
