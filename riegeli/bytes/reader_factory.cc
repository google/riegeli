// Copyright 2021 Google LLC
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

#include "riegeli/bytes/reader_factory.h"

#include <stddef.h>

#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/base/thread_annotations.h"
#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/external_ref.h"
#include "riegeli/base/null_safe_memcpy.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/pullable_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

class ReaderFactoryBase::ConcurrentReader : public PullableReader {
 public:
  explicit ConcurrentReader(Shared* shared, Position initial_pos);

  ConcurrentReader(const ConcurrentReader&) = delete;
  ConcurrentReader& operator=(const ConcurrentReader&) = delete;

  bool ToleratesReadingAhead() override;
  bool SupportsRandomAccess() override { return true; }
  bool SupportsNewReader() override { return true; }

 protected:
  void Done() override;
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;
  void SetReadAllHintImpl(bool read_all_hint) override;
  bool PullBehindScratch(size_t recommended_length) override;
  using PullableReader::ReadBehindScratch;
  bool ReadBehindScratch(size_t length, char* dest) override;
  bool ReadBehindScratch(size_t length, Chain& dest) override;
  bool ReadBehindScratch(size_t length, absl::Cord& dest) override;
  using PullableReader::CopyBehindScratch;
  bool CopyBehindScratch(Position length, Writer& dest) override;
  using PullableReader::ReadOrPullSomeBehindScratch;
  bool ReadOrPullSomeBehindScratch(
      size_t max_length, absl::FunctionRef<char*(size_t&)> get_dest) override;
  void ReadHintBehindScratch(size_t min_length,
                             size_t recommended_length) override;
  bool SyncBehindScratch(SyncType sync_type) override;
  bool SeekBehindScratch(Position new_pos) override;
  std::optional<Position> SizeImpl() override;
  std::unique_ptr<Reader> NewReaderImpl(Position initial_pos) override;

 private:
  bool SyncPos() ABSL_SHARED_LOCKS_REQUIRED(shared_->mutex);
  bool ReadSome() ABSL_SHARED_LOCKS_REQUIRED(shared_->mutex);

  Shared* shared_;
  ReadBufferSizer buffer_sizer_;
  // Buffered data, read directly before the original position which is
  // `start_pos() + (secondary_buffer_.size() - iter_.CharIndexInChain())`
  // when scratch is not used.
  Chain secondary_buffer_;
  // Invariant: `iter_.chain() == (is_open() ? &secondary_buffer_ : nullptr)`
  Chain::BlockIterator iter_;

  // Invariants if `is_open()` and scratch is not used:
  //   `start() == (iter_ == secondary_buffer_.blocks().cend() ? nullptr
  //                                                           : iter_->data())`
  //   `start_to_limit() ==
  //        (iter_ == secondary_buffer_.blocks().cend() ? 0 : iter_->size())`
};

inline ReaderFactoryBase::ConcurrentReader::ConcurrentReader(
    Shared* shared, Position initial_pos)
    : shared_(shared),
      buffer_sizer_(shared->buffer_options),
      iter_(secondary_buffer_.blocks().cend()) {
  set_limit_pos(initial_pos);
  buffer_sizer_.BeginRun(limit_pos());
}

void ReaderFactoryBase::ConcurrentReader::Done() {
  PullableReader::Done();
  secondary_buffer_ = Chain();
  iter_ = Chain::BlockIterator();
}

absl::Status ReaderFactoryBase::ConcurrentReader::AnnotateStatusImpl(
    absl::Status status) {
  if (is_open()) {
    absl::MutexLock lock(shared_->mutex);
    shared_->reader->Seek(pos());
    return shared_->reader->AnnotateStatus(std::move(status));
  }
  return status;
}

inline bool ReaderFactoryBase::ConcurrentReader::SyncPos() {
  if (ABSL_PREDICT_FALSE(!shared_->reader->Seek(limit_pos()))) {
    if (ABSL_PREDICT_FALSE(!shared_->reader->ok())) {
      return FailWithoutAnnotation(shared_->reader->status());
    }
    return false;
  }
  return true;
}

inline bool ReaderFactoryBase::ConcurrentReader::ReadSome() {
  if (!shared_->reader->ReadSome(buffer_sizer_.BufferLength(limit_pos()),
                                 secondary_buffer_)) {
    if (ABSL_PREDICT_FALSE(!shared_->reader->ok())) {
      return FailWithoutAnnotation(shared_->reader->status());
    }
    return false;
  }
  iter_ = secondary_buffer_.blocks().cbegin();
  return true;
}

void ReaderFactoryBase::ConcurrentReader::SetReadAllHintImpl(
    bool read_all_hint) {
  buffer_sizer_.set_read_all_hint(read_all_hint);
}

bool ReaderFactoryBase::ConcurrentReader::PullBehindScratch(
    size_t recommended_length) {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of PullableReader::PullBehindScratch(): "
         "enough data available, use Pull() instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::PullBehindScratch(): "
         "scratch used";
  if (iter_ != secondary_buffer_.blocks().cend()) ++iter_;
  set_buffer();
  for (;;) {
    while (iter_ != secondary_buffer_.blocks().cend()) {
      if (ABSL_PREDICT_TRUE(!iter_->empty())) {
        set_buffer(iter_->data(), iter_->size());
        move_limit_pos(available());
        return true;
      }
      ++iter_;
    }

    if (ABSL_PREDICT_FALSE(!ok())) return false;
    secondary_buffer_.Clear();
    iter_ = secondary_buffer_.blocks().cend();
    absl::MutexLock lock(shared_->mutex);
    if (ABSL_PREDICT_FALSE(!SyncPos())) return false;
    if (ABSL_PREDICT_FALSE(!ReadSome())) return false;
  }
}

bool ReaderFactoryBase::ConcurrentReader::ReadBehindScratch(size_t length,
                                                            char* dest) {
  RIEGELI_ASSERT_LT(available(), length)
      << "Failed precondition of PullableReader::ReadBehindScratch(char*): "
         "enough data available, use Read(char*) instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::ReadBehindScratch(char*): "
         "scratch used";
  if (iter_ != secondary_buffer_.blocks().cend()) {
    const size_t available_length = available();
    riegeli::null_safe_memcpy(dest, cursor(), available_length);
    move_cursor(available_length);
    dest += available_length;
    length -= available_length;
    ++iter_;
  }
  set_buffer();
  for (;;) {
    while (iter_ != secondary_buffer_.blocks().cend()) {
      move_limit_pos(iter_->size());
      if (length <= iter_->size()) {
        set_buffer(iter_->data(), iter_->size(), length);
        std::memcpy(dest, start(), start_to_cursor());
        return true;
      }
      std::memcpy(dest, iter_->data(), iter_->size());
      dest += iter_->size();
      length -= iter_->size();
      ++iter_;
    }

    if (ABSL_PREDICT_FALSE(!ok())) return false;
    secondary_buffer_.Clear();
    iter_ = secondary_buffer_.blocks().cend();
    absl::MutexLock lock(shared_->mutex);
    if (ABSL_PREDICT_FALSE(!SyncPos())) return false;
    if (length >= buffer_sizer_.BufferLength(pos())) {
      // Read directly to `dest`.
      if (ABSL_PREDICT_FALSE(!shared_->reader->Read(length, dest))) {
        set_limit_pos(shared_->reader->pos());
        if (ABSL_PREDICT_FALSE(!shared_->reader->ok())) {
          return FailWithoutAnnotation(shared_->reader->status());
        }
        return false;
      }
      move_limit_pos(length);
      return true;
    }
    if (ABSL_PREDICT_FALSE(!ReadSome())) return false;
  }
}

bool ReaderFactoryBase::ConcurrentReader::ReadBehindScratch(size_t length,
                                                            Chain& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of PullableReader::ReadBehindScratch(Chain&): "
         "enough data available, use Read(Chain&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of PullableReader::ReadBehindScratch(Chain&): "
         "Chain size overflow";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::ReadBehindScratch(Chain&): "
         "scratch used";
  if (length <= available()) {
    dest.Append(ExternalRef(*iter_, absl::string_view(cursor(), length)));
    move_cursor(length);
    return true;
  }
  if (iter_ != secondary_buffer_.blocks().cend()) {
    dest.Append(ExternalRef(*iter_, absl::string_view(cursor(), available())));
    length -= available();
    ++iter_;
  }
  set_buffer();
  for (;;) {
    while (iter_ != secondary_buffer_.blocks().cend()) {
      move_limit_pos(iter_->size());
      if (length <= iter_->size()) {
        set_buffer(iter_->data(), iter_->size(), length);
        dest.Append(
            ExternalRef(*iter_, absl::string_view(start(), start_to_cursor())));
        return true;
      }
      dest.Append(*iter_);
      length -= iter_->size();
      ++iter_;
    }

    if (ABSL_PREDICT_FALSE(!ok())) return false;
    secondary_buffer_.Clear();
    iter_ = secondary_buffer_.blocks().cend();
    absl::MutexLock lock(shared_->mutex);
    if (ABSL_PREDICT_FALSE(!SyncPos())) return false;
    if (length >= buffer_sizer_.BufferLength(pos())) {
      // Read directly to `dest`.
      if (ABSL_PREDICT_FALSE(!shared_->reader->ReadAndAppend(length, dest))) {
        set_limit_pos(shared_->reader->pos());
        if (ABSL_PREDICT_FALSE(!shared_->reader->ok())) {
          return FailWithoutAnnotation(shared_->reader->status());
        }
        return false;
      }
      move_limit_pos(length);
      return true;
    }
    if (ABSL_PREDICT_FALSE(!ReadSome())) return false;
  }
}

bool ReaderFactoryBase::ConcurrentReader::ReadBehindScratch(size_t length,
                                                            absl::Cord& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of PullableReader::ReadBehindScratch(Cord&): "
         "enough data available, use Read(Chain&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of PullableReader::ReadBehindScratch(Cord&): "
         "Chain size overflow";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::ReadBehindScratch(Cord&): "
         "scratch used";
  if (length <= available()) {
    ExternalRef(*iter_, absl::string_view(cursor(), length)).AppendTo(dest);
    move_cursor(length);
    return true;
  }
  if (iter_ != secondary_buffer_.blocks().cend()) {
    ExternalRef(*iter_, absl::string_view(cursor(), available()))
        .AppendTo(dest);
    length -= available();
    ++iter_;
  }
  set_buffer();
  for (;;) {
    while (iter_ != secondary_buffer_.blocks().cend()) {
      move_limit_pos(iter_->size());
      if (length <= iter_->size()) {
        set_buffer(iter_->data(), iter_->size(), length);
        ExternalRef(*iter_, absl::string_view(start(), start_to_cursor()))
            .AppendTo(dest);
        return true;
      }
      ExternalRef(*iter_).AppendTo(dest);
      length -= iter_->size();
      ++iter_;
    }

    if (ABSL_PREDICT_FALSE(!ok())) return false;
    secondary_buffer_.Clear();
    iter_ = secondary_buffer_.blocks().cend();
    absl::MutexLock lock(shared_->mutex);
    if (ABSL_PREDICT_FALSE(!SyncPos())) return false;
    if (length >= buffer_sizer_.BufferLength(pos())) {
      // Read directly to `dest`.
      if (ABSL_PREDICT_FALSE(!shared_->reader->ReadAndAppend(length, dest))) {
        set_limit_pos(shared_->reader->pos());
        if (ABSL_PREDICT_FALSE(!shared_->reader->ok())) {
          return FailWithoutAnnotation(shared_->reader->status());
        }
        return false;
      }
      move_limit_pos(length);
      return true;
    }
    if (ABSL_PREDICT_FALSE(!ReadSome())) return false;
  }
}

bool ReaderFactoryBase::ConcurrentReader::CopyBehindScratch(Position length,
                                                            Writer& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of PullableReader::CopyBehindScratch(Writer&): "
         "enough data available, use Copy(Writer&) instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::CopyBehindScratch(Writer&): "
         "scratch used";
  if (length <= available()) {
    if (ABSL_PREDICT_FALSE(!dest.Write(
            ExternalRef(*iter_, absl::string_view(cursor(), length))))) {
      return false;
    }
    move_cursor(length);
    return true;
  }
  if (iter_ != secondary_buffer_.blocks().cend()) {
    if (ABSL_PREDICT_FALSE(!dest.Write(
            ExternalRef(*iter_, absl::string_view(cursor(), available()))))) {
      return false;
    }
    length -= available();
    ++iter_;
  }
  set_buffer();
  for (;;) {
    while (iter_ != secondary_buffer_.blocks().cend()) {
      move_limit_pos(iter_->size());
      if (length <= iter_->size()) {
        set_buffer(iter_->data(), iter_->size(), length);
        return dest.Write(
            ExternalRef(*iter_, absl::string_view(start(), start_to_cursor())));
      }
      if (ABSL_PREDICT_FALSE(!dest.Write(*iter_))) return false;
      length -= iter_->size();
      ++iter_;
    }

    if (ABSL_PREDICT_FALSE(!ok())) return false;
    secondary_buffer_.Clear();
    iter_ = secondary_buffer_.blocks().cend();
    set_buffer();
    absl::MutexLock lock(shared_->mutex);
    if (ABSL_PREDICT_FALSE(!SyncPos())) return false;
    if (length >= buffer_sizer_.BufferLength(pos())) {
      // Read directly to `dest`.
      if (ABSL_PREDICT_FALSE(!shared_->reader->Copy(length, dest))) {
        set_limit_pos(shared_->reader->pos());
        if (ABSL_PREDICT_FALSE(!shared_->reader->ok())) {
          return FailWithoutAnnotation(shared_->reader->status());
        }
        return false;
      }
      move_limit_pos(length);
      return true;
    }
    if (ABSL_PREDICT_FALSE(!ReadSome())) return false;
  }
}

bool ReaderFactoryBase::ConcurrentReader::ReadOrPullSomeBehindScratch(
    size_t max_length, absl::FunctionRef<char*(size_t&)> get_dest) {
  RIEGELI_ASSERT_GT(max_length, 0u)
      << "Failed precondition of Reader::ReadOrPullSomeBehindScratch(): "
         "nothing to read, use ReadOrPullSome() instead";
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of Reader::ReadOrPullSomeBehindScratch(): "
         "some data available, use ReadOrPullSome() instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of Reader::ReadOrPullSomeBehindScratch(): "
         "scratch used";
  if (iter_ != secondary_buffer_.blocks().cend()) ++iter_;
  set_buffer();
  for (;;) {
    while (iter_ != secondary_buffer_.blocks().cend()) {
      if (ABSL_PREDICT_TRUE(!iter_->empty())) {
        set_buffer(iter_->data(), iter_->size());
        move_limit_pos(available());
        return true;
      }
      ++iter_;
    }

    if (ABSL_PREDICT_FALSE(!ok())) return false;
    secondary_buffer_.Clear();
    iter_ = secondary_buffer_.blocks().cend();
    absl::MutexLock lock(shared_->mutex);
    if (ABSL_PREDICT_FALSE(!SyncPos())) return false;
    if (max_length >= buffer_sizer_.BufferLength(pos())) {
      // Read directly to `get_dest(max_length)`.
      size_t length_read;
      const bool read_ok =
          shared_->reader->ReadOrPullSome(max_length, get_dest, &length_read);
      move_limit_pos(length_read);
      if (ABSL_PREDICT_FALSE(!read_ok)) {
        if (ABSL_PREDICT_FALSE(!shared_->reader->ok())) {
          return FailWithoutAnnotation(shared_->reader->status());
        }
        return false;
      }
      if (length_read > 0) return true;
    }
    if (ABSL_PREDICT_FALSE(!ReadSome())) return false;
  }
}

void ReaderFactoryBase::ConcurrentReader::ReadHintBehindScratch(
    size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of PullableReader::ReadHintBehindScratch(): "
         "enough data available, use ReadHint() instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::ReadHintBehindScratch(): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!ok())) return;
  const size_t secondary_buffered_length =
      secondary_buffer_.size() - iter_.CharIndexInChain(start_to_cursor());
  if (secondary_buffered_length < min_length) {
    set_limit_pos(pos());
    set_buffer();
    secondary_buffer_.RemovePrefix(secondary_buffer_.size() -
                                   secondary_buffered_length);
    const size_t min_length_to_read = min_length - secondary_buffered_length;
    const size_t recommended_length_to_read =
        UnsignedMax(recommended_length, min_length) - secondary_buffered_length;
    {
      absl::MutexLock lock(shared_->mutex);
      if (ABSL_PREDICT_FALSE(!shared_->reader->Seek(
              limit_pos() + secondary_buffered_length))) {
        if (ABSL_PREDICT_FALSE(!shared_->reader->ok())) {
          FailWithoutAnnotation(shared_->reader->status());
        }
      } else {
        if (recommended_length_to_read > min_length_to_read) {
          shared_->reader->ReadHint(min_length_to_read,
                                    recommended_length_to_read);
        }
        if (ABSL_PREDICT_FALSE(!shared_->reader->ReadAndAppend(
                min_length_to_read, secondary_buffer_))) {
          if (ABSL_PREDICT_FALSE(!shared_->reader->ok())) {
            FailWithoutAnnotation(shared_->reader->status());
          }
        }
      }
    }
    iter_ = secondary_buffer_.blocks().cbegin();
    if (iter_ != secondary_buffer_.blocks().cend()) {
      set_buffer(iter_->data(), iter_->size());
      move_limit_pos(available());
    }
  }
}

bool ReaderFactoryBase::ConcurrentReader::SyncBehindScratch(
    SyncType sync_type) {
  const Position new_pos = pos();
  buffer_sizer_.EndRun(new_pos);
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  secondary_buffer_.Clear();
  iter_ = secondary_buffer_.blocks().cend();
  set_buffer();
  set_limit_pos(new_pos);
  buffer_sizer_.BeginRun(limit_pos());
  if (sync_type == SyncType::kFromObject) return true;
  absl::MutexLock lock(shared_->mutex);
  return shared_->reader->Sync(sync_type);
}

bool ReaderFactoryBase::ConcurrentReader::ToleratesReadingAhead() {
  if (buffer_sizer_.read_all_hint()) return true;
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  absl::MutexLock lock(shared_->mutex);
  return shared_->reader->ToleratesReadingAhead();
}

bool ReaderFactoryBase::ConcurrentReader::SeekBehindScratch(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of PullableReader::SeekBehindScratch(): "
         "position in the buffer, use Seek() instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::SeekBehindScratch(): "
         "scratch used";
  const Position secondary_buffer_begin =
      start_pos() - iter_.CharIndexInChain();
  const Position secondary_buffer_end =
      secondary_buffer_begin + secondary_buffer_.size();
  if (new_pos >= secondary_buffer_begin && new_pos <= secondary_buffer_end) {
    // Seeking within `secondary_buffer_`.
    if (new_pos == secondary_buffer_end) {
      iter_ = secondary_buffer_.blocks().cend();
      set_buffer();
      set_limit_pos(secondary_buffer_end);
      return true;
    }
    const Chain::BlockAndChar block_and_char =
        secondary_buffer_.BlockAndCharIndex(
            IntCast<size_t>(new_pos - secondary_buffer_begin));
    iter_ = block_and_char.block_iter;
    set_buffer(iter_->data(), iter_->size(), block_and_char.char_index);
    set_limit_pos(new_pos + available());
    return true;
  }

  buffer_sizer_.EndRun(pos());
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  secondary_buffer_.Clear();
  iter_ = secondary_buffer_.blocks().cend();
  set_buffer();
  set_limit_pos(secondary_buffer_end);
  if (new_pos > secondary_buffer_end) {
    // Seeking forwards.
    std::optional<Position> size;
    {
      absl::MutexLock lock(shared_->mutex);
      size = shared_->reader->Size();
      if (ABSL_PREDICT_FALSE(size == std::nullopt)) {
        return FailWithoutAnnotation(shared_->reader->status());
      }
    }
    if (ABSL_PREDICT_FALSE(new_pos > *size)) {
      // Source ends.
      set_limit_pos(*size);
      buffer_sizer_.BeginRun(limit_pos());
      return false;
    }
  }
  set_limit_pos(new_pos);
  buffer_sizer_.BeginRun(limit_pos());
  return true;
}

std::optional<Position> ReaderFactoryBase::ConcurrentReader::SizeImpl() {
  if (ABSL_PREDICT_FALSE(!ok())) return std::nullopt;
  absl::MutexLock lock(shared_->mutex);
  const std::optional<Position> size = shared_->reader->Size();
  if (ABSL_PREDICT_FALSE(size == std::nullopt)) {
    FailWithoutAnnotation(shared_->reader->status());
  }
  return size;
}

std::unique_ptr<Reader> ReaderFactoryBase::ConcurrentReader::NewReaderImpl(
    Position initial_pos) {
  return std::make_unique<ConcurrentReader>(shared_, initial_pos);
}

void ReaderFactoryBase::Initialize(BufferOptions buffer_options, Reader* src) {
  RIEGELI_ASSERT_NE(src, nullptr)
      << "Failed precondition of ReaderFactory: null Reader pointer";
  RIEGELI_ASSERT(src->SupportsRandomAccess())
      << "Failed precondition of ReaderFactory: "
         "the original Reader does not support random access";
  initial_pos_ = src->pos();
  if (!src->SupportsNewReader()) {
    shared_ = std::make_unique<Shared>(buffer_options, src);
  }
  if (ABSL_PREDICT_FALSE(!src->ok())) FailWithoutAnnotation(src->status());
}

void ReaderFactoryBase::Done() { shared_.reset(); }

absl::Status ReaderFactoryBase::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) {
    if (shared_ == nullptr) {
      Reader& src = *SrcReader();
      return src.AnnotateStatus(std::move(status));
    } else {
      absl::MutexLock lock(shared_->mutex);
      return shared_->reader->AnnotateStatus(std::move(status));
    }
  }
  return status;
}

std::unique_ptr<Reader> ReaderFactoryBase::NewReader(Position initial_pos) const
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  if (ABSL_PREDICT_FALSE(!ok())) return nullptr;
  if (shared_ == nullptr) {
    Reader& src = const_cast<Reader&>(*SrcReader());
    std::unique_ptr<Reader> reader = src.NewReader(initial_pos);
    RIEGELI_ASSERT_NE(reader, nullptr)
        << "Failed postcondition of Reader::NewReader(): "
           "returned null but Reader is ok() and SupportsNewReader()";
    return reader;
  } else {
    return std::make_unique<ConcurrentReader>(shared_.get(), initial_pos);
  }
}

}  // namespace riegeli
