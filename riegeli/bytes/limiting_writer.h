// Copyright 2018 Google LLC
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

#ifndef RIEGELI_BYTES_LIMITING_WRITER_H_
#define RIEGELI_BYTES_LIMITING_WRITER_H_

#include <memory>
#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "absl/utility/utility.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Template parameter invariant part of LimitingWriter.
class LimitingWriterBase : public Writer {
 public:
  // An infinite size limit.
  static constexpr Position kNoSizeLimit() {
    return std::numeric_limits<Position>::max();
  }

  // Changes the size limit.
  //
  // Precondition: size_limit >= pos()
  void set_size_limit(Position size_limit);

  // Returns the current size limit.
  Position size_limit() const { return size_limit_; }

  // Returns the original Writer. Unchanged by Close().
  virtual Writer* dest_writer() = 0;
  virtual const Writer* dest_writer() const = 0;

  bool Flush(FlushType flush_type) override;
  bool SupportsRandomAccess() const override;
  bool Size(Position* size) override;
  bool SupportsTruncate() const override;
  bool Truncate(Position new_size) override;

 protected:
  LimitingWriterBase() noexcept : Writer(State::kClosed) {}

  explicit LimitingWriterBase(Position size_limit)
      : Writer(State::kOpen), size_limit_(size_limit) {}

  LimitingWriterBase(LimitingWriterBase&& that) noexcept;
  LimitingWriterBase& operator=(LimitingWriterBase&& that) noexcept;

  void Done() override;
  bool PushSlow() override;
  using Writer::WriteSlow;
  bool WriteSlow(absl::string_view src) override;
  bool WriteSlow(std::string&& src) override;
  bool WriteSlow(const Chain& src) override;
  bool WriteSlow(Chain&& src) override;
  bool SeekSlow(Position new_pos) override;

  // Sets cursor of dest to cursor of this.
  void SyncBuffer(Writer* dest);

  // Sets buffer pointers of this to buffer pointers of dest, adjusting them for
  // the size limit. Fails this if dest failed.
  void MakeBuffer(Writer* dest);

  Position size_limit_ = kNoSizeLimit();

 private:
  template <typename Src>
  bool WriteInternal(Src&& src);

  // Invariants if healthy():
  //   start_ == dest_writer()->start_
  //   limit_ <= dest_writer()->limit_
  //   start_pos_ == dest_writer()->start_pos_
  //   limit_pos() <= UnsignedMin(size_limit_, dest_writer()->limit_pos())
};

// A Writer which writes to another Writer up to the specified size limit.
// An attempt to write more fails, leaving destination contents unspecified.
//
// The Dest template parameter specifies the type of the object providing and
// possibly owning the original Writer. Dest must support
// Dependency<Writer*, Dest>, e.g. Writer* (not owned, default),
// unique_ptr<Writer> (owned), ChainWriter<> (owned).
//
// The original Writer must not be accessed until the LimitingWriter is closed
// or no longer used, except that it is allowed to read the destination of the
// original Writer immediately after Flush().
template <typename Dest = Writer*>
class LimitingWriter : public LimitingWriterBase {
 public:
  // Creates a closed LimitingWriter.
  LimitingWriter() noexcept {}

  // Will write to the original Writer provided by dest.
  //
  // Precondition: size_limit >= dest->pos()
  explicit LimitingWriter(Dest dest, Position size_limit = kNoSizeLimit());

  LimitingWriter(LimitingWriter&& that) noexcept;
  LimitingWriter& operator=(LimitingWriter&& that) noexcept;

  // Returns the object providing and possibly owning the original Writer.
  // Unchanged by Close().
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  Writer* dest_writer() override { return dest_.ptr(); }
  const Writer* dest_writer() const override { return dest_.ptr(); }

 protected:
  void Done() override;

 private:
  void MoveDest(LimitingWriter&& that);

  // The object providing and possibly owning the original Writer.
  Dependency<Writer*, Dest> dest_;
};

// Implementation details follow.

inline LimitingWriterBase::LimitingWriterBase(
    LimitingWriterBase&& that) noexcept
    : Writer(std::move(that)),
      size_limit_(absl::exchange(that.size_limit_, kNoSizeLimit())) {}

inline LimitingWriterBase& LimitingWriterBase::operator=(
    LimitingWriterBase&& that) noexcept {
  Writer::operator=(std::move(that));
  size_limit_ = absl::exchange(that.size_limit_, kNoSizeLimit());
  return *this;
}

inline void LimitingWriterBase::set_size_limit(Position size_limit) {
  RIEGELI_ASSERT_GE(size_limit, pos())
      << "Failed precondition of LimitingWriterBase::set_size_limit(): "
         "size limit smaller than current position";
  size_limit_ = size_limit;
  if (limit_pos() > size_limit_) {
    limit_ -= IntCast<size_t>(limit_pos() - size_limit_);
  }
}

inline void LimitingWriterBase::SyncBuffer(Writer* dest) {
  dest->set_cursor(cursor_);
}

inline void LimitingWriterBase::MakeBuffer(Writer* dest) {
  start_ = dest->start();
  cursor_ = dest->cursor();
  limit_ = dest->limit();
  start_pos_ = dest->pos() - dest->written_to_buffer();  // dest->start_pos_
  if (limit_pos() > size_limit_) {
    limit_ -= IntCast<size_t>(limit_pos() - size_limit_);
  }
  if (ABSL_PREDICT_FALSE(!dest->healthy())) Fail(*dest);
}

template <typename Dest>
inline LimitingWriter<Dest>::LimitingWriter(Dest dest, Position size_limit)
    : LimitingWriterBase(size_limit), dest_(std::move(dest)) {
  RIEGELI_ASSERT(dest_.ptr() != nullptr)
      << "Failed precondition of LimitingWriter<Dest>::LimitingWriter(Dest): "
         "null Writer pointer";
  RIEGELI_ASSERT_GE(size_limit_, dest_->pos())
      << "Failed precondition of LimitingWriter<Dest>::LimitingWriter(Dest): "
         "size limit smaller than current position";
  MakeBuffer(dest_.ptr());
}

template <typename Dest>
inline LimitingWriter<Dest>::LimitingWriter(LimitingWriter&& that) noexcept
    : LimitingWriterBase(std::move(that)) {
  MoveDest(std::move(that));
}

template <typename Dest>
inline LimitingWriter<Dest>& LimitingWriter<Dest>::operator=(
    LimitingWriter&& that) noexcept {
  LimitingWriterBase::operator=(std::move(that));
  MoveDest(std::move(that));
  return *this;
}

template <typename Dest>
inline void LimitingWriter<Dest>::MoveDest(LimitingWriter&& that) {
  if (dest_.kIsStable()) {
    dest_ = std::move(that.dest_);
  } else {
    SyncBuffer(dest_.ptr());
    dest_ = std::move(that.dest_);
    MakeBuffer(dest_.ptr());
  }
}

template <typename Dest>
void LimitingWriter<Dest>::Done() {
  LimitingWriterBase::Done();
  if (dest_.kIsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) Fail(*dest_);
  }
}

extern template class LimitingWriter<Writer*>;
extern template class LimitingWriter<std::unique_ptr<Writer>>;

}  // namespace riegeli

#endif  // RIEGELI_BYTES_LIMITING_WRITER_H_
