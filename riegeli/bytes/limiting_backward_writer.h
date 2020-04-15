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

#ifndef RIEGELI_BYTES_LIMITING_BACKWARD_WRITER_H_
#define RIEGELI_BYTES_LIMITING_BACKWARD_WRITER_H_

#include <stddef.h>

#include <limits>
#include <tuple>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/resetter.h"
#include "riegeli/bytes/backward_writer.h"

namespace riegeli {

// Template parameter independent part of `LimitingBackwardWriter`.
class LimitingBackwardWriterBase : public BackwardWriter {
 public:
  // An infinite size limit.
  static constexpr Position kNoSizeLimit = std::numeric_limits<Position>::max();

  // Changes the size limit.
  //
  // Precondition: `size_limit >= pos()`
  void set_size_limit(Position size_limit);

  // Returns the current size limit.
  Position size_limit() const { return size_limit_; }

  // Returns the original `BackwardWriter`. Unchanged by `Close()`.
  virtual BackwardWriter* dest_writer() = 0;
  virtual const BackwardWriter* dest_writer() const = 0;

  bool Flush(FlushType flush_type) override;
  bool SupportsTruncate() const override;
  bool Truncate(Position new_size) override;

 protected:
  LimitingBackwardWriterBase() noexcept : BackwardWriter(kInitiallyClosed) {}

  explicit LimitingBackwardWriterBase(Position size_limit);

  LimitingBackwardWriterBase(LimitingBackwardWriterBase&& that) noexcept;
  LimitingBackwardWriterBase& operator=(
      LimitingBackwardWriterBase&& that) noexcept;

  void Reset();
  void Reset(Position size_limit);
  void Initialize(BackwardWriter* dest);

  void Done() override;
  bool PushSlow(size_t min_length, size_t recommended_length) override;
  using BackwardWriter::WriteSlow;
  bool WriteSlow(absl::string_view src) override;
  bool WriteSlow(const Chain& src) override;
  bool WriteSlow(Chain&& src) override;
  bool WriteSlow(const absl::Cord& src) override;
  bool WriteSlow(absl::Cord&& src) override;
  void WriteHintSlow(size_t length) override;

  // Sets cursor of `*dest` to cursor of `*this`. Fails `*this` if size limit is
  // exceeded.
  //
  // Postcondition: pos() <= size_limit_
  bool SyncBuffer(BackwardWriter* dest);

  // Sets buffer pointers of `*this` to buffer pointers of `*dest`. Fails
  // `*this` if `*dest` failed.
  void MakeBuffer(BackwardWriter* dest);

  // Invariant: start_pos() <= size_limit_
  Position size_limit_ = kNoSizeLimit;

 private:
  // This template is defined and used only in limiting_backward_writer.cc.
  template <typename Src>
  bool WriteInternal(Src&& src);

  // Invariants if `healthy()`:
  //   `start() == dest_writer()->start()`
  //   `limit() == dest_writer()->limit()`
  //   `start_pos() == dest_writer()->start_pos()`
};

// A `BackwardWriter` which writes to another `BackwardWriter` up to the
// specified size limit. An attempt to write more fails, leaving destination
// contents unspecified.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the original `BackwardWriter`. `Dest` must support
// `Dependency<BackwardWriter*, Dest>`, e.g.
// `BackwardWriter*` (not owned, default),
// `std::unique_ptr<BackwardWriter>` (owned), `ChainBackwardWriter<>` (owned).
//
// The original `BackwardWriter` must not be accessed until the
// `LimitingBackwardWriter` is closed or no longer used, except that it is
// allowed to read the destination of the original `BackwardWriter` immediately
// after `Flush()`.
template <typename Dest = BackwardWriter*>
class LimitingBackwardWriter : public LimitingBackwardWriterBase {
 public:
  // Creates a closed `LimitingBackwardWriter`.
  LimitingBackwardWriter() noexcept {}

  // Will write to the original `BackwardWriter` provided by `dest`.
  //
  // Precondition: `size_limit >= dest->pos()`
  explicit LimitingBackwardWriter(const Dest& dest,
                                  Position size_limit = kNoSizeLimit);
  explicit LimitingBackwardWriter(Dest&& dest,
                                  Position size_limit = kNoSizeLimit);

  // Will write to the original `BackwardWriter` provided by a `Dest`
  // constructed from elements of `dest_args`. This avoids constructing a
  // temporary `Dest` and moving from it.
  //
  // Precondition: `size_limit >= dest->pos()`
  template <typename... DestArgs>
  explicit LimitingBackwardWriter(std::tuple<DestArgs...> dest_args,
                                  Position size_limit = kNoSizeLimit);

  LimitingBackwardWriter(LimitingBackwardWriter&& that) noexcept;
  LimitingBackwardWriter& operator=(LimitingBackwardWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `LimitingBackwardWriter`.
  // This avoids constructing a temporary `LimitingBackwardWriter` and moving
  // from it.
  void Reset();
  void Reset(const Dest& dest, Position size_limit = kNoSizeLimit);
  void Reset(Dest&& dest, Position size_limit = kNoSizeLimit);
  template <typename... DestArgs>
  void Reset(std::tuple<DestArgs...> dest_args,
             Position size_limit = kNoSizeLimit);

  // Returns the object providing and possibly owning the original
  // `BackwardWriter`. Unchanged by `Close()`.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  BackwardWriter* dest_writer() override { return dest_.get(); }
  const BackwardWriter* dest_writer() const override { return dest_.get(); }

 protected:
  void Done() override;

 private:
  void MoveDest(LimitingBackwardWriter&& that);

  // The object providing and possibly owning the original `BackwardWriter`.
  Dependency<BackwardWriter*, Dest> dest_;
};

// Implementation details follow.

inline LimitingBackwardWriterBase::LimitingBackwardWriterBase(
    Position size_limit)
    : BackwardWriter(kInitiallyOpen), size_limit_(size_limit) {}

inline LimitingBackwardWriterBase::LimitingBackwardWriterBase(
    LimitingBackwardWriterBase&& that) noexcept
    : BackwardWriter(std::move(that)), size_limit_(that.size_limit_) {}

inline LimitingBackwardWriterBase& LimitingBackwardWriterBase::operator=(
    LimitingBackwardWriterBase&& that) noexcept {
  BackwardWriter::operator=(std::move(that));
  size_limit_ = that.size_limit_;
  return *this;
}

inline void LimitingBackwardWriterBase::Reset() {
  BackwardWriter::Reset(kInitiallyClosed);
  size_limit_ = kNoSizeLimit;
}

inline void LimitingBackwardWriterBase::Reset(Position size_limit) {
  BackwardWriter::Reset(kInitiallyOpen);
  size_limit_ = size_limit;
}

inline void LimitingBackwardWriterBase::Initialize(BackwardWriter* dest) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of LimitingBackwardWriter: "
         "null BackwardWriter pointer";
  RIEGELI_ASSERT_GE(size_limit_, dest->pos())
      << "Failed precondition of LimitingBackwardWriter: "
         "size limit smaller than current position";
  MakeBuffer(dest);
}

inline void LimitingBackwardWriterBase::set_size_limit(Position size_limit) {
  RIEGELI_ASSERT_GE(size_limit, pos())
      << "Failed precondition of LimitingBackwardWriterBase::set_size_limit(): "
         "size limit smaller than current position";
  size_limit_ = size_limit;
}

inline bool LimitingBackwardWriterBase::SyncBuffer(BackwardWriter* dest) {
  if (ABSL_PREDICT_FALSE(pos() > size_limit_)) return FailOverflow();
  dest->set_cursor(cursor());
  return true;
}

inline void LimitingBackwardWriterBase::MakeBuffer(BackwardWriter* dest) {
  set_buffer(dest->limit(), dest->buffer_size(), dest->written_to_buffer());
  set_start_pos(dest->pos() - written_to_buffer());
  if (ABSL_PREDICT_FALSE(!dest->healthy())) FailWithoutAnnotation(*dest);
}

template <typename Dest>
inline LimitingBackwardWriter<Dest>::LimitingBackwardWriter(const Dest& dest,
                                                            Position size_limit)
    : LimitingBackwardWriterBase(size_limit), dest_(dest) {
  Initialize(dest_.get());
}

template <typename Dest>
inline LimitingBackwardWriter<Dest>::LimitingBackwardWriter(Dest&& dest,
                                                            Position size_limit)
    : LimitingBackwardWriterBase(size_limit), dest_(std::move(dest)) {
  Initialize(dest_.get());
}

template <typename Dest>
template <typename... DestArgs>
inline LimitingBackwardWriter<Dest>::LimitingBackwardWriter(
    std::tuple<DestArgs...> dest_args, Position size_limit)
    : LimitingBackwardWriterBase(size_limit), dest_(std::move(dest_args)) {
  Initialize(dest_.get());
}

template <typename Dest>
inline LimitingBackwardWriter<Dest>::LimitingBackwardWriter(
    LimitingBackwardWriter&& that) noexcept
    : LimitingBackwardWriterBase(std::move(that)) {
  MoveDest(std::move(that));
}

template <typename Dest>
inline LimitingBackwardWriter<Dest>& LimitingBackwardWriter<Dest>::operator=(
    LimitingBackwardWriter&& that) noexcept {
  LimitingBackwardWriterBase::operator=(std::move(that));
  MoveDest(std::move(that));
  return *this;
}

template <typename Dest>
inline void LimitingBackwardWriter<Dest>::Reset() {
  LimitingBackwardWriterBase::Reset();
  dest_.Reset();
}

template <typename Dest>
inline void LimitingBackwardWriter<Dest>::Reset(const Dest& dest,
                                                Position size_limit) {
  LimitingBackwardWriterBase::Reset(size_limit);
  dest_.Reset(dest);
  Initialize(dest_.get());
}

template <typename Dest>
inline void LimitingBackwardWriter<Dest>::Reset(Dest&& dest,
                                                Position size_limit) {
  LimitingBackwardWriterBase::Reset(size_limit);
  dest_.Reset(std::move(dest));
  Initialize(dest_.get());
}

template <typename Dest>
template <typename... DestArgs>
inline void LimitingBackwardWriter<Dest>::Reset(
    std::tuple<DestArgs...> dest_args, Position size_limit) {
  LimitingBackwardWriterBase::Reset(size_limit);
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get());
}

template <typename Dest>
inline void LimitingBackwardWriter<Dest>::MoveDest(
    LimitingBackwardWriter&& that) {
  if (dest_.kIsStable()) {
    dest_ = std::move(that.dest_);
  } else {
    // Buffer pointers are already moved so `SyncBuffer()` is called on `*this`,
    // `dest_` is not moved yet so `dest_` is taken from `that`.
    const bool ok = SyncBuffer(that.dest_.get());
    dest_ = std::move(that.dest_);
    if (ABSL_PREDICT_TRUE(ok)) MakeBuffer(dest_.get());
  }
}

template <typename Dest>
void LimitingBackwardWriter<Dest>::Done() {
  LimitingBackwardWriterBase::Done();
  if (dest_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) FailWithoutAnnotation(*dest_);
  }
}

template <typename Dest>
struct Resetter<LimitingBackwardWriter<Dest>>
    : ResetterByReset<LimitingBackwardWriter<Dest>> {};

}  // namespace riegeli

#endif  // RIEGELI_BYTES_LIMITING_BACKWARD_WRITER_H_
