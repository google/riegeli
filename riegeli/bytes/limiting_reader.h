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

#ifndef RIEGELI_BYTES_LIMITING_READER_H_
#define RIEGELI_BYTES_LIMITING_READER_H_

#include <stddef.h>

#include <limits>
#include <tuple>
#include <utility>

#include "absl/base/optimization.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/resetter.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Template parameter invariant part of LimitingReader.
class LimitingReaderBase : public Reader {
 public:
  // An infinite size limit.
  static constexpr Position kNoSizeLimit = std::numeric_limits<Position>::max();

  // Changes the size limit.
  //
  // Precondition: size_limit >= pos()
  void set_size_limit(Position size_limit);

  // Returns the current size limit.
  Position size_limit() const { return size_limit_; }

  // Returns the original Reader. Unchanged by Close().
  virtual Reader* src_reader() = 0;
  virtual const Reader* src_reader() const = 0;

  bool SupportsRandomAccess() const override;
  bool Size(Position* size) override;

 protected:
  LimitingReaderBase() noexcept : Reader(kInitiallyClosed) {}

  explicit LimitingReaderBase(Position size_limit);

  LimitingReaderBase(LimitingReaderBase&& that) noexcept;
  LimitingReaderBase& operator=(LimitingReaderBase&& that) noexcept;

  void Reset();
  void Reset(Position size_limit);
  void Initialize(Reader* src);

  void Done() override;
  bool PullSlow(size_t min_length, size_t recommended_length) override;
  using Reader::ReadSlow;
  bool ReadSlow(char* dest, size_t length) override;
  bool ReadSlow(Chain* dest, size_t length) override;
  using Reader::CopyToSlow;
  bool CopyToSlow(Writer* dest, Position length) override;
  bool CopyToSlow(BackwardWriter* dest, size_t length) override;
  bool SeekSlow(Position new_pos) override;

  // Sets cursor of src to cursor of this.
  void SyncBuffer(Reader* src);

  // Sets buffer pointers of this to buffer pointers of src, adjusting them for
  // the size limit. Fails this if src failed.
  void MakeBuffer(Reader* src);

  Position size_limit_ = kNoSizeLimit;

 private:
  template <typename Dest>
  bool ReadInternal(Dest* dest, size_t length);

  // Invariants if healthy():
  //   start_ == src_reader()->start_
  //   limit_ <= src_reader()->limit_
  //   start_pos() == src_reader()->start_pos()
  //   limit_pos_ <= UnsignedMin(src_reader()->limit_pos_, size_limit_)
};

// A Reader which reads from another Reader up to the specified size limit,
// then pretends that the source ends.
//
// The Src template parameter specifies the type of the object providing and
// possibly owning the original Reader. Src must support
// Dependency<Reader*, Src>, e.g. Reader* (not owned, default),
// unique_ptr<Reader> (owned), ChainReader<> (owned).
//
// The original Reader must not be accessed until the LimitingReader is closed
// or no longer used.
template <typename Src = Reader*>
class LimitingReader : public LimitingReaderBase {
 public:
  // Creates a closed LimitingReader.
  LimitingReader() noexcept {}

  // Will read from the original Reader provided by src.
  //
  // Precondition: size_limit >= src->pos()
  explicit LimitingReader(const Src& src, Position size_limit = kNoSizeLimit);
  explicit LimitingReader(Src&& src, Position size_limit = kNoSizeLimit);

  // Will read from the original Reader provided by a Src constructed from
  // elements of src_args. This avoids constructing a temporary Src and moving
  // from it.
  //
  // Precondition: size_limit >= src->pos()
  template <typename... SrcArgs>
  explicit LimitingReader(std::tuple<SrcArgs...> src_args,
                          Position size_limit = kNoSizeLimit);

  LimitingReader(LimitingReader&& that) noexcept;
  LimitingReader& operator=(LimitingReader&& that) noexcept;

  // Makes *this equivalent to a newly constructed LimitingReader. This avoids
  // constructing a temporary LimitingReader and moving from it.
  void Reset();
  void Reset(const Src& src, Position size_limit = kNoSizeLimit);
  void Reset(Src&& src, Position size_limit = kNoSizeLimit);
  template <typename... SrcArgs>
  void Reset(std::tuple<SrcArgs...> src_args,
             Position size_limit = kNoSizeLimit);

  // Returns the object providing and possibly owning the original Reader.
  // Unchanged by Close().
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  Reader* src_reader() override { return src_.get(); }
  const Reader* src_reader() const override { return src_.get(); }

 protected:
  void Done() override;
  void VerifyEnd() override;

 private:
  void MoveSrc(LimitingReader&& that);

  // The object providing and possibly owning the original Reader.
  Dependency<Reader*, Src> src_;
};

// Sets the size limit of a LimitingReader in the constructor and restores it in
// the destructor.
//
// Temporarily changing the size limit is more efficient than making a new
// LimitingReader reading from a LimitingReader.
class SizeLimitSetter {
 public:
  explicit SizeLimitSetter(LimitingReaderBase* limiting_reader,
                           Position size_limit)
      : limiting_reader_(limiting_reader),
        old_size_limit_(limiting_reader_->size_limit()) {
    limiting_reader->set_size_limit(size_limit);
  }

  SizeLimitSetter(const SizeLimitSetter&) = delete;
  SizeLimitSetter& operator=(const SizeLimitSetter&) = delete;

  ~SizeLimitSetter() { limiting_reader_->set_size_limit(old_size_limit_); }

 private:
  LimitingReaderBase* limiting_reader_;
  Position old_size_limit_;
};

// Implementation details follow.

inline LimitingReaderBase::LimitingReaderBase(Position size_limit)
    : Reader(kInitiallyOpen), size_limit_(size_limit) {}

inline LimitingReaderBase::LimitingReaderBase(
    LimitingReaderBase&& that) noexcept
    : Reader(std::move(that)), size_limit_(that.size_limit_) {}

inline LimitingReaderBase& LimitingReaderBase::operator=(
    LimitingReaderBase&& that) noexcept {
  Reader::operator=(std::move(that));
  size_limit_ = that.size_limit_;
  return *this;
}

inline void LimitingReaderBase::Reset() {
  Reader::Reset(kInitiallyClosed);
  size_limit_ = kNoSizeLimit;
}

inline void LimitingReaderBase::Reset(Position size_limit) {
  Reader::Reset(kInitiallyOpen);
  size_limit_ = size_limit;
}

inline void LimitingReaderBase::Initialize(Reader* src) {
  RIEGELI_ASSERT(src != nullptr)
      << "Failed precondition of LimitingReader: null Reader pointer";
  RIEGELI_ASSERT_GE(size_limit_, src->pos())
      << "Failed precondition of LimitingReader: "
         "size limit smaller than current position";
  MakeBuffer(src);
}

inline void LimitingReaderBase::set_size_limit(Position size_limit) {
  RIEGELI_ASSERT_GE(size_limit, pos())
      << "Failed precondition of LimitingReaderBase::set_size_limit(): "
         "size limit smaller than current position";
  size_limit_ = size_limit;
  if (limit_pos_ > size_limit_) {
    limit_ -= IntCast<size_t>(limit_pos_ - size_limit_);
    limit_pos_ = size_limit_;
  }
}

inline void LimitingReaderBase::SyncBuffer(Reader* src) {
  src->set_cursor(cursor_);
}

inline void LimitingReaderBase::MakeBuffer(Reader* src) {
  start_ = src->start();
  cursor_ = src->cursor();
  limit_ = src->limit();
  limit_pos_ = src->pos() + src->available();  // src->limit_pos_
  if (limit_pos_ > size_limit_) {
    limit_ -= IntCast<size_t>(limit_pos_ - size_limit_);
    limit_pos_ = size_limit_;
  }
  if (ABSL_PREDICT_FALSE(!src->healthy())) Fail(*src);
}

template <typename Src>
inline LimitingReader<Src>::LimitingReader(const Src& src, Position size_limit)
    : LimitingReaderBase(size_limit), src_(src) {
  Initialize(src_.get());
}

template <typename Src>
inline LimitingReader<Src>::LimitingReader(Src&& src, Position size_limit)
    : LimitingReaderBase(size_limit), src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline LimitingReader<Src>::LimitingReader(std::tuple<SrcArgs...> src_args,
                                           Position size_limit)
    : LimitingReaderBase(size_limit), src_(std::move(src_args)) {
  Initialize(src_.get());
}

template <typename Src>
inline LimitingReader<Src>::LimitingReader(LimitingReader&& that) noexcept
    : LimitingReaderBase(std::move(that)) {
  MoveSrc(std::move(that));
}

template <typename Src>
inline LimitingReader<Src>& LimitingReader<Src>::operator=(
    LimitingReader&& that) noexcept {
  LimitingReaderBase::operator=(std::move(that));
  MoveSrc(std::move(that));
  return *this;
}

template <typename Src>
inline void LimitingReader<Src>::Reset() {
  LimitingReaderBase::Reset();
  src_.Reset();
}

template <typename Src>
inline void LimitingReader<Src>::Reset(const Src& src, Position size_limit) {
  LimitingReaderBase::Reset(size_limit);
  src_.Reset(src);
  Initialize(src_.get());
}

template <typename Src>
inline void LimitingReader<Src>::Reset(Src&& src, Position size_limit) {
  LimitingReaderBase::Reset(size_limit);
  src_.Reset(std::move(src));
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline void LimitingReader<Src>::Reset(std::tuple<SrcArgs...> src_args,
                                       Position size_limit) {
  LimitingReaderBase::Reset(size_limit);
  src_.Reset(std::move(src_args));
  Initialize(src_.get());
}

template <typename Src>
inline void LimitingReader<Src>::MoveSrc(LimitingReader&& that) {
  if (src_.kIsStable()) {
    src_ = std::move(that.src_);
  } else {
    SyncBuffer(src_.get());
    src_ = std::move(that.src_);
    MakeBuffer(src_.get());
  }
}

template <typename Src>
void LimitingReader<Src>::Done() {
  LimitingReaderBase::Done();
  if (src_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) Fail(*src_);
  }
}

template <typename Src>
void LimitingReader<Src>::VerifyEnd() {
  LimitingReaderBase::VerifyEnd();
  if (src_.is_owning() && ABSL_PREDICT_TRUE(healthy())) {
    SyncBuffer(src_.get());
    src_->VerifyEnd();
    MakeBuffer(src_.get());
  }
}

template <typename Src>
struct Resetter<LimitingReader<Src>> : ResetterByReset<LimitingReader<Src>> {};

}  // namespace riegeli

#endif  // RIEGELI_BYTES_LIMITING_READER_H_
