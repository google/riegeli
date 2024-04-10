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
#include <memory>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

class BackwardWriter;
class ScopedLimiter;
class Writer;

// Template parameter independent part of `LimitingReader`.
class LimitingReaderBase : public Reader {
 public:
  class Options {
   public:
    Options() noexcept {}

    // The limit expressed as an absolute position.
    //
    // `absl::nullopt` means no limit, unless `max_length()` is set.
    //
    // `max_pos()` and `max_length()` must not be both set.
    //
    // Default: `absl::nullopt`.
    Options& set_max_pos(absl::optional<Position> max_pos) & {
      max_pos_ = max_pos;
      return *this;
    }
    Options&& set_max_pos(absl::optional<Position> max_pos) && {
      return std::move(set_max_pos(max_pos));
    }
    absl::optional<Position> max_pos() const { return max_pos_; }

    // A shortcut for `set_max_pos(pos)` with `set_exact(true)`.
    Options& set_exact_pos(Position exact_pos) & {
      return set_max_pos(exact_pos).set_exact(true);
    }
    Options&& set_exact_pos(Position exact_pos) && {
      return std::move(set_exact_pos(exact_pos));
    }

    // The limit expressed as a length relative to the current position.
    //
    // `absl::nullopt` means no limit, unless `max_pos()` is set.
    //
    // `max_pos()` and `max_length()` must not be both set.
    //
    // Default: `absl::nullopt`.
    Options& set_max_length(absl::optional<Position> max_length) & {
      max_length_ = max_length;
      return *this;
    }
    Options&& set_max_length(absl::optional<Position> max_length) && {
      return std::move(set_max_length(max_length));
    }
    absl::optional<Position> max_length() const { return max_length_; }

    // A shortcut for `set_max_length(length)` with `set_exact(true)`.
    Options& set_exact_length(Position exact_length) & {
      return set_max_length(exact_length).set_exact(true);
    }
    Options&& set_exact_length(Position exact_length) && {
      return std::move(set_exact_length(exact_length));
    }

    // If `false`, `LimitingReader` will read data at most up to the limit.
    // Reading will end cleanly when either the limit is reached or the source
    // ends.
    //
    // If `true`, `LimitingReader` will read data exactly up to the limit.
    // Reading will end cleanly when the limit is reached, but will fail if the
    // source ends before the limit.
    //
    // Default: `false`.
    Options& set_exact(bool exact) & {
      exact_ = exact;
      return *this;
    }
    Options&& set_exact(bool exact) && { return std::move(set_exact(exact)); }
    bool exact() const { return exact_; }

    // If `false`, `LimitingReader` will allow the original source to exceed the
    // limit.
    //
    // If `true`, `LimitingReader` will require the original source to end
    // before or at the limit (depending on `exact()`), but will fail if the
    // original source exceeds the limit. This is checked when `LimitingReader`
    // is closed while positioned at its end.
    //
    // Default: `false`.
    Options& set_fail_if_longer(bool fail_if_longer) & {
      fail_if_longer_ = fail_if_longer;
      return *this;
    }
    Options&& set_fail_if_longer(bool fail_if_longer) && {
      return std::move(set_fail_if_longer(fail_if_longer));
    }
    bool fail_if_longer() const { return fail_if_longer_; }

   private:
    absl::optional<Position> max_pos_;
    absl::optional<Position> max_length_;
    bool exact_ = false;
    bool fail_if_longer_ = false;
  };

  // Returns the original `Reader`. Unchanged by `Close()`.
  virtual Reader* SrcReader() const = 0;

  // Accesses the limit expressed as an absolute position.
  //
  // If `set_max_length()` was used, `max_pos()` returns the same limit
  // translated to an absolute position.
  //
  // If no limit is set, returns `std::numeric_limits<Position>::max()`.
  //
  // If possible, `ScopedLimiter` is recommended over using `set_max_pos()`
  // directly.
  void set_max_pos(Position max_pos);
  Position max_pos() const { return max_pos_; }

  // Accesses the limit expressed as a length relative to the current position,
  // i.e. the length remaining to the limit.
  //
  // If `set_max_pos()` was used, `max_length()` returns the same limit
  // translated to a length relative to the current position.
  //
  // If no limit is set, returns `std::numeric_limits<Position>::max() - pos()`.
  //
  // If possible, `ScopedLimiter` is recommended over using `set_max_length()`
  // directly.
  void set_max_length(Position max_length);
  Position max_length() const;

  // Clears the limit.
  void clear_limit() { max_pos_ = std::numeric_limits<Position>::max(); }

  // Accesses the exactness setting.
  //
  // If `false`, `LimitingReader` will read data at most up to the limit.
  // Reading will end cleanly when either the limit is reached or the source
  // ends.
  //
  // If `true`, `LimitingReader` will read data exactly up to the limit.
  // Reading will end cleanly when the limit is reached, but will fail if the
  // source ends before the limit.
  //
  // If possible, `ScopedLimiter` is recommended over using `set_exact()`
  // directly.
  void set_exact(bool exact) { exact_ = exact; }
  bool exact() const { return exact_; }

  // Accesses the failure if larger setting.
  //
  // If `false`, `LimitingReader` will allow the original source to exceed the
  // limit.
  //
  // If `true`, `LimitingReader` will require the original source to end before
  // or at the limit (depending on `exact()`), but will fail if the original
  // source exceeds the limit. This is checked when `LimitingReader` is closed
  // while positioned at its end.
  //
  // If possible, `ScopedLimiter` is recommended over using
  // `set_fail_if_longer()` directly.
  void set_fail_if_longer(bool fail_if_longer) {
    fail_if_longer_ = fail_if_longer;
  }
  bool fail_if_longer() const { return fail_if_longer_; }

  bool ToleratesReadingAhead() override;
  bool SupportsRandomAccess() override;
  bool SupportsRewind() override;
  bool SupportsSize() override;
  bool SupportsNewReader() override;

 protected:
  explicit LimitingReaderBase(Closed) noexcept : Reader(kClosed) {}

  explicit LimitingReaderBase(bool exact, bool fail_if_longer);

  LimitingReaderBase(LimitingReaderBase&& that) noexcept;
  LimitingReaderBase& operator=(LimitingReaderBase&& that) noexcept;

  void Reset(Closed);
  void Reset(bool exact, bool fail_if_longer);
  void Initialize(Reader* src, Options&& options);

  // Sets cursor of `src` to cursor of `*this`.
  void SyncBuffer(Reader& src);

  // Sets buffer pointers of `*this` to buffer pointers of `src`, adjusting
  // them for `max_pos_`. Fails `*this` if `src` failed.
  void MakeBuffer(Reader& src);

  void Done() override;
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;
  bool PullSlow(size_t min_length, size_t recommended_length) override;
  using Reader::ReadSlow;
  bool ReadSlow(size_t length, char* dest) override;
  bool ReadSlow(size_t length, Chain& dest) override;
  bool ReadSlow(size_t length, absl::Cord& dest) override;
  using Reader::CopySlow;
  bool CopySlow(Position length, Writer& dest) override;
  bool CopySlow(size_t length, BackwardWriter& dest) override;
  using Reader::ReadOrPullSomeSlow;
  bool ReadOrPullSomeSlow(size_t max_length,
                          absl::FunctionRef<char*(size_t&)> get_dest) override;
  void ReadHintSlow(size_t min_length, size_t recommended_length) override;
  bool SeekSlow(Position new_pos) override;
  absl::optional<Position> SizeImpl() override;
  std::unique_ptr<Reader> NewReaderImpl(Position initial_pos) override;

 private:
  // For `FailNotEnoughEarly()`, `FailLengthOverflow()`, and
  // `FailPositionLimitExceeded()`.
  friend class ScopedLimiter;

  bool CheckEnough();
  ABSL_ATTRIBUTE_COLD bool FailNotEnough();
  ABSL_ATTRIBUTE_COLD void FailNotEnoughEarly(Position expected);
  ABSL_ATTRIBUTE_COLD void FailLengthOverflow(Position max_length);
  ABSL_ATTRIBUTE_COLD void FailPositionLimitExceeded();

  // This template is defined and used only in limiting_reader.cc.
  template <typename Dest>
  bool ReadInternal(size_t length, Dest& dest);

  // Invariant: `pos() <= max_pos_`
  Position max_pos_ = std::numeric_limits<Position>::max();

  bool exact_ = false;
  bool fail_if_longer_ = false;

  // Invariants if `is_open()`:
  //   `start() == SrcReader()->start() || start() == nullptr`
  //   `limit() <= SrcReader()->limit() || limit() == nullptr`
  //   `start_pos() == SrcReader()->start_pos() || start() == nullptr`
  //   `limit_pos() <= max_pos_`
};

// A `Reader` which reads from another `Reader` up to the specified limit, then
// pretends that the source ends, or fails if configured to fail and the source
// is longer.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the original `Reader`. `Src` must support
// `Dependency<Reader*, Src>`, e.g. `Reader*` (not owned, default),
// `ChainReader<>` (owned), `std::unique_ptr<Reader>` (owned),
// `AnyDependency<Reader*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as
// `InitializerTargetT` of the type of the first constructor argument.
// This requires C++17.
//
// The original `Reader` must not be accessed until the `LimitingReader` is
// closed or no longer used.
//
// For reading multiple delimited fragments, two techniques can be used:
//
//  * Create a `LimitingReader` without a limit. For each delimited fragment
//    create a `ScopedLimiter`.
//
//  * Create a `LimitingReader` without a limit. For each delimited fragment
//    use `set_max_length()` or `set_max_pos()`, and also possibly
//    `clear_limit()` to read data between fragments.
template <typename Src = Reader*>
class LimitingReader : public LimitingReaderBase {
 public:
  // Creates a closed `LimitingReader`.
  explicit LimitingReader(Closed) noexcept : LimitingReaderBase(kClosed) {}

  // Will read from the original `Reader` provided by `src`.
  explicit LimitingReader(Initializer<Src> src, Options options = Options());

  LimitingReader(LimitingReader&& that) noexcept;
  LimitingReader& operator=(LimitingReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `LimitingReader`. This
  // avoids constructing a temporary `LimitingReader` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Src> src,
                                          Options options = Options());

  // Returns the object providing and possibly owning the original `Reader`.
  // Unchanged by `Close()`.
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  Reader* SrcReader() const override { return src_.get(); }

 protected:
  void Done() override;
  bool SyncImpl(SyncType sync_type) override;

 private:
  // Moves `that.src_` to `src_`. Buffer pointers are already moved from `src_`
  // to `*this`; adjust them to match `src_`.
  void MoveSrc(LimitingReader&& that);

  // The object providing and possibly owning the original `Reader`.
  Dependency<Reader*, Src> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit LimitingReader(Closed) -> LimitingReader<DeleteCtad<Closed>>;
template <typename Src>
explicit LimitingReader(Src&& src, LimitingReaderBase::Options options =
                                       LimitingReaderBase::Options())
    -> LimitingReader<InitializerTargetT<Src>>;
#endif

// Changes the options of a `LimitingReader` in the constructor, and restores
// them in the destructor.
class ScopedLimiter {
 public:
  using Options = LimitingReaderBase::Options;

  // Changes the effective options of `*reader` to be more strict than either
  // the provided options or the previous options. The limit can become only
  // smaller, and `exact()` can change only from `false` to `true`.
  //
  // This is similar to making a new `LimitingReader` reading from the previous
  // `LimitingReader` but more efficient. Differences:
  //
  //  * If `options.exact()` is `true`, the limit should not exceed the previous
  //    limit. If it does, i.e. the previous `LimitingReader` ends before the
  //    new limit, the `LimitingReader` fails immediately rather than when the
  //    previous limit is reached.
  //
  //  * If `options.exact()` is `true`, either `options.max_pos()` or
  //    `options.max_length()` must be set.
  //
  // The reason of the differences is that providing the expected semantics of
  // `options.exact()`, with the source being the previous `LimitingReader`,
  // would require an unusual behavior of failing when the limit is exceeded.
  // That behavior would not be useful in practice because reading would never
  // end cleanly.
  explicit ScopedLimiter(LimitingReaderBase* reader, Options options);

  ScopedLimiter(const ScopedLimiter&) = delete;
  ScopedLimiter& operator=(const ScopedLimiter&) = delete;

  // Restores the options.
  //
  // Precondition:
  //   `reader->max_pos()` is not smaller than it was
  //       when the `ScopedLimiter` was constructed.
  ~ScopedLimiter();

 private:
  LimitingReaderBase* reader_;
  Position old_max_pos_;
  bool old_exact_;
  bool fail_if_longer_;
};

// Implementation details follow.

inline LimitingReaderBase::LimitingReaderBase(bool exact, bool fail_if_longer)
    : exact_(exact), fail_if_longer_(fail_if_longer) {}

inline LimitingReaderBase::LimitingReaderBase(
    LimitingReaderBase&& that) noexcept
    : Reader(static_cast<Reader&&>(that)),
      max_pos_(that.max_pos_),
      exact_(that.exact_),
      fail_if_longer_(that.fail_if_longer_) {}

inline LimitingReaderBase& LimitingReaderBase::operator=(
    LimitingReaderBase&& that) noexcept {
  Reader::operator=(static_cast<Reader&&>(that));
  max_pos_ = that.max_pos_;
  exact_ = that.exact_;
  fail_if_longer_ = that.fail_if_longer_;
  return *this;
}

inline void LimitingReaderBase::Reset(Closed) {
  Reader::Reset(kClosed);
  max_pos_ = std::numeric_limits<Position>::max();
  exact_ = false;
  fail_if_longer_ = false;
}

inline void LimitingReaderBase::Reset(bool exact, bool fail_if_longer) {
  Reader::Reset();
  // `max_pos_` will be set by `Initialize()`.
  exact_ = exact;
  fail_if_longer_ = fail_if_longer;
}

inline void LimitingReaderBase::Initialize(Reader* src, Options&& options) {
  RIEGELI_ASSERT(src != nullptr)
      << "Failed precondition of LimitingReader: null Reader pointer";
  RIEGELI_ASSERT(options.max_pos() == absl::nullopt ||
                 options.max_length() == absl::nullopt)
      << "Failed precondition of LimitingReader: "
         "Options::max_pos() and Options::max_length() are both set";
  set_buffer(src->start(), src->start_to_limit(), src->start_to_cursor());
  set_limit_pos(src->limit_pos());
  if (ABSL_PREDICT_FALSE(!src->ok())) FailWithoutAnnotation(src->status());
  if (options.max_pos() != absl::nullopt) {
    set_max_pos(*options.max_pos());
  } else if (options.max_length() != absl::nullopt) {
    set_max_length(*options.max_length());
  } else {
    clear_limit();
  }
}

inline void LimitingReaderBase::set_max_pos(Position max_pos) {
  max_pos_ = max_pos;
  if (limit_pos() > max_pos_) {
    if (ABSL_PREDICT_FALSE(pos() > max_pos_)) {
      set_buffer();
      set_limit_pos(max_pos_);
      CheckEnough();
      return;
    }
    set_buffer(start(),
               start_to_limit() - IntCast<size_t>(limit_pos() - max_pos_),
               start_to_cursor());
    set_limit_pos(max_pos_);
  }
}

inline void LimitingReaderBase::set_max_length(Position max_length) {
  if (ABSL_PREDICT_FALSE(max_length >
                         std::numeric_limits<Position>::max() - pos())) {
    max_pos_ = std::numeric_limits<Position>::max();
    if (exact_) FailLengthOverflow(max_length);
    return;
  }
  max_pos_ = pos() + max_length;
  if (limit_pos() > max_pos_) {
    set_buffer(start(),
               start_to_limit() - IntCast<size_t>(limit_pos() - max_pos_),
               start_to_cursor());
    set_limit_pos(max_pos_);
  }
}

inline Position LimitingReaderBase::max_length() const {
  RIEGELI_ASSERT_GE(max_pos_, pos())
      << "Failed invariant of LimitingReaderBase: "
         "position already exceeds its limit";
  return max_pos_ - pos();
}

inline void LimitingReaderBase::SyncBuffer(Reader& src) {
  if (ABSL_PREDICT_TRUE(cursor() != nullptr)) src.set_cursor(cursor());
}

inline void LimitingReaderBase::MakeBuffer(Reader& src) {
  set_buffer(src.start(), src.start_to_limit(), src.start_to_cursor());
  set_limit_pos(src.limit_pos());
  if (limit_pos() > max_pos_) {
    if (ABSL_PREDICT_FALSE(pos() > max_pos_)) {
      set_buffer();
    } else {
      set_buffer(start(),
                 start_to_limit() - IntCast<size_t>(limit_pos() - max_pos_),
                 start_to_cursor());
    }
    set_limit_pos(max_pos_);
  }
  if (ABSL_PREDICT_FALSE(!src.ok())) FailWithoutAnnotation(src.status());
}

inline bool LimitingReaderBase::CheckEnough() {
  if (ABSL_PREDICT_FALSE(exact_)) return FailNotEnough();
  return false;
}

template <typename Src>
inline LimitingReader<Src>::LimitingReader(Initializer<Src> src,
                                           Options options)
    : LimitingReaderBase(options.exact(), options.fail_if_longer()),
      src_(std::move(src)) {
  Initialize(src_.get(), std::move(options));
}

template <typename Src>
inline LimitingReader<Src>::LimitingReader(LimitingReader&& that) noexcept
    : LimitingReaderBase(static_cast<LimitingReaderBase&&>(that)) {
  MoveSrc(std::move(that));
}

template <typename Src>
inline LimitingReader<Src>& LimitingReader<Src>::operator=(
    LimitingReader&& that) noexcept {
  LimitingReaderBase::operator=(static_cast<LimitingReaderBase&&>(that));
  MoveSrc(std::move(that));
  return *this;
}

template <typename Src>
inline void LimitingReader<Src>::Reset(Closed) {
  LimitingReaderBase::Reset(kClosed);
  src_.Reset();
}

template <typename Src>
inline void LimitingReader<Src>::Reset(Initializer<Src> src, Options options) {
  LimitingReaderBase::Reset(options.exact(), options.fail_if_longer());
  src_.Reset(std::move(src));
  Initialize(src_.get(), std::move(options));
}

template <typename Src>
inline void LimitingReader<Src>::MoveSrc(LimitingReader&& that) {
  if (src_.kIsStable || that.src_ == nullptr) {
    src_ = std::move(that.src_);
  } else {
    // Buffer pointers are already moved so `SyncBuffer()` is called on `*this`,
    // `src_` is not moved yet so `src_` is taken from `that`.
    SyncBuffer(*that.src_);
    src_ = std::move(that.src_);
    MakeBuffer(*src_);
  }
}

template <typename Src>
void LimitingReader<Src>::Done() {
  LimitingReaderBase::Done();
  if (src_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) {
      FailWithoutAnnotation(src_->status());
    }
  }
}

template <typename Src>
bool LimitingReader<Src>::SyncImpl(SyncType sync_type) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  SyncBuffer(*src_);
  bool sync_ok = true;
  if (sync_type != SyncType::kFromObject || src_.IsOwning()) {
    sync_ok = src_->Sync(sync_type);
  }
  MakeBuffer(*src_);
  return sync_ok;
}

inline ScopedLimiter::ScopedLimiter(LimitingReaderBase* reader, Options options)
    : reader_(RIEGELI_ASSERT_NOTNULL(reader)),
      old_max_pos_(reader_->max_pos()),
      old_exact_(reader_->exact()),
      fail_if_longer_(options.fail_if_longer()) {
  RIEGELI_ASSERT(options.max_pos() == absl::nullopt ||
                 options.max_length() == absl::nullopt)
      << "Failed precondition of ScopedLimiter: "
         "Options::max_pos() and Options::max_length() are both set";
  if (options.max_pos() != absl::nullopt) {
    if (ABSL_PREDICT_FALSE(*options.max_pos() > reader_->max_pos())) {
      if (options.exact()) reader_->FailNotEnoughEarly(*options.max_pos());
    } else {
      reader_->set_max_pos(*options.max_pos());
    }
  } else if (options.max_length() != absl::nullopt) {
    if (ABSL_PREDICT_FALSE(*options.max_length() >
                           std::numeric_limits<Position>::max() -
                               reader_->pos())) {
      if (options.exact()) reader_->FailLengthOverflow(*options.max_length());
    } else {
      const Position max_pos = reader_->pos() + *options.max_length();
      if (ABSL_PREDICT_FALSE(max_pos > reader_->max_pos())) {
        if (options.exact()) reader_->FailNotEnoughEarly(max_pos);
      } else {
        reader_->set_max_pos(max_pos);
      }
    }
  } else {
    RIEGELI_ASSERT(!options.exact())
        << "Failed precondtion of ScopedLimiter: "
           "Options::exact() requires Options::max_pos() or "
           "Options::max_length()";
  }
  reader_->set_exact(reader_->exact() || options.exact());
}

inline ScopedLimiter::~ScopedLimiter() {
  RIEGELI_ASSERT_GE(old_max_pos_, reader_->max_pos())
      << "Failed precondtion of ~ScopedLimiter: "
         "The underlying LimitingReader increased its limit "
         "while the ScopedLimiter was active";
  const Position inner_max_pos = reader_->max_pos();
  reader_->set_max_pos(old_max_pos_);
  reader_->set_exact(old_exact_);
  if (fail_if_longer_ && reader_->pos() == inner_max_pos &&
      ABSL_PREDICT_FALSE(!reader_->Pull())) {
    reader_->FailPositionLimitExceeded();
  }
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_LIMITING_READER_H_
