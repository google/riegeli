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
#include <optional>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/moving_dependency.h"
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
    // `std::nullopt` means no limit, unless `max_length()` is set.
    //
    // Default: `std::nullopt`.
    Options& set_max_pos(std::optional<Position> max_pos) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      max_pos_ = max_pos;
      max_length_ = std::nullopt;
      return *this;
    }
    Options&& set_max_pos(std::optional<Position> max_pos) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_max_pos(max_pos));
    }
    std::optional<Position> max_pos() const { return max_pos_; }

    // A shortcut for `set_max_pos(pos)` with `set_exact(true)`.
    Options& set_exact_pos(Position exact_pos) & ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return set_max_pos(exact_pos).set_exact(true);
    }
    Options&& set_exact_pos(Position exact_pos) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_exact_pos(exact_pos));
    }

    // The limit expressed as a length relative to the current position.
    //
    // `std::nullopt` means no limit, unless `max_pos()` is set.
    //
    // Default: `std::nullopt`.
    Options& set_max_length(std::optional<Position> max_length) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      max_length_ = max_length;
      max_pos_ = std::nullopt;
      return *this;
    }
    Options&& set_max_length(std::optional<Position> max_length) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_max_length(max_length));
    }
    std::optional<Position> max_length() const { return max_length_; }

    // A shortcut for `set_max_length(length)` with `set_exact(true)`.
    Options& set_exact_length(Position exact_length) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return set_max_length(exact_length).set_exact(true);
    }
    Options&& set_exact_length(Position exact_length) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
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
    Options& set_exact(bool exact) & ABSL_ATTRIBUTE_LIFETIME_BOUND {
      exact_ = exact;
      return *this;
    }
    Options&& set_exact(bool exact) && ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_exact(exact));
    }
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
    Options& set_fail_if_longer(bool fail_if_longer) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      fail_if_longer_ = fail_if_longer;
      return *this;
    }
    Options&& set_fail_if_longer(bool fail_if_longer) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_fail_if_longer(fail_if_longer));
    }
    bool fail_if_longer() const { return fail_if_longer_; }

   private:
    std::optional<Position> max_pos_;
    std::optional<Position> max_length_;
    bool exact_ = false;
    bool fail_if_longer_ = false;
  };

  // Returns the original `Reader`. Unchanged by `Close()`.
  virtual Reader* SrcReader() const ABSL_ATTRIBUTE_LIFETIME_BOUND = 0;

  // Accesses the limit expressed as an absolute position.
  //
  // If `set_max_length()` was used, `max_pos()` returns the same limit
  // translated to an absolute position.
  //
  // If no limit is set, returns `std::numeric_limits<Position>::max()`.
  //
  // `ScopedLimiter` is often easier to use correctly than calling
  // `set_max_pos()` directly.
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
  // `ScopedLimiter` is often easier to use correctly than calling
  // `set_max_length()` directly.
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
  // `ScopedLimiter` is often easier to use correctly than calling `set_exact()`
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
  void Initialize(Reader* src, const Options& options);

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
  std::optional<Position> SizeImpl() override;
  std::unique_ptr<Reader> NewReaderImpl(Position initial_pos) override;

 private:
  // For `FailNotEnoughAtPos()`, `FailNotEnoughAtLength()`,
  // `FailNotEnoughAtEnd()`, and `FailPositionLimitExceeded()`.
  friend class ScopedLimiter;

  bool CheckEnough();
  ABSL_ATTRIBUTE_COLD bool FailNotEnough();
  ABSL_ATTRIBUTE_COLD void FailNotEnoughAtPos(Position expected_pos);
  ABSL_ATTRIBUTE_COLD void FailNotEnoughAtLength(Position expected_length);
  ABSL_ATTRIBUTE_COLD void FailNotEnoughAtEnd();
  ABSL_ATTRIBUTE_COLD void FailLengthOverflow(Position max_length);
  ABSL_ATTRIBUTE_COLD void FailPositionLimitExceeded();

  void restore_max_pos(Position max_pos);

  void MakeBufferSlow();

  // This template is defined and used only in limiting_reader.cc.
  template <typename Dest>
  bool ReadInternal(size_t length, Dest& dest);

  // Invariant: `pos() <= max_pos_`
  Position max_pos_ = std::numeric_limits<Position>::max();

  bool exact_ = false;
  bool fail_if_longer_ = false;

  // Invariants if `is_open()`:
  //   `start() >= SrcReader()->start()`
  //   `limit() <= SrcReader()->limit()`
  //   `start_pos() >= SrcReader()->start_pos()`
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
// `Any<Reader*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as `TargetT` of the
// type of the first constructor argument.
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

  LimitingReader(LimitingReader&& that) = default;
  LimitingReader& operator=(LimitingReader&& that) = default;

  // Makes `*this` equivalent to a newly constructed `LimitingReader`. This
  // avoids constructing a temporary `LimitingReader` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Src> src,
                                          Options options = Options());

  // Returns the object providing and possibly owning the original `Reader`.
  // Unchanged by `Close()`.
  Src& src() ABSL_ATTRIBUTE_LIFETIME_BOUND { return src_.manager(); }
  const Src& src() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return src_.manager();
  }
  Reader* SrcReader() const ABSL_ATTRIBUTE_LIFETIME_BOUND override {
    return src_.get();
  }

 protected:
  void Done() override;
  bool SyncImpl(SyncType sync_type) override;

 private:
  class Mover;

  // The object providing and possibly owning the original `Reader`.
  MovingDependency<Reader*, Src, Mover> src_;
};

explicit LimitingReader(Closed) -> LimitingReader<DeleteCtad<Closed>>;
template <typename Src>
explicit LimitingReader(Src&& src, LimitingReaderBase::Options options =
                                       LimitingReaderBase::Options())
    -> LimitingReader<TargetT<Src>>;

// `ReaderSpan` specifies a span of `Reader` contents from the current position
// with the given length. The type of the `Reader` is specified as a template
// parameter so that `LimitingReaderBase` can be treated specially.
//
// This can express the span as a single object, which is sometimes
// convenient.
//
// `ReaderSpan` supports `Dependency<Reader*, ReaderSpan<ReaderType>>`,
// which internally applies a `ScopedLimiterOrLimitingReader`. Some functions
// treat a parameter of type `ReaderSpan` specially to enable a more efficient
// implementation.
template <typename ReaderType = LimitingReaderBase>
class ReaderSpan {
 public:
  // An empty object. It can only be assigned to.
  ReaderSpan() = default;

  // Specifies the span from the current position of `*reader` with `length`.
  explicit ReaderSpan(ReaderType* reader, Position length)
      : reader_(reader), length_(length) {}

  template <typename OtherReaderType,
            std::enable_if_t<
                std::conjunction_v<
                    std::negation<std::is_same<OtherReaderType, ReaderType>>,
                    std::is_convertible<OtherReaderType*, ReaderType*>>,
                int> = 0>
  /*implicit*/ ReaderSpan(ReaderSpan<OtherReaderType> src)
      : ReaderSpan(&src.reader(), src.length()) {}

  ReaderSpan(const ReaderSpan& that) = default;
  ReaderSpan& operator=(const ReaderSpan& that) = default;

  ReaderType& reader() const { return *reader_; }
  Position length() const { return length_; }

 private:
  ReaderType* reader_ = nullptr;
  Position length_ = 0;
};

template <typename ReaderType>
explicit ReaderSpan(ReaderType* reader, Position length)
    -> ReaderSpan<ReaderType>;

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
  // `LimitingReader` but more efficient. A difference is when `options.exact()`
  // is `true` and the new limit exceeds the old limit. In this case the
  // `LimitingReader` fails immediately rather than when the old limit is
  // reached, because it is already known that it cannot eventually succeed.
  explicit ScopedLimiter(LimitingReaderBase* reader
                             ABSL_ATTRIBUTE_LIFETIME_BOUND,
                         Options options);

  explicit ScopedLimiter(ReaderSpan<> src)
      : ScopedLimiter(
            &src.reader(),
            LimitingReaderBase::Options().set_exact_length(src.length())) {}

  ScopedLimiter(const ScopedLimiter&) = delete;
  ScopedLimiter& operator=(const ScopedLimiter&) = delete;

  // Returns the underlying `LimitingReaderBase`.
  LimitingReaderBase& reader() const { return *reader_; }

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

// If `src` is a `LimitingReaderBase`, creates a `ScopedLimiter`, otherwise
// creates a new `LimitingReader`.
//
// The primary template applies if `Src` is a `Reader` other than
// `LimitingReaderBase`.
template <typename Src, typename Enable = void>
class ScopedLimiterOrLimitingReader {
 public:
  using Options = LimitingReaderBase::Options;

  explicit ScopedLimiterOrLimitingReader(Src* src ABSL_ATTRIBUTE_LIFETIME_BOUND,
                                         Options options)
      : reader_(src, options) {}

  explicit ScopedLimiterOrLimitingReader(ReaderSpan<Src> src)
      : reader_(&src.reader(),
                LimitingReaderBase::Options().set_exact_length(src.length())) {}

  ScopedLimiterOrLimitingReader(const ScopedLimiterOrLimitingReader&) = delete;
  ScopedLimiterOrLimitingReader& operator=(
      const ScopedLimiterOrLimitingReader&) = delete;

  // Returns the `LimitingReaderBase` from which data should be read.
  LimitingReaderBase& reader() { return reader_; }

  // Closes the `LimitingReaderBase`. If this fails, `reader().status()` can be
  // used.
  //
  // For consistency with the specialization if `Src` is a `LimitingReaderBase`,
  // the original `Reader` must not be changed between `Close()` and destroying
  // the `ScopedLimiterOrLimitingReader`.
  bool Close() { return reader_.Close(); }

 private:
  LimitingReader<> reader_;
};

// Specialization of `ScopedLimiterOrLimitingReader` if `Src` is a
// `LimitingReaderBase`.
//
// In this specialization the original `LimitingReaderBase` is accessed directly
// while its limits have been changed.
template <typename Src>
class ScopedLimiterOrLimitingReader<
    Src, std::enable_if_t<std::is_base_of_v<LimitingReaderBase, Src>>> {
 public:
  using Options = LimitingReaderBase::Options;

  ABSL_ATTRIBUTE_ALWAYS_INLINE
  explicit ScopedLimiterOrLimitingReader(Src* src ABSL_ATTRIBUTE_LIFETIME_BOUND,
                                         Options options)
      : limiter_(src, options) {}

  explicit ScopedLimiterOrLimitingReader(ReaderSpan<Src> src)
      : limiter_(&src.reader(),
                 LimitingReaderBase::Options().set_exact_length(src.length())) {
  }

  ScopedLimiterOrLimitingReader(const ScopedLimiterOrLimitingReader&) = delete;
  ScopedLimiterOrLimitingReader& operator=(
      const ScopedLimiterOrLimitingReader&) = delete;

  ABSL_ATTRIBUTE_ALWAYS_INLINE
  ~ScopedLimiterOrLimitingReader() = default;

  // Returns the `LimitingReaderBase` from which data should be read.
  LimitingReaderBase& reader() { return limiter_.reader(); }

  // Does nothing. The state of the original `LimitingReaderBase` is restored by
  // the destructor.instead.
  //
  // For consistency with the primary template, `Close()` should still be
  // called. The original `Reader` must not be changed between `Close()` and
  // destroying the `ScopedLimiterOrLimitingReader`.
  bool Close() { return reader().ok(); }

 private:
  ScopedLimiter limiter_;
};

template <typename Src>
explicit ScopedLimiterOrLimitingReader(Src* src,
                                       LimitingReaderBase::Options options)
    -> ScopedLimiterOrLimitingReader<Src>;

template <typename Src>
explicit ScopedLimiterOrLimitingReader(ReaderSpan<Src> src)
    -> ScopedLimiterOrLimitingReader<Src>;

// Specialization of `DependencyImpl<Reader*, ReaderSpan<ReaderType>>`.
template <typename ReaderType>
class DependencyImpl<Reader*, ReaderSpan<ReaderType>> {
 public:
  explicit DependencyImpl(ReaderSpan<ReaderType> span)
      : scoped_limiter_(
            &span.reader(),
            LimitingReaderBase::Options().set_exact_length(span.length())) {}

  DependencyImpl(const DependencyImpl&) = delete;
  DependencyImpl& operator=(const DependencyImpl&) = delete;

  ReaderSpan<ReaderType>& manager() ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return span_;
  }
  const ReaderSpan<ReaderType>& manager() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return span_;
  }

  LimitingReaderBase* get() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return &scoped_limiter_.reader();
  }

  bool IsOwning() const { return false; }

  static constexpr bool kIsStable = false;

 protected:
  DependencyImpl(DependencyImpl&& that) = default;
  DependencyImpl& operator=(DependencyImpl&& that) = default;

  ~DependencyImpl() = default;

 private:
  ReaderSpan<ReaderType> span_;
  mutable ScopedLimiterOrLimitingReader<ReaderType> scoped_limiter_;
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
  max_pos_ = std::numeric_limits<Position>::max();
  exact_ = exact;
  fail_if_longer_ = fail_if_longer;
}

inline void LimitingReaderBase::restore_max_pos(Position max_pos) {
  RIEGELI_ASSERT_GE(max_pos, max_pos_)
      << "Failed precondition of LimitingReaderBase::restore_max_pos(): "
         "the limit is being reduced";
  max_pos_ = max_pos;
}

inline Position LimitingReaderBase::max_length() const {
  RIEGELI_ASSERT_GE(max_pos_, pos())
      << "Failed invariant of LimitingReaderBase: "
         "position already exceeds its limit";
  return max_pos_ - pos();
}

inline void LimitingReaderBase::set_max_pos(Position max_pos) {
  max_pos_ = max_pos;
  if (ABSL_PREDICT_FALSE(limit_pos() > max_pos_)) MakeBufferSlow();
}

inline void LimitingReaderBase::set_max_length(Position max_length) {
  max_pos_ = pos() + max_length;  // Wrap-around is not an error.
  if (ABSL_PREDICT_FALSE(max_pos_ < max_length)) {
    max_pos_ = std::numeric_limits<Position>::max();
    if (exact_) FailLengthOverflow(max_length);
    return;
  }
  if (limit_pos() > max_pos_) {
    set_buffer(start(),
               start_to_limit() - IntCast<size_t>(limit_pos() - max_pos_),
               start_to_cursor());
    set_limit_pos(max_pos_);
  }
}

inline void LimitingReaderBase::SyncBuffer(Reader& src) {
  src.set_cursor(cursor());
}

inline void LimitingReaderBase::MakeBuffer(Reader& src) {
  set_buffer(src.start(), src.start_to_limit(), src.start_to_cursor());
  set_limit_pos(src.limit_pos());
  if (ABSL_PREDICT_FALSE(limit_pos() > max_pos_)) MakeBufferSlow();
  if (ABSL_PREDICT_FALSE(!src.ok())) FailWithoutAnnotation(src.status());
}

template <typename Src>
class LimitingReader<Src>::Mover {
 public:
  static auto member() { return &LimitingReader::src_; }

  explicit Mover(LimitingReader& self, LimitingReader& that)
      : uses_buffer_(self.start() != nullptr) {
    // Buffer pointers are already moved so `SyncBuffer()` is called on `self`.
    // `src_` is not moved yet so `src_` is taken from `that`.
    if (uses_buffer_) self.SyncBuffer(*that.src_);
  }

  void Done(LimitingReader& self) {
    if (uses_buffer_) self.MakeBuffer(*self.src_);
  }

 private:
  bool uses_buffer_;
};

template <typename Src>
inline LimitingReader<Src>::LimitingReader(Initializer<Src> src,
                                           Options options)
    : LimitingReaderBase(options.exact(), options.fail_if_longer()),
      src_(std::move(src)) {
  Initialize(src_.get(), options);
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
  Initialize(src_.get(), options);
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

ABSL_ATTRIBUTE_ALWAYS_INLINE
inline ScopedLimiter::ScopedLimiter(
    LimitingReaderBase* reader ABSL_ATTRIBUTE_LIFETIME_BOUND, Options options)
    : reader_(RIEGELI_EVAL_ASSERT_NOTNULL(reader)),
      old_max_pos_(reader_->max_pos()),
      old_exact_(reader_->exact()),
      fail_if_longer_(options.fail_if_longer()) {
  if (options.max_pos() != std::nullopt) {
    if (ABSL_PREDICT_FALSE(*options.max_pos() > reader_->max_pos())) {
      if (options.exact()) reader_->FailNotEnoughAtPos(*options.max_pos());
    } else {
      reader_->set_max_pos(*options.max_pos());
    }
  } else if (options.max_length() != std::nullopt) {
    if (ABSL_PREDICT_FALSE(*options.max_length() > reader_->max_length())) {
      if (options.exact())
        reader_->FailNotEnoughAtLength(*options.max_length());
    } else {
      reader_->set_max_length(*options.max_length());
    }
  } else if (ABSL_PREDICT_FALSE(reader_->max_pos() <
                                    std::numeric_limits<Position>::max() &&
                                options.exact())) {
    reader_->FailNotEnoughAtEnd();
  }
  reader_->set_exact(true);
}

ABSL_ATTRIBUTE_ALWAYS_INLINE
inline ScopedLimiter::~ScopedLimiter() {
  RIEGELI_ASSERT_GE(old_max_pos_, reader_->max_pos())
      << "Failed precondtion of ~ScopedLimiter: "
         "The underlying LimitingReader increased its limit "
         "while the ScopedLimiter was active";
  const Position inner_max_pos = reader_->max_pos();
  reader_->restore_max_pos(old_max_pos_);
  reader_->set_exact(old_exact_);
  if (fail_if_longer_ && reader_->pos() == inner_max_pos &&
      ABSL_PREDICT_FALSE(reader_->Pull())) {
    reader_->FailPositionLimitExceeded();
  }
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_LIMITING_READER_H_
