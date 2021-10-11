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
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

class ScopedLimiter;

// Template parameter independent part of `LimitingReader`.
class LimitingReaderBase : public Reader {
 public:
  class Options {
   public:
    Options() noexcept {}

    // The limit expressed as an absolute position. It must be at least as large
    // as the current position.
    //
    // `absl::nullopt` means no limit, unless `max_length()` is set.
    //
    // `max_pos()` and `max_length()` must not be both set.
    //
    // Default: `absl::nullopt`
    Options& set_max_pos(absl::optional<Position> max_pos) & {
      max_pos_ = max_pos;
      return *this;
    }
    Options&& set_max_pos(absl::optional<Position> max_pos) && {
      return std::move(set_max_pos(max_pos));
    }
    absl::optional<Position> max_pos() const { return max_pos_; }

    // A shortcut for `set_max_pos(pos)` with `set_exact(true)`.
    Options& set_exact_pos(Position pos) & {
      return set_max_pos(pos).set_exact(true);
    }
    Options&& set_exact_pos(Position pos) && {
      return std::move(set_exact_pos(pos));
    }

    // The limit expressed as a length relative to the current position.
    //
    // `absl::nullopt` means no limit, unless `max_pos()` is set.
    //
    // `max_pos()` and `max_length()` must not be both set.
    //
    // Default: `absl::nullopt`
    Options& set_max_length(absl::optional<Position> max_length) & {
      max_length_ = max_length;
      return *this;
    }
    Options&& set_max_length(absl::optional<Position> max_length) && {
      return std::move(set_max_length(max_length));
    }
    absl::optional<Position> max_length() const { return max_length_; }

    // A shortcut for `set_max_length(length)` with `set_exact(true)`.
    Options& set_exact_length(Position length) & {
      return set_max_length(length).set_exact(true);
    }
    Options&& set_exact_length(Position length) && {
      return std::move(set_exact_length(length));
    }

    // If `false`, `LimitingReader` will read data at most up to the limit.
    // Reading will end cleanly when either the limit is reached or the source
    // ends.
    //
    // If `true`, `LimitingReader` will read data exactly up to the limit.
    // Reading will end cleanly when the limit is reached, but will fail if the
    // source ends before the limit.
    //
    // Default: `false`
    Options& set_exact(bool exact) & {
      exact_ = exact;
      return *this;
    }
    Options&& set_exact(bool exact) && { return std::move(set_exact(exact)); }
    bool exact() const { return exact_; }

   private:
    absl::optional<Position> max_pos_;
    absl::optional<Position> max_length_;
    bool exact_ = false;
  };

  // Accesses the limit expressed as an absolute position.
  //
  // If `set_max_length()` was used, `max_pos()` returns the same limit
  // translated to an absolute position.
  //
  // Precondition of `set_max_pos()`: `max_pos >= pos()`
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

  // Returns the original `Reader`. Unchanged by `Close()`.
  virtual Reader* src_reader() = 0;
  virtual const Reader* src_reader() const = 0;

  bool SupportsRandomAccess() override;
  bool SupportsRewind() override;
  bool SupportsSize() override;

 protected:
  LimitingReaderBase() noexcept : Reader(kInitiallyClosed) {}

  explicit LimitingReaderBase(bool exact);

  LimitingReaderBase(LimitingReaderBase&& that) noexcept;
  LimitingReaderBase& operator=(LimitingReaderBase&& that) noexcept;

  void Reset();
  void Reset(bool exact);
  void Initialize(Reader* src, Options&& options);

  void Done() override;
  bool PullSlow(size_t min_length, size_t recommended_length) override;
  using Reader::ReadSlow;
  bool ReadSlow(size_t length, char* dest) override;
  bool ReadSlow(size_t length, Chain& dest) override;
  bool ReadSlow(size_t length, absl::Cord& dest) override;
  using Reader::CopySlow;
  bool CopySlow(Position length, Writer& dest) override;
  bool CopySlow(size_t length, BackwardWriter& dest) override;
  void ReadHintSlow(size_t length) override;
  bool SeekSlow(Position new_pos) override;
  absl::optional<Position> SizeImpl() override;

  // Sets cursor of `src` to cursor of `*this`.
  void SyncBuffer(Reader& src);

  // Sets buffer pointers of `*this` to buffer pointers of `src`, adjusting
  // them for `max_pos_`. Fails `*this` if `src` failed.
  void MakeBuffer(Reader& src);

  // Invariant: `pos() <= max_pos_`
  Position max_pos_ = std::numeric_limits<Position>::max();

  bool exact_ = false;

 private:
  // For `FailLengthOverflow()` and `FailNotEnoughEarly()`.
  friend class ScopedLimiter;

  bool CheckEnough();
  ABSL_ATTRIBUTE_COLD void FailLengthOverflow(Position max_length);
  ABSL_ATTRIBUTE_COLD void FailNotEnoughEarly(Position expected);

  // This template is defined and used only in limiting_reader.cc.
  template <typename Dest>
  bool ReadInternal(size_t length, Dest& dest);

  // Invariants if `is_open()`:
  //   `start() == src_reader()->start()`
  //   `limit() <= src_reader()->limit()`
  //   `start_pos() == src_reader()->start_pos()`
  //   `limit_pos() <= max_pos_`
};

// A `Reader` which reads from another `Reader` up to the specified limit, then
// pretends that the source ends.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the original `Reader`. `Src` must support
// `Dependency<Reader*, Src>`, e.g. `Reader*` (not owned, default),
// `std::unique_ptr<Reader>` (owned), `ChainReader<>` (owned).
//
// By relying on CTAD the template argument can be deduced as the value type of
// the first constructor argument. This requires C++17.
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
  LimitingReader() noexcept {}

  // Will read from the original `Reader` provided by `src`.
  explicit LimitingReader(const Src& src, Options options = Options());
  explicit LimitingReader(Src&& src, Options options = Options());

  // Will read from the original `Reader` provided by a `Src` constructed from
  // elements of `src_args`. This avoids constructing a temporary `Src` and
  // moving from it.
  template <typename... SrcArgs>
  explicit LimitingReader(std::tuple<SrcArgs...> src_args,
                          Options options = Options());

  LimitingReader(LimitingReader&& that) noexcept;
  LimitingReader& operator=(LimitingReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `LimitingReader`. This
  // avoids constructing a temporary `LimitingReader` and moving from it.
  void Reset();
  void Reset(const Src& src, Options options = Options());
  void Reset(Src&& src, Options options = Options());
  template <typename... SrcArgs>
  void Reset(std::tuple<SrcArgs...> src_args, Options options = Options());

  // Returns the object providing and possibly owning the original `Reader`.
  // Unchanged by `Close()`.
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  Reader* src_reader() override { return src_.get(); }
  const Reader* src_reader() const override { return src_.get(); }

  void VerifyEnd() override;

 protected:
  void Done() override;
  bool SyncImpl(SyncType sync_type) override;

 private:
  void MoveSrc(LimitingReader&& that);

  // The object providing and possibly owning the original `Reader`.
  Dependency<Reader*, Src> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
LimitingReader()->LimitingReader<DeleteCtad<>>;
template <typename Src>
explicit LimitingReader(const Src& src, LimitingReaderBase::Options options =
                                            LimitingReaderBase::Options())
    -> LimitingReader<std::decay_t<Src>>;
template <typename Src>
explicit LimitingReader(Src&& src, LimitingReaderBase::Options options =
                                       LimitingReaderBase::Options())
    -> LimitingReader<std::decay_t<Src>>;
template <typename... SrcArgs>
explicit LimitingReader(
    std::tuple<SrcArgs...> src_args,
    LimitingReaderBase::Options options = LimitingReaderBase::Options())
    -> LimitingReader<DeleteCtad<std::tuple<SrcArgs...>>>;
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
};

// Implementation details follow.

inline LimitingReaderBase::LimitingReaderBase(bool exact)
    : Reader(kInitiallyOpen), exact_(exact) {}

inline LimitingReaderBase::LimitingReaderBase(
    LimitingReaderBase&& that) noexcept
    : Reader(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      max_pos_(that.max_pos_),
      exact_(that.exact_) {}

inline LimitingReaderBase& LimitingReaderBase::operator=(
    LimitingReaderBase&& that) noexcept {
  Reader::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  max_pos_ = that.max_pos_;
  exact_ = that.exact_;
  return *this;
}

inline void LimitingReaderBase::Reset() {
  Reader::Reset(kInitiallyClosed);
  max_pos_ = std::numeric_limits<Position>::max();
  exact_ = false;
}

inline void LimitingReaderBase::Reset(bool exact) {
  Reader::Reset(kInitiallyOpen);
  // `max_pos_` will be set by `Initialize()`.
  exact_ = exact;
}

inline void LimitingReaderBase::Initialize(Reader* src, Options&& options) {
  RIEGELI_ASSERT(src != nullptr)
      << "Failed precondition of LimitingReader: null Reader pointer";
  RIEGELI_ASSERT(options.max_pos() == absl::nullopt ||
                 options.max_length() == absl::nullopt)
      << "Failed precondition of LimitingReader: "
         "Options::max_pos() and Options::max_length() are both set";
  if (options.max_pos() != absl::nullopt) {
    RIEGELI_ASSERT_GE(*options.max_pos(), src->pos())
        << "Failed precondition of LimitingReader: "
           "position already exceeds its limit";
    max_pos_ = *options.max_pos();
  } else if (options.max_length() != absl::nullopt) {
    if (ABSL_PREDICT_FALSE(*options.max_length() >
                           std::numeric_limits<Position>::max() - src->pos())) {
      max_pos_ = std::numeric_limits<Position>::max();
      if (exact_) FailLengthOverflow(*options.max_length());
    } else {
      max_pos_ = src->pos() + *options.max_length();
    }
  } else {
    max_pos_ = std::numeric_limits<Position>::max();
  }
  MakeBuffer(*src);
}

inline void LimitingReaderBase::set_max_pos(Position max_pos) {
  RIEGELI_ASSERT_GE(max_pos, pos())
      << "Failed precondition of LimitingReaderBase::set_max_pos(): "
         "position already exceeds its limit";
  max_pos_ = max_pos;
  if (limit_pos() > max_pos_) {
    set_buffer(start(), buffer_size() - IntCast<size_t>(limit_pos() - max_pos_),
               read_from_buffer());
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
  set_max_pos(pos() + max_length);
}

inline Position LimitingReaderBase::max_length() const {
  RIEGELI_ASSERT_GE(max_pos_, pos())
      << "Failed invariant of LimitingReaderBase: "
         "position already exceeds its limit";
  return max_pos_ - pos();
}

inline void LimitingReaderBase::SyncBuffer(Reader& src) {
  src.set_cursor(cursor());
}

inline void LimitingReaderBase::MakeBuffer(Reader& src) {
  set_buffer(src.start(), src.buffer_size(), src.read_from_buffer());
  set_limit_pos(src.pos() + src.available());
  if (limit_pos() > max_pos_) {
    set_buffer(start(), buffer_size() - IntCast<size_t>(limit_pos() - max_pos_),
               read_from_buffer());
    set_limit_pos(max_pos_);
  }
  if (ABSL_PREDICT_FALSE(!src.healthy())) FailWithoutAnnotation(src);
}

template <typename Src>
inline LimitingReader<Src>::LimitingReader(const Src& src, Options options)
    : LimitingReaderBase(options.exact()), src_(src) {
  Initialize(src_.get(), std::move(options));
}

template <typename Src>
inline LimitingReader<Src>::LimitingReader(Src&& src, Options options)
    : LimitingReaderBase(options.exact()), src_(std::move(src)) {
  Initialize(src_.get(), std::move(options));
}

template <typename Src>
template <typename... SrcArgs>
inline LimitingReader<Src>::LimitingReader(std::tuple<SrcArgs...> src_args,
                                           Options options)
    : LimitingReaderBase(options.exact()), src_(std::move(src_args)) {
  Initialize(src_.get(), std::move(options));
}

template <typename Src>
inline LimitingReader<Src>::LimitingReader(LimitingReader&& that) noexcept
    : LimitingReaderBase(std::move(that)) {
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  MoveSrc(std::move(that));
}

template <typename Src>
inline LimitingReader<Src>& LimitingReader<Src>::operator=(
    LimitingReader&& that) noexcept {
  LimitingReaderBase::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  MoveSrc(std::move(that));
  return *this;
}

template <typename Src>
inline void LimitingReader<Src>::Reset() {
  LimitingReaderBase::Reset();
  src_.Reset();
}

template <typename Src>
inline void LimitingReader<Src>::Reset(const Src& src, Options options) {
  LimitingReaderBase::Reset(options.exact());
  src_.Reset(src);
  Initialize(src_.get(), std::move(options));
}

template <typename Src>
inline void LimitingReader<Src>::Reset(Src&& src, Options options) {
  LimitingReaderBase::Reset(options.exact());
  src_.Reset(std::move(src));
  Initialize(src_.get(), std::move(options));
}

template <typename Src>
template <typename... SrcArgs>
inline void LimitingReader<Src>::Reset(std::tuple<SrcArgs...> src_args,
                                       Options options) {
  LimitingReaderBase::Reset(options.exact());
  src_.Reset(std::move(src_args));
  Initialize(src_.get(), std::move(options));
}

template <typename Src>
inline void LimitingReader<Src>::MoveSrc(LimitingReader&& that) {
  if (src_.kIsStable()) {
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
  if (src_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) FailWithoutAnnotation(*src_);
  }
}

template <typename Src>
void LimitingReader<Src>::VerifyEnd() {
  LimitingReaderBase::VerifyEnd();
  if (src_.is_owning() && ABSL_PREDICT_TRUE(healthy())) {
    SyncBuffer(*src_);
    src_->VerifyEnd();
    MakeBuffer(*src_);
  }
}

template <typename Src>
bool LimitingReader<Src>::SyncImpl(SyncType sync_type) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  SyncBuffer(*src_);
  bool ok = true;
  if (sync_type != SyncType::kFromObject || src_.is_owning()) {
    ok = src_->Sync(sync_type);
  }
  MakeBuffer(*src_);
  return ok;
}

inline ScopedLimiter::ScopedLimiter(LimitingReaderBase* reader, Options options)
    : reader_(RIEGELI_ASSERT_NOTNULL(reader)),
      old_max_pos_(reader_->max_pos()),
      old_exact_(reader_->exact()) {
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
  reader_->set_exact(options.exact() || reader_->exact());
}

inline ScopedLimiter::~ScopedLimiter() {
  RIEGELI_ASSERT_GE(old_max_pos_, reader_->max_pos())
      << "Failed precondtion of ~ScopedLimiter: "
         "The underlying LimitingReader increased its limit "
         "while the ScopedLimiter was active";
  reader_->set_max_pos(old_max_pos_);
  reader_->set_exact(old_exact_);
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_LIMITING_READER_H_
