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
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/backward_writer.h"

namespace riegeli {

// Template parameter independent part of `LimitingBackwardWriter`.
class LimitingBackwardWriterBase : public BackwardWriter {
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
    Options& set_exact_length(Position length) & {
      return set_max_length(length).set_exact(true);
    }
    Options&& set_exact_length(Position length) && {
      return std::move(set_exact_length(length));
    }

    // If `false`, `LimitingBackwardWriter` will write data at most up to the
    // limit. Writing will fail if the limit is exceeded.
    //
    // If `true`, `LimitingBackwardWriter` will write data exactly up to the
    // limit. Writing will fail if the limit is exceeded, and `Close()` will
    // fail if the current position at that time is before the limit.
    //
    // Default: `false`.
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

  // Returns the original `BackwardWriter`. Unchanged by `Close()`.
  virtual BackwardWriter* dest_writer() = 0;
  virtual const BackwardWriter* dest_writer() const = 0;

  // Accesses the limit expressed as an absolute position.
  //
  // If `set_max_length()` was used, `max_pos()` returns the same limit
  // translated to an absolute position.
  //
  // Precondition of `set_max_pos()`: `max_pos >= pos()`
  //
  // If no limit is set, returns `std::numeric_limits<Position>::max()`.
  void set_max_pos(Position max_pos);
  Position max_pos() const { return max_pos_; }

  // Accesses the limit expressed as a length relative to the current position,
  // i.e. the length remaining to the limit.
  //
  // If `set_max_pos()` was used, `max_length()` returns the same limit
  // translated to a length relative to the current position.
  //
  // If no limit is set, returns `std::numeric_limits<Position>::max() - pos()`.
  void set_max_length(Position max_length);
  Position max_length() const { return SaturatingSub(max_pos_, pos()); }

  // Clears the limit.
  void clear_limit() { max_pos_ = std::numeric_limits<Position>::max(); }

  bool PrefersCopying() const override;
  bool SupportsTruncate() override;

 protected:
  explicit LimitingBackwardWriterBase(Closed) noexcept
      : BackwardWriter(kClosed) {}

  explicit LimitingBackwardWriterBase(bool exact);

  LimitingBackwardWriterBase(LimitingBackwardWriterBase&& that) noexcept;
  LimitingBackwardWriterBase& operator=(
      LimitingBackwardWriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset(bool exact);
  void Initialize(BackwardWriter* dest, Options&& options);
  bool exact() const { return exact_; }

  void Done() override;
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;
  bool PushSlow(size_t min_length, size_t recommended_length) override;
  using BackwardWriter::WriteSlow;
  bool WriteSlow(absl::string_view src) override;
  bool WriteSlow(const Chain& src) override;
  bool WriteSlow(Chain&& src) override;
  bool WriteSlow(const absl::Cord& src) override;
  bool WriteSlow(absl::Cord&& src) override;
  bool WriteZerosSlow(Position length) override;
  bool TruncateImpl(Position new_size) override;

  // Sets cursor of `dest` to cursor of `*this`. Fails `*this` if the limit is
  // exceeded.
  bool SyncBuffer(BackwardWriter& dest);

  // Sets buffer pointers of `*this` to buffer pointers of `dest`. Fails `*this`
  // if `dest` failed.
  void MakeBuffer(BackwardWriter& dest);

 private:
  ABSL_ATTRIBUTE_COLD bool FailLimitExceeded(BackwardWriter& dest);
  ABSL_ATTRIBUTE_COLD void FailLengthOverflow(Position max_length);

  // This template is defined and used only in limiting_backward_writer.cc.
  template <typename Src, typename RemovePrefix>
  bool WriteInternal(Src&& src, RemovePrefix&& remove_prefix);

  // Invariant: `start_pos() <= max_pos_`
  Position max_pos_ = std::numeric_limits<Position>::max();

  bool exact_ = false;

  // Invariants if `ok()`:
  //   `start() == dest_writer()->start()`
  //   `limit() == dest_writer()->limit()`
  //   `start_pos() == dest_writer()->start_pos()`
};

// A `BackwardWriter` which writes to another `BackwardWriter` up to the
// specified size limit. An attempt to write more fails, after writing to the
// destination everything up to the limit.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the original `BackwardWriter`. `Dest` must support
// `Dependency<BackwardWriter*, Dest>`, e.g.
// `BackwardWriter*` (not owned, default),
// `std::unique_ptr<BackwardWriter>` (owned), `ChainBackwardWriter<>` (owned).
//
// By relying on CTAD the template argument can be deduced as the value type of
// the first constructor argument. This requires C++17.
//
// The original `BackwardWriter` must not be accessed until the
// `LimitingBackwardWriter` is closed or no longer used, except that it is
// allowed to read the destination of the original `BackwardWriter` immediately
// after `Flush()`.
template <typename Dest = BackwardWriter*>
class LimitingBackwardWriter : public LimitingBackwardWriterBase {
 public:
  // Creates a closed `LimitingBackwardWriter`.
  explicit LimitingBackwardWriter(Closed) noexcept
      : LimitingBackwardWriterBase(kClosed) {}

  // Will write to the original `BackwardWriter` provided by `dest`.
  explicit LimitingBackwardWriter(const Dest& dest,
                                  Options options = Options());
  explicit LimitingBackwardWriter(Dest&& dest, Options options = Options());

  // Will write to the original `BackwardWriter` provided by a `Dest`
  // constructed from elements of `dest_args`. This avoids constructing a
  // temporary `Dest` and moving from it.
  template <typename... DestArgs>
  explicit LimitingBackwardWriter(std::tuple<DestArgs...> dest_args,
                                  Options options = Options());

  LimitingBackwardWriter(LimitingBackwardWriter&& that) noexcept;
  LimitingBackwardWriter& operator=(LimitingBackwardWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `LimitingBackwardWriter`.
  // This avoids constructing a temporary `LimitingBackwardWriter` and moving
  // from it.
  void Reset(Closed);
  void Reset(const Dest& dest, Options options = Options());
  void Reset(Dest&& dest, Options options = Options());
  template <typename... DestArgs>
  void Reset(std::tuple<DestArgs...> dest_args, Options options = Options());

  // Returns the object providing and possibly owning the original
  // `BackwardWriter`. Unchanged by `Close()`.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  BackwardWriter* dest_writer() override { return dest_.get(); }
  const BackwardWriter* dest_writer() const override { return dest_.get(); }

  void SetWriteSizeHint(absl::optional<Position> write_size_hint) override;

 protected:
  void Done() override;
  bool FlushImpl(FlushType flush_type) override;

 private:
  void MoveDest(LimitingBackwardWriter&& that);

  // The object providing and possibly owning the original `BackwardWriter`.
  Dependency<BackwardWriter*, Dest> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit LimitingBackwardWriter(Closed)
    ->LimitingBackwardWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit LimitingBackwardWriter(const Dest& dest,
                                LimitingBackwardWriterBase::Options options =
                                    LimitingBackwardWriterBase::Options())
    -> LimitingBackwardWriter<std::decay_t<Dest>>;
template <typename Dest>
explicit LimitingBackwardWriter(Dest&& dest,
                                LimitingBackwardWriterBase::Options options =
                                    LimitingBackwardWriterBase::Options())
    -> LimitingBackwardWriter<std::decay_t<Dest>>;
template <typename... DestArgs>
explicit LimitingBackwardWriter(std::tuple<DestArgs...> dest_args,
                                LimitingBackwardWriterBase::Options options =
                                    LimitingBackwardWriterBase::Options())
    -> LimitingBackwardWriter<DeleteCtad<std::tuple<DestArgs...>>>;
#endif

// Implementation details follow.

inline LimitingBackwardWriterBase::LimitingBackwardWriterBase(bool exact)
    : exact_(exact) {}

inline LimitingBackwardWriterBase::LimitingBackwardWriterBase(
    LimitingBackwardWriterBase&& that) noexcept
    : BackwardWriter(static_cast<BackwardWriter&&>(that)),
      max_pos_(that.max_pos_),
      exact_(that.exact_) {}

inline LimitingBackwardWriterBase& LimitingBackwardWriterBase::operator=(
    LimitingBackwardWriterBase&& that) noexcept {
  BackwardWriter::operator=(static_cast<BackwardWriter&&>(that));
  max_pos_ = that.max_pos_;
  exact_ = that.exact_;
  return *this;
}

inline void LimitingBackwardWriterBase::Reset(Closed) {
  BackwardWriter::Reset(kClosed);
  max_pos_ = std::numeric_limits<Position>::max();
  exact_ = false;
}

inline void LimitingBackwardWriterBase::Reset(bool exact) {
  BackwardWriter::Reset();
  // `max_pos_` will be set by `Initialize()`.
  exact_ = exact;
}

inline void LimitingBackwardWriterBase::Initialize(BackwardWriter* dest,
                                                   Options&& options) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of LimitingBackwardWriter: "
         "null BackwardWriter pointer";
  RIEGELI_ASSERT(options.max_pos() == absl::nullopt ||
                 options.max_length() == absl::nullopt)
      << "Failed precondition of LimitingBackwardWriter: "
         "Options::max_pos() and Options::max_length() are both set";
  if (options.max_pos() != absl::nullopt) {
    RIEGELI_ASSERT_GE(*options.max_pos(), dest->pos())
        << "Failed precondition of LimitingBackwardWriter: "
           "position already exceeds its limit";
    max_pos_ = *options.max_pos();
  } else if (options.max_length() != absl::nullopt) {
    if (ABSL_PREDICT_FALSE(*options.max_length() >
                           std::numeric_limits<Position>::max() -
                               dest->pos())) {
      max_pos_ = std::numeric_limits<Position>::max();
      if (exact_) FailLengthOverflow(*options.max_length());
    } else {
      max_pos_ = dest->pos() + *options.max_length();
    }
  } else {
    max_pos_ = std::numeric_limits<Position>::max();
  }
  MakeBuffer(*dest);
}

inline void LimitingBackwardWriterBase::set_max_pos(Position max_pos) {
  RIEGELI_ASSERT_GE(max_pos, pos())
      << "Failed precondition of LimitingBackwardWriterBase::set_max_pos(): "
         "position already exceeds its limit";
  max_pos_ = max_pos;
}

inline void LimitingBackwardWriterBase::set_max_length(Position max_length) {
  if (ABSL_PREDICT_FALSE(max_length >
                         std::numeric_limits<Position>::max() - pos())) {
    max_pos_ = std::numeric_limits<Position>::max();
    if (exact_) FailLengthOverflow(max_length);
    return;
  }
  max_pos_ = pos() + max_length;
}

inline bool LimitingBackwardWriterBase::SyncBuffer(BackwardWriter& dest) {
  if (ABSL_PREDICT_FALSE(pos() > max_pos_)) {
    dest.set_cursor(cursor() + IntCast<size_t>(pos() - max_pos_));
    return FailLimitExceeded(dest);
  }
  dest.set_cursor(cursor());
  return true;
}

inline void LimitingBackwardWriterBase::MakeBuffer(BackwardWriter& dest) {
  set_buffer(dest.limit(), dest.start_to_limit(), dest.start_to_cursor());
  set_start_pos(dest.start_pos());
  if (ABSL_PREDICT_FALSE(!dest.ok())) FailWithoutAnnotation(dest.status());
}

template <typename Dest>
inline LimitingBackwardWriter<Dest>::LimitingBackwardWriter(const Dest& dest,
                                                            Options options)
    : LimitingBackwardWriterBase(options.exact()), dest_(dest) {
  Initialize(dest_.get(), std::move(options));
  if (dest_.is_owning() && exact()) dest_->SetWriteSizeHint(max_length());
}

template <typename Dest>
inline LimitingBackwardWriter<Dest>::LimitingBackwardWriter(Dest&& dest,
                                                            Options options)
    : LimitingBackwardWriterBase(options.exact()), dest_(std::move(dest)) {
  Initialize(dest_.get(), std::move(options));
  if (dest_.is_owning() && exact()) dest_->SetWriteSizeHint(max_length());
}

template <typename Dest>
template <typename... DestArgs>
inline LimitingBackwardWriter<Dest>::LimitingBackwardWriter(
    std::tuple<DestArgs...> dest_args, Options options)
    : LimitingBackwardWriterBase(options.exact()), dest_(std::move(dest_args)) {
  Initialize(dest_.get(), std::move(options));
  if (dest_.is_owning() && exact()) dest_->SetWriteSizeHint(max_length());
}

template <typename Dest>
inline LimitingBackwardWriter<Dest>::LimitingBackwardWriter(
    LimitingBackwardWriter&& that) noexcept
    : LimitingBackwardWriterBase(
          static_cast<LimitingBackwardWriterBase&&>(that)) {
  MoveDest(std::move(that));
}

template <typename Dest>
inline LimitingBackwardWriter<Dest>& LimitingBackwardWriter<Dest>::operator=(
    LimitingBackwardWriter&& that) noexcept {
  LimitingBackwardWriterBase::operator=(
      static_cast<LimitingBackwardWriterBase&&>(that));
  MoveDest(std::move(that));
  return *this;
}

template <typename Dest>
inline void LimitingBackwardWriter<Dest>::Reset(Closed) {
  LimitingBackwardWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void LimitingBackwardWriter<Dest>::Reset(const Dest& dest,
                                                Options options) {
  LimitingBackwardWriterBase::Reset(options.exact());
  dest_.Reset(dest);
  Initialize(dest_.get(), std::move(options));
  if (dest_.is_owning() && exact()) dest_->SetWriteSizeHint(max_length());
}

template <typename Dest>
inline void LimitingBackwardWriter<Dest>::Reset(Dest&& dest, Options options) {
  LimitingBackwardWriterBase::Reset(options.exact());
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), std::move(options));
  if (dest_.is_owning() && exact()) dest_->SetWriteSizeHint(max_length());
}

template <typename Dest>
template <typename... DestArgs>
inline void LimitingBackwardWriter<Dest>::Reset(
    std::tuple<DestArgs...> dest_args, Options options) {
  LimitingBackwardWriterBase::Reset(options.exact());
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get(), std::move(options));
  if (dest_.is_owning() && exact()) dest_->SetWriteSizeHint(max_length());
}

template <typename Dest>
inline void LimitingBackwardWriter<Dest>::MoveDest(
    LimitingBackwardWriter&& that) {
  if (dest_.kIsStable || that.dest_ == nullptr) {
    dest_ = std::move(that.dest_);
  } else {
    // Buffer pointers are already moved so `SyncBuffer()` is called on `*this`,
    // `dest_` is not moved yet so `dest_` is taken from `that`.
    const bool sync_buffer_ok = SyncBuffer(*that.dest_);
    dest_ = std::move(that.dest_);
    if (ABSL_PREDICT_TRUE(sync_buffer_ok)) MakeBuffer(*dest_);
  }
}

template <typename Dest>
void LimitingBackwardWriter<Dest>::Done() {
  LimitingBackwardWriterBase::Done();
  if (dest_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) {
      FailWithoutAnnotation(dest_->status());
    }
  }
}

template <typename Dest>
void LimitingBackwardWriter<Dest>::SetWriteSizeHint(
    absl::optional<Position> write_size_hint) {
  if (dest_.is_owning() && !exact()) {
    dest_->SetWriteSizeHint(
        write_size_hint == absl::nullopt
            ? absl::nullopt
            : absl::make_optional(UnsignedMin(*write_size_hint, max_length())));
  }
}

template <typename Dest>
bool LimitingBackwardWriter<Dest>::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (ABSL_PREDICT_FALSE(!SyncBuffer(*dest_))) return false;
  bool flush_ok = true;
  if (flush_type != FlushType::kFromObject || dest_.is_owning()) {
    flush_ok = dest_->Flush(flush_type);
  }
  MakeBuffer(*dest_);
  return flush_ok;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_LIMITING_BACKWARD_WRITER_H_
