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
#include "riegeli/bytes/writer.h"

namespace riegeli {

class Reader;

// Template parameter independent part of `LimitingWriter`.
class LimitingWriterBase : public Writer {
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

    // If `false`, `LimitingWriter` will write data at most up to the limit.
    // Writing will fail if the limit is exceeded.
    //
    // If `true`, `LimitingWriter` will write data exactly up to the limit.
    // Writing will fail if the limit is exceeded, and `Close()` will fail if
    // the current position at that time is before the limit.
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

  // Returns the original `Writer`. Unchanged by `Close()`.
  virtual Writer* dest_writer() = 0;
  virtual const Writer* dest_writer() const = 0;

  // Accesses the limit expressed as an absolute position.
  //
  // If `set_max_length()` was used, `max_pos()` returns the same limit
  // translated to an absolute position.
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
  bool SupportsRandomAccess() override;
  bool SupportsSize() override;
  bool SupportsTruncate() override;
  bool SupportsReadMode() override;

 protected:
  explicit LimitingWriterBase(Closed) noexcept : Writer(kClosed) {}

  explicit LimitingWriterBase(bool exact);

  LimitingWriterBase(LimitingWriterBase&& that) noexcept;
  LimitingWriterBase& operator=(LimitingWriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset(bool exact);
  void Initialize(Writer* dest, Options&& options);
  bool exact() const { return exact_; }

  // Sets cursor of `dest` to cursor of `*this`. Fails `*this` if the limit is
  // exceeded.
  bool SyncBuffer(Writer& dest);

  // Sets buffer pointers of `*this` to buffer pointers of `dest`. Fails `*this`
  // if `dest` failed.
  void MakeBuffer(Writer& dest);

  void Done() override;
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;
  bool PushSlow(size_t min_length, size_t recommended_length) override;
  using Writer::WriteSlow;
  bool WriteSlow(absl::string_view src) override;
  bool WriteSlow(const Chain& src) override;
  bool WriteSlow(Chain&& src) override;
  bool WriteSlow(const absl::Cord& src) override;
  bool WriteSlow(absl::Cord&& src) override;
  bool WriteZerosSlow(Position length) override;
  bool SeekSlow(Position new_pos) override;
  absl::optional<Position> SizeImpl() override;
  bool TruncateImpl(Position new_size) override;
  Reader* ReadModeImpl(Position initial_pos) override;

 private:
  ABSL_ATTRIBUTE_COLD bool FailLimitExceeded();
  ABSL_ATTRIBUTE_COLD bool FailLimitExceeded(Writer& dest);
  ABSL_ATTRIBUTE_COLD void FailLengthOverflow(Position max_length);

  // This template is defined and used only in limiting_writer.cc.
  template <typename Src, typename RemoveSuffix>
  bool WriteInternal(Src&& src, RemoveSuffix&& remove_suffix);

  // Invariant: `start_pos() <= max_pos_`
  Position max_pos_ = std::numeric_limits<Position>::max();

  bool exact_ = false;

  // Invariants if `ok()`:
  //   `start() == dest_writer()->start()`
  //   `limit() == dest_writer()->limit()`
  //   `start_pos() == dest_writer()->start_pos()`
};

// A `Writer` which writes to another `Writer` up to the specified size limit.
// An attempt to write more fails, after writing to the destination everything
// up to the limit.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the original `Writer`. `Dest` must support
// `Dependency<Writer*, Dest>`, e.g. `Writer*` (not owned, default),
// `std::unique_ptr<Writer>` (owned), `ChainWriter<>` (owned).
//
// By relying on CTAD the template argument can be deduced as the value type of
// the first constructor argument. This requires C++17.
//
// The original `Writer` must not be accessed until the `LimitingWriter` is
// closed or no longer used, except that it is allowed to read the destination
// of the original `Writer` immediately after `Flush()`.
template <typename Dest = Writer*>
class LimitingWriter : public LimitingWriterBase {
 public:
  // Creates a closed `LimitingWriter`.
  explicit LimitingWriter(Closed) noexcept : LimitingWriterBase(kClosed) {}

  // Will write to the original `Writer` provided by `dest`.
  explicit LimitingWriter(const Dest& dest, Options options = Options());
  explicit LimitingWriter(Dest&& dest, Options options = Options());

  // Will write to the original `Writer` provided by a `Dest` constructed from
  // elements of `dest_args`. This avoids constructing a temporary `Dest` and
  // moving from it.
  template <typename... DestArgs>
  explicit LimitingWriter(std::tuple<DestArgs...> dest_args,
                          Options options = Options());

  LimitingWriter(LimitingWriter&& that) noexcept;
  LimitingWriter& operator=(LimitingWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `LimitingWriter`. This
  // avoids constructing a temporary `LimitingWriter` and moving from it.
  void Reset(Closed);
  void Reset(const Dest& dest, Options options = Options());
  void Reset(Dest&& dest, Options options = Options());
  template <typename... DestArgs>
  void Reset(std::tuple<DestArgs...> dest_args, Options options = Options());

  // Returns the object providing and possibly owning the original `Writer`.
  // Unchanged by `Close()`.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  Writer* dest_writer() override { return dest_.get(); }
  const Writer* dest_writer() const override { return dest_.get(); }

 protected:
  void Done() override;
  void SetWriteSizeHintImpl(absl::optional<Position> write_size_hint) override;
  bool FlushImpl(FlushType flush_type) override;

 private:
  void MoveDest(LimitingWriter&& that);

  // The object providing and possibly owning the original `Writer`.
  Dependency<Writer*, Dest> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit LimitingWriter(Closed)->LimitingWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit LimitingWriter(const Dest& dest, LimitingWriterBase::Options options =
                                              LimitingWriterBase::Options())
    -> LimitingWriter<std::decay_t<Dest>>;
template <typename Dest>
explicit LimitingWriter(Dest&& dest, LimitingWriterBase::Options options =
                                         LimitingWriterBase::Options())
    -> LimitingWriter<std::decay_t<Dest>>;
template <typename... DestArgs>
explicit LimitingWriter(
    std::tuple<DestArgs...> dest_args,
    LimitingWriterBase::Options options = LimitingWriterBase::Options())
    -> LimitingWriter<DeleteCtad<std::tuple<DestArgs...>>>;
template <typename Dest>
explicit LimitingWriter(const Dest& dest, Position max_pos)
    -> LimitingWriter<std::decay_t<Dest>>;
template <typename Dest>
explicit LimitingWriter(Dest&& dest, Position max_pos)
    -> LimitingWriter<std::decay_t<Dest>>;
template <typename... DestArgs>
explicit LimitingWriter(std::tuple<DestArgs...> dest_args, Position max_pos)
    -> LimitingWriter<DeleteCtad<std::tuple<DestArgs...>>>;
#endif

// Implementation details follow.

inline LimitingWriterBase::LimitingWriterBase(bool exact) : exact_(exact) {}

inline LimitingWriterBase::LimitingWriterBase(
    LimitingWriterBase&& that) noexcept
    : Writer(static_cast<Writer&&>(that)),
      max_pos_(that.max_pos_),
      exact_(that.exact_) {}

inline LimitingWriterBase& LimitingWriterBase::operator=(
    LimitingWriterBase&& that) noexcept {
  Writer::operator=(static_cast<Writer&&>(that));
  max_pos_ = that.max_pos_;
  exact_ = that.exact_;
  return *this;
}

inline void LimitingWriterBase::Reset(Closed) {
  Writer::Reset(kClosed);
  max_pos_ = std::numeric_limits<Position>::max();
  exact_ = false;
}

inline void LimitingWriterBase::Reset(bool exact) {
  Writer::Reset();
  // `max_pos_` will be set by `Initialize()`.
  exact_ = exact;
}

inline void LimitingWriterBase::Initialize(Writer* dest, Options&& options) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of LimitingWriter: null Writer pointer";
  RIEGELI_ASSERT(options.max_pos() == absl::nullopt ||
                 options.max_length() == absl::nullopt)
      << "Failed precondition of LimitingWriter: "
         "Options::max_pos() and Options::max_length() are both set";
  MakeBuffer(*dest);
  if (options.max_pos() != absl::nullopt) {
    set_max_pos(*options.max_pos());
  } else if (options.max_length() != absl::nullopt) {
    set_max_length(*options.max_length());
  } else {
    clear_limit();
  }
}

inline void LimitingWriterBase::set_max_pos(Position max_pos) {
  max_pos_ = max_pos;
  if (ABSL_PREDICT_FALSE(pos() > max_pos_)) FailLimitExceeded();
}

inline void LimitingWriterBase::set_max_length(Position max_length) {
  if (ABSL_PREDICT_FALSE(max_length >
                         std::numeric_limits<Position>::max() - pos())) {
    max_pos_ = std::numeric_limits<Position>::max();
    if (exact_) FailLengthOverflow(max_length);
    return;
  }
  max_pos_ = pos() + max_length;
}

inline bool LimitingWriterBase::SyncBuffer(Writer& dest) {
  if (ABSL_PREDICT_FALSE(pos() > max_pos_)) {
    dest.set_cursor(cursor() - IntCast<size_t>(pos() - max_pos_));
    return FailLimitExceeded(dest);
  }
  dest.set_cursor(cursor());
  return true;
}

inline void LimitingWriterBase::MakeBuffer(Writer& dest) {
  set_buffer(dest.start(), dest.start_to_limit(), dest.start_to_cursor());
  set_start_pos(dest.start_pos());
  if (ABSL_PREDICT_FALSE(!dest.ok())) FailWithoutAnnotation(dest.status());
}

template <typename Dest>
inline LimitingWriter<Dest>::LimitingWriter(const Dest& dest, Options options)
    : LimitingWriterBase(options.exact()), dest_(dest) {
  Initialize(dest_.get(), std::move(options));
  if (dest_.is_owning() && exact()) dest_->SetWriteSizeHint(max_length());
}

template <typename Dest>
inline LimitingWriter<Dest>::LimitingWriter(Dest&& dest, Options options)
    : LimitingWriterBase(options.exact()), dest_(std::move(dest)) {
  Initialize(dest_.get(), std::move(options));
  if (dest_.is_owning() && exact()) dest_->SetWriteSizeHint(max_length());
}

template <typename Dest>
template <typename... DestArgs>
inline LimitingWriter<Dest>::LimitingWriter(std::tuple<DestArgs...> dest_args,
                                            Options options)
    : LimitingWriterBase(options.exact()), dest_(std::move(dest_args)) {
  Initialize(dest_.get(), std::move(options));
  if (dest_.is_owning() && exact()) dest_->SetWriteSizeHint(max_length());
}

template <typename Dest>
inline LimitingWriter<Dest>::LimitingWriter(LimitingWriter&& that) noexcept
    : LimitingWriterBase(static_cast<LimitingWriterBase&&>(that)) {
  MoveDest(std::move(that));
}

template <typename Dest>
inline LimitingWriter<Dest>& LimitingWriter<Dest>::operator=(
    LimitingWriter&& that) noexcept {
  LimitingWriterBase::operator=(static_cast<LimitingWriterBase&&>(that));
  MoveDest(std::move(that));
  return *this;
}

template <typename Dest>
inline void LimitingWriter<Dest>::Reset(Closed) {
  LimitingWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void LimitingWriter<Dest>::Reset(const Dest& dest, Options options) {
  LimitingWriterBase::Reset(options.exact());
  dest_.Reset(dest);
  Initialize(dest_.get(), std::move(options));
  if (dest_.is_owning() && exact()) dest_->SetWriteSizeHint(max_length());
}

template <typename Dest>
inline void LimitingWriter<Dest>::Reset(Dest&& dest, Options options) {
  LimitingWriterBase::Reset(options.exact());
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), std::move(options));
  if (dest_.is_owning() && exact()) dest_->SetWriteSizeHint(max_length());
}

template <typename Dest>
template <typename... DestArgs>
inline void LimitingWriter<Dest>::Reset(std::tuple<DestArgs...> dest_args,
                                        Options options) {
  LimitingWriterBase::Reset(options.exact());
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get(), std::move(options));
  if (dest_.is_owning() && exact()) dest_->SetWriteSizeHint(max_length());
}

template <typename Dest>
inline void LimitingWriter<Dest>::MoveDest(LimitingWriter&& that) {
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
void LimitingWriter<Dest>::Done() {
  LimitingWriterBase::Done();
  if (dest_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) {
      FailWithoutAnnotation(dest_->status());
    }
  }
}

template <typename Dest>
void LimitingWriter<Dest>::SetWriteSizeHintImpl(
    absl::optional<Position> write_size_hint) {
  if (dest_.is_owning() && !exact()) {
    dest_->SetWriteSizeHint(
        write_size_hint == absl::nullopt
            ? absl::nullopt
            : absl::make_optional(UnsignedMin(*write_size_hint, max_length())));
  }
}

template <typename Dest>
bool LimitingWriter<Dest>::FlushImpl(FlushType flush_type) {
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

#endif  // RIEGELI_BYTES_LIMITING_WRITER_H_
