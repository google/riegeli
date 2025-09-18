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
#include <optional>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/byte_fill.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/external_ref.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/moving_dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
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

    // If `false`, `LimitingWriter` will write data at most up to the limit.
    // Writing will fail if the limit is exceeded.
    //
    // If `true`, `LimitingWriter` will write data exactly up to the limit.
    // Writing will fail if the limit is exceeded, and `Close()` will fail if
    // the current position at that time is before the limit.
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

   private:
    std::optional<Position> max_pos_;
    std::optional<Position> max_length_;
    bool exact_ = false;
  };

  // Returns the original `Writer`. Unchanged by `Close()`.
  virtual Writer* DestWriter() const ABSL_ATTRIBUTE_LIFETIME_BOUND = 0;

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

  bool SupportsRandomAccess() override;
  bool SupportsTruncate() override;
  bool SupportsReadMode() override;

 protected:
  explicit LimitingWriterBase(Closed) noexcept : Writer(kClosed) {}

  explicit LimitingWriterBase(bool exact);

  LimitingWriterBase(LimitingWriterBase&& that) noexcept;
  LimitingWriterBase& operator=(LimitingWriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset(bool exact);
  void Initialize(Writer* dest, const Options& options, bool is_owning);
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
  bool WriteSlow(ExternalRef src) override;
  bool WriteSlow(const Chain& src) override;
  bool WriteSlow(Chain&& src) override;
  bool WriteSlow(const absl::Cord& src) override;
  bool WriteSlow(absl::Cord&& src) override;
  bool WriteSlow(ByteFill src) override;
  bool SeekSlow(Position new_pos) override;
  std::optional<Position> SizeImpl() override;
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
  //   `start() == DestWriter()->start()`
  //   `limit() == DestWriter()->limit()`
  //   `start_pos() == DestWriter()->start_pos()`
};

// A `Writer` which writes to another `Writer` up to the specified size limit.
// An attempt to write more fails, after writing to the destination everything
// up to the limit.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the original `Writer`. `Dest` must support
// `Dependency<Writer*, Dest>`, e.g. `Writer*` (not owned, default),
// `ChainWriter<>` (owned), `std::unique_ptr<Writer>` (owned),
// `Any<Writer*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as `TargetT` of the
// type of the first constructor argument.
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
  explicit LimitingWriter(Initializer<Dest> dest, Options options = Options());

  LimitingWriter(LimitingWriter&& that) = default;
  LimitingWriter& operator=(LimitingWriter&& that) = default;

  // Makes `*this` equivalent to a newly constructed `LimitingWriter`. This
  // avoids constructing a temporary `LimitingWriter` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Dest> dest,
                                          Options options = Options());

  // Returns the object providing and possibly owning the original `Writer`.
  // Unchanged by `Close()`.
  Dest& dest() ABSL_ATTRIBUTE_LIFETIME_BOUND { return dest_.manager(); }
  const Dest& dest() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return dest_.manager();
  }
  Writer* DestWriter() const ABSL_ATTRIBUTE_LIFETIME_BOUND override {
    return dest_.get();
  }

 protected:
  void Done() override;
  void SetWriteSizeHintImpl(std::optional<Position> write_size_hint) override;
  bool FlushImpl(FlushType flush_type) override;

 private:
  class Mover;

  // The object providing and possibly owning the original `Writer`.
  MovingDependency<Writer*, Dest, Mover> dest_;
};

explicit LimitingWriter(Closed) -> LimitingWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit LimitingWriter(Dest&& dest, LimitingWriterBase::Options options =
                                         LimitingWriterBase::Options())
    -> LimitingWriter<TargetT<Dest>>;

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
  max_pos_ = std::numeric_limits<Position>::max();
  exact_ = exact;
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
  if (ABSL_PREDICT_FALSE(start_pos() > max_pos_)) {
    set_buffer(cursor());
    set_start_pos(max_pos_);
    FailLimitExceeded();
  }
  if (ABSL_PREDICT_FALSE(!dest.ok())) FailWithoutAnnotation(dest.status());
}

template <typename Dest>
class LimitingWriter<Dest>::Mover {
 public:
  static auto member() { return &LimitingWriter::dest_; }

  explicit Mover(LimitingWriter& self, LimitingWriter& that)
      : uses_buffer_(self.start() != nullptr) {
    // Buffer pointers are already moved so `SyncBuffer()` is called on `self`.
    // `dest_` is not moved yet so `dest_` is taken from `that`.
    if (uses_buffer_) {
      if (ABSL_PREDICT_FALSE(!self.SyncBuffer(*that.dest_))) {
        uses_buffer_ = false;
      }
    }
  }

  void Done(LimitingWriter& self) {
    if (uses_buffer_) self.MakeBuffer(*self.dest_);
  }

 private:
  bool uses_buffer_;
};

template <typename Dest>
inline LimitingWriter<Dest>::LimitingWriter(Initializer<Dest> dest,
                                            Options options)
    : LimitingWriterBase(options.exact()), dest_(std::move(dest)) {
  Initialize(dest_.get(), options, dest_.IsOwning());
}

template <typename Dest>
inline void LimitingWriter<Dest>::Reset(Closed) {
  LimitingWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void LimitingWriter<Dest>::Reset(Initializer<Dest> dest,
                                        Options options) {
  LimitingWriterBase::Reset(options.exact());
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), options, dest_.IsOwning());
}

template <typename Dest>
void LimitingWriter<Dest>::Done() {
  LimitingWriterBase::Done();
  if (dest_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) {
      FailWithoutAnnotation(dest_->status());
    }
  }
}

template <typename Dest>
void LimitingWriter<Dest>::SetWriteSizeHintImpl(
    std::optional<Position> write_size_hint) {
  if (dest_.IsOwning() && !exact()) {
    if (ABSL_PREDICT_FALSE(!SyncBuffer(*dest_))) return;
    dest_->SetWriteSizeHint(
        write_size_hint == std::nullopt
            ? std::nullopt
            : std::make_optional(UnsignedMin(*write_size_hint, max_length())));
    MakeBuffer(*dest_);
  }
}

template <typename Dest>
bool LimitingWriter<Dest>::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (ABSL_PREDICT_FALSE(!SyncBuffer(*dest_))) return false;
  bool flush_ok = true;
  if (flush_type != FlushType::kFromObject || dest_.IsOwning()) {
    flush_ok = dest_->Flush(flush_type);
  }
  MakeBuffer(*dest_);
  return flush_ok;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_LIMITING_WRITER_H_
