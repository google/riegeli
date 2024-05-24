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

#ifndef RIEGELI_BYTES_STRING_WRITER_H_
#define RIEGELI_BYTES_STRING_WRITER_H_

#include <stddef.h>

#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/meta/type_traits.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/moving_dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

class Reader;
template <typename Src>
class StringReader;

// Template parameter independent part of `StringWriter`.
class StringWriterBase : public Writer {
 public:
  class Options : public BufferOptionsBase<Options> {
   public:
    Options() noexcept {}

    // If `false`, replaces existing contents of the destination, clearing it
    // first.
    //
    // If `true`, appends to existing contents of the destination.
    //
    // Default: `false`.
    Options& set_append(bool append) & {
      append_ = append;
      return *this;
    }
    Options&& set_append(bool append) && {
      return std::move(set_append(append));
    }
    bool append() const { return append_; }

   private:
    bool append_ = false;
  };

  // Returns the `std::string` being written to. Unchanged by `Close()`.
  virtual std::string* DestString() const = 0;
  std::string& Digest() {
    Flush();
    return *DestString();
  }

  bool SupportsRandomAccess() override { return true; }
  bool SupportsReadMode() override { return true; }

 protected:
  explicit StringWriterBase(Closed) noexcept : Writer(kClosed) {}

  explicit StringWriterBase(BufferOptions buffer_options);

  StringWriterBase(StringWriterBase&& that) noexcept;
  StringWriterBase& operator=(StringWriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset(BufferOptions buffer_options);
  void Initialize(std::string* dest, bool append);
  bool uses_secondary_buffer() const { return !secondary_buffer_.empty(); }

  void Done() override;
  void SetWriteSizeHintImpl(absl::optional<Position> write_size_hint) override;
  bool PushSlow(size_t min_length, size_t recommended_length) override;
  using Writer::WriteSlow;
  bool WriteSlow(const Chain& src) override;
  bool WriteSlow(Chain&& src) override;
  bool WriteSlow(const absl::Cord& src) override;
  bool WriteSlow(absl::Cord&& src) override;
  bool WriteZerosSlow(Position length) override;
  bool FlushImpl(FlushType flush_type) override;
  bool SeekSlow(Position new_pos) override;
  absl::optional<Position> SizeImpl() override;
  bool TruncateImpl(Position new_size) override;
  Reader* ReadModeImpl(Position initial_pos) override;

 private:
  // Returns the amount of data written, either to `*DestString()` or to
  // `secondary_buffer_`.
  size_t used_size() const;

  // Discards uninitialized space from the end of `dest`, so that it contains
  // only actual data written.
  //
  // Precondition: `!uses_secondary_buffer()`
  void SyncDestBuffer(std::string& dest);

  // Sets buffer pointers to `dest`.
  //
  // Precondition: `!uses_secondary_buffer()`
  void MakeDestBuffer(std::string& dest, size_t cursor_index);

  // Appends some uninitialized space to `dest` if this can be done without
  // reallocation. Sets buffer pointers to `dest`.
  //
  // Precondition: `!uses_secondary_buffer()`
  void GrowDestToCapacityAndMakeBuffer(std::string& dest, size_t cursor_index);

  // Discards uninitialized space from the end of `secondary_buffer_`, so that
  // it contains only actual data written.
  void SyncSecondaryBuffer();

  // Appends uninitialized space to `secondary_buffer_`.
  void MakeSecondaryBuffer(size_t min_length = 0,
                           size_t recommended_length = 0);

  // Move `secondary_buffer_`, adjusting buffer pointers if they point to it.
  void MoveSecondaryBuffer(StringWriterBase& that);

  Chain::Options options_;
  // Buffered data which did not fit under `DestString()->capacity()`.
  Chain secondary_buffer_;

  // Size of written data is always `UnsignedMax(pos(), written_size_)`.
  // This is used to determine the size after seeking backwards.
  //
  // Invariant: if `uses_secondary_buffer()` then `written_size_ == 0`.
  size_t written_size_ = 0;

  AssociatedReader<StringReader<absl::string_view>> associated_reader_;

  // If `!uses_secondary_buffer()`, then `*DestString()` contains the data
  // before the current position of length `pos()`, followed by the data after
  // the current position of length `SaturatingSub(written_size_, pos())`,
  // followed by free space of length
  // `DestString()->size() - UnsignedMax(pos(), written_size_)`.
  //
  // If `uses_secondary_buffer()`, then `*DestString()` contains some prefix of
  // the data, and `secondary_buffer_` contains the rest of the data followed by
  // free space of length `available()`. In this case there is no data after the
  // current position.
  //
  // Invariants if `ok()`:
  //   `!uses_secondary_buffer() &&
  //    start() == &(*DestString())[0] &&
  //    start_to_limit() == DestString()->size() &&
  //    start_pos() == 0` or
  //       `uses_secondary_buffer() &&
  //        limit() == secondary_buffer_.blocks().back().data() +
  //                   secondary_buffer_.blocks().back().size()` or
  //       `start() == nullptr`
  //   `limit_pos() >= secondary_buffer_.size()`
  //   `UnsignedMax(limit_pos(), written_size_) ==
  //        DestString()->size() + secondary_buffer_.size()`
};

// A `Writer` which appends to a `std::string`, resizing it as necessary.
//
// It supports `Seek()` and `ReadMode()`.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the `std::string` being written to. `Dest` must support
// `Dependency<std::string*, Dest>`, e.g. `std::string*` (not owned, default),
// `std::string` (owned), `AnyDependency<std::string*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as `std::string`
// if there are no constructor arguments or the only argument is `Options`,
// otherwise as `InitializerTargetT` of the type of the first constructor
// argument, except that CTAD is deleted if the first constructor argument is a
// `std::string&` or `const std::string&` (to avoid writing to an
// unintentionally separate copy of an existing object). This requires C++17.
//
// The `std::string` must not be accessed until the `StringWriter` is closed or
// no longer used, except that it is allowed to read the `std::string`
// immediately after `Flush()`.
template <typename Dest = std::string*>
class StringWriter : public StringWriterBase {
 public:
  // Creates a closed `StringWriter`.
  explicit StringWriter(Closed) noexcept : StringWriterBase(kClosed) {}

  // Will append to the `std::string` provided by `dest`.
  explicit StringWriter(Initializer<Dest> dest, Options options = Options());

  // Will append to an owned `std::string` which can be accessed by `dest()`.
  // This constructor is present only if `Dest` is `std::string`.
  template <typename DependentDest = Dest,
            std::enable_if_t<std::is_same<DependentDest, std::string>::value,
                             int> = 0>
  explicit StringWriter(Options options = Options());

  StringWriter(StringWriter&& that) = default;
  StringWriter& operator=(StringWriter&& that) = default;

  // Makes `*this` equivalent to a newly constructed `StringWriter`. This avoids
  // constructing a temporary `StringWriter` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Dest> dest,
                                          Options options = Options());
  template <typename DependentDest = Dest,
            std::enable_if_t<std::is_same<DependentDest, std::string>::value,
                             int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Options options = Options());

  // Returns the object providing and possibly owning the `std::string` being
  // written to. Unchanged by `Close()`.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  std::string* DestString() const override { return dest_.get(); }

 private:
  class Mover;

  // The object providing and possibly owning the `std::string` being written
  // to, with uninitialized space appended (possibly empty); `cursor()` points
  // to the uninitialized space.
  MovingDependency<std::string*, Dest, Mover> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit StringWriter(Closed) -> StringWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit StringWriter(Dest&& dest, StringWriterBase::Options options =
                                       StringWriterBase::Options())
    -> StringWriter<std::conditional_t<
        absl::conjunction<std::is_lvalue_reference<Dest>,
                          std::is_convertible<std::remove_reference_t<Dest>*,
                                              const std::string*>>::value,
        DeleteCtad<Dest&&>, InitializerTargetT<Dest>>>;
explicit StringWriter(
    StringWriterBase::Options options = StringWriterBase::Options())
    -> StringWriter<std::string>;
#endif

// Implementation details follow.

inline StringWriterBase::StringWriterBase(BufferOptions buffer_options)
    : options_(Chain::Options()
                   .set_min_block_size(buffer_options.min_buffer_size())
                   .set_max_block_size(buffer_options.max_buffer_size())) {}

inline StringWriterBase::StringWriterBase(StringWriterBase&& that) noexcept
    : Writer(static_cast<Writer&&>(that)),
      options_(that.options_),
      written_size_(that.written_size_),
      associated_reader_(std::move(that.associated_reader_)) {
  MoveSecondaryBuffer(that);
}

inline StringWriterBase& StringWriterBase::operator=(
    StringWriterBase&& that) noexcept {
  Writer::operator=(static_cast<Writer&&>(that));
  options_ = that.options_;
  written_size_ = that.written_size_;
  associated_reader_ = std::move(that.associated_reader_);
  MoveSecondaryBuffer(that);
  return *this;
}

inline void StringWriterBase::Reset(Closed) {
  Writer::Reset(kClosed);
  options_ = Chain::Options();
  secondary_buffer_ = Chain();
  written_size_ = 0;
  associated_reader_.Reset();
}

inline void StringWriterBase::Reset(BufferOptions buffer_options) {
  Writer::Reset();
  options_ = Chain::Options()
                 .set_min_block_size(buffer_options.min_buffer_size())
                 .set_max_block_size(buffer_options.max_buffer_size());
  secondary_buffer_.Clear();
  written_size_ = 0;
  associated_reader_.Reset();
}

inline void StringWriterBase::Initialize(std::string* dest, bool append) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of StringWriter: null string pointer";
  if (append) {
    set_start_pos(dest->size());
  } else {
    dest->clear();
  }
}

inline void StringWriterBase::MoveSecondaryBuffer(StringWriterBase& that) {
  // Buffer pointers are already moved so `start()` is taken from `*this`.
  // `secondary_buffer_` is not moved yet so `uses_secondary_buffer()` is called
  // on `that`.
  const bool uses_buffer = start() != nullptr && that.uses_secondary_buffer();
  const size_t saved_start_to_limit = start_to_limit();
  const size_t saved_start_to_cursor = start_to_cursor();
  if (uses_buffer) {
    RIEGELI_ASSERT(that.secondary_buffer_.blocks().back().data() +
                       that.secondary_buffer_.blocks().back().size() ==
                   limit())
        << "Failed invariant of StringWriter: "
           "secondary buffer inconsistent with buffer pointers";
  }
  secondary_buffer_ = std::move(that.secondary_buffer_);
  if (uses_buffer) {
    const absl::string_view last_block = secondary_buffer_.blocks().back();
    set_buffer(const_cast<char*>(last_block.data() + last_block.size()) -
                   saved_start_to_limit,
               saved_start_to_limit, saved_start_to_cursor);
  }
}

template <typename Dest>
class StringWriter<Dest>::Mover {
 public:
  static auto member() { return &StringWriter::dest_; }

  explicit Mover(StringWriter& self, StringWriter& that)
      : uses_buffer_(self.start() != nullptr && !self.uses_secondary_buffer()),
        start_to_cursor_(self.start_to_cursor()) {
    if (uses_buffer_) {
      RIEGELI_ASSERT(&(*that.dest_)[0] == self.start())
          << "StringWriter destination changed unexpectedly";
      RIEGELI_ASSERT_EQ(that.dest_->size(), self.start_to_limit())
          << "StringWriter destination changed unexpectedly";
    }
  }

  void Done(StringWriter& self) {
    if (uses_buffer_) {
      std::string& dest = *self.dest_;
      self.set_buffer(&dest[0], dest.size(), start_to_cursor_);
    }
  }

 private:
  bool uses_buffer_;
  size_t start_to_cursor_;
};

template <typename Dest>
inline StringWriter<Dest>::StringWriter(Initializer<Dest> dest, Options options)
    : StringWriterBase(options.buffer_options()), dest_(std::move(dest)) {
  Initialize(dest_.get(), options.append());
}

template <typename Dest>
template <
    typename DependentDest,
    std::enable_if_t<std::is_same<DependentDest, std::string>::value, int>>
inline StringWriter<Dest>::StringWriter(Options options)
    : StringWriter(riegeli::Maker(), std::move(options)) {}

template <typename Dest>
inline void StringWriter<Dest>::Reset(Closed) {
  StringWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void StringWriter<Dest>::Reset(Initializer<Dest> dest, Options options) {
  StringWriterBase::Reset(options.buffer_options());
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), options.append());
}

template <typename Dest>
template <
    typename DependentDest,
    std::enable_if_t<std::is_same<DependentDest, std::string>::value, int>>
inline void StringWriter<Dest>::Reset(Options options) {
  Reset(riegeli::Maker(), std::move(options));
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_STRING_WRITER_H_
