// Copyright 2019 Google LLC
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

#ifndef RIEGELI_BYTES_READER_ISTREAM_H_
#define RIEGELI_BYTES_READER_ISTREAM_H_

#include <ios>
#include <iosfwd>
#include <istream>
#include <streambuf>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/moving_dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

namespace stream_internal {

class ReaderStreambuf : public std::streambuf {
 public:
  explicit ReaderStreambuf(Closed) noexcept : state_(kClosed) {}

  ReaderStreambuf() noexcept {}

  ReaderStreambuf(ReaderStreambuf&& that) noexcept;
  ReaderStreambuf& operator=(ReaderStreambuf&& that) noexcept;

  void Initialize(Reader* src);
  void MoveBegin();
  void MoveEnd(Reader* src);
  void Done();

  bool ok() const { return state_.ok(); }
  bool is_open() const { return state_.is_open(); }
  bool not_failed() const { return state_.not_failed(); }
  absl::Status status() const { return state_.status(); }
  void MarkClosed() { state_.MarkClosed(); }
  ABSL_ATTRIBUTE_COLD void Fail();

 protected:
  int sync() override;
  std::streamsize showmanyc() override;
  int underflow() override;
  std::streamsize xsgetn(char* dest, std::streamsize length) override;
  std::streampos seekoff(std::streamoff off, std::ios_base::seekdir dir,
                         std::ios_base::openmode which) override;
  std::streampos seekpos(std::streampos pos,
                         std::ios_base::openmode which) override;

 private:
  class BufferSync;

  ObjectState state_;
  Reader* reader_ = nullptr;

  // Invariants:
  //   `eback() == (is_open() ? reader_->start() : nullptr)`
  //   `egptr() == (is_open() ? reader_->limit() : nullptr)`
};

}  // namespace stream_internal

// Template parameter independent part of `ReaderIStream`.
class ReaderIStreamBase : public std::istream {
 public:
  class Options {
   public:
    Options() noexcept {}
  };

  // Returns the `Reader`. Unchanged by `close()`.
  virtual Reader* SrcReader() const ABSL_ATTRIBUTE_LIFETIME_BOUND = 0;

  // If `!is_open()`, does nothing. Otherwise:
  //  * Synchronizes the current `ReaderIStream` position to the `Reader`.
  //  * Closes the `Reader` if it is owned.
  //
  // Also, propagates `Reader` failures to `rdstate() & std::ios_base::badbit`
  // (doing this during reading is not feasible without throwing exceptions).
  //
  // Returns `true` if the `Reader` did not fail, i.e. if it was OK just before
  // becoming closed.
  //
  // Destroying or assigning to a `ReaderIStream` closes it implicitly, but an
  // explicit `close()` call allows to detect failures (use `status()` for
  // failure details).
  bool close();

  // Returns `true` if the `ReaderIStream` is OK, i.e. open and not failed.
  bool ok() const { return streambuf_.ok(); }

  // Returns `true` if the `ReaderIStream` is open, i.e. not closed.
  bool is_open() const { return streambuf_.is_open(); }

  // Returns `true` if the `ReaderIStream` is not failed.
  bool not_failed() const { return streambuf_.not_failed(); }

  // Returns an `absl::Status` describing the failure if the `ReaderIStream`
  // is failed, or an `absl::FailedPreconditionError()` if the `ReaderIStream`
  // is successfully closed, or `absl::OkStatus()` if the `ReaderIStream` is OK.
  absl::Status status() const { return streambuf_.status(); }

  // Support `Dependency`.
  friend MakerType<Closed> RiegeliDependencySentinel(ReaderIStreamBase*) {
    return {kClosed};
  }

 protected:
  explicit ReaderIStreamBase(Closed) noexcept
      : std::istream(&streambuf_), streambuf_(kClosed) {}

  ReaderIStreamBase() noexcept : std::istream(&streambuf_) {}

  ReaderIStreamBase(ReaderIStreamBase&& that) noexcept;
  ReaderIStreamBase& operator=(ReaderIStreamBase&& that) noexcept;

  void Reset(Closed);
  void Reset();
  void Initialize(Reader* src);

  virtual void Done() = 0;

  stream_internal::ReaderStreambuf streambuf_;

  // Invariant: `rdbuf() == &streambuf_`
};

// Adapts a `Reader` to a `std::istream`.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the `Reader`. `Src` must support `Dependency<Reader*, Src>`,
// e.g. `Reader*` (not owned, default), `ChainReader<>` (owned),
// `std::unique_ptr<Reader>` (owned), `Any<Reader*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as
// `InitializerTargetT` of the type of the first constructor argument.
// This requires C++17.
//
// The `Reader` must not be accessed until the `ReaderIStream` is closed or no
// longer used.
template <typename Src = Reader*>
class ReaderIStream : public ReaderIStreamBase {
 public:
  // Creates a closed `ReaderIStream`.
  explicit ReaderIStream(Closed) noexcept : ReaderIStreamBase(kClosed) {}

  // Will read from the `Reader` provided by `src`.
  explicit ReaderIStream(Initializer<Src> src, Options options = Options());

  // These operations cannot be defaulted because `ReaderIStreamBase` virtually
  // derives from `std::ios` which has these operations deleted.
  ReaderIStream(ReaderIStream&& that) noexcept
#if __cpp_concepts
    requires std::is_move_constructible<Dependency<Reader*, Src>>::value
#endif
      : ReaderIStreamBase(static_cast<ReaderIStreamBase&&>(that)),
        src_(std::move(that.src_), *this, that) {
  }
  ReaderIStream& operator=(ReaderIStream&& that) noexcept
#if __cpp_concepts
    requires(std::is_move_assignable<Dependency<Reader*, Src>>::value)
#endif
  {
    ReaderIStreamBase::operator=(static_cast<ReaderIStreamBase&&>(that));
    src_.Reset(std::move(that.src_), *this, that);
    return *this;
  }

  ~ReaderIStream() override { Done(); }

  // Makes `*this` equivalent to a newly constructed `ReaderIStream`. This
  // avoids constructing a temporary `ReaderIStream` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Src> src,
                                          Options options = Options());

  // Returns the object providing and possibly owning the `Reader`. Unchanged by
  // `close()`.
  Src& src() ABSL_ATTRIBUTE_LIFETIME_BOUND { return src_.manager(); }
  const Src& src() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return src_.manager();
  }
  Reader* SrcReader() const ABSL_ATTRIBUTE_LIFETIME_BOUND override {
    return src_.get();
  }

 protected:
  void Done() override;

 private:
  class Mover;

  // The object providing and possibly owning the `Reader`.
  MovingDependency<Reader*, Src, Mover> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit ReaderIStream(Closed) -> ReaderIStream<DeleteCtad<Closed>>;
template <typename Src>
explicit ReaderIStream(Src&& src, ReaderIStreamBase::Options options =
                                      ReaderIStreamBase::Options())
    -> ReaderIStream<InitializerTargetT<Src>>;
#endif

// Implementation details follow.

namespace stream_internal {

inline ReaderStreambuf::ReaderStreambuf(ReaderStreambuf&& that) noexcept
    : std::streambuf(that),
      state_(std::move(that.state_)),
      reader_(that.reader_) {
  that.setg(nullptr, nullptr, nullptr);
}

inline ReaderStreambuf& ReaderStreambuf::operator=(
    ReaderStreambuf&& that) noexcept {
  if (ABSL_PREDICT_TRUE(&that != this)) {
    std::streambuf::operator=(that);
    state_ = std::move(that.state_);
    reader_ = that.reader_;
    that.setg(nullptr, nullptr, nullptr);
  }
  return *this;
}

inline void ReaderStreambuf::Initialize(Reader* src) {
  RIEGELI_ASSERT_NE(src, nullptr)
      << "Failed precondition of ReaderStreambuf: null Reader pointer";
  reader_ = src;
  setg(const_cast<char*>(reader_->start()),
       const_cast<char*>(reader_->cursor()),
       const_cast<char*>(reader_->limit()));
  if (ABSL_PREDICT_FALSE(!reader_->ok()) && reader_->available() == 0) Fail();
}

inline void ReaderStreambuf::MoveBegin() {
  // In a closed `ReaderIStream`, `ReaderIStream::src_ != nullptr`
  // does not imply `ReaderStreambuf::reader_ != nullptr`, because
  // `ReaderIStream::streambuf_` can be left uninitialized.
  if (reader_ == nullptr) return;
  reader_->set_cursor(gptr());
}

inline void ReaderStreambuf::MoveEnd(Reader* src) {
  // In a closed `ReaderIStream`, `ReaderIStream::src_ != nullptr`
  // does not imply `ReaderStreambuf::reader_ != nullptr`, because
  // `ReaderIStream::streambuf_` can be left uninitialized.
  if (reader_ == nullptr) return;
  reader_ = src;
  setg(const_cast<char*>(reader_->start()),
       const_cast<char*>(reader_->cursor()),
       const_cast<char*>(reader_->limit()));
}

inline void ReaderStreambuf::Done() {
  reader_->set_cursor(gptr());
  setg(nullptr, nullptr, nullptr);
}

}  // namespace stream_internal

inline ReaderIStreamBase::ReaderIStreamBase(ReaderIStreamBase&& that) noexcept
    : std::istream(static_cast<std::istream&&>(that)),
      streambuf_(std::move(that.streambuf_)) {
  set_rdbuf(&streambuf_);
}

inline ReaderIStreamBase& ReaderIStreamBase::operator=(
    ReaderIStreamBase&& that) noexcept {
  if (ABSL_PREDICT_TRUE(&that != this)) {
    Done();
    std::istream::operator=(static_cast<std::istream&&>(that));
    streambuf_ = std::move(that.streambuf_);
  }
  return *this;
}

inline void ReaderIStreamBase::Reset(Closed) {
  Done();
  streambuf_ = stream_internal::ReaderStreambuf(kClosed);
  init(&streambuf_);
}

inline void ReaderIStreamBase::Reset() {
  Done();
  streambuf_ = stream_internal::ReaderStreambuf();
  init(&streambuf_);
}

inline void ReaderIStreamBase::Initialize(Reader* src) {
  streambuf_.Initialize(src);
  if (ABSL_PREDICT_FALSE(!streambuf_.ok())) setstate(std::ios_base::badbit);
}

template <typename Src>
class ReaderIStream<Src>::Mover {
 public:
  static auto member() { return &ReaderIStream::src_; }

  explicit Mover(ReaderIStream& self) { self.streambuf_.MoveBegin(); }

  void Done(ReaderIStream& self) { self.streambuf_.MoveEnd(self.src_.get()); }
};

template <typename Src>
inline ReaderIStream<Src>::ReaderIStream(Initializer<Src> src, Options options)
    : src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
inline void ReaderIStream<Src>::Reset(Closed) {
  ReaderIStreamBase::Reset(kClosed);
  src_.Reset();
}

template <typename Src>
inline void ReaderIStream<Src>::Reset(Initializer<Src> src, Options options) {
  ReaderIStreamBase::Reset();
  src_.Reset(std::move(src));
  Initialize(src_.get());
}

template <typename Src>
void ReaderIStream<Src>::Done() {
  if (ABSL_PREDICT_FALSE(!is_open())) return;
  streambuf_.Done();
  if (src_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) streambuf_.Fail();
  }
  if (ABSL_PREDICT_FALSE(!streambuf_.ok())) setstate(std::ios_base::badbit);
  streambuf_.MarkClosed();
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_READER_ISTREAM_H_
