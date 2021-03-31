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

#include <istream>
#include <streambuf>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "riegeli/base/base.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

namespace internal {

class ReaderStreambuf : public std::streambuf {
 public:
  explicit ReaderStreambuf(ObjectState::InitiallyClosed) noexcept
      : state_(ObjectState::kInitiallyClosed) {}
  explicit ReaderStreambuf(ObjectState::InitiallyOpen) noexcept
      : state_(ObjectState::kInitiallyOpen) {}

  ReaderStreambuf(ReaderStreambuf&& that) noexcept;
  ReaderStreambuf& operator=(ReaderStreambuf&& that) noexcept;

  void Initialize(Reader* src);
  void MoveBegin();
  void MoveEnd(Reader* src);
  void Done();

  bool healthy() const { return state_.healthy(); }
  bool is_open() const { return state_.is_open(); }
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
  Reader* src_ = nullptr;

  // Invariants:
  //   `eback() == (is_open() ? src_->start() : nullptr)`
  //   `egptr() == (is_open() ? src_->limit() : nullptr)`
};

}  // namespace internal

// Template parameter independent part of `ReaderIstream`.
class ReaderIstreamBase : public std::istream {
 public:
  // Returns the `Reader`. Unchanged by `close()`.
  virtual Reader* src_reader() = 0;
  virtual const Reader* src_reader() const = 0;

  // If `!is_open()`, does nothing. Otherwise:
  //  * Synchronizes the current `ReaderIstream` position to the `Reader`.
  //  * Closes the `Reader` if it is owned.
  //
  // Also, propagates `Reader` failures so that converting the `ReaderIstream`
  // to `bool` indicates whether `Reader` was healthy before closing (doing this
  // during reading is not feasible without throwing exceptions).
  //
  // Returns `*this` for convenience of checking for failures.
  //
  // Destroying or assigning to a `ReaderIstream` closes it implicitly, but an
  // explicit `close()` call allows to detect failures (use `status()` for
  // failure details).
  virtual ReaderIstreamBase& close() = 0;

  // Returns `true` if the `ReaderIstream` is healthy, i.e. open and not failed.
  bool healthy() const { return streambuf_.healthy(); }

  // Returns `true` if the `ReaderIstream` is open, i.e. not closed.
  bool is_open() const { return streambuf_.is_open(); }

  // Returns an `absl::Status` describing the failure if the `ReaderIstream`
  // is failed, or an `absl::FailedPreconditionError()` if the `ReaderIstream`
  // is closed, or `absl::OkStatus()` if the `ReaderIstream` is healthy.
  absl::Status status() const { return streambuf_.status(); }

 protected:
  explicit ReaderIstreamBase(ObjectState::InitiallyClosed) noexcept
      : std::istream(&streambuf_), streambuf_(ObjectState::kInitiallyClosed) {}
  explicit ReaderIstreamBase(ObjectState::InitiallyOpen) noexcept
      : std::istream(&streambuf_), streambuf_(ObjectState::kInitiallyOpen) {}

  ReaderIstreamBase(ReaderIstreamBase&& that) noexcept;
  ReaderIstreamBase& operator=(ReaderIstreamBase&& that) noexcept;

  void Reset(ObjectState::InitiallyClosed);
  void Reset(ObjectState::InitiallyOpen);
  void Initialize(Reader* src);

  internal::ReaderStreambuf streambuf_;

  // Invariant: `rdbuf() == &streambuf_`
};

// Adapts a `Reader` to a `std::istream`.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the `Reader`. `Src` must support `Dependency<Reader*, Src>`,
// e.g. `Reader*` (not owned, default), `std::unique_ptr<Reader>` (owned),
// `ChainReader<>` (owned).
//
// By relying on CTAD the template argument can be deduced as the value type of
// the first constructor argument. This requires C++17.
//
// The `Reader` must not be accessed until the `ReaderIstream` is closed or no
// longer used.
template <typename Src = Reader*>
class ReaderIstream : public ReaderIstreamBase {
 public:
  // Creates a closed `ReaderIstream`.
  ReaderIstream() noexcept : ReaderIstreamBase(ObjectState::kInitiallyClosed) {}

  // Will read from the `Reader` provided by `src`.
  explicit ReaderIstream(const Src& src);
  explicit ReaderIstream(Src&& src);

  // Will read from the `Reader` provided by a `Src` constructed from elements
  // of `src_args`. This avoids constructing a temporary `Src` and moving from
  // it.
  template <typename... SrcArgs>
  explicit ReaderIstream(std::tuple<SrcArgs...> src_args);

  ReaderIstream(ReaderIstream&& that) noexcept;
  ReaderIstream& operator=(ReaderIstream&& that) noexcept;

  ~ReaderIstream() override { close(); }

  // Makes `*this` equivalent to a newly constructed `ReaderIstream`. This
  // avoids constructing a temporary `ReaderIstream` and moving from it.
  void Reset();
  void Reset(const Src& src);
  void Reset(Src&& src);
  template <typename... SrcArgs>
  void Reset(std::tuple<SrcArgs...> src_args);

  // Returns the object providing and possibly owning the `Reader`. Unchanged by
  // `close()`.
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  Reader* src_reader() override { return src_.get(); }
  const Reader* src_reader() const override { return src_.get(); }

  ReaderIstream& close() override;

 private:
  void MoveSrc(ReaderIstream&& that);

  // The object providing and possibly owning the `Reader`.
  Dependency<Reader*, Src> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
template <typename Src>
ReaderIstream(Src&& src) -> ReaderIstream<std::decay_t<Src>>;
template <typename... SrcArgs>
ReaderIstream(std::tuple<SrcArgs...> src_args)
    -> ReaderIstream<void>;  // Delete.
#endif

// Implementation details follow.

namespace internal {

inline ReaderStreambuf::ReaderStreambuf(ReaderStreambuf&& that) noexcept
    : std::streambuf(that), state_(std::move(that.state_)), src_(that.src_) {
  that.setg(nullptr, nullptr, nullptr);
}

inline ReaderStreambuf& ReaderStreambuf::operator=(
    ReaderStreambuf&& that) noexcept {
  if (ABSL_PREDICT_TRUE(&that != this)) {
    std::streambuf::operator=(that);
    state_ = std::move(that.state_);
    src_ = that.src_;
    that.setg(nullptr, nullptr, nullptr);
  }
  return *this;
}

inline void ReaderStreambuf::Initialize(Reader* src) {
  RIEGELI_ASSERT(src != nullptr)
      << "Failed precondition of ReaderStreambuf: null Reader pointer";
  src_ = src;
  setg(const_cast<char*>(src_->start()), const_cast<char*>(src_->cursor()),
       const_cast<char*>(src_->limit()));
  if (ABSL_PREDICT_FALSE(!src_->healthy()) && src_->available() == 0) Fail();
}

inline void ReaderStreambuf::MoveBegin() { src_->set_cursor(gptr()); }

inline void ReaderStreambuf::MoveEnd(Reader* src) {
  src_ = src;
  setg(const_cast<char*>(src_->start()), const_cast<char*>(src_->cursor()),
       const_cast<char*>(src_->limit()));
}

inline void ReaderStreambuf::Done() {
  src_->set_cursor(gptr());
  setg(nullptr, nullptr, nullptr);
}

}  // namespace internal

inline ReaderIstreamBase::ReaderIstreamBase(ReaderIstreamBase&& that) noexcept
    : std::istream(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      streambuf_(std::move(that.streambuf_)) {
  set_rdbuf(&streambuf_);
}

inline ReaderIstreamBase& ReaderIstreamBase::operator=(
    ReaderIstreamBase&& that) noexcept {
  std::istream::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  streambuf_ = std::move(that.streambuf_);
  return *this;
}

inline void ReaderIstreamBase::Reset(ObjectState::InitiallyClosed) {
  streambuf_ = internal::ReaderStreambuf(ObjectState::kInitiallyClosed);
  init(&streambuf_);
}

inline void ReaderIstreamBase::Reset(ObjectState::InitiallyOpen) {
  streambuf_ = internal::ReaderStreambuf(ObjectState::kInitiallyOpen);
  init(&streambuf_);
}

inline void ReaderIstreamBase::Initialize(Reader* src) {
  streambuf_.Initialize(src);
  if (ABSL_PREDICT_FALSE(!streambuf_.healthy())) {
    setstate(std::ios_base::badbit);
  }
}

template <typename Src>
inline ReaderIstream<Src>::ReaderIstream(const Src& src)
    : ReaderIstreamBase(ObjectState::kInitiallyOpen), src_(src) {
  Initialize(src_.get());
}

template <typename Src>
inline ReaderIstream<Src>::ReaderIstream(Src&& src)
    : ReaderIstreamBase(ObjectState::kInitiallyOpen), src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline ReaderIstream<Src>::ReaderIstream(std::tuple<SrcArgs...> src_args)
    : ReaderIstreamBase(ObjectState::kInitiallyOpen),
      src_(std::move(src_args)) {
  Initialize(src_.get());
}

template <typename Src>
inline ReaderIstream<Src>::ReaderIstream(ReaderIstream&& that) noexcept
    : ReaderIstreamBase(std::move(that)) {
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  MoveSrc(std::move(that));
}

template <typename Src>
inline ReaderIstream<Src>& ReaderIstream<Src>::operator=(
    ReaderIstream&& that) noexcept {
  if (ABSL_PREDICT_TRUE(&that != this)) {
    close();
    ReaderIstreamBase::operator=(std::move(that));
    // Using `that` after it was moved is correct because only the base class
    // part was moved.
    MoveSrc(std::move(that));
  }
  return *this;
}

template <typename Src>
inline void ReaderIstream<Src>::Reset() {
  close();
  ReaderIstreamBase::Reset(ObjectState::kInitiallyClosed);
  src_.Reset();
}

template <typename Src>
inline void ReaderIstream<Src>::Reset(const Src& src) {
  close();
  ReaderIstreamBase::Reset(ObjectState::kInitiallyOpen);
  src_.Reset(src);
  Initialize(src_.get());
}

template <typename Src>
inline void ReaderIstream<Src>::Reset(Src&& src) {
  close();
  ReaderIstreamBase::Reset(ObjectState::kInitiallyOpen);
  src_.Reset(std::move(src));
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline void ReaderIstream<Src>::Reset(std::tuple<SrcArgs...> src_args) {
  close();
  ReaderIstreamBase::Reset(ObjectState::kInitiallyOpen);
  src_.Reset(std::move(src_args));
  Initialize(src_.get());
}

template <typename Src>
inline void ReaderIstream<Src>::MoveSrc(ReaderIstream&& that) {
  if (src_.kIsStable()) {
    src_ = std::move(that.src_);
  } else {
    streambuf_.MoveBegin();
    src_ = std::move(that.src_);
    streambuf_.MoveEnd(src_.get());
  }
}

template <typename Src>
inline ReaderIstream<Src>& ReaderIstream<Src>::close() {
  if (ABSL_PREDICT_TRUE(is_open())) {
    streambuf_.Done();
    if (src_.is_owning()) {
      if (ABSL_PREDICT_FALSE(!src_->Close())) streambuf_.Fail();
    }
    if (ABSL_PREDICT_FALSE(!streambuf_.healthy())) {
      setstate(std::ios_base::badbit);
    }
    streambuf_.MarkClosed();
  }
  return *this;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_READER_ISTREAM_H_
