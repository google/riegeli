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

#ifndef RIEGELI_BYTES_WRITER_OSTREAM_H_
#define RIEGELI_BYTES_WRITER_OSTREAM_H_

#include <ostream>
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
#include "riegeli/bytes/writer.h"

namespace riegeli {

namespace internal {

class WriterStreambuf : public std::streambuf {
 public:
  explicit WriterStreambuf(ObjectState::InitiallyClosed) noexcept
      : state_(ObjectState::kInitiallyClosed) {}
  explicit WriterStreambuf(ObjectState::InitiallyOpen) noexcept
      : state_(ObjectState::kInitiallyOpen) {}

  WriterStreambuf(WriterStreambuf&& that) noexcept;
  WriterStreambuf& operator=(WriterStreambuf&& that) noexcept;

  void Initialize(Writer* dest);
  void MoveBegin();
  void MoveEnd(Writer* dest);
  void Done();

  bool healthy() const { return state_.healthy(); }
  bool is_open() const { return state_.is_open(); }
  absl::Status status() const { return state_.status(); }
  void MarkClosed() { state_.MarkClosed(); }
  ABSL_ATTRIBUTE_COLD void Fail();

 protected:
  int sync() override;
  int overflow(int ch) override;
  std::streamsize xsputn(const char* src, std::streamsize length) override;
  std::streampos seekoff(std::streamoff off, std::ios_base::seekdir dir,
                         std::ios_base::openmode which) override;
  std::streampos seekpos(std::streampos pos,
                         std::ios_base::openmode which) override;

 private:
  class BufferSync;

  ObjectState state_;
  Writer* dest_ = nullptr;

  // Invariants:
  //   `is_open() ? pbase() >= dest_->start() : pbase() == nullptr`
  //   `epptr() == (is_open() ? dest_->limit() : nullptr)`
};

}  // namespace internal

// Template parameter independent part of `WriterOstream`.
class WriterOstreamBase : public std::ostream {
 public:
  // Returns the `Writer`. Unchanged by `close()`.
  virtual Writer* dest_writer() = 0;
  virtual const Writer* dest_writer() const = 0;

  // If `!is_open()`, does nothing. Otherwise:
  //  * Synchronizes the current `WriterOstream` position to the `Writer`.
  //  * Closes the `Writer` if it is owned.
  //
  // Returns `*this` for convenience of checking for failures.
  //
  // Destroying or assigning to a `WriterOstream` closes it implicitly, but an
  // explicit `close()` call allows to detect failures (use `status()` for
  // failure details).
  virtual WriterOstreamBase& close() = 0;

  // Returns `true` if the `WriterOstream` is healthy, i.e. open and not failed.
  bool healthy() const { return streambuf_.healthy(); }

  // Returns `true` if the `WriterOstream` is open, i.e. not closed.
  bool is_open() const { return streambuf_.is_open(); }

  // Returns an `absl::Status` describing the failure if the `WriterOstream`
  // is failed, or an `absl::FailedPreconditionError()` if the `WriterOstream`
  // is closed, or `absl::OkStatus()` if the `WriterOstream` is healthy.
  absl::Status status() const { return streambuf_.status(); }

 protected:
  explicit WriterOstreamBase(ObjectState::InitiallyClosed) noexcept
      : std::ostream(&streambuf_), streambuf_(ObjectState::kInitiallyClosed) {}
  explicit WriterOstreamBase(ObjectState::InitiallyOpen) noexcept
      : std::ostream(&streambuf_), streambuf_(ObjectState::kInitiallyOpen) {}

  WriterOstreamBase(WriterOstreamBase&& that) noexcept;
  WriterOstreamBase& operator=(WriterOstreamBase&& that) noexcept;

  void Reset(ObjectState::InitiallyClosed);
  void Reset(ObjectState::InitiallyOpen);
  void Initialize(Writer* dest);

  internal::WriterStreambuf streambuf_;

  // Invariant: `rdbuf() == &streambuf_`
};

// Adapts a `Writer` to a `std::ostream`.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the `Writer`. `Dest` must support
// `Dependency<Writer*, Dest>`, e.g. `Writer*` (not owned, default),
// `std::unique_ptr<Writer>` (owned), `ChainWriter<>` (owned).
//
// By relying on CTAD the template argument can be deduced as the value type of
// the first constructor argument. This requires C++17.
//
// The `Writer` must not be accessed until the `WriterOstream` is closed or no
// longer used, except that it is allowed to read the destination of the
// `Writer` immediately after `flush()`.
//
// Destroying or assigning to a `WriterOstream` closes it first.
template <typename Dest = Writer*>
class WriterOstream : public WriterOstreamBase {
 public:
  // Creates a closed `WriterOstream`.
  WriterOstream() noexcept : WriterOstreamBase(ObjectState::kInitiallyClosed) {}

  // Will write to the `Writer` provided by `dest`.
  explicit WriterOstream(const Dest& dest);
  explicit WriterOstream(Dest&& dest);

  // Will write to the `Writer` provided by a `Dest` constructed from elements
  // of `dest_args`. This avoids constructing a temporary `Dest` and moving from
  // it.
  template <typename... DestArgs>
  explicit WriterOstream(std::tuple<DestArgs...> dest_args);

  WriterOstream(WriterOstream&& that) noexcept;
  WriterOstream& operator=(WriterOstream&& that) noexcept;

  ~WriterOstream() override { close(); }

  // Makes `*this` equivalent to a newly constructed `WriterOstream`. This
  // avoids constructing a temporary `WriterOstream` and moving from it.
  void Reset();
  void Reset(const Dest& dest);
  void Reset(Dest&& dest);
  template <typename... DestArgs>
  void Reset(std::tuple<DestArgs...> dest_args);

  // Returns the object providing and possibly owning the `Writer`. Unchanged by
  // `close()`.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  Writer* dest_writer() override { return dest_.get(); }
  const Writer* dest_writer() const override { return dest_.get(); }

  WriterOstream& close() override;

 private:
  void MoveDest(WriterOstream&& that);

  // The object providing and possibly owning the `Writer`.
  Dependency<Writer*, Dest> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
template <typename Dest>
WriterOstream(Dest&& dest) -> WriterOstream<std::decay_t<Dest>>;
template <typename... DestArgs>
WriterOstream(std::tuple<DestArgs...> dest_args)
    -> WriterOstream<void>;  // Delete.
#endif

// Implementation details follow.

namespace internal {

inline WriterStreambuf::WriterStreambuf(WriterStreambuf&& that) noexcept
    : std::streambuf(that), state_(std::move(that.state_)), dest_(that.dest_) {
  that.setp(nullptr, nullptr);
}

inline WriterStreambuf& WriterStreambuf::operator=(
    WriterStreambuf&& that) noexcept {
  if (ABSL_PREDICT_TRUE(&that != this)) {
    std::streambuf::operator=(that);
    state_ = std::move(that.state_);
    dest_ = that.dest_;
    that.setp(nullptr, nullptr);
  }
  return *this;
}

inline void WriterStreambuf::Initialize(Writer* dest) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of WriterStreambuf: null Writer pointer";
  dest_ = dest;
  setp(dest_->cursor(), dest_->limit());
  if (ABSL_PREDICT_FALSE(!dest_->healthy())) Fail();
}

inline void WriterStreambuf::MoveBegin() { dest_->set_cursor(pptr()); }

inline void WriterStreambuf::MoveEnd(Writer* dest) {
  dest_ = dest;
  setp(dest_->cursor(), dest_->limit());
}

inline void WriterStreambuf::Done() {
  dest_->set_cursor(pptr());
  setp(nullptr, nullptr);
}

}  // namespace internal

inline WriterOstreamBase::WriterOstreamBase(WriterOstreamBase&& that) noexcept
    : std::ostream(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      streambuf_(std::move(that.streambuf_)) {
  set_rdbuf(&streambuf_);
}

inline WriterOstreamBase& WriterOstreamBase::operator=(
    WriterOstreamBase&& that) noexcept {
  std::ostream::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  streambuf_ = std::move(that.streambuf_);
  return *this;
}

inline void WriterOstreamBase::Reset(ObjectState::InitiallyClosed) {
  streambuf_ = internal::WriterStreambuf(ObjectState::kInitiallyClosed);
  init(&streambuf_);
}

inline void WriterOstreamBase::Reset(ObjectState::InitiallyOpen) {
  streambuf_ = internal::WriterStreambuf(ObjectState::kInitiallyOpen);
  init(&streambuf_);
}

inline void WriterOstreamBase::Initialize(Writer* dest) {
  streambuf_.Initialize(dest);
  if (ABSL_PREDICT_FALSE(!streambuf_.healthy())) {
    setstate(std::ios_base::badbit);
  }
}

template <typename Dest>
inline WriterOstream<Dest>::WriterOstream(const Dest& dest)
    : WriterOstreamBase(ObjectState::kInitiallyOpen), dest_(dest) {
  Initialize(dest_.get());
}

template <typename Dest>
inline WriterOstream<Dest>::WriterOstream(Dest&& dest)
    : WriterOstreamBase(ObjectState::kInitiallyOpen), dest_(std::move(dest)) {
  Initialize(dest_.get());
}

template <typename Dest>
template <typename... DestArgs>
inline WriterOstream<Dest>::WriterOstream(std::tuple<DestArgs...> dest_args)
    : WriterOstreamBase(ObjectState::kInitiallyOpen),
      dest_(std::move(dest_args)) {
  Initialize(dest_.get());
}

template <typename Dest>
inline WriterOstream<Dest>::WriterOstream(WriterOstream&& that) noexcept
    : WriterOstreamBase(std::move(that)) {
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  MoveDest(std::move(that));
}

template <typename Dest>
inline WriterOstream<Dest>& WriterOstream<Dest>::operator=(
    WriterOstream&& that) noexcept {
  if (ABSL_PREDICT_TRUE(&that != this)) {
    close();
    WriterOstreamBase::operator=(std::move(that));
    // Using `that` after it was moved is correct because only the base class
    // part was moved.
    MoveDest(std::move(that));
  }
  return *this;
}

template <typename Dest>
inline void WriterOstream<Dest>::Reset() {
  close();
  WriterOstreamBase::Reset(ObjectState::kInitiallyClosed);
  dest_.Reset();
}

template <typename Dest>
inline void WriterOstream<Dest>::Reset(const Dest& dest) {
  close();
  WriterOstreamBase::Reset(ObjectState::kInitiallyOpen);
  dest_.Reset(dest);
  Initialize(dest_.get());
}

template <typename Dest>
inline void WriterOstream<Dest>::Reset(Dest&& dest) {
  close();
  WriterOstreamBase::Reset(ObjectState::kInitiallyOpen);
  dest_.Reset(std::move(dest));
  Initialize(dest_.get());
}

template <typename Dest>
template <typename... DestArgs>
inline void WriterOstream<Dest>::Reset(std::tuple<DestArgs...> dest_args) {
  close();
  WriterOstreamBase::Reset(ObjectState::kInitiallyOpen);
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get());
}

template <typename Dest>
inline void WriterOstream<Dest>::MoveDest(WriterOstream&& that) {
  if (dest_.kIsStable()) {
    dest_ = std::move(that.dest_);
  } else {
    streambuf_.MoveBegin();
    dest_ = std::move(that.dest_);
    streambuf_.MoveEnd(dest_.get());
  }
}

template <typename Dest>
inline WriterOstream<Dest>& WriterOstream<Dest>::close() {
  if (ABSL_PREDICT_TRUE(is_open())) {
    streambuf_.Done();
    if (dest_.is_owning()) {
      if (ABSL_PREDICT_FALSE(!dest_->Close())) streambuf_.Fail();
    }
    if (ABSL_PREDICT_FALSE(!streambuf_.healthy())) {
      setstate(std::ios_base::badbit);
    }
    streambuf_.MarkClosed();
  }
  return *this;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_WRITER_OSTREAM_H_
