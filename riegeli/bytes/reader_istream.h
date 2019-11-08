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
#include <utility>

#include "absl/base/optimization.h"
#include "riegeli/base/base.h"
#include "riegeli/base/canonical_errors.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

namespace internal {

class ReaderStreambuf : public std::streambuf {
 public:
  ReaderStreambuf() noexcept {}

  ReaderStreambuf(ReaderStreambuf&& that) noexcept;
  ReaderStreambuf& operator=(ReaderStreambuf&& that) noexcept;

  void Initialize(Reader* src);
  void MoveBegin();
  void MoveEnd(Reader* src);
  void close();
  bool is_open() const { return src_ != nullptr; }
  Status status() const;

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

  Reader* src_ = nullptr;

  // Invariants if `is_open()`:
  //   `eback() == src_->start()` else `eback() == nullptr`
  //   `egptr() == src_->limit()` else `egptr() == nullptr`
};

}  // namespace internal

// Template parameter independent part of `ReaderIstream`.
class ReaderIstreamBase : public std::istream {
 public:
  // Returns the `Reader`. Unchanged by `Close()`.
  virtual Reader* src_reader() = 0;
  virtual const Reader* src_reader() const = 0;

  // If `!is_open()`, does nothing. Otherwise synchronizes the `Reader` to
  // account for data read from the `ReaderIstream`, and closes the `Reader`
  // if it is owned.
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

  // Returns `true` if the `ReaderIstream` is open.
  bool is_open() const { return streambuf_.is_open(); }

  // Returns a `Status` describing the failure if the `ReaderIstream` is failed,
  // or a `FailedPreconditionError()` if the `ReaderIstream` is closed, or
  // `OkStatus()` if the `ReaderIstream` is healthy.
  Status status() const { return streambuf_.status(); }

 protected:
  ReaderIstreamBase() noexcept : std::istream(&streambuf_) {}

  ReaderIstreamBase(ReaderIstreamBase&& that) noexcept;
  ReaderIstreamBase& operator=(ReaderIstreamBase&& that) noexcept;

  void Initialize(Reader* src);

 protected:
  internal::ReaderStreambuf streambuf_;

  // Invariant: rdbuf() == &streambuf_
};

// Adapts a `Reader` to a `std::istream`.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the `Reader`. `Src` must support `Dependency<Reader*, Src>`,
// e.g. `Reader*` (not owned, default), `std::unique_ptr<Reader>` (owned),
// `ChainReader<>` (owned).
//
// The `Reader` must not be accessed until the `ReaderIstream` is closed or no
// longer used.
template <typename Src = Reader*>
class ReaderIstream : public ReaderIstreamBase {
 public:
  // Creates a closed `ReaderIstream`.
  ReaderIstream() noexcept {}

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

  ~ReaderIstream();

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

// Implementation details follow.

namespace internal {

inline ReaderStreambuf::ReaderStreambuf(ReaderStreambuf&& that) noexcept
    : std::streambuf(that), src_(std::exchange(that.src_, nullptr)) {
  that.setg(nullptr, nullptr, nullptr);
}

inline ReaderStreambuf& ReaderStreambuf::operator=(
    ReaderStreambuf&& that) noexcept {
  if (ABSL_PREDICT_TRUE(&that != this)) {
    std::streambuf::operator=(that);
    src_ = std::exchange(that.src_, nullptr);
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
}

inline void ReaderStreambuf::MoveBegin() { src_->set_cursor(gptr()); }

inline void ReaderStreambuf::MoveEnd(Reader* src) {
  src_ = src;
  setg(const_cast<char*>(src_->start()), const_cast<char*>(src_->cursor()),
       const_cast<char*>(src_->limit()));
}

inline void ReaderStreambuf::close() {
  if (ABSL_PREDICT_FALSE(!is_open())) return;
  src_->set_cursor(gptr());
  setg(nullptr, nullptr, nullptr);
  src_ = nullptr;
}

inline Status ReaderStreambuf::status() const {
  if (ABSL_PREDICT_FALSE(!is_open())) {
    return FailedPreconditionError("Object closed");
  }
  return src_->status();
}

}  // namespace internal

inline ReaderIstreamBase::ReaderIstreamBase(ReaderIstreamBase&& that) noexcept
    : std::istream(std::move(that)), streambuf_(std::move(that.streambuf_)) {
  set_rdbuf(&streambuf_);
}

inline ReaderIstreamBase& ReaderIstreamBase::operator=(
    ReaderIstreamBase&& that) noexcept {
  std::istream::operator=(std::move(that));
  streambuf_ = std::move(that.streambuf_);
  return *this;
}

inline void ReaderIstreamBase::Initialize(Reader* src) {
  streambuf_.Initialize(src);
  if (ABSL_PREDICT_FALSE(!src->healthy()) && src->available() == 0) {
    setstate(std::ios_base::badbit);
  }
}

template <typename Src>
inline ReaderIstream<Src>::ReaderIstream(const Src& src) : src_(src) {
  Initialize(src_.get());
}

template <typename Src>
inline ReaderIstream<Src>::ReaderIstream(Src&& src) : src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline ReaderIstream<Src>::ReaderIstream(std::tuple<SrcArgs...> src_args)
    : src_(std::move(src_args)) {
  Initialize(src_.get());
}

template <typename Src>
inline ReaderIstream<Src>::ReaderIstream(ReaderIstream&& that) noexcept
    : ReaderIstreamBase(std::move(that)) {
  MoveSrc(std::move(that));
}

template <typename Src>
inline ReaderIstream<Src>& ReaderIstream<Src>::operator=(
    ReaderIstream&& that) noexcept {
  if (ABSL_PREDICT_TRUE(&that != this)) {
    close();
    ReaderIstreamBase::operator=(std::move(that));
    MoveSrc(std::move(that));
  }
  return *this;
}

template <typename Src>
inline ReaderIstream<Src>::~ReaderIstream() {
  close();
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
    streambuf_.close();
    if (ABSL_PREDICT_FALSE(src_.is_owning() ? !src_->Close()
                                            : !src_->healthy())) {
      setstate(std::ios_base::badbit);
    }
  }
  return *this;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_READER_ISTREAM_H_
