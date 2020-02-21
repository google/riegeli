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
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "riegeli/base/base.h"
#include "riegeli/base/dependency.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

namespace internal {

class WriterStreambuf : public std::streambuf {
 public:
  WriterStreambuf() noexcept {}

  WriterStreambuf(WriterStreambuf&& that) noexcept;
  WriterStreambuf& operator=(WriterStreambuf&& that) noexcept;

  void Initialize(Writer* dest);
  void MoveBegin();
  void MoveEnd(Writer* dest);
  void close();
  bool is_open() const { return dest_ != nullptr; }
  absl::Status status() const;

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

  Writer* dest_ = nullptr;

  // Invariants if `is_open()`:
  //   `pbase() >= dest_->start()` else `pbase() == nullptr`
  //   `epptr() == dest_->limit()` else `epptr() == nullptr`
};

}  // namespace internal

// Template parameter independent part of `WriterOstream`.
class WriterOstreamBase : public std::ostream {
 public:
  // Returns the `Writer`. Unchanged by `Close()`.
  virtual Writer* dest_writer() = 0;
  virtual const Writer* dest_writer() const = 0;

  // If `!is_open()`, does nothing. Otherwise synchronizes the `Writer` to
  // account for data written to the `WriterOstream`, and closes the `Writer`
  // if it is owned.
  //
  // Returns `*this` for convenience of checking for failures.
  //
  // Destroying or assigning to a `WriterOstream` closes it implicitly, but an
  // explicit `close()` call allows to detect failures (use `status()` for
  // failure details).
  virtual WriterOstreamBase& close() = 0;

  // Returns `true` if the `WriterOstream` is open.
  bool is_open() const { return streambuf_.is_open(); }

  // Returns an `absl::Status` describing the failure if the `WriterOstream` is
  // failed, or an `absl::FailedPreconditionError()` if the `WriterOstream` is
  // closed, or `absl::OkStatus()` if the `WriterOstream` is healthy.
  absl::Status status() const { return streambuf_.status(); }

 protected:
  WriterOstreamBase() noexcept : std::ostream(&streambuf_) {}

  WriterOstreamBase(WriterOstreamBase&& that) noexcept;
  WriterOstreamBase& operator=(WriterOstreamBase&& that) noexcept;

  void Initialize(Writer* dest);

 protected:
  internal::WriterStreambuf streambuf_;

  // Invariant: rdbuf() == &streambuf_
};

// Adapts a `Writer` to a `std::ostream`.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the `Writer`. `Dest` must support
// `Dependency<Writer*, Dest>`, e.g. `Writer*` (not owned, default),
// `std::unique_ptr<Writer>` (owned), `ChainWriter<>` (owned).
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
  WriterOstream() noexcept {}

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

  ~WriterOstream();

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

// Implementation details follow.

namespace internal {

inline WriterStreambuf::WriterStreambuf(WriterStreambuf&& that) noexcept
    : std::streambuf(that), dest_(std::exchange(that.dest_, nullptr)) {
  that.setp(nullptr, nullptr);
}

inline WriterStreambuf& WriterStreambuf::operator=(
    WriterStreambuf&& that) noexcept {
  if (ABSL_PREDICT_TRUE(&that != this)) {
    std::streambuf::operator=(that);
    dest_ = std::exchange(that.dest_, nullptr);
    that.setp(nullptr, nullptr);
  }
  return *this;
}

inline void WriterStreambuf::Initialize(Writer* dest) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of WriterStreambuf: null Writer pointer";
  dest_ = dest;
  setp(dest_->cursor(), dest_->limit());
}

inline void WriterStreambuf::MoveBegin() { dest_->set_cursor(pptr()); }

inline void WriterStreambuf::MoveEnd(Writer* dest) {
  dest_ = dest;
  setp(dest_->cursor(), dest_->limit());
}

inline void WriterStreambuf::close() {
  if (ABSL_PREDICT_FALSE(!is_open())) return;
  dest_->set_cursor(pptr());
  setp(nullptr, nullptr);
  dest_ = nullptr;
}

inline absl::Status WriterStreambuf::status() const {
  if (ABSL_PREDICT_FALSE(!is_open())) {
    return absl::FailedPreconditionError("Object closed");
  }
  return dest_->status();
}

}  // namespace internal

inline WriterOstreamBase::WriterOstreamBase(WriterOstreamBase&& that) noexcept
    : std::ostream(std::move(that)), streambuf_(std::move(that.streambuf_)) {
  set_rdbuf(&streambuf_);
}

inline WriterOstreamBase& WriterOstreamBase::operator=(
    WriterOstreamBase&& that) noexcept {
  std::ostream::operator=(std::move(that));
  streambuf_ = std::move(that.streambuf_);
  return *this;
}

inline void WriterOstreamBase::Initialize(Writer* dest) {
  streambuf_.Initialize(dest);
  if (ABSL_PREDICT_FALSE(!dest->healthy())) setstate(std::ios_base::badbit);
}

template <typename Dest>
inline WriterOstream<Dest>::WriterOstream(const Dest& dest) : dest_(dest) {
  Initialize(dest_.get());
}

template <typename Dest>
inline WriterOstream<Dest>::WriterOstream(Dest&& dest)
    : dest_(std::move(dest)) {
  Initialize(dest_.get());
}

template <typename Dest>
template <typename... DestArgs>
inline WriterOstream<Dest>::WriterOstream(std::tuple<DestArgs...> dest_args)
    : dest_(std::move(dest_args)) {
  Initialize(dest_.get());
}

template <typename Dest>
inline WriterOstream<Dest>::WriterOstream(WriterOstream&& that) noexcept
    : WriterOstreamBase(std::move(that)) {
  MoveDest(std::move(that));
}

template <typename Dest>
inline WriterOstream<Dest>& WriterOstream<Dest>::operator=(
    WriterOstream&& that) noexcept {
  if (ABSL_PREDICT_TRUE(&that != this)) {
    close();
    WriterOstreamBase::operator=(std::move(that));
    MoveDest(std::move(that));
  }
  return *this;
}

template <typename Dest>
inline WriterOstream<Dest>::~WriterOstream() {
  close();
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
    streambuf_.close();
    if (ABSL_PREDICT_FALSE(dest_.is_owning() ? !dest_->Close()
                                             : !dest_->healthy())) {
      setstate(std::ios_base::badbit);
    }
  }
  return *this;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_WRITER_OSTREAM_H_
