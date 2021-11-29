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

#include <iostream>
#include <streambuf>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

namespace internal {

class WriterStreambuf : public std::streambuf {
 public:
  explicit WriterStreambuf(Closed) noexcept : state_(kClosed) {}

  WriterStreambuf() noexcept {}

  WriterStreambuf(WriterStreambuf&& that) noexcept;
  WriterStreambuf& operator=(WriterStreambuf&& that) noexcept;

  void Initialize(Writer* dest);
  absl::optional<Position> MoveBegin();
  void MoveEnd(Writer* dest, absl::optional<Position> reader_pos);
  void Done();

  bool healthy() const { return state_.healthy(); }
  bool is_open() const { return state_.is_open(); }
  absl::Status status() const { return state_.status(); }
  void MarkClosed() { state_.MarkClosed(); }
  ABSL_ATTRIBUTE_COLD void FailReader();
  ABSL_ATTRIBUTE_COLD void FailWriter();

 protected:
  int sync() override;
  std::streamsize showmanyc() override;
  int underflow() override;
  std::streamsize xsgetn(char* dest, std::streamsize length) override;
  int overflow(int ch) override;
  std::streamsize xsputn(const char* src, std::streamsize length) override;
  std::streampos seekoff(std::streamoff off, std::ios_base::seekdir dir,
                         std::ios_base::openmode which) override;
  std::streampos seekpos(std::streampos pos,
                         std::ios_base::openmode which) override;

 private:
  class BufferSync;

  bool ReadMode();
  bool WriteMode();

  ObjectState state_;
  Writer* writer_ = nullptr;
  // If `nullptr`, `*writer_` was used last time. If not `nullptr`, `*reader_`
  // was used last time.
  Reader* reader_ = nullptr;

  // Invariants:
  //   `is_open() && reader_ == nullptr ? pbase() >= writer_->start()
  //                                    : pbase() == nullptr`
  //   `epptr() == (is_open() && reader_ == nullptr ? writer_->limit()
  //                                                : nullptr)`
  //   `eback() == (is_open() && reader_ != nullptr ? reader_->start()
  //                                                : nullptr)`
  //   `egptr() == (is_open() && reader_ != nullptr ? reader_->limit()
  //                                                : nullptr)`
};

}  // namespace internal

// Template parameter independent part of `WriterOstream`.
class WriterOstreamBase : public std::iostream {
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

  // Returns `true` if the `WriterOstream` is healthy, i.e. open and not
  // failed.
  bool healthy() const { return streambuf_.healthy(); }

  // Returns `true` if the `WriterOstream` is open, i.e. not closed.
  bool is_open() const { return streambuf_.is_open(); }

  // Returns an `absl::Status` describing the failure if the `WriterOstream`
  // is failed, or an `absl::FailedPreconditionError()` if the `WriterOstream`
  // is closed, or `absl::OkStatus()` if the `WriterOstream` is healthy.
  absl::Status status() const { return streambuf_.status(); }

 protected:
  explicit WriterOstreamBase(Closed) noexcept
      : std::iostream(&streambuf_), streambuf_(kClosed) {}

  WriterOstreamBase() noexcept : std::iostream(&streambuf_) {}

  WriterOstreamBase(WriterOstreamBase&& that) noexcept;
  WriterOstreamBase& operator=(WriterOstreamBase&& that) noexcept;

  void Reset(Closed);
  void Reset();
  void Initialize(Writer* dest);

  internal::WriterStreambuf streambuf_;

  // Invariant: `rdbuf() == &streambuf_`
};

// Adapts a `Writer` to a `std::iostream`.
//
// The `std::iostream` supports reading and writing if
// `Writer::SupportsReadMode()`, with a single position maintained for both
// reading and writing. Otherwise the `std::iostream` is write-only, and only
// the `std::ostream` aspect of it is functional.
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
  explicit WriterOstream(Closed) noexcept : WriterOstreamBase(kClosed) {}

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
  void Reset(Closed);
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
explicit WriterOstream(Closed)->WriterOstream<DeleteCtad<Closed>>;
template <typename Dest>
explicit WriterOstream(const Dest& dest) -> WriterOstream<std::decay_t<Dest>>;
template <typename Dest>
explicit WriterOstream(Dest&& dest) -> WriterOstream<std::decay_t<Dest>>;
template <typename... DestArgs>
explicit WriterOstream(std::tuple<DestArgs...> dest_args)
    -> WriterOstream<DeleteCtad<std::tuple<DestArgs...>>>;
#endif

// Implementation details follow.

namespace internal {

inline WriterStreambuf::WriterStreambuf(WriterStreambuf&& that) noexcept
    : std::streambuf(that),
      state_(std::move(that.state_)),
      writer_(that.writer_),
      reader_(that.reader_) {
  that.setg(nullptr, nullptr, nullptr);
  that.setp(nullptr, nullptr);
}

inline WriterStreambuf& WriterStreambuf::operator=(
    WriterStreambuf&& that) noexcept {
  if (ABSL_PREDICT_TRUE(&that != this)) {
    std::streambuf::operator=(that);
    state_ = std::move(that.state_);
    writer_ = that.writer_;
    reader_ = that.reader_;
    that.setg(nullptr, nullptr, nullptr);
    that.setp(nullptr, nullptr);
  }
  return *this;
}

inline void WriterStreambuf::Initialize(Writer* dest) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of WriterStreambuf: null Writer pointer";
  writer_ = dest;
  setp(writer_->cursor(), writer_->limit());
  if (ABSL_PREDICT_FALSE(!writer_->healthy())) FailWriter();
}

inline absl::optional<Position> WriterStreambuf::MoveBegin() {
  // In a closed `WriterOstream`, `WriterOstream::writer_.get() != nullptr`
  // does not imply `WriterStreambuf::writer_ != nullptr`, because
  // `WriterOstream::streambuf_` can be left uninitialized.
  if (writer_ == nullptr) return absl::nullopt;
  if (reader_ != nullptr) {
    reader_->set_cursor(gptr());
    return reader_->pos();
  } else {
    writer_->set_cursor(pptr());
    return absl::nullopt;
  }
}

inline void WriterStreambuf::Done() {
  if (reader_ != nullptr) {
    reader_->set_cursor(gptr());
    setg(nullptr, nullptr, nullptr);
  } else {
    writer_->set_cursor(pptr());
    setp(nullptr, nullptr);
  }
}

}  // namespace internal

inline WriterOstreamBase::WriterOstreamBase(WriterOstreamBase&& that) noexcept
    : std::iostream(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      streambuf_(std::move(that.streambuf_)) {
  set_rdbuf(&streambuf_);
}

inline WriterOstreamBase& WriterOstreamBase::operator=(
    WriterOstreamBase&& that) noexcept {
  std::iostream::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  streambuf_ = std::move(that.streambuf_);
  return *this;
}

inline void WriterOstreamBase::Reset(Closed) {
  streambuf_ = internal::WriterStreambuf(kClosed);
  init(&streambuf_);
}

inline void WriterOstreamBase::Reset() {
  streambuf_ = internal::WriterStreambuf();
  init(&streambuf_);
}

inline void WriterOstreamBase::Initialize(Writer* dest) {
  streambuf_.Initialize(dest);
  if (ABSL_PREDICT_FALSE(!streambuf_.healthy())) {
    setstate(std::ios_base::badbit);
  }
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
inline void WriterOstream<Dest>::Reset(Closed) {
  close();
  WriterOstreamBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void WriterOstream<Dest>::Reset(const Dest& dest) {
  close();
  WriterOstreamBase::Reset();
  dest_.Reset(dest);
  Initialize(dest_.get());
}

template <typename Dest>
inline void WriterOstream<Dest>::Reset(Dest&& dest) {
  close();
  WriterOstreamBase::Reset();
  dest_.Reset(std::move(dest));
  Initialize(dest_.get());
}

template <typename Dest>
template <typename... DestArgs>
inline void WriterOstream<Dest>::Reset(std::tuple<DestArgs...> dest_args) {
  close();
  WriterOstreamBase::Reset();
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get());
}

template <typename Dest>
inline void WriterOstream<Dest>::MoveDest(WriterOstream&& that) {
  if (dest_.kIsStable()) {
    dest_ = std::move(that.dest_);
  } else {
    const absl::optional<Position> reader_pos = streambuf_.MoveBegin();
    dest_ = std::move(that.dest_);
    streambuf_.MoveEnd(dest_.get(), reader_pos);
  }
}

template <typename Dest>
inline WriterOstream<Dest>& WriterOstream<Dest>::close() {
  if (ABSL_PREDICT_TRUE(is_open())) {
    streambuf_.Done();
    if (dest_.is_owning()) {
      if (ABSL_PREDICT_FALSE(!dest_->Close())) streambuf_.FailWriter();
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
