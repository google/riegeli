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

#include <ios>
#include <iosfwd>
#include <istream>
#include <streambuf>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/types/optional.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

class Reader;

namespace stream_internal {

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

  bool ok() const { return state_.ok(); }
  bool is_open() const { return state_.is_open(); }
  bool not_failed() const { return state_.not_failed(); }
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

}  // namespace stream_internal

// Template parameter independent part of `WriterOStream`.
class WriterOStreamBase : public std::iostream {
 public:
  class Options {
   public:
    Options() noexcept {}
  };

  // Returns the `Writer`. Unchanged by `close()`.
  virtual Writer* DestWriter() const = 0;

  // If `!is_open()`, does nothing. Otherwise:
  //  * Synchronizes the current `WriterOStream` position to the `Writer`.
  //  * Closes the `Writer` if it is owned.
  //
  // Returns `true` if the `Writer` did not fail, i.e. if it was OK just before
  // becoming closed.
  //
  // Destroying or assigning to a `WriterOStream` closes it implicitly, but an
  // explicit `close()` call allows to detect failures (use `status()` for
  // failure details).
  bool close();

  // Returns `true` if the `WriterOStream` is OK, i.e. open and not failed.
  bool ok() const { return streambuf_.ok(); }

  // Returns `true` if the `WriterOStream` is open, i.e. not closed.
  bool is_open() const { return streambuf_.is_open(); }

  // Returns `true` if the `WriterOStream` is not failed.
  bool not_failed() const { return streambuf_.not_failed(); }

  // Returns an `absl::Status` describing the failure if the `WriterOStream`
  // is failed, or an `absl::FailedPreconditionError()` if the `WriterOStream`
  // is successfully closed, or `absl::OkStatus()` if the `WriterOStream` is OK.
  absl::Status status() const { return streambuf_.status(); }

  // Support `Dependency`.
  friend std::tuple<Closed> RiegeliDependencySentinel(WriterOStreamBase*) {
    return {kClosed};
  }

 protected:
  explicit WriterOStreamBase(Closed) noexcept
      : std::iostream(&streambuf_), streambuf_(kClosed) {}

  WriterOStreamBase() noexcept : std::iostream(&streambuf_) {}

  WriterOStreamBase(WriterOStreamBase&& that) noexcept;
  WriterOStreamBase& operator=(WriterOStreamBase&& that) noexcept;

  void Reset(Closed);
  void Reset();
  void Initialize(Writer* dest);

  virtual void Done() = 0;

  stream_internal::WriterStreambuf streambuf_;

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
// `ChainWriter<>` (owned), `std::unique_ptr<Writer>` (owned),
// `AnyDependency<Writer*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as the value type of
// the first constructor argument. This requires C++17.
//
// The `Writer` must not be accessed until the `WriterOStream` is closed or no
// longer used, except that it is allowed to read the destination of the
// `Writer` immediately after `flush()`.
//
// Destroying or assigning to a `WriterOStream` closes it first.
template <typename Dest = Writer*>
class WriterOStream : public WriterOStreamBase {
 public:
  // Creates a closed `WriterOStream`.
  explicit WriterOStream(Closed) noexcept : WriterOStreamBase(kClosed) {}

  // Will write to the `Writer` provided by `dest`.
  explicit WriterOStream(Initializer<Dest> dest, Options options = Options());

  WriterOStream(WriterOStream&& that) noexcept;
  WriterOStream& operator=(WriterOStream&& that) noexcept;

  ~WriterOStream() override { Done(); }

  // Makes `*this` equivalent to a newly constructed `WriterOStream`. This
  // avoids constructing a temporary `WriterOStream` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Dest> dest,
                                          Options options = Options());

  // Returns the object providing and possibly owning the `Writer`. Unchanged by
  // `close()`.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  Writer* DestWriter() const override { return dest_.get(); }

 protected:
  void Done() override;

 private:
  // Moves `that.dest_` to `dest_`. Buffer pointers are already moved from
  // `dest_` to `*this`; adjust them to match `dest_`.
  void MoveDest(WriterOStream&& that);

  // The object providing and possibly owning the `Writer`.
  Dependency<Writer*, Dest> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit WriterOStream(Closed) -> WriterOStream<DeleteCtad<Closed>>;
template <typename Dest>
explicit WriterOStream(Dest&& dest, WriterOStreamBase::Options options =
                                        WriterOStreamBase::Options())
    -> WriterOStream<std::decay_t<Dest>>;
template <typename... DestArgs>
explicit WriterOStream(
    std::tuple<DestArgs...> dest_args,
    WriterOStreamBase::Options options = WriterOStreamBase::Options())
    -> WriterOStream<DeleteCtad<std::tuple<DestArgs...>>>;
#endif

// Implementation details follow.

namespace stream_internal {

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
  if (ABSL_PREDICT_FALSE(!writer_->ok())) FailWriter();
}

}  // namespace stream_internal

inline WriterOStreamBase::WriterOStreamBase(WriterOStreamBase&& that) noexcept
    : std::iostream(static_cast<std::iostream&&>(that)),
      streambuf_(std::move(that.streambuf_)) {
  set_rdbuf(&streambuf_);
}

inline WriterOStreamBase& WriterOStreamBase::operator=(
    WriterOStreamBase&& that) noexcept {
  std::iostream::operator=(static_cast<std::iostream&&>(that));
  streambuf_ = std::move(that.streambuf_);
  return *this;
}

inline void WriterOStreamBase::Reset(Closed) {
  streambuf_ = stream_internal::WriterStreambuf(kClosed);
  init(&streambuf_);
}

inline void WriterOStreamBase::Reset() {
  streambuf_ = stream_internal::WriterStreambuf();
  init(&streambuf_);
}

inline void WriterOStreamBase::Initialize(Writer* dest) {
  streambuf_.Initialize(dest);
  if (ABSL_PREDICT_FALSE(!streambuf_.ok())) setstate(std::ios_base::badbit);
}

template <typename Dest>
inline WriterOStream<Dest>::WriterOStream(Initializer<Dest> dest,
                                          Options options)
    : dest_(std::move(dest)) {
  Initialize(dest_.get());
}

template <typename Dest>
inline WriterOStream<Dest>::WriterOStream(WriterOStream&& that) noexcept
    : WriterOStreamBase(static_cast<WriterOStreamBase&&>(that)) {
  MoveDest(std::move(that));
}

template <typename Dest>
inline WriterOStream<Dest>& WriterOStream<Dest>::operator=(
    WriterOStream&& that) noexcept {
  if (ABSL_PREDICT_TRUE(&that != this)) {
    Done();
    WriterOStreamBase::operator=(static_cast<WriterOStreamBase&&>(that));
    MoveDest(std::move(that));
  }
  return *this;
}

template <typename Dest>
inline void WriterOStream<Dest>::Reset(Closed) {
  Done();
  WriterOStreamBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void WriterOStream<Dest>::Reset(Initializer<Dest> dest,
                                       Options options) {
  Done();
  WriterOStreamBase::Reset();
  dest_.Reset(std::move(dest));
  Initialize(dest_.get());
}

template <typename Dest>
inline void WriterOStream<Dest>::MoveDest(WriterOStream&& that) {
  if (dest_.kIsStable) {
    dest_ = std::move(that.dest_);
  } else {
    const absl::optional<Position> reader_pos = streambuf_.MoveBegin();
    dest_ = std::move(that.dest_);
    streambuf_.MoveEnd(dest_.get(), reader_pos);
  }
}

template <typename Dest>
void WriterOStream<Dest>::Done() {
  if (ABSL_PREDICT_TRUE(is_open())) {
    streambuf_.Done();
    if (dest_.IsOwning()) {
      if (ABSL_PREDICT_FALSE(!dest_->Close())) streambuf_.FailWriter();
    }
    if (ABSL_PREDICT_FALSE(!streambuf_.ok())) setstate(std::ios_base::badbit);
    streambuf_.MarkClosed();
  }
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_WRITER_OSTREAM_H_
