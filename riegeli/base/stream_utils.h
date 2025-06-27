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

#ifndef RIEGELI_BASE_STREAM_UTILS_H_
#define RIEGELI_BASE_STREAM_UTILS_H_

#include <stddef.h>

#include <cassert>
#include <cstring>
#include <ios>
#include <ostream>
#include <streambuf>
#include <string>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/strings/string_view.h"

namespace riegeli {

// Writes `length` copies of `fill` to `dest`.
void WritePadding(std::ostream& dest, size_t length, char fill);

// Writes a value to `dest`, including padding configured in `dest`.
// Resets `dest.width()` to 0 afterwards.
//
// `length` is the number of characters in the value. `callback()` is called
// to write the value; it should use unformatted output, i.e. `dest.write()`.
template <typename Callback>
void WriteWithPadding(std::ostream& dest, size_t length, Callback&& callback) {
  std::ostream::sentry sentry(dest);
  if (sentry) {
    size_t left_pad = 0;
    size_t right_pad = 0;
    if (dest.width() > 0 && static_cast<size_t>(dest.width()) > length) {
      const size_t pad = static_cast<size_t>(dest.width()) - length;
      if ((dest.flags() & dest.adjustfield) == dest.left) {
        right_pad = pad;
      } else {
        left_pad = pad;
      }
    }
    if (left_pad > 0) WritePadding(dest, left_pad, dest.fill());
    std::forward<Callback>(callback)();
    if (right_pad > 0) WritePadding(dest, right_pad, dest.fill());
    dest.width(0);
  }
}

// A sink for `AbslStringify()` which writes to an array. The array must have
// sufficient size.
class UncheckedArrayAbslStringifySink {
 public:
  explicit UncheckedArrayAbslStringifySink(
      char* dest ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : cursor_(dest) {}

  UncheckedArrayAbslStringifySink(const UncheckedArrayAbslStringifySink& that) =
      default;
  UncheckedArrayAbslStringifySink& operator=(
      const UncheckedArrayAbslStringifySink& that) = default;

  char* cursor() { return cursor_; }

  void Append(size_t length, char fill) {
    std::memset(cursor_, fill, length);
    cursor_ += length;
  }
  void Append(absl::string_view src) {
    std::memcpy(cursor_, src.data(), src.size());
    cursor_ += src.size();
  }
  friend void AbslFormatFlush(UncheckedArrayAbslStringifySink* dest,
                              absl::string_view src) {
    dest->Append(src);
  }

 private:
  char* cursor_;
};

// A sink for `AbslStringify()` which appends to a `std::string`.
class StringAbslStringifySink {
 public:
  explicit StringAbslStringifySink(
      std::string* dest ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : dest_(dest) {}

  StringAbslStringifySink(const StringAbslStringifySink& that) = default;
  StringAbslStringifySink& operator=(const StringAbslStringifySink& that) =
      default;

  std::string* dest() { return dest_; }

  void Append(size_t length, char fill) { dest_->append(length, fill); }
  void Append(absl::string_view src) {
    // TODO: When `absl::string_view` becomes C++17 `std::string_view`:
    // `dest_->append(src)`
    dest_->append(src.data(), src.size());
  }
  friend void AbslFormatFlush(StringAbslStringifySink* dest,
                              absl::string_view src) {
    dest->Append(src);
  }

 private:
  std::string* dest_;
};

// Adapts `std::ostream` to a sink for `AbslStringify()`.
class OStreamAbslStringifySink {
 public:
  explicit OStreamAbslStringifySink(
      std::ostream* dest ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : dest_(dest) {}

  OStreamAbslStringifySink(const OStreamAbslStringifySink& that) = default;
  OStreamAbslStringifySink& operator=(const OStreamAbslStringifySink& that) =
      default;

  std::ostream* dest() const { return dest_; }

  void Append(size_t length, char fill) { WritePadding(*dest_, length, fill); }
  void Append(absl::string_view src) {
    dest_->write(src.data(), static_cast<std::streamsize>(src.size()));
  }
  friend void AbslFormatFlush(OStreamAbslStringifySink* dest,
                              absl::string_view src) {
    dest->Append(src);
  }

 private:
  std::ostream* dest_;
};

// Adapts a sink for `AbslStringify()` to `std::ostream`.
template <typename Sink>
class AbslStringifyOStream final : public std::ostream {
 public:
  explicit AbslStringifyOStream(Sink* dest ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : std::ostream(&streambuf_), streambuf_(dest) {}

  AbslStringifyOStream(AbslStringifyOStream&& that) noexcept
      : std::ostream(static_cast<std::ostream&&>(that)),
        streambuf_(std::move(that.streambuf_)) {
    set_rdbuf(&streambuf_);
  }
  AbslStringifyOStream& operator=(AbslStringifyOStream&& that) noexcept {
    std::ostream::operator=(static_cast<std::ostream&&>(that));
    streambuf_ = std::move(that.streambuf_);
    return *this;
  }

 private:
  class AbslStringifyStreambuf;

  AbslStringifyStreambuf streambuf_;
};

template <typename Sink>
explicit AbslStringifyOStream(Sink* dest) -> AbslStringifyOStream<Sink>;

template <>
class AbslStringifyOStream<StringAbslStringifySink> final
    : public std::ostream {
 public:
  explicit AbslStringifyOStream(std::string* dest ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : std::ostream(&streambuf_), streambuf_(dest) {}

  explicit AbslStringifyOStream(
      StringAbslStringifySink* sink ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : AbslStringifyOStream(sink->dest()) {}

  AbslStringifyOStream(AbslStringifyOStream&& that) noexcept
      : std::ostream(static_cast<std::ostream&&>(that)),
        streambuf_(std::move(that.streambuf_)) {
    set_rdbuf(&streambuf_);
  }
  AbslStringifyOStream& operator=(AbslStringifyOStream&& that) noexcept {
    std::ostream::operator=(static_cast<std::ostream&&>(that));
    streambuf_ = std::move(that.streambuf_);
    return *this;
  }

  std::string* dest() const { return streambuf_.dest(); }

 private:
  class StringStreambuf final : public std::streambuf {
   public:
    explicit StringStreambuf(std::string* dest ABSL_ATTRIBUTE_LIFETIME_BOUND)
        : dest_(dest) {}

    StringStreambuf(const StringStreambuf& that) = default;
    StringStreambuf& operator=(const StringStreambuf& that) = default;

    std::string* dest() const { return dest_; }

   protected:
    int overflow(int src) override;
    std::streamsize xsputn(const char* src, std::streamsize length) override;

   private:
    std::string* dest_;
  };

  StringStreambuf streambuf_;
};

// A faster version of `std::ostringstream`. It does not own the `std::string`
// and does not support random access.
//
// This is similar to `absl::strings_internal::OStringStream`.
using StringOStream = AbslStringifyOStream<StringAbslStringifySink>;

// Implementation details follow.

template <typename Sink>
class AbslStringifyOStream<Sink>::AbslStringifyStreambuf final
    : public std::streambuf {
 public:
  explicit AbslStringifyStreambuf(Sink* dest ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : dest_(dest) {}

  AbslStringifyStreambuf(const AbslStringifyStreambuf& that) = default;
  AbslStringifyStreambuf& operator=(const AbslStringifyStreambuf& that) =
      default;

 protected:
  int overflow(int src) override;
  std::streamsize xsputn(const char* src, std::streamsize length) override;

 private:
  Sink* dest_;
};

template <typename Sink>
int AbslStringifyOStream<Sink>::AbslStringifyStreambuf::overflow(int src) {
  if (src != traits_type::eof()) {
    const char ch = static_cast<char>(src);
    dest_->Append(absl::string_view(&ch, 1));
  }
  return traits_type::not_eof(src);
}

template <typename Sink>
std::streamsize AbslStringifyOStream<Sink>::AbslStringifyStreambuf::xsputn(
    const char* src, std::streamsize length) {
  assert(length >= 0);
  dest_->Append(absl::string_view(src, static_cast<size_t>(length)));
  return length;
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_STREAM_UTILS_H_
