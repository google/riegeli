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
#include <ios>
#include <ostream>
#include <streambuf>
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

// Adapts `std::ostream` to a sink for `AbslStringify()`.
class OStreamAbslStringifySink {
 public:
  explicit OStreamAbslStringifySink(
      std::ostream* dest ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : dest_(dest) {}

  OStreamAbslStringifySink(const OStreamAbslStringifySink& that) = default;
  OStreamAbslStringifySink& operator=(const OStreamAbslStringifySink& that) =
      default;

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
class AbslStringifyOStream : public std::ostream {
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

// Implementation details follow.

template <typename Sink>
class AbslStringifyOStream<Sink>::AbslStringifyStreambuf
    : public std::streambuf {
 public:
  explicit AbslStringifyStreambuf(Sink* dest ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : dest_(dest) {}

  AbslStringifyStreambuf(const AbslStringifyStreambuf& that) = default;
  AbslStringifyStreambuf& operator=(const AbslStringifyStreambuf& that) =
      default;

 protected:
  int overflow(int src) override {
    if (src != traits_type::eof()) {
      const char ch = static_cast<char>(src);
      dest_->Append(absl::string_view(&ch, 1));
    }
    return traits_type::not_eof(src);
  }

  std::streamsize xsputn(const char* src, std::streamsize length) override {
    assert(length >= 0);
    dest_->Append(absl::string_view(src, static_cast<size_t>(length)));
    return length;
  }

 private:
  Sink* dest_;
};

}  // namespace riegeli

#endif  // RIEGELI_BASE_STREAM_UTILS_H_
