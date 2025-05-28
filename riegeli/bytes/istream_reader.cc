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

#include "riegeli/bytes/istream_reader.h"

#include <stddef.h>

#include <cerrno>
#include <ios>
#include <istream>
#include <limits>
#include <optional>
#include <string>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/global.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffered_reader.h"

namespace riegeli {

void IStreamReaderBase::Initialize(std::istream* src,
                                   std::optional<Position> assumed_pos) {
  RIEGELI_ASSERT_NE(src, nullptr)
      << "Failed precondition of IStreamReader: null stream pointer";
  RIEGELI_ASSERT(!supports_random_access_)
      << "Failed precondition of IStreamReaderBase::Initialize(): "
         "supports_random_access_ not reset";
  RIEGELI_ASSERT_OK(random_access_status_)
      << "Failed precondition of IStreamReaderBase::Initialize(): "
         "random_access_status_ not reset";
  if (ABSL_PREDICT_FALSE(src->fail())) {
    // Either constructing the stream failed or the stream was already in a
    // failed state. In any case `IStreamReaderBase` should fail.
    FailOperation("istream::istream()");
    return;
  }
  // A sticky `std::ios_base::eofbit` breaks future operations like
  // `std::istream::peek()` and `std::istream::tellg()`.
  src->clear(src->rdstate() & ~std::ios_base::eofbit);
  if (assumed_pos != std::nullopt) {
    if (ABSL_PREDICT_FALSE(
            *assumed_pos >
            Position{std::numeric_limits<std::streamoff>::max()})) {
      FailOverflow();
      return;
    }
    set_limit_pos(*assumed_pos);
    // `supports_random_access_` is left as `false`.
    random_access_status_ = Global([] {
      return absl::UnimplementedError(
          "IStreamReaderBase::Options::assumed_pos() excludes random access");
    });
  } else {
    errno = 0;
    const std::streamoff stream_pos = src->tellg();
    if (stream_pos < 0) {
      // Random access is not supported. Assume 0 as the initial position.
      // `supports_random_access_` is left as `false`.
      random_access_status_ = FailedOperationStatus("istream::tellg()");
      return;
    }
    set_limit_pos(IntCast<Position>(stream_pos));

    // Check the size, and whether random access is supported.
    src->seekg(0, std::ios_base::end);
    if (src->fail()) {
      // Random access is not supported. `supports_random_access_` is left as
      // `false`.
      random_access_status_ = FailedOperationStatus("istream::seekg()");
      src->clear(src->rdstate() & ~std::ios_base::failbit);
      return;
    }
    std::streamoff stream_size = src->tellg();
    if (ABSL_PREDICT_FALSE(stream_size < 0)) {
      FailOperation("istream::tellg()");
      return;
    }
    if (limit_pos() != IntCast<Position>(stream_size)) {
      src->seekg(IntCast<std::streamoff>(limit_pos()), std::ios_base::beg);
      if (ABSL_PREDICT_FALSE(src->fail())) {
        FailOperation("istream::seekg()");
        return;
      }
    }
#ifndef _WIN32
    if (stream_size == 0 && limit_pos() == 0) {
      // Some "/proc" and "/sys" files claim to have zero size but have
      // non-empty contents when read.
      if (BufferedReader::PullSlow(1, 0)) {
        if (growing_source_) {
          // Check the size again. Maybe the stream has grown.
          src->seekg(0, std::ios_base::end);
          if (ABSL_PREDICT_FALSE(src->fail())) {
            FailOperation("istream::seekg()");
            return;
          }
          stream_size = src->tellg();
          if (ABSL_PREDICT_FALSE(stream_size < 0)) {
            FailOperation("istream::tellg()");
            return;
          }
          if (limit_pos() != IntCast<Position>(stream_size)) {
            src->seekg(IntCast<std::streamoff>(limit_pos()),
                       std::ios_base::beg);
            if (ABSL_PREDICT_FALSE(src->fail())) {
              FailOperation("istream::seekg()");
              return;
            }
          }
          if (stream_size > 0) goto regular;
        }
        // This is one of "/proc" or "/sys" files which claim to have zero size
        // but have non-empty contents when read. Random access is not
        // supported. `supports_random_access_` is left as `false`.
        random_access_status_ = Global([] {
          return absl::UnimplementedError(
              "Random access is not supported because "
              "the file claims zero size but has non-empty contents when read");
        });
        return;
      }
      if (ABSL_PREDICT_FALSE(!ok())) return;
      // This is a regular empty stream.
    }
  regular:
#endif
    // Random access is supported.
    supports_random_access_ = true;
    if (!growing_source_) set_exact_size(IntCast<Position>(stream_size));
  }
  BeginRun();
}

void IStreamReaderBase::Done() {
  BufferedReader::Done();
  random_access_status_ = absl::OkStatus();
}

inline absl::Status IStreamReaderBase::FailedOperationStatus(
    absl::string_view operation) {
  // There is no way to get details why a stream operation failed without
  // letting the stream throw exceptions. Hopefully low level failures have set
  // `errno` as a side effect.
  //
  // This requires resetting `errno` to 0 before the stream operation because
  // the operation may fail without setting `errno`.
  const int error_number = errno;
  const std::string message = absl::StrCat(operation, " failed");
  return error_number == 0 ? absl::UnknownError(message)
                           : absl::ErrnoToStatus(error_number, message);
}

bool IStreamReaderBase::FailOperation(absl::string_view operation) {
  return Fail(FailedOperationStatus(operation));
}

bool IStreamReaderBase::ReadInternal(size_t min_length, size_t max_length,
                                     char* dest) {
  RIEGELI_ASSERT_GT(min_length, 0u)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "nothing to read";
  RIEGELI_ASSERT_GE(max_length, min_length)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "max_length < min_length";
  RIEGELI_ASSERT_OK(*this)
      << "Failed precondition of BufferedReader::ReadInternal()";
  std::istream& src = *SrcStream();
  errno = 0;
  for (;;) {
    if (ABSL_PREDICT_FALSE(
            limit_pos() >=
            Position{std::numeric_limits<std::streamoff>::max()})) {
      return FailOverflow();
    }
    std::streamsize length_to_read = IntCast<std::streamsize>(UnsignedMin(
        min_length,
        Position{std::numeric_limits<std::streamoff>::max()} - limit_pos()));
    const std::streamsize max_length_to_read =
        IntCast<std::streamsize>(UnsignedMin(
            max_length, Position{std::numeric_limits<std::streamoff>::max()} -
                            limit_pos()));
    std::streamsize length_read;
    if (length_to_read < max_length_to_read) {
      // Use `std::istream::readsome()` to read as much data as is available,
      // up to `max_length_to_read`.
      //
      // `std::istream::peek()` asks to read some characters into the buffer,
      // otherwise `std::istream::readsome()` may return 0.
      if (ABSL_PREDICT_FALSE(src.peek() == std::char_traits<char>::eof())) {
        if (ABSL_PREDICT_FALSE(src.fail())) {
          return FailOperation("istream::peek()");
        }
        // A sticky `std::ios_base::eofbit` breaks future operations like
        // `std::istream::peek()` and `std::istream::tellg()`.
        src.clear(src.rdstate() & ~std::ios_base::eofbit);
        if (!growing_source_) set_exact_size(limit_pos());
        return false;
      }
      length_read = src.readsome(dest, max_length_to_read);
      RIEGELI_ASSERT_GE(length_read, 0) << "negative istream::readsome()";
      RIEGELI_ASSERT_LE(length_read, max_length_to_read)
          << "istream::readsome() read more than requested";
      if (ABSL_PREDICT_TRUE(length_read > 0)) goto fragment_read;
      // `std::istream::peek()` returned non-`eof()` but
      // `std::istream::readsome()` returned 0. This might happen if
      // `src.rdbuf()->sgetc()` does not use the get area but leaves the next
      // character buffered elsewhere, e.g. for `std::cin` synchronized to
      // stdio. Fall back to `std::istream::read()`.
    }
    // Use `std::istream::read()` to read a fixed length of `length_to_read`.
    src.read(dest, length_to_read);
    length_read = src.gcount();
    RIEGELI_ASSERT_GE(length_read, 0) << "negative istream::gcount()";
    RIEGELI_ASSERT_LE(length_read, length_to_read)
        << "istream::read() read more than requested";
  fragment_read:
    move_limit_pos(IntCast<size_t>(length_read));
    if (ABSL_PREDICT_FALSE(src.fail())) {
      if (ABSL_PREDICT_FALSE(src.bad())) {
        FailOperation("istream::read()");
      } else {
        // End of stream is not a failure.
        //
        // A sticky `std::ios_base::eofbit` breaks future operations like
        // `std::istream::peek()` and `std::istream::tellg()`.
        src.clear(src.rdstate() &
                  ~(std::ios_base::eofbit | std::ios_base::failbit));
        if (!growing_source_) set_exact_size(limit_pos());
      }
      return IntCast<size_t>(length_read) >= min_length;
    }
    if (IntCast<size_t>(length_read) >= min_length) return true;
    dest += length_read;
    min_length -= IntCast<size_t>(length_read);
    max_length -= IntCast<size_t>(length_read);
  }
}

bool IStreamReaderBase::SeekBehindBuffer(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of BufferedReader::SeekBehindBuffer(): "
         "position in the buffer, use Seek() instead";
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedReader::SeekBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!IStreamReaderBase::SupportsRandomAccess())) {
    if (ABSL_PREDICT_FALSE(new_pos < start_pos())) {
      if (ok()) Fail(random_access_status_);
      return false;
    }
    return BufferedReader::SeekBehindBuffer(new_pos);
  }
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  std::istream& src = *SrcStream();
  errno = 0;
  if (new_pos > limit_pos()) {
    // Seeking forwards.
    if (exact_size() != std::nullopt) {
      if (ABSL_PREDICT_FALSE(new_pos > *exact_size())) {
        // Stream ends.
        src.seekg(IntCast<std::streamoff>(*exact_size()), std::ios_base::beg);
        if (ABSL_PREDICT_FALSE(src.fail())) {
          return FailOperation("istream::seekg()");
        }
        set_limit_pos(*exact_size());
        return false;
      }
    } else {
      src.seekg(0, std::ios_base::end);
      if (ABSL_PREDICT_FALSE(src.fail())) {
        return FailOperation("istream::seekg()");
      }
      const std::streamoff stream_size = src.tellg();
      if (ABSL_PREDICT_FALSE(stream_size < 0)) {
        return FailOperation("istream::tellg()");
      }
      if (!growing_source_) set_exact_size(IntCast<Position>(stream_size));
      if (ABSL_PREDICT_FALSE(new_pos > IntCast<Position>(stream_size))) {
        // Stream ends.
        set_limit_pos(IntCast<Position>(stream_size));
        return false;
      }
    }
  }
  src.seekg(IntCast<std::streamoff>(new_pos), std::ios_base::beg);
  if (ABSL_PREDICT_FALSE(src.fail())) return FailOperation("istream::seekg()");
  set_limit_pos(new_pos);
  return true;
}

std::optional<Position> IStreamReaderBase::SizeImpl() {
  if (ABSL_PREDICT_FALSE(!ok())) return std::nullopt;
  if (exact_size() != std::nullopt) return *exact_size();
  if (ABSL_PREDICT_FALSE(!IStreamReaderBase::SupportsRandomAccess())) {
    Fail(random_access_status_);
    return std::nullopt;
  }
  std::istream& src = *SrcStream();
  errno = 0;
  src.seekg(0, std::ios_base::end);
  if (ABSL_PREDICT_FALSE(src.fail())) {
    FailOperation("istream::seekg()");
    return std::nullopt;
  }
  const std::streamoff stream_size = src.tellg();
  if (ABSL_PREDICT_FALSE(stream_size < 0)) {
    FailOperation("istream::tellg()");
    return std::nullopt;
  }
  src.seekg(IntCast<std::streamoff>(limit_pos()), std::ios_base::beg);
  if (ABSL_PREDICT_FALSE(src.fail())) {
    FailOperation("istream::seekg()");
    return std::nullopt;
  }
  if (!growing_source_) set_exact_size(IntCast<Position>(stream_size));
  return IntCast<Position>(stream_size);
}

}  // namespace riegeli
