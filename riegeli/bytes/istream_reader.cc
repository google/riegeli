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
#include <istream>
#include <limits>
#include <string>

#include "absl/base/optimization.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/canonical_errors.h"
#include "riegeli/base/errno_mapping.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/buffered_reader.h"

namespace riegeli {

bool IstreamReaderBase::FailOperation(absl::string_view operation) {
  // There is no way to get details why a stream operation failed without
  // letting the stream throw exceptions. Hopefully low level failures have set
  // `errno` as a side effect.
  //
  // This requires resetting `errno` to 0 before the stream operation because
  // the operation may fail without setting `errno`.
  const int error_number = errno;
  const std::string message = absl::StrCat(operation, " failed");
  return Fail(error_number == 0
                  ? UnknownError(message)
                  : ErrnoToCanonicalStatus(error_number, message));
}

void IstreamReaderBase::Initialize(std::istream* src,
                                   absl::optional<Position> assumed_pos) {
  RIEGELI_ASSERT(src != nullptr)
      << "Failed precondition of IstreamReader: null stream pointer";
  if (ABSL_PREDICT_FALSE(src->fail())) {
    // Either constructing the stream failed or the stream was already in a
    // failed state. In any case `IstreamReaderBase` should fail.
    FailOperation("istream::istream()");
    return;
  }
  // A sticky `std::ios_base::eofbit` breaks future operations like
  // `std::istream::peek()` and `std::istream::tellg()`.
  src->clear(src->rdstate() & ~std::ios_base::eofbit);
  if (assumed_pos != absl::nullopt) {
    if (ABSL_PREDICT_FALSE(
            *assumed_pos >
            Position{std::numeric_limits<std::streamoff>::max()})) {
      FailOverflow();
      return;
    }
    set_limit_pos(*assumed_pos);
  } else {
    const std::streamoff stream_pos = src->tellg();
    if (ABSL_PREDICT_FALSE(stream_pos < 0)) {
      FailOperation("istream::tellg()");
      return;
    }
    set_limit_pos(IntCast<Position>(stream_pos));
  }
}

bool IstreamReaderBase::SyncPos(std::istream* src) {
  if (available() > 0) {
    errno = 0;
    src->seekg(-IntCast<std::streamoff>(available()), std::ios_base::cur);
    if (ABSL_PREDICT_FALSE(src->fail())) {
      return FailOperation("istream::seekg()");
    }
  }
  return true;
}

bool IstreamReaderBase::ReadInternal(char* dest, size_t min_length,
                                     size_t max_length) {
  RIEGELI_ASSERT_GT(min_length, 0u)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "nothing to read";
  RIEGELI_ASSERT_GE(max_length, min_length)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "max_length < min_length";
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of BufferedReader::ReadInternal(): " << status();
  std::istream* const src = src_stream();
  if (ABSL_PREDICT_FALSE(max_length >
                         Position{std::numeric_limits<std::streamoff>::max()} -
                             limit_pos())) {
    return FailOverflow();
  }
  errno = 0;
  for (;;) {
    std::streamsize length_read;
    if (min_length == max_length) {
      // Use `std::istream::read()` to read a fixed length.
      src->read(dest,
                IntCast<std::streamsize>(UnsignedMin(
                    min_length,
                    size_t{std::numeric_limits<std::streamsize>::max()})));
      length_read = src->gcount();
      RIEGELI_ASSERT_GE(length_read, 0) << "negataive istream::gcount()";
      RIEGELI_ASSERT_LE(IntCast<size_t>(length_read), min_length)
          << "istream::read() read more than requested";
    } else {
      // Use `std::istream::readsome()` to read as much data as is available,
      // up to `max_length`.
      //
      // `std::istream::peek()` forces some characters to be read into the
      // buffer, otherwise `std::istream::readsome()` may return 0.
      if (ABSL_PREDICT_FALSE(src->peek() == std::char_traits<char>::eof())) {
        if (ABSL_PREDICT_FALSE(src->fail())) {
          FailOperation("istream::peek()");
        } else {
          // A sticky `std::ios_base::eofbit` breaks future operations like
          // `std::istream::peek()` and `std::istream::tellg()`.
          src->clear(src->rdstate() & ~std::ios_base::eofbit);
        }
        return false;
      }
      length_read = src->readsome(
          dest, IntCast<std::streamsize>(UnsignedMin(
                    max_length,
                    size_t{std::numeric_limits<std::streamsize>::max()})));
      RIEGELI_ASSERT_GT(length_read, 0)
          << "istream::readsome() returned 0 "
             "even though istream::peek() returned non-eof()";
      RIEGELI_ASSERT_LE(IntCast<size_t>(length_read), max_length)
          << "istream::readsome() read more than requested";
    }
    move_limit_pos(IntCast<size_t>(length_read));
    if (ABSL_PREDICT_FALSE(src->fail())) {
      if (ABSL_PREDICT_FALSE(src->bad())) {
        FailOperation("istream::read()");
      } else {
        // End of stream is not a failure.
        //
        // A sticky `std::ios_base::eofbit` breaks future operations like
        // `std::istream::peek()` and `std::istream::tellg()`.
        src->clear(src->rdstate() &
                   ~(std::ios_base::eofbit | std::ios_base::failbit));
      }
      return IntCast<size_t>(length_read) >= min_length;
    }
    if (IntCast<size_t>(length_read) >= min_length) return true;
    dest += length_read;
    min_length -= IntCast<size_t>(length_read);
    max_length -= IntCast<size_t>(length_read);
  }
}

bool IstreamReaderBase::Sync() {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (!random_access_) return true;
  std::istream* const src = src_stream();
  const bool ok = SyncPos(src);
  set_limit_pos(pos());
  ClearBuffer();
  if (ABSL_PREDICT_FALSE(!ok)) return false;
  if (ABSL_PREDICT_FALSE(src->sync() != 0)) {
    return FailOperation("istream::sync()");
  }
  return true;
}

bool IstreamReaderBase::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of Reader::SeekSlow(): "
         "position in the buffer, use Seek() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(!random_access_)) {
    return BufferedReader::SeekSlow(new_pos);
  }
  ClearBuffer();
  std::istream* const src = src_stream();
  errno = 0;
  if (new_pos >= limit_pos()) {
    // Seeking forwards.
    src->seekg(0, std::ios_base::end);
    if (ABSL_PREDICT_FALSE(src->fail())) {
      return FailOperation("istream::seekg()");
    }
    const std::streamoff stream_size = src->tellg();
    if (ABSL_PREDICT_FALSE(stream_size < 0)) {
      return FailOperation("istream::tellg()");
    }
    if (ABSL_PREDICT_FALSE(new_pos > IntCast<Position>(stream_size))) {
      // Stream ends.
      set_limit_pos(IntCast<Position>(stream_size));
      return false;
    }
  }
  src->seekg(IntCast<std::streamoff>(new_pos), std::ios_base::beg);
  if (ABSL_PREDICT_FALSE(src->fail())) {
    return FailOperation("istream::seekg()");
  }
  set_limit_pos(new_pos);
  return true;
}

absl::optional<Position> IstreamReaderBase::Size() {
  if (ABSL_PREDICT_FALSE(!healthy())) return absl::nullopt;
  if (ABSL_PREDICT_FALSE(!random_access_)) {
    Fail(UnimplementedError("IstreamReaderBase::Size() not supported"));
    return absl::nullopt;
  }
  std::istream* const src = src_stream();
  errno = 0;
  src->seekg(0, std::ios_base::end);
  if (ABSL_PREDICT_FALSE(src->fail())) {
    FailOperation("istream::seekg()");
    return absl::nullopt;
  }
  const std::streamoff stream_size = src->tellg();
  if (ABSL_PREDICT_FALSE(stream_size < 0)) {
    FailOperation("istream::tellg()");
    return absl::nullopt;
  }
  src->seekg(IntCast<std::streamoff>(limit_pos()), std::ios_base::beg);
  if (ABSL_PREDICT_FALSE(src->fail())) {
    FailOperation("istream::seekg()");
    return absl::nullopt;
  }
  return IntCast<Position>(stream_size);
}

}  // namespace riegeli
