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

#include "riegeli/bytes/reader_istream.h"

#include <stddef.h>

#include <limits>

#include "absl/base/optimization.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {
namespace internal {

class ReaderStreambuf::BufferSync {
 public:
  explicit BufferSync(ReaderStreambuf* streambuf) : streambuf_(streambuf) {
    streambuf_->reader_->set_cursor(streambuf_->gptr());
  }

  BufferSync(const BufferSync&) = delete;
  BufferSync& operator=(const BufferSync&) = delete;

  ~BufferSync() {
    streambuf_->setg(const_cast<char*>(streambuf_->reader_->start()),
                     const_cast<char*>(streambuf_->reader_->cursor()),
                     const_cast<char*>(streambuf_->reader_->limit()));
  }
  ReaderStreambuf* streambuf_;
};

void ReaderStreambuf::Fail() { state_.Fail(reader_->status()); }

int ReaderStreambuf::sync() {
  if (ABSL_PREDICT_FALSE(!healthy())) return -1;
  BufferSync buffer_sync(this);
  if (ABSL_PREDICT_FALSE(!reader_->Sync())) {
    Fail();
    return -1;
  }
  return 0;
}

std::streamsize ReaderStreambuf::showmanyc() {
  if (ABSL_PREDICT_FALSE(!healthy())) return -1;
  BufferSync buffer_sync(this);
  if (ABSL_PREDICT_FALSE(!reader_->Pull())) {
    if (ABSL_PREDICT_FALSE(!reader_->healthy())) Fail();
    return -1;
  }
  return IntCast<std::streamsize>(
      UnsignedMin(reader_->available(),
                  size_t{std::numeric_limits<std::streamsize>::max()}));
}

int ReaderStreambuf::underflow() {
  if (ABSL_PREDICT_FALSE(!healthy())) return traits_type::eof();
  BufferSync buffer_sync(this);
  if (ABSL_PREDICT_FALSE(!reader_->Pull())) {
    if (ABSL_PREDICT_FALSE(!reader_->healthy())) Fail();
    return traits_type::eof();
  }
  return traits_type::to_int_type(*reader_->cursor());
}

std::streamsize ReaderStreambuf::xsgetn(char* dest, std::streamsize length) {
  RIEGELI_ASSERT_GE(length, 0)
      << "Failed precondition of streambuf::xsgetn(): negative length";
  if (ABSL_PREDICT_FALSE(!healthy())) return 0;
  BufferSync buffer_sync(this);
  const Position pos_before = reader_->pos();
  if (ABSL_PREDICT_FALSE(!reader_->Read(IntCast<size_t>(length), dest))) {
    if (ABSL_PREDICT_FALSE(!reader_->healthy())) Fail();
    RIEGELI_ASSERT_GE(reader_->pos(), pos_before)
        << "Reader::Read(char*) decreased pos()";
    const Position length_read = reader_->pos() - pos_before;
    RIEGELI_ASSERT_LE(length_read, IntCast<size_t>(length))
        << "Reader::Read(char*) read more than requested";
    return IntCast<std::streamsize>(length_read);
  }
  return length;
}

std::streampos ReaderStreambuf::seekoff(std::streamoff off,
                                        std::ios_base::seekdir dir,
                                        std::ios_base::openmode which) {
  if (ABSL_PREDICT_FALSE(!healthy())) return std::streampos(std::streamoff{-1});
  BufferSync buffer_sync(this);
  Position new_pos;
  switch (dir) {
    case std::ios_base::beg:
      if (ABSL_PREDICT_FALSE(off < 0)) {
        return std::streampos(std::streamoff{-1});
      }
      new_pos = IntCast<Position>(off);
      break;
    case std::ios_base::cur:
      new_pos = reader_->pos();
      if (off < 0) {
        if (ABSL_PREDICT_FALSE(IntCast<Position>(-off) > new_pos)) {
          return std::streampos(std::streamoff{-1});
        }
        new_pos -= IntCast<Position>(-off);
        if (ABSL_PREDICT_FALSE(
                new_pos >
                Position{std::numeric_limits<std::streamoff>::max()})) {
          return std::streampos(std::streamoff{-1});
        }
      } else {
        if (ABSL_PREDICT_FALSE(
                new_pos >
                    Position{std::numeric_limits<std::streamoff>::max()} ||
                IntCast<Position>(off) >
                    Position{std::numeric_limits<std::streamoff>::max()} -
                        new_pos)) {
          return std::streampos(std::streamoff{-1});
        }
        new_pos += IntCast<Position>(off);
      }
      break;
    case std::ios_base::end: {
      if (ABSL_PREDICT_FALSE(!reader_->SupportsSize())) {
        // Indicate that `seekoff(std::ios_base::end)` is not supported.
        return std::streampos(std::streamoff{-1});
      }
      const absl::optional<Position> size = reader_->Size();
      if (ABSL_PREDICT_FALSE(size == absl::nullopt)) {
        Fail();
        return std::streampos(std::streamoff{-1});
      }
      if (ABSL_PREDICT_FALSE(off > 0 || IntCast<Position>(-off) > *size)) {
        return std::streampos(std::streamoff{-1});
      }
      new_pos = *size - IntCast<Position>(-off);
      if (ABSL_PREDICT_FALSE(
              new_pos > Position{std::numeric_limits<std::streamoff>::max()})) {
        return std::streampos(std::streamoff{-1});
      }
    } break;
    default:
      RIEGELI_ASSERT_UNREACHABLE()
          << "Unknown seek direction: " << static_cast<int>(dir);
  }
  if (new_pos == reader_->pos()) {
    // Seeking to the current position is supported even if random access is
    // not.
  } else {
    if (ABSL_PREDICT_FALSE(!reader_->SupportsRewind())) {
      // Indicate that `seekoff()` is not supported.
      return std::streampos(std::streamoff{-1});
    }
    if (ABSL_PREDICT_FALSE(!reader_->Seek(new_pos))) {
      if (ABSL_PREDICT_FALSE(!reader_->healthy())) Fail();
      return std::streampos(std::streamoff{-1});
    }
  }
  return std::streampos(IntCast<std::streamoff>(new_pos));
}

std::streampos ReaderStreambuf::seekpos(std::streampos pos,
                                        std::ios_base::openmode which) {
  return seekoff(std::streamoff(pos), std::ios_base::beg, which);
}

}  // namespace internal
}  // namespace riegeli
