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
#include <streambuf>

#include "absl/base/optimization.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {
namespace internal {

class ReaderStreambuf::BufferSync {
 public:
  explicit BufferSync(ReaderStreambuf* streambuf) : streambuf_(streambuf) {
    streambuf_->src_->set_cursor(streambuf_->gptr());
  }

  BufferSync(const BufferSync&) = delete;
  BufferSync& operator=(const BufferSync&) = delete;

  ~BufferSync() {
    streambuf_->setg(const_cast<char*>(streambuf_->src_->start()),
                     const_cast<char*>(streambuf_->src_->cursor()),
                     const_cast<char*>(streambuf_->src_->limit()));
  }
  ReaderStreambuf* streambuf_;
};

int ReaderStreambuf::sync() {
  if (ABSL_PREDICT_FALSE(!is_open())) return 0;
  src_->set_cursor(gptr());
  return 0;
}

std::streamsize ReaderStreambuf::showmanyc() {
  if (ABSL_PREDICT_FALSE(!is_open())) return -1;
  BufferSync buffer_sync(this);
  if (ABSL_PREDICT_FALSE(!src_->Pull())) return -1;
  return IntCast<std::streamsize>(UnsignedMin(
      src_->available(), size_t{std::numeric_limits<std::streamsize>::max()}));
}

int ReaderStreambuf::underflow() {
  if (ABSL_PREDICT_FALSE(!is_open())) return traits_type::eof();
  BufferSync buffer_sync(this);
  if (ABSL_PREDICT_FALSE(!src_->Pull())) return traits_type::eof();
  return traits_type::to_int_type(*src_->cursor());
}

std::streamsize ReaderStreambuf::xsgetn(char* dest, std::streamsize length) {
  RIEGELI_ASSERT_GE(length, 0)
      << "Failed precondition of streambuf::xsgetn(): negative length";
  if (ABSL_PREDICT_FALSE(!is_open())) return 0;
  BufferSync buffer_sync(this);
  const Position pos_before = src_->pos();
  if (ABSL_PREDICT_FALSE(!src_->Read(dest, IntCast<size_t>(length)))) {
    RIEGELI_ASSERT_GE(src_->pos(), pos_before)
        << "Reader::Read(char*) decreased pos()";
    const Position length_read = src_->pos() - pos_before;
    RIEGELI_ASSERT_LE(length_read, IntCast<size_t>(length))
        << "Reader::Read(char*) read more than requested";
    return IntCast<std::streamsize>(length_read);
  }
  return length;
}

std::streampos ReaderStreambuf::seekoff(std::streamoff off,
                                        std::ios_base::seekdir dir,
                                        std::ios_base::openmode which) {
  if (ABSL_PREDICT_FALSE(!is_open())) {
    return std::streampos(std::streamoff{-1});
  }
  BufferSync buffer_sync(this);
  Position pos;
  switch (dir) {
    case std::ios_base::beg:
      if (ABSL_PREDICT_FALSE(off < 0)) {
        return std::streampos(std::streamoff{-1});
      }
      pos = IntCast<Position>(off);
      break;
    case std::ios_base::cur:
      pos = src_->pos();
      if (off < 0) {
        if (ABSL_PREDICT_FALSE(IntCast<Position>(-off) > pos)) {
          return std::streampos(std::streamoff{-1});
        }
        pos -= IntCast<Position>(-off);
        if (ABSL_PREDICT_FALSE(
                pos > Position{std::numeric_limits<std::streamoff>::max()})) {
          return std::streampos(std::streamoff{-1});
        }
      } else {
        if (ABSL_PREDICT_FALSE(
                pos > Position{std::numeric_limits<std::streamoff>::max()} ||
                IntCast<Position>(off) >
                    Position{std::numeric_limits<std::streamoff>::max()} -
                        pos)) {
          return std::streampos(std::streamoff{-1});
        }
        pos += IntCast<Position>(off);
      }
      break;
    case std::ios_base::end:
      if (ABSL_PREDICT_FALSE(!src_->Size(&pos) || off > 0 ||
                             IntCast<Position>(-off) > pos)) {
        return std::streampos(std::streamoff{-1});
      }
      pos -= IntCast<Position>(-off);
      if (ABSL_PREDICT_FALSE(
              pos > Position{std::numeric_limits<std::streamoff>::max()})) {
        return std::streampos(std::streamoff{-1});
      }
      break;
    default:
      RIEGELI_ASSERT_UNREACHABLE()
          << "Unknown seek direction: " << static_cast<int>(dir);
  }
  if (ABSL_PREDICT_FALSE(!src_->Seek(pos))) {
    return std::streampos(std::streamoff{-1});
  }
  return std::streampos(IntCast<std::streamoff>(pos));
}

std::streampos ReaderStreambuf::seekpos(std::streampos pos,
                                        std::ios_base::openmode which) {
  return seekoff(std::streamoff(pos), std::ios_base::beg, which);
}

}  // namespace internal
}  // namespace riegeli
