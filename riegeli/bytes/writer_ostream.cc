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

#include "riegeli/bytes/writer_ostream.h"

#include <stddef.h>

#include <limits>
#include <streambuf>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {
namespace internal {

class WriterStreambuf::BufferSync {
 public:
  explicit BufferSync(WriterStreambuf* streambuf) : streambuf_(streambuf) {
    streambuf_->dest_->set_cursor(streambuf_->pptr());
  }

  BufferSync(const BufferSync&) = delete;
  BufferSync& operator=(const BufferSync&) = delete;

  ~BufferSync() {
    streambuf_->setp(streambuf_->dest_->cursor(), streambuf_->dest_->limit());
  }
  WriterStreambuf* streambuf_;
};

int WriterStreambuf::sync() {
  if (ABSL_PREDICT_FALSE(!is_open())) return 0;
  BufferSync buffer_sync(this);
  if (ABSL_PREDICT_FALSE(!dest_->Flush(FlushType::kFromObject))) return -1;
  return 0;
}

int WriterStreambuf::overflow(int ch) {
  if (ABSL_PREDICT_FALSE(!is_open())) return traits_type::eof();
  BufferSync buffer_sync(this);
  if (ABSL_PREDICT_FALSE(!dest_->Push())) return traits_type::eof();
  if (ch != traits_type::eof()) {
    *dest_->cursor() = traits_type::to_char_type(ch);
    dest_->move_cursor(1);
  }
  return traits_type::not_eof(ch);
}

std::streamsize WriterStreambuf::xsputn(const char* src,
                                        std::streamsize length) {
  RIEGELI_ASSERT_GE(length, 0)
      << "Failed precondition of streambuf::xsputn(): negative length";
  if (ABSL_PREDICT_FALSE(!is_open())) return 0;
  BufferSync buffer_sync(this);
  const Position pos_before = dest_->pos();
  if (ABSL_PREDICT_FALSE(
          !dest_->Write(absl::string_view(src, IntCast<size_t>(length))))) {
    RIEGELI_ASSERT_GE(dest_->pos(), pos_before)
        << "Writer::Write(absl::string_view) decreased pos()";
    const Position length_written = dest_->pos() - pos_before;
    RIEGELI_ASSERT_LE(length_written, IntCast<size_t>(length))
        << "Writer::Write(absl::string_view) wrote more than requested";
    return IntCast<std::streamsize>(length_written);
  }
  return length;
}

std::streampos WriterStreambuf::seekoff(std::streamoff off,
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
      pos = dest_->pos();
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
      if (ABSL_PREDICT_FALSE(!dest_->Size(&pos) || off > 0 ||
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
  if (ABSL_PREDICT_FALSE(!dest_->Seek(pos))) {
    return std::streampos(std::streamoff{-1});
  }
  return std::streampos(IntCast<std::streamoff>(pos));
}

std::streampos WriterStreambuf::seekpos(std::streampos pos,
                                        std::ios_base::openmode which) {
  return seekoff(std::streamoff(pos), std::ios_base::beg, which);
}

}  // namespace internal
}  // namespace riegeli
