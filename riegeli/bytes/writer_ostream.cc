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
#include <string>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

namespace stream_internal {

class WriterStreambuf::BufferSync {
 public:
  explicit BufferSync(WriterStreambuf* streambuf) : streambuf_(streambuf) {
    if (streambuf_->reader_ != nullptr) {
      streambuf_->reader_->set_cursor(streambuf_->gptr());
    } else {
      streambuf_->writer_->set_cursor(streambuf_->pptr());
    }
  }

  BufferSync(const BufferSync&) = delete;
  BufferSync& operator=(const BufferSync&) = delete;

  ~BufferSync() {
    if (streambuf_->reader_ != nullptr) {
      streambuf_->setg(const_cast<char*>(streambuf_->reader_->start()),
                       const_cast<char*>(streambuf_->reader_->cursor()),
                       const_cast<char*>(streambuf_->reader_->limit()));
    } else {
      streambuf_->setp(streambuf_->writer_->cursor(),
                       streambuf_->writer_->limit());
    }
  }
  WriterStreambuf* streambuf_;
};

absl::optional<Position> WriterStreambuf::MoveBegin() {
  // In a closed `WriterOstream`, `WriterOstream::writer_ != nullptr`
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

void WriterStreambuf::MoveEnd(Writer* dest,
                              absl::optional<Position> reader_pos) {
  // In a closed `WriterOStream`, `WriterOStream::writer_ != nullptr`
  // does not imply `WriterStreambuf::writer_ != nullptr`, because
  // `WriterOStream::streambuf_` can be left uninitialized.
  if (writer_ == nullptr) return;
  writer_ = dest;
  if (reader_pos != absl::nullopt) {
    reader_ = writer_->ReadMode(*reader_pos);
    if (ABSL_PREDICT_FALSE(reader_ == nullptr)) {
      FailWriter();
      setg(nullptr, nullptr, nullptr);
      return;
    }
    if (ABSL_PREDICT_FALSE(reader_->pos() != *reader_pos)) {
      if (!reader_->ok()) {
        FailReader();
      } else {
        state_.Fail(absl::OutOfRangeError(absl::StrCat(
            "Current read position out of range for reading: position ",
            *reader_pos, " > stream size ", reader_->pos())));
      }
      setg(nullptr, nullptr, nullptr);
      return;
    }
    setg(const_cast<char*>(reader_->start()),
         const_cast<char*>(reader_->cursor()),
         const_cast<char*>(reader_->limit()));
  } else {
    setp(writer_->cursor(), writer_->limit());
  }
}

void WriterStreambuf::Done() {
  if (reader_ != nullptr) {
    reader_->set_cursor(gptr());
    setg(nullptr, nullptr, nullptr);
  } else {
    writer_->set_cursor(pptr());
    setp(nullptr, nullptr);
  }
}

void WriterStreambuf::FailReader() { state_.Fail(reader_->status()); }

void WriterStreambuf::FailWriter() { state_.Fail(writer_->status()); }

bool WriterStreambuf::ReadMode() {
  if (ABSL_PREDICT_FALSE(reader_ == nullptr)) {
    setp(nullptr, nullptr);
    const Position new_pos = writer_->pos();
    reader_ = writer_->ReadMode(new_pos);
    if (ABSL_PREDICT_FALSE(reader_ == nullptr)) {
      FailWriter();
      return false;
    }
    if (ABSL_PREDICT_FALSE(reader_->pos() != new_pos)) {
      if (!reader_->ok()) {
        FailReader();
      } else {
        state_.Fail(absl::OutOfRangeError(absl::StrCat(
            "Current write position out of range for reading: position ",
            new_pos, " > stream size ", reader_->pos())));
      }
      return false;
    }
  }
  return true;
}

bool WriterStreambuf::WriteMode() {
  if (ABSL_PREDICT_FALSE(reader_ != nullptr)) {
    setg(nullptr, nullptr, nullptr);
    const Position new_pos = reader_->pos();
    reader_ = nullptr;
    if (ABSL_PREDICT_FALSE(!writer_->Seek(new_pos))) {
      if (!writer_->ok()) {
        FailWriter();
      } else {
        state_.Fail(absl::OutOfRangeError(absl::StrCat(
            "Current read position out of range for writing: position ",
            new_pos, " > stream size ", writer_->pos())));
      }
      return false;
    }
  }
  return true;
}

int WriterStreambuf::sync() {
  if (ABSL_PREDICT_FALSE(!ok())) return -1;
  BufferSync buffer_sync(this);
  if (reader_ != nullptr) {
    if (ABSL_PREDICT_FALSE(!reader_->Sync())) {
      FailReader();
      return -1;
    }
  } else {
    if (ABSL_PREDICT_FALSE(!writer_->Flush())) {
      FailWriter();
      return -1;
    }
  }
  return 0;
}

std::streamsize WriterStreambuf::showmanyc() {
  if (ABSL_PREDICT_FALSE(!ok())) return -1;
  BufferSync buffer_sync(this);
  if (reader_ == nullptr && ABSL_PREDICT_FALSE(!writer_->SupportsReadMode())) {
    // Indicate that reading is not supported.
    return -1;
  }
  if (ABSL_PREDICT_FALSE(!ReadMode())) return -1;
  if (ABSL_PREDICT_FALSE(!reader_->Pull())) {
    if (ABSL_PREDICT_FALSE(!reader_->ok())) FailReader();
    return -1;
  }
  return IntCast<std::streamsize>(
      UnsignedMin(reader_->available(),
                  size_t{std::numeric_limits<std::streamsize>::max()}));
}

int WriterStreambuf::underflow() {
  if (ABSL_PREDICT_FALSE(!ok())) return traits_type::eof();
  BufferSync buffer_sync(this);
  if (reader_ == nullptr && ABSL_PREDICT_FALSE(!writer_->SupportsReadMode())) {
    // Indicate that reading is not supported.
    return traits_type::eof();
  }
  if (ABSL_PREDICT_FALSE(!ReadMode())) return traits_type::eof();
  if (ABSL_PREDICT_FALSE(!reader_->Pull())) {
    if (ABSL_PREDICT_FALSE(!reader_->ok())) FailReader();
    return traits_type::eof();
  }
  return traits_type::to_int_type(*reader_->cursor());
}

std::streamsize WriterStreambuf::xsgetn(char* dest, std::streamsize length) {
  RIEGELI_ASSERT_GE(length, 0)
      << "Failed precondition of streambuf::xsgetn(): negative length";
  if (ABSL_PREDICT_FALSE(!ok())) return 0;
  BufferSync buffer_sync(this);
  if (reader_ == nullptr && ABSL_PREDICT_FALSE(!writer_->SupportsReadMode())) {
    // Indicate that reading is not supported.
    return 0;
  }
  if (ABSL_PREDICT_FALSE(!ReadMode())) return 0;
  size_t length_read;
  if (ABSL_PREDICT_FALSE(
          !reader_->Read(IntCast<size_t>(length), dest, &length_read)) &&
      ABSL_PREDICT_FALSE(!reader_->ok())) {
    FailReader();
  }
  return IntCast<std::streamsize>(length_read);
}

int WriterStreambuf::overflow(int ch) {
  if (ABSL_PREDICT_FALSE(!ok())) return traits_type::eof();
  BufferSync buffer_sync(this);
  if (ABSL_PREDICT_FALSE(!WriteMode())) return traits_type::eof();
  if (ABSL_PREDICT_FALSE(!writer_->Push())) {
    FailWriter();
    return traits_type::eof();
  }
  if (ch != traits_type::eof()) {
    *writer_->cursor() = traits_type::to_char_type(ch);
    writer_->move_cursor(1);
  }
  return traits_type::not_eof(ch);
}

std::streamsize WriterStreambuf::xsputn(const char* src,
                                        std::streamsize length) {
  RIEGELI_ASSERT_GE(length, 0)
      << "Failed precondition of streambuf::xsputn(): negative length";
  if (ABSL_PREDICT_FALSE(!ok())) return 0;
  BufferSync buffer_sync(this);
  if (ABSL_PREDICT_FALSE(!WriteMode())) return 0;
  const Position pos_before = writer_->pos();
  if (ABSL_PREDICT_FALSE(!writer_->Write(src, IntCast<size_t>(length)))) {
    FailWriter();
    // `Write()` could have decreased `pos()` on failure.
    const Position length_written = SaturatingSub(writer_->pos(), pos_before);
    RIEGELI_ASSERT_LE(length_written, IntCast<size_t>(length))
        << "Writer::Write(absl::string_view) wrote more than requested";
    return IntCast<std::streamsize>(length_written);
  }
  return length;
}

std::streampos WriterStreambuf::seekoff(std::streamoff off,
                                        std::ios_base::seekdir dir,
                                        std::ios_base::openmode which) {
  if (ABSL_PREDICT_FALSE(!ok())) return std::streampos(std::streamoff{-1});
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
      new_pos = reader_ != nullptr ? reader_->pos() : writer_->pos();
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
      absl::optional<Position> size;
      if (reader_ != nullptr) {
        if (ABSL_PREDICT_FALSE(!reader_->SupportsSize())) {
          // Indicate that `seekoff(std::ios_base::end)` is not supported.
          return std::streampos(std::streamoff{-1});
        }
        size = reader_->Size();
        if (ABSL_PREDICT_FALSE(size == absl::nullopt)) {
          FailReader();
          return std::streampos(std::streamoff{-1});
        }
      } else {
        if (ABSL_PREDICT_FALSE(!writer_->SupportsSize())) {
          // Indicate that `seekoff(std::ios_base::end)` is not supported.
          return std::streampos(std::streamoff{-1});
        }
        size = writer_->Size();
        if (ABSL_PREDICT_FALSE(size == absl::nullopt)) {
          FailWriter();
          return std::streampos(std::streamoff{-1});
        }
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
  if ((which & std::ios_base::in) != 0) {
    // Switch to read mode.
    bool seek_ok;
    if (ABSL_PREDICT_FALSE(reader_ == nullptr)) {
      setp(nullptr, nullptr);
      reader_ = writer_->ReadMode(new_pos);
      if (ABSL_PREDICT_FALSE(reader_ == nullptr)) {
        FailWriter();
        return std::streampos(std::streamoff{-1});
      }
      seek_ok = reader_->pos() == new_pos;
    } else {
      RIEGELI_ASSERT(reader_->SupportsRewind())
          << "Failed postcondition of Writer::ReadMode(): "
             "SupportsRewind() is false";
      seek_ok = reader_->Seek(new_pos);
    }
    if (ABSL_PREDICT_FALSE(!seek_ok)) {
      if (ABSL_PREDICT_FALSE(!reader_->ok())) FailReader();
      return std::streampos(std::streamoff{-1});
    }
  } else {
    // Switch to write mode.
    if (ABSL_PREDICT_FALSE(reader_ != nullptr)) {
      setg(nullptr, nullptr, nullptr);
      reader_ = nullptr;
    }
    if (new_pos == writer_->pos()) {
      // Seeking to the current position is supported even if random access is
      // not.
    } else {
      if (ABSL_PREDICT_FALSE(!writer_->SupportsRandomAccess())) {
        // Indicate that `seekoff()` is not supported.
        return std::streampos(std::streamoff{-1});
      }
      if (ABSL_PREDICT_FALSE(!writer_->Seek(new_pos))) {
        if (ABSL_PREDICT_FALSE(!writer_->ok())) FailWriter();
        return std::streampos(std::streamoff{-1});
      }
    }
  }
  return std::streampos(IntCast<std::streamoff>(new_pos));
}

std::streampos WriterStreambuf::seekpos(std::streampos pos,
                                        std::ios_base::openmode which) {
  return seekoff(std::streamoff(pos), std::ios_base::beg, which);
}

}  // namespace stream_internal

bool WriterOStreamBase::close() {
  Done();
  return not_failed();
}

}  // namespace riegeli
