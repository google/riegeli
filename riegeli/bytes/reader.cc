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

#include "riegeli/bytes/reader.h"

#include <stddef.h>
#include <cstring>
#include <limits>
#include <string>
#include <utility>

#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/string_view.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

bool Reader::VerifyEndAndClose() {
  if (RIEGELI_UNLIKELY(Pull())) Fail("End of data expected");
  return Close();
}

bool Reader::FailOverflow() { return Fail("Reader position overflows"); }

bool Reader::ReadSlow(char* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, available())
      << "Failed precondition of Reader::ReadSlow(char*): "
         "length too small, use Read(char*) instead";
  if (available() == 0) goto skip_copy;  // memcpy(_, nullptr, 0) is undefined.
  do {
    {
      const size_t available_length = available();
      std::memcpy(dest, cursor_, available_length);
      cursor_ = limit_;
      dest += available_length;
      length -= available_length;
    }
  skip_copy:
    if (RIEGELI_UNLIKELY(!PullSlow())) return false;
  } while (length > available());
  std::memcpy(dest, cursor_, length);
  cursor_ += length;
  return true;
}

bool Reader::ReadSlow(std::string* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, available())
      << "Failed precondition of Reader::ReadSlow(string*): "
         "length too small, use Read(string*) instead";
  RIEGELI_ASSERT_LE(length, dest->max_size() - dest->size())
      << "Failed precondition of Reader::ReadSlow(string*): "
         "string size overflows";
  const size_t dest_pos = dest->size();
  dest->resize(dest_pos + length);
  const Position pos_before = pos();
  if (RIEGELI_UNLIKELY(!ReadSlow(&(*dest)[dest_pos], length))) {
    RIEGELI_ASSERT_GE(pos(), pos_before)
        << "Reader::ReadSlow(char*) decreased pos()";
    const Position length_read = pos() - pos_before;
    RIEGELI_ASSERT_LE(length_read, length)
        << "Reader::ReadSlow(char*) read more than requested";
    dest->resize(dest_pos + IntCast<size_t>(length_read));
    return false;
  }
  return true;
}

bool Reader::ReadSlow(string_view* dest, std::string* scratch, size_t length) {
  RIEGELI_ASSERT_GT(length, available())
      << "Failed precondition of Reader::ReadSlow(string_view*): "
         "length too small, use Read(string_view*) instead";
  RIEGELI_ASSERT_LE(length, scratch->max_size())
      << "Failed precondition of Reader::ReadSlow(string_view*): "
         "string size overflows";
  if (available() == 0) {
    if (RIEGELI_UNLIKELY(!PullSlow())) return false;
    if (length <= available()) {
      *dest = string_view(cursor_, length);
      cursor_ += length;
      return true;
    }
  }
  scratch->clear();
  const bool ok = ReadSlow(scratch, length);
  *dest = *scratch;
  return ok;
}

bool Reader::ReadSlow(Chain* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy()))
      << "Failed precondition of Reader::ReadSlow(Chain*): "
         "length too small, use Read(Chain*) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Reader::ReadSlow(Chain*): "
         "Chain size overflows";
  const Chain::Buffer buffer = dest->MakeAppendBuffer(length);
  const Position pos_before = pos();
  const bool ok = Read(buffer.data(), length);
  if (RIEGELI_UNLIKELY(!ok)) {
    RIEGELI_ASSERT_GE(pos(), pos_before)
        << "Reader::Read(char*) decreased pos()";
    const Position length_read = pos() - pos_before;
    RIEGELI_ASSERT_LE(length_read, length)
        << "Reader::Read(char*) read more than requested";
    length = IntCast<size_t>(length_read);
  }
  dest->RemoveSuffix(buffer.size() - length);
  return ok;
}

bool Reader::CopyToSlow(Writer* dest, Position length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy()))
      << "Failed precondition of Reader::CopyToSlow(Writer*): "
         "length too small, use CopyTo(Writer*) instead";
  while (length > available()) {
    const string_view data(cursor_, available());
    cursor_ = limit_;
    if (RIEGELI_UNLIKELY(!dest->Write(data))) return false;
    length -= data.size();
    if (RIEGELI_UNLIKELY(!PullSlow())) return false;
  }
  const string_view data(cursor_, length);
  cursor_ += length;
  return dest->Write(data);
}

bool Reader::CopyToSlow(BackwardWriter* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy()))
      << "Failed precondition of Reader::CopyToSlow(BackwardWriter*): "
         "length too small, use CopyTo(BackwardWriter*) instead";
  if (length <= available()) {
    const string_view data(cursor_, length);
    cursor_ += length;
    return dest->Write(data);
  }
  if (length <= kMaxBytesToCopy()) {
    char buffer[kMaxBytesToCopy()];
    if (RIEGELI_UNLIKELY(!ReadSlow(buffer, length))) return false;
    return dest->Write(string_view(buffer, length));
  }
  Chain data;
  if (RIEGELI_UNLIKELY(!ReadSlow(&data, length))) return false;
  return dest->Write(std::move(data));
}

bool Reader::HopeForMoreSlow() const {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of Reader::HopeForMoreSlow(): "
         "data available, use HopeForMore() instead";
  return healthy();
}

bool Reader::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos_)
      << "Failed precondition of Reader::SeekSlow(): "
         "position in the buffer, use Seek() instead";
  if (RIEGELI_UNLIKELY(new_pos <= limit_pos_)) return false;
  // Seeking forwards.
  do {
    cursor_ = limit_;
    if (RIEGELI_UNLIKELY(!PullSlow())) return false;
  } while (new_pos > limit_pos_);
  const Position available_length = limit_pos_ - new_pos;
  RIEGELI_ASSERT_LE(available_length, buffer_size())
      << "Reader::PullSlow() skipped some data";
  cursor_ = limit_ - available_length;
  return true;
}

}  // namespace riegeli
