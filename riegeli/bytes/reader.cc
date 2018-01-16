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
#include <string>
#include <utility>

#include "riegeli/base/assert.h"
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

bool Reader::ReadSlow(char* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, available());
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
  RIEGELI_ASSERT_GT(length, available());
  const size_t dest_pos = dest->size();
  dest->resize(dest_pos + length);
  const Position pos_before = pos();
  if (RIEGELI_UNLIKELY(!ReadSlow(&(*dest)[dest_pos], length))) {
    RIEGELI_ASSERT_GE(pos(), pos_before);
    const Position length_read = pos() - pos_before;
    RIEGELI_ASSERT_LE(length_read, length);
    dest->resize(dest_pos + length_read);
    return false;
  }
  return true;
}

bool Reader::ReadSlow(string_view* dest, std::string* scratch, size_t length) {
  RIEGELI_ASSERT_GT(length, available());
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
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy()));
  const Chain::Buffer buffer = dest->MakeAppendBuffer(length);
  const Position pos_before = pos();
  Position length_read = length;
  const bool ok = Read(buffer.data(), length);
  if (RIEGELI_UNLIKELY(!ok)) {
    RIEGELI_ASSERT_GE(pos(), pos_before);
    length_read = pos() - pos_before;
    RIEGELI_ASSERT_LE(length_read, length);
  }
  dest->RemoveSuffix(buffer.size() - length_read);
  return ok;
}

bool Reader::CopyToSlow(Writer* dest, Position length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy()));
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
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy()));
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
  RIEGELI_ASSERT_EQ(available(), 0u);
  return healthy();
}

bool Reader::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos_);
  if (RIEGELI_UNLIKELY(new_pos <= limit_pos_)) return false;
  // Seeking forwards.
  do {
    cursor_ = limit_;
    if (RIEGELI_UNLIKELY(!PullSlow())) return false;
  } while (new_pos > limit_pos_);
  cursor_ = limit_ - (limit_pos_ - new_pos);
  return true;
}

}  // namespace riegeli
