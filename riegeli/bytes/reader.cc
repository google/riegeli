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

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/base.h"
#include "riegeli/base/canonical_errors.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

bool Reader::VerifyEndAndClose() {
  VerifyEnd();
  return Close();
}

void Reader::VerifyEnd() {
  if (ABSL_PREDICT_FALSE(Pull())) {
    Fail(DataLossError("End of data expected"));
  }
}

bool Reader::FailOverflow() {
  return Fail(ResourceExhaustedError("Reader position overflow"));
}

bool Reader::ReadSlow(char* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, available())
      << "Failed precondition of Reader::ReadSlow(char*): "
         "length too small, use Read(char*) instead";
  do {
    const size_t available_length = available();
    if (available_length > 0) {  // memcpy(_, nullptr, 0) is undefined.
      std::memcpy(dest, cursor_, available_length);
      cursor_ = limit_;
      dest += available_length;
      length -= available_length;
    }
    if (ABSL_PREDICT_FALSE(!PullSlow(1, length))) return false;
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
         "string size overflow";
  const size_t dest_pos = dest->size();
  dest->resize(dest_pos + length);
  const Position pos_before = pos();
  if (ABSL_PREDICT_FALSE(!ReadSlow(&(*dest)[dest_pos], length))) {
    RIEGELI_ASSERT_GE(pos(), pos_before)
        << "Reader::ReadSlow(char*) decreased pos()";
    const Position length_read = pos() - pos_before;
    RIEGELI_ASSERT_LE(length_read, length)
        << "Reader::ReadSlow(char*) read more than requested";
    dest->erase(dest_pos + IntCast<size_t>(length_read));
    return false;
  }
  return true;
}

bool Reader::ReadSlow(Chain* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::ReadSlow(Chain*): "
         "length too small, use Read(Chain*) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Reader::ReadSlow(Chain*): "
         "Chain size overflow";
  do {
    const absl::Span<char> buffer = dest->AppendBuffer(1, length, length);
    const Position pos_before = pos();
    if (ABSL_PREDICT_FALSE(!Read(buffer.data(), buffer.size()))) {
      RIEGELI_ASSERT_GE(pos(), pos_before)
          << "Reader::Read(char*) decreased pos()";
      const Position length_read = pos() - pos_before;
      RIEGELI_ASSERT_LE(length_read, buffer.size())
          << "Reader::Read(char*) read more than requested";
      dest->RemoveSuffix(buffer.size() - IntCast<size_t>(length_read));
      return false;
    }
    length -= buffer.size();
  } while (length > 0);
  return true;
}

bool Reader::CopyToSlow(Writer* dest, Position length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::CopyToSlow(Writer*): "
         "length too small, use CopyTo(Writer*) instead";
  while (length > available()) {
    const absl::string_view data(cursor_, available());
    cursor_ = limit_;
    if (ABSL_PREDICT_FALSE(!dest->Write(data))) return false;
    length -= data.size();
    if (ABSL_PREDICT_FALSE(!PullSlow(1, length))) return false;
  }
  const absl::string_view data(cursor_, length);
  cursor_ += length;
  return dest->Write(data);
}

bool Reader::CopyToSlow(BackwardWriter* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::CopyToSlow(BackwardWriter*): "
         "length too small, use CopyTo(BackwardWriter*) instead";
  if (length <= available()) {
    const absl::string_view data(cursor_, length);
    cursor_ += length;
    return dest->Write(data);
  }
  if (length <= kMaxBytesToCopy) {
    if (ABSL_PREDICT_FALSE(!dest->Push(length))) return false;
    dest->set_cursor(dest->cursor() - length);
    if (ABSL_PREDICT_FALSE(!ReadSlow(dest->cursor(), length))) {
      dest->set_cursor(dest->cursor() + length);
      return false;
    }
    return true;
  }
  Chain data;
  if (ABSL_PREDICT_FALSE(!ReadSlow(&data, length))) return false;
  return dest->Write(std::move(data));
}

bool Reader::ReadAll(std::string* dest) {
  if (SupportsRandomAccess()) {
    Position size;
    if (ABSL_PREDICT_FALSE(!Size(&size))) return false;
    const Position remaining = SaturatingSub(size, pos());
    if (ABSL_PREDICT_FALSE(remaining > dest->max_size() - dest->size())) {
      return Fail(ResourceExhaustedError("Destination size overflow"));
    }
    if (ABSL_PREDICT_FALSE(!Read(dest, IntCast<size_t>(remaining)))) {
      return healthy();
    }
    return true;
  } else {
    do {
      if (ABSL_PREDICT_FALSE(available() > dest->max_size() - dest->size())) {
        return Fail(ResourceExhaustedError("Destination size overflow"));
      }
      dest->append(cursor_, available());
      cursor_ = limit_;
    } while (PullSlow(1, 0));
    return healthy();
  }
}

bool Reader::ReadAll(Chain* dest) {
  if (SupportsRandomAccess()) {
    Position size;
    if (ABSL_PREDICT_FALSE(!Size(&size))) return false;
    const Position remaining = SaturatingSub(size, pos());
    if (ABSL_PREDICT_FALSE(remaining >
                           std::numeric_limits<size_t>::max() - dest->size())) {
      return Fail(ResourceExhaustedError("Destination size overflow"));
    }
    if (ABSL_PREDICT_FALSE(!Read(dest, IntCast<size_t>(remaining)))) {
      return healthy();
    }
    return true;
  } else {
    do {
      if (ABSL_PREDICT_FALSE(available() > std::numeric_limits<size_t>::max() -
                                               dest->size())) {
        return Fail(ResourceExhaustedError("Destination size overflow"));
      }
      if (ABSL_PREDICT_FALSE(!Read(dest, available()))) return false;
    } while (Pull());
    return healthy();
  }
}

bool Reader::CopyAllTo(Writer* dest) {
  if (SupportsRandomAccess()) {
    Position size;
    if (ABSL_PREDICT_FALSE(!Size(&size))) return false;
    const Position remaining = SaturatingSub(size, pos());
    if (ABSL_PREDICT_FALSE(!CopyTo(dest, remaining))) {
      return dest->healthy() && healthy();
    }
    return true;
  } else {
    do {
      if (ABSL_PREDICT_FALSE(!CopyTo(dest, available()))) return false;
    } while (Pull());
    return healthy();
  }
}

bool Reader::CopyAllTo(BackwardWriter* dest) {
  if (SupportsRandomAccess()) {
    Position size;
    if (ABSL_PREDICT_FALSE(!Size(&size))) return false;
    const Position remaining = SaturatingSub(size, pos());
    if (ABSL_PREDICT_FALSE(remaining > std::numeric_limits<size_t>::max())) {
      return Fail(ResourceExhaustedError("Destination size overflow"));
    }
    if (ABSL_PREDICT_FALSE(!CopyTo(dest, IntCast<size_t>(remaining)))) {
      return dest->healthy() && healthy();
    }
    return true;
  } else {
    Chain data;
    if (ABSL_PREDICT_FALSE(!ReadAll(&data))) return false;
    return dest->Write(std::move(data));
  }
}

bool Reader::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos_)
      << "Failed precondition of Reader::SeekSlow(): "
         "position in the buffer, use Seek() instead";
  if (ABSL_PREDICT_FALSE(new_pos <= limit_pos_)) {
    return Fail(UnimplementedError("Reader::Seek() backwards not supported"));
  }
  // Seeking forwards.
  do {
    cursor_ = limit_;
    if (ABSL_PREDICT_FALSE(!PullSlow(1, 0))) return false;
  } while (new_pos > limit_pos_);
  const Position available_length = limit_pos_ - new_pos;
  RIEGELI_ASSERT_LE(available_length, buffer_size())
      << "Reader::PullSlow() skipped some data";
  cursor_ = limit_ - available_length;
  return true;
}

bool Reader::Size(Position* size) {
  return Fail(UnimplementedError("Reader::Size() not supported"));
}

}  // namespace riegeli
