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
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

bool Reader::VerifyEndAndClose() {
  VerifyEnd();
  return Close();
}

void Reader::VerifyEnd() {
  if (ABSL_PREDICT_FALSE(Pull())) {
    Fail(absl::DataLossError(absl::StrCat("End of data expected")));
  }
}

bool Reader::Fail(absl::Status status) {
  RIEGELI_ASSERT(!status.ok())
      << "Failed precondition of Object::Fail(): status not failed";
  RIEGELI_ASSERT(!closed())
      << "Failed precondition of Object::Fail(): Object closed";
  return FailWithoutAnnotation(
      Annotate(status, absl::StrCat("at byte ", pos())));
}

bool Reader::FailWithoutAnnotation(absl::Status status) {
  RIEGELI_ASSERT(!status.ok())
      << "Failed precondition of Reader::FailWithoutAnnotation(): "
         "status not failed";
  RIEGELI_ASSERT(!closed())
      << "Failed precondition of Reader::FailWithoutAnnotation(): "
         "Object closed";
  return Object::Fail(std::move(status));
}

bool Reader::FailWithoutAnnotation(const Object& dependency) {
  RIEGELI_ASSERT(!dependency.healthy())
      << "Failed precondition of Reader::FailWithoutAnnotation(): "
         "dependency healthy";
  RIEGELI_ASSERT(!closed())
      << "Failed precondition of Reader::FailWithoutAnnotation(): "
         "Object closed";
  return FailWithoutAnnotation(dependency.status());
}

bool Reader::FailOverflow() {
  return Fail(absl::ResourceExhaustedError("Reader position overflow"));
}

bool Reader::ReadSlow(char* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, available())
      << "Failed precondition of Reader::ReadSlow(char*): "
         "length too small, use Read(char*) instead";
  do {
    const size_t available_length = available();
    if (
        // `std::memcpy(_, nullptr, 0)` is undefined.
        available_length > 0) {
      std::memcpy(dest, cursor(), available_length);
      move_cursor(available_length);
      dest += available_length;
      length -= available_length;
    }
    if (ABSL_PREDICT_FALSE(!PullSlow(1, length))) return false;
  } while (length > available());
  std::memcpy(dest, cursor(), length);
  move_cursor(length);
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

bool Reader::ReadSlow(absl::Cord* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::ReadSlow(Cord*): "
         "length too small, use Read(Cord*) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Reader::ReadSlow(Cord*): "
         "Cord size overflow";
  while (length > available()) {
    const size_t available_length = available();
    dest->Append(absl::string_view(cursor(), available_length));
    move_cursor(available_length);
    length -= available_length;
    if (ABSL_PREDICT_FALSE(!PullSlow(1, length))) return false;
  }
  dest->Append(absl::string_view(cursor(), length));
  move_cursor(length);
  return true;
}

bool Reader::CopyToSlow(Writer* dest, Position length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::CopyToSlow(Writer*): "
         "length too small, use CopyTo(Writer*) instead";
  while (length > available()) {
    const absl::string_view data(cursor(), available());
    move_cursor(data.size());
    if (ABSL_PREDICT_FALSE(!dest->Write(data))) return false;
    length -= data.size();
    if (ABSL_PREDICT_FALSE(!PullSlow(1, length))) return false;
  }
  const absl::string_view data(cursor(), IntCast<size_t>(length));
  move_cursor(IntCast<size_t>(length));
  return dest->Write(data);
}

bool Reader::CopyToSlow(BackwardWriter* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::CopyToSlow(BackwardWriter*): "
         "length too small, use CopyTo(BackwardWriter*) instead";
  if (length <= available()) {
    const absl::string_view data(cursor(), length);
    move_cursor(length);
    return dest->Write(data);
  }
  if (length <= kMaxBytesToCopy) {
    if (ABSL_PREDICT_FALSE(!dest->Push(length))) return false;
    dest->move_cursor(length);
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

void Reader::ReadHintSlow(size_t length) {
  RIEGELI_ASSERT_GT(length, available())
      << "Failed precondition of Reader::ReadHintSlow(): "
         "length too small, use ReadHint() instead";
}

bool Reader::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of Reader::SeekSlow(): "
         "position in the buffer, use Seek() instead";
  if (ABSL_PREDICT_FALSE(new_pos <= limit_pos())) {
    return Fail(
        absl::UnimplementedError("Reader::Seek() backwards not supported"));
  }
  // Seeking forwards.
  do {
    move_cursor(available());
    if (ABSL_PREDICT_FALSE(!PullSlow(1, 0))) return false;
  } while (new_pos > limit_pos());
  const Position available_length = limit_pos() - new_pos;
  RIEGELI_ASSERT_LE(available_length, buffer_size())
      << "Reader::PullSlow() skipped some data";
  set_cursor(limit() - available_length);
  return true;
}

absl::optional<Position> Reader::Size() {
  Fail(absl::UnimplementedError("Reader::Size() not supported"));
  return absl::nullopt;
}

}  // namespace riegeli
