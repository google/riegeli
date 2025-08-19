// Copyright 2022 Google LLC
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

#include "riegeli/lines/text_reader.h"

#include <stddef.h>

#include <cstring>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/lines/newline.h"

namespace riegeli {

void TextReaderBase::Initialize(Reader* src) {
  RIEGELI_ASSERT_NE(src, nullptr)
      << "Failed precondition of TextReader: null Reader pointer";
  if (ABSL_PREDICT_FALSE(!src->ok()) && src->available() == 0) {
    FailWithoutAnnotation(AnnotateOverSrc(src->status()));
    return;
  }
  initial_original_pos_ = src->pos();
}

absl::Status TextReaderBase::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) {
    Reader& src = *SrcReader();
    status = src.AnnotateStatus(std::move(status));
  }
  // The status might have been annotated by `src` with the original position.
  // Clarify that the current position is the position with LF newlines instead
  // of delegating to `BufferedReader::AnnotateStatusImpl()`.
  return AnnotateOverSrc(std::move(status));
}

absl::Status TextReaderBase::AnnotateOverSrc(absl::Status status) {
  if (is_open()) {
    return Annotate(status, absl::StrCat("with LF newlines at byte ", pos()));
  }
  return status;
}

bool TextReaderBase::ToleratesReadingAhead() {
  Reader* const src = SrcReader();
  return src != nullptr && src->ToleratesReadingAhead();
}

bool TextReaderBase::SupportsRewind() {
  Reader* const src = SrcReader();
  return src != nullptr && src->SupportsRewind();
}

bool TextReaderBase::SeekBehindBuffer(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of BufferedReader::SeekBehindBuffer(): "
         "position in the buffer, use Seek() instead";
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedReader::SeekBehindBuffer(): "
         "buffer not empty";
  if (new_pos <= limit_pos()) {
    // Seeking backwards.
    if (ABSL_PREDICT_FALSE(!ok())) return false;
    Reader& src = *SrcReader();
    set_buffer();
    set_limit_pos(0);
    if (ABSL_PREDICT_FALSE(!src.Seek(initial_original_pos_))) {
      return FailWithoutAnnotation(AnnotateOverSrc(src.StatusOrAnnotate(
          absl::DataLossError("Zstd-compressed stream got truncated"))));
    }
    if (new_pos == 0) return true;
  }
  return BufferedReader::SeekBehindBuffer(new_pos);
}

namespace text_reader_internal {

void TextReaderImpl<ReadNewline::kCrLfOrLf>::Initialize(Reader* src) {
  pending_cr_ = false;
  TextReaderBase::Initialize(src);
}

bool TextReaderImpl<ReadNewline::kCrLfOrLf>::ReadInternal(size_t min_length,
                                                          size_t max_length,
                                                          char* dest) {
  RIEGELI_ASSERT_GT(min_length, 0u)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "nothing to read";
  RIEGELI_ASSERT_GE(max_length, min_length)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "max_length < min_length";
  RIEGELI_ASSERT_OK(*this)
      << "Failed precondition of BufferedReader::ReadInternal()";
  Reader& src = *SrcReader();
  for (;;) {
    if (ABSL_PREDICT_FALSE(!src.Pull(1, max_length))) {
      if (ABSL_PREDICT_FALSE(pending_cr_)) {
        pending_cr_ = false;
        dest[0] = '\r';
        move_limit_pos(1);
        return min_length <= 1;
      }
      return false;
    }
    size_t length;
    if (ABSL_PREDICT_FALSE(pending_cr_)) {
      pending_cr_ = false;
      if (src.cursor()[0] == '\n') {
        src.move_cursor(1);
        dest[0] = '\n';
      } else {
        dest[0] = '\r';
      }
      ++dest;
      length = 1;
    } else {
      length = UnsignedMin(src.available(), max_length);
      const char* cr_ptr =
          static_cast<const char*>(std::memchr(src.cursor(), '\r', length));
      if (cr_ptr == nullptr) {
        std::memcpy(dest, src.cursor(), length);
        src.move_cursor(length);
        dest += length;
      } else {
        length = PtrDistance(src.cursor(), cr_ptr);
        std::memcpy(dest, src.cursor(), length);
        if (ABSL_PREDICT_FALSE(cr_ptr == src.limit() - 1)) {
          src.move_cursor(length + 1);
          dest += length;
          pending_cr_ = true;
        } else {
          ++length;
          src.move_cursor(length);
          dest += length;
          if (cr_ptr[1] == '\n') {
            src.move_cursor(1);
            dest[-1] = '\n';
          } else {
            dest[-1] = '\r';
          }
        }
      }
    }
    move_limit_pos(length);
    if (length >= min_length) return true;
    min_length -= length;
    max_length -= length;
  }
}

bool TextReaderImpl<ReadNewline::kCrLfOrLf>::SeekBehindBuffer(
    Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of BufferedReader::SeekBehindBuffer(): "
         "position in the buffer, use Seek() instead";
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedReader::SeekBehindBuffer(): "
         "buffer not empty";
  if (new_pos <= limit_pos()) {
    // Seeking backwards.
    pending_cr_ = false;
  }
  return TextReaderBase::SeekBehindBuffer(new_pos);
}

}  // namespace text_reader_internal

}  // namespace riegeli
