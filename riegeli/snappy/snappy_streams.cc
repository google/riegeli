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

#include "riegeli/snappy/snappy_streams.h"

#include <stddef.h>

#include <functional>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {
namespace internal {

void WriterSnappySink::Append(const char* src, size_t length) {
  RIEGELI_ASSERT(std::less_equal<>()(src, dest_->cursor()) ||
                 std::greater_equal<>()(src, dest_->limit()))
      << "Failed precondition of Sink::Append(): "
         "appending a pointer to the middle of GetAppendBuffer()";
  // Check also `dest_->available()` because `dest_->cursor()` might point to
  // the end of a memory block, and `src` to the beginning of an unrelated
  // memory block.
  if (src == dest_->cursor() && ABSL_PREDICT_TRUE(dest_->available() > 0)) {
    // Appending a prefix of the result of `GetAppendBuffer()`.
    RIEGELI_ASSERT_LE(length, dest_->available())
        << "Failed precondition of Sink::Append(): "
           "appending the result of GetAppendBuffer() with length too large";
    dest_->move_cursor(length);
  } else {
    dest_->Write(absl::string_view(src, length));
  }
}

char* WriterSnappySink::GetAppendBuffer(size_t length, char* scratch) {
  if (ABSL_PREDICT_TRUE(dest_->Push(length))) {
    return dest_->cursor();
  } else {
    return scratch;
  }
}

void WriterSnappySink::AppendAndTakeOwnership(
    char* src, size_t length, void (*deleter)(void*, const char*, size_t),
    void* deleter_arg) {
  dest_->Write(Chain::FromExternal(
      [deleter, deleter_arg](absl::string_view data) {
        deleter(deleter_arg, data.data(), data.size());
      },
      absl::string_view(src, length)));
}

char* WriterSnappySink::GetAppendBufferVariable(size_t min_length,
                                                size_t recommended_length,
                                                char* scratch,
                                                size_t scratch_length,
                                                size_t* result_length) {
  RIEGELI_ASSERT_GE(scratch_length, min_length)
      << "Failed precondition of Sink::GetAppendBufferVariable(): "
         "scratch length too small";
  if (ABSL_PREDICT_TRUE(dest_->Push(min_length, recommended_length))) {
    *result_length = dest_->available();
    return dest_->cursor();
  } else {
    *result_length = scratch_length;
    return scratch;
  }
}

size_t ReaderSnappySource::Available() const {
  return SaturatingIntCast<size_t>(SaturatingSub(size_, src_->pos()));
}

const char* ReaderSnappySource::Peek(size_t* length) {
  src_->Pull();
  *length = src_->available();
  return src_->cursor();
}

void ReaderSnappySource::Skip(size_t length) { src_->Skip(length); }

}  // namespace internal
}  // namespace riegeli
