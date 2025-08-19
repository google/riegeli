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

#include "riegeli/lines/text_writer.h"

#include <stddef.h>

#include <cstring>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/lines/line_writing.h"
#include "riegeli/lines/newline.h"

namespace riegeli {

absl::Status TextWriterBase::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) {
    Writer& dest = *DestWriter();
    status = dest.AnnotateStatus(std::move(status));
  }
  // The status might have been annotated by `dest` with the original position.
  // Clarify that the current position is the position with LF newlines instead
  // of delegating to `BufferedWriter::AnnotateStatusImpl()`.
  return AnnotateOverDest(std::move(status));
}

absl::Status TextWriterBase::AnnotateOverDest(absl::Status status) {
  if (is_open()) {
    return Annotate(status, absl::StrCat("with LF newlines at byte ", pos()));
  }
  return status;
}

namespace text_writer_internal {

template <WriteNewline newline>
bool TextWriterImpl<newline>::WriteInternal(absl::string_view src) {
  RIEGELI_ASSERT(!src.empty())
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "nothing to write";
  RIEGELI_ASSERT_OK(*this)
      << "Failed precondition of BufferedWriter::WriteInternal()";
  Writer& dest = *DestWriter();
  for (;;) {
    const char* const lf_ptr =
        static_cast<const char*>(std::memchr(src.data(), '\n', src.size()));
    if (lf_ptr == nullptr) break;
    const size_t length = PtrDistance(src.data(), lf_ptr);
    if (ABSL_PREDICT_FALSE(
            !WriteLine(absl::string_view(src.data(), length), dest, newline))) {
      return FailWithoutAnnotation(AnnotateOverDest(dest.status()));
    }
    src.remove_prefix(length + 1);
    move_start_pos(length + 1);
  }
  if (ABSL_PREDICT_FALSE(!dest.Write(src))) {
    return FailWithoutAnnotation(AnnotateOverDest(dest.status()));
  }
  move_start_pos(src.size());
  return true;
}

template class TextWriterImpl<WriteNewline::kCrLf>;

}  // namespace text_writer_internal

}  // namespace riegeli
