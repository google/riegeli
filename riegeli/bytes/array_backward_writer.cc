// Copyright 2018 Google LLC
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

#include "riegeli/bytes/array_backward_writer.h"

#include <stddef.h>

#include "absl/base/optimization.h"
#include "absl/types/span.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/pushable_backward_writer.h"

namespace riegeli {

bool ArrayBackwardWriterBase::PushSlow(size_t min_length,
                                       size_t recommended_length) {
  RIEGELI_ASSERT_GT(min_length, available())
      << "Failed precondition of BackwardWriter::PushSlow(): "
         "length too small, use Push() instead";
  if (ABSL_PREDICT_FALSE(!PushUsingScratch(min_length, recommended_length))) {
    return available() >= min_length;
  }
  return FailOverflow();
}

bool ArrayBackwardWriterBase::Flush(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!SyncScratch())) return false;
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  written_ = absl::Span<char>(cursor(), written_to_buffer());
  return true;
}

bool ArrayBackwardWriterBase::Truncate(Position new_size) {
  if (ABSL_PREDICT_FALSE(!SyncScratch())) return false;
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(new_size > written_to_buffer())) return false;
  set_cursor(start() - new_size);
  return true;
}

}  // namespace riegeli
