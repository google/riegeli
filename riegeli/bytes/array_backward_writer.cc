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

namespace riegeli {

bool ArrayBackwardWriterBase::PushBehindScratch(size_t recommended_length) {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of PushableBackwardWriter::PushBehindScratch(): "
         "some space available, use Push() instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableBackwardWriter::PushBehindScratch(): "
         "scratch used";
  return FailOverflow();
}

bool ArrayBackwardWriterBase::FlushBehindScratch(FlushType flush_type) {
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableBackwardWriter::FlushBehindScratch(): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  written_ = absl::Span<char>(cursor(), start_to_cursor());
  return true;
}

bool ArrayBackwardWriterBase::TruncateBehindScratch(Position new_size) {
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of "
         "PushableBackwardWriter::TruncateBehindScratch(): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (ABSL_PREDICT_FALSE(new_size > start_to_cursor())) return false;
  set_cursor(start() - new_size);
  return true;
}

}  // namespace riegeli
