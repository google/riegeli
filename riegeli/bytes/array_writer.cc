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

#include "riegeli/bytes/array_writer.h"

#include <stddef.h>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/pushable_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/string_reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void ArrayWriterBase::Done() {
  PushableWriter::Done();
  associated_reader_.Reset();
}

bool ArrayWriterBase::PushBehindScratch(size_t recommended_length) {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of PushableWriter::PushBehindScratch(): "
         "some space available, use Push() instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableWriter::PushBehindScratch(): "
         "scratch used";
  return FailOverflow();
}

bool ArrayWriterBase::FlushBehindScratch(FlushType flush_type) {
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableWriter::FlushBehindScratch(): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  written_ = absl::MakeSpan(start(), start_to_cursor());
  return true;
}

bool ArrayWriterBase::TruncateBehindScratch(Position new_size) {
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableWriter::TruncateBehindScratch(): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (ABSL_PREDICT_FALSE(new_size > start_to_cursor())) return false;
  set_cursor(start() + new_size);
  return true;
}

Reader* ArrayWriterBase::ReadModeBehindScratch(Position initial_pos) {
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableWriter::ReadModeBehindScratch(): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!ok())) return nullptr;
  StringReader<>* const reader =
      associated_reader_.ResetReader(start(), start_to_cursor());
  reader->Seek(initial_pos);
  return reader;
}

}  // namespace riegeli
