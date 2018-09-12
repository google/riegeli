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

#include "absl/base/optimization.h"
#include "absl/types/span.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void ArrayWriterBase::Done() {
  if (ABSL_PREDICT_TRUE(healthy())) {
    written_ = absl::Span<char>(start_, written_to_buffer());
    start_pos_ = pos();
  }
  Writer::Done();
}

bool ArrayWriterBase::PushSlow() {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of Writer::PushSlow(): "
         "space available, use Push() instead";
  return FailOverflow();
}

bool ArrayWriterBase::Flush(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  written_ = absl::Span<char>(start_, written_to_buffer());
  return true;
}

bool ArrayWriterBase::Truncate(Position new_size) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(new_size > written_to_buffer())) return false;
  cursor_ = start_ + new_size;
  return true;
}

template class ArrayWriter<absl::Span<char>>;
template class ArrayWriter<std::string*>;
template class ArrayWriter<std::string>;

}  // namespace riegeli
