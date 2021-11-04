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

#include "riegeli/bytes/string_reader.h"

#include <stddef.h>

#include <memory>

#include "absl/base/optimization.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

bool StringReaderBase::PullSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Reader::PullSlow(): "
         "enough data available, use Pull() instead";
  return false;
}

bool StringReaderBase::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of Reader::SeekSlow(): "
         "position in the buffer, use Seek() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  RIEGELI_ASSERT_EQ(start_pos(), 0u)
      << "Failed invariant of StringReader: non-zero position of buffer start";
  // Seeking forwards. Source ends.
  set_cursor(limit());
  return false;
}

absl::optional<Position> StringReaderBase::SizeImpl() {
  if (ABSL_PREDICT_FALSE(!healthy())) return absl::nullopt;
  return limit_pos();
}

std::unique_ptr<Reader> StringReaderBase::NewReaderImpl(Position initial_pos) {
  if (ABSL_PREDICT_FALSE(!healthy())) return nullptr;
  std::unique_ptr<Reader> reader =
      std::make_unique<StringReader<>>(start(), start_to_limit());
  reader->Seek(initial_pos);
  return reader;
}

}  // namespace riegeli
