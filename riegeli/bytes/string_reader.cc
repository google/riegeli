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

#include "riegeli/base/base.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

void StringReader::Done() { Reader::Done(); }

bool StringReader::PullSlow() {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of Reader::PullSlow(): "
         "data available, use Pull() instead";
  return false;
}

bool StringReader::HopeForMoreSlow() const {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of Reader::HopeForMoreSlow(): "
         "data available, use HopeForMore() instead";
  return false;
}

bool StringReader::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT_EQ(start_pos(), 0u)
      << "Failed invariant of StringReader: non-zero position of buffer start";
  RIEGELI_ASSERT_GT(new_pos, limit_pos_)
      << "Failed precondition of Reader::SeekSlow(): "
         "position in the buffer, use Seek() instead";
  // Seeking forwards. Source ends.
  cursor_ = limit_;
  return false;
}

}  // namespace riegeli
