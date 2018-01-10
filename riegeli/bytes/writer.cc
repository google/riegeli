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

#include "riegeli/bytes/writer.h"

#include <stddef.h>
#include <cstring>
#include <string>
#include <utility>

#include "riegeli/base/assert.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/base/string_view.h"

namespace riegeli {

bool Writer::Fail(string_view message) {
  cursor_ = start_;
  limit_ = start_;
  return Object::Fail(message);
}

bool Writer::WriteSlow(string_view src) {
  RIEGELI_ASSERT_GT(src.size(), available());
  if (available() == 0) goto skip_copy;  // memcpy(nullptr, _, 0) is undefined.
  do {
    {
      const size_t available_length = available();
      std::memcpy(cursor_, src.data(), available_length);
      cursor_ = limit_;
      src.remove_prefix(available_length);
    }
  skip_copy:
    if (RIEGELI_UNLIKELY(!PushSlow())) return false;
  } while (src.size() > available());
  std::memcpy(cursor_, src.data(), src.size());
  cursor_ += src.size();
  return true;
}

bool Writer::WriteSlow(std::string&& src) {
  RIEGELI_ASSERT_GT(src.size(), UnsignedMin(available(), kMaxBytesToCopy()));
  // No std::move(): forward to Write(string_view).
  return Write(src);
}

bool Writer::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_GT(src.size(), UnsignedMin(available(), kMaxBytesToCopy()));
  for (string_view fragment : src.blocks()) {
    if (RIEGELI_UNLIKELY(!Write(fragment))) return false;
  }
  return true;
}

bool Writer::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_GT(src.size(), UnsignedMin(available(), kMaxBytesToCopy()));
  // No std::move(): forward to WriteSlow(const Chain&).
  return WriteSlow(src);
}

}  // namespace riegeli
