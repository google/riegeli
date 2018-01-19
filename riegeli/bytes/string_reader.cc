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

#include <string>
#include <utility>

#include "riegeli/base/assert.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

StringReader::StringReader() { MarkClosed(); }

StringReader::StringReader(const char* src, size_t size) {
  start_ = src;
  cursor_ = src;
  limit_ = src + size;
  limit_pos_ = size;
}

StringReader::StringReader(const std::string* src)
    : StringReader(src->data(), src->size()) {}

StringReader::StringReader(StringReader&& src) noexcept
    : Reader(std::move(src)) {}

StringReader& StringReader::operator=(StringReader&& src) noexcept {
  if (&src != this) Reader::operator=(std::move(src));
  return *this;
}

StringReader::~StringReader() = default;

void StringReader::Done() { Reader::Done(); }

bool StringReader::HopeForMoreSlow() const {
  RIEGELI_ASSERT_EQ(available(), 0u);
  return false;
}

bool StringReader::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT_EQ(start_pos(), 0u);
  RIEGELI_ASSERT(new_pos > limit_pos_);
  // Seeking forwards. Source ends.
  cursor_ = limit_;
  return false;
}

}  // namespace riegeli
