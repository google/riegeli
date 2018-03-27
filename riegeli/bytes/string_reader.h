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

#ifndef RIEGELI_BYTES_STRING_READER_H_
#define RIEGELI_BYTES_STRING_READER_H_

#include <stddef.h>
#include <string>
#include <utility>

#include "riegeli/base/base.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

// A Reader which reads from a char array. It supports random access.
class StringReader final : public Reader {
 public:
  // Creates a closed StringReader.
  StringReader() noexcept : Reader(State::kClosed) {}

  // Will read from the array which is not owned by this StringReader and must
  // be kept alive but not changed until the StringReader is closed.
  StringReader(const char* src, size_t size);

  // Will read from the string which is not owned by this StringReader and must
  // be kept alive but not changed until the StringReader is closed.
  //
  // In order to read from an owned string, use ChainReader(Chain(*src)) or
  // ChainReader(Chain(std::move(*src))) instead.
  explicit StringReader(const std::string* src);

  StringReader(StringReader&& src) noexcept;
  StringReader& operator=(StringReader&& src) noexcept;

  bool SupportsRandomAccess() const override { return true; }
  bool Size(Position* size) const override;

 protected:
  void Done() override;
  bool PullSlow() override;
  bool HopeForMoreSlow() const override;
  bool SeekSlow(Position new_pos) override;

  // Invariants:
  //   if !healthy() then start_ == nullptr
  //   start_pos() == 0
};

// Implementation details follow.

inline StringReader::StringReader(const char* src, size_t size)
    : Reader(State::kOpen) {
  start_ = src;
  cursor_ = src;
  limit_ = src + size;
  limit_pos_ = size;
}

inline StringReader::StringReader(const std::string* src)
    : StringReader(src->data(), src->size()) {}

inline StringReader::StringReader(StringReader&& src) noexcept
    : Reader(std::move(src)) {}

inline StringReader& StringReader::operator=(StringReader&& src) noexcept {
  Reader::operator=(std::move(src));
  return *this;
}

inline bool StringReader::Size(Position* size) const {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  *size = limit_pos_;
  return true;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_STRING_READER_H_
