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

#include "riegeli/base/stream_utils.h"

#include <stddef.h>

#include <cassert>
#include <cstring>
#include <ios>
#include <ostream>

namespace riegeli {

void WritePadding(std::ostream& dest, size_t length, char fill) {
  char buffer[64];
  std::memset(buffer, fill, sizeof(buffer));
  while (length > sizeof(buffer)) {
    dest.write(buffer, std::streamsize{sizeof(buffer)});
    length -= sizeof(buffer);
  }
  dest.write(buffer, static_cast<std::streamsize>(length));
}

int StringifyOStream<StringStringifySink>::StringStreambuf::overflow(int src) {
  if (src != traits_type::eof()) dest_->push_back(static_cast<char>(src));
  return traits_type::not_eof(src);
}

std::streamsize StringifyOStream<StringStringifySink>::StringStreambuf::xsputn(
    const char* src, std::streamsize length) {
  assert(length >= 0);
  dest_->append(src, static_cast<size_t>(length));
  return length;
}

}  // namespace riegeli
