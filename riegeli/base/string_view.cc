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

#include "riegeli/base/string_view.h"

#if !RIEGELI_INTERNAL_HAS_STD_STRING_VIEW

#include <stddef.h>
#include <algorithm>
#include <cstring>
#include <ostream>

#include "riegeli/base/assert.h"
#include "riegeli/base/base.h"

namespace riegeli {

namespace {

void WritePadding(std::ostream& out, size_t pad) {
  char buf[64];
  std::memset(buf, out.fill(), sizeof(buf));
  while (pad > 0) {
    const size_t length = UnsignedMin(pad, sizeof(buf));
    out.write(buf, length);
    pad -= length;
  }
}

}  // namespace

constexpr string_view::size_type string_view::npos;

string_view::size_type string_view::find(string_view needle,
                                         size_type pos) const noexcept {
  if (pos > size()) return npos;
  if (needle.empty()) return 0;
  const iterator found =
      std::search(begin() + pos, end(), needle.begin(), needle.end());
  return found == end() ? npos : found - begin();
}

string_view::size_type string_view::find(char needle, size_type pos) const
    noexcept {
  if (pos >= size()) return npos;
  const iterator found = std::find(begin() + pos, end(), needle);
  return found == end() ? npos : found - begin();
}

string_view::size_type string_view::rfind(string_view needle,
                                          size_type pos) const noexcept {
  if (needle.size() > size()) return npos;
  if (needle.empty()) return UnsignedMin(pos, size());
  const iterator limit =
      begin() + UnsignedMin(pos, size() - needle.size()) + needle.size();
  const iterator found =
      std::find_end(begin(), limit, needle.begin(), needle.end());
  return found == limit ? npos : found - begin();
}

string_view::size_type string_view::rfind(char needle, size_type pos) const
    noexcept {
  if (empty()) return npos;
  const reverse_iterator found =
      std::find(rend() - UnsignedMin(pos, size() - 1) - 1, rend(), needle);
  return found == rend() ? npos : rend() - found - 1;
}

std::ostream& operator<<(std::ostream& out, string_view str) {
  std::ostream::sentry sentry(out);
  if (sentry) {
    size_t lpad = 0;
    size_t rpad = 0;
    RIEGELI_ASSERT_GE(out.width(), 0);
    if (static_cast<size_t>(out.width()) > str.size()) {
      size_t pad = out.width() - str.size();
      if ((out.flags() & out.adjustfield) == out.left) {
        rpad = pad;
      } else {
        lpad = pad;
      }
    }
    if (lpad > 0) WritePadding(out, lpad);
    if (!str.empty()) out.write(str.data(), str.size());
    if (rpad > 0) WritePadding(out, rpad);
    out.width(0);
  }
  return out;
}

}  // namespace riegeli

#endif  // !RIEGELI_INTERNAL_HAS_STD_STRING_VIEW
