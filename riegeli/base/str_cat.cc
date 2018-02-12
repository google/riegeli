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

#include "riegeli/base/str_cat.h"

#include <initializer_list>
#include <string>
#include <type_traits>

#include "riegeli/base/base.h"
#include "riegeli/base/string_view.h"

namespace riegeli {
namespace internal {

namespace {

template <typename Int>
string_view SignedToBuffer(Int value, char (&buffer)[kIntToBufferSize()]) {
  char* cursor = buffer + kIntToBufferSize();
  typename std::make_unsigned<Int>::type abs_value =
      static_cast<typename std::make_unsigned<Int>::type>(value);
  if (value < 0) abs_value = -abs_value;
  do {
    *--cursor = '0' + abs_value % 10;
    abs_value /= 10;
  } while (abs_value != 0);
  if (value < 0) *--cursor = '-';
  return string_view(cursor, PtrDistance(cursor, buffer + kIntToBufferSize()));
}

template <typename Int>
string_view UnsignedToBuffer(Int value, char (&buffer)[kIntToBufferSize()]) {
  char* cursor = buffer + kIntToBufferSize();
  do {
    *--cursor = '0' + value % 10;
    value /= 10;
  } while (value != 0);
  return string_view(cursor, PtrDistance(cursor, buffer + kIntToBufferSize()));
}

}  // namespace

string_view IntToBuffer(int value, char (&buffer)[kIntToBufferSize()]) {
  return SignedToBuffer(value, buffer);
}

string_view IntToBuffer(unsigned value, char (&buffer)[kIntToBufferSize()]) {
  return UnsignedToBuffer(value, buffer);
}

string_view IntToBuffer(long value, char (&buffer)[kIntToBufferSize()]) {
  return SignedToBuffer(value, buffer);
}

string_view IntToBuffer(unsigned long value,
                        char (&buffer)[kIntToBufferSize()]) {
  return UnsignedToBuffer(value, buffer);
}

string_view IntToBuffer(long long value, char (&buffer)[kIntToBufferSize()]) {
  return SignedToBuffer(value, buffer);
}

string_view IntToBuffer(unsigned long long value,
                        char (&buffer)[kIntToBufferSize()]) {
  return UnsignedToBuffer(value, buffer);
}

std::string StrCatImpl(string_view a, string_view b) {
  std::string result;
  RIEGELI_CHECK_LE(b.size(), result.max_size() - a.size())
      << "Failed precondition of StrCat(): string size overflow";
  result.reserve(a.size() + b.size());
  result.append(a.data(), a.size());
  result.append(b.data(), b.size());
  return result;
}

std::string StrCatImpl(string_view a, string_view b, string_view c) {
  std::string result;
  RIEGELI_CHECK_LE(b.size(), result.max_size() - a.size())
      << "Failed precondition of StrCat(): string size overflow";
  size_t size = a.size() + b.size();
  RIEGELI_CHECK_LE(c.size(), result.max_size() - size)
      << "Failed precondition of StrCat(): string size overflow";
  size += c.size();
  result.reserve(size);
  result.append(a.data(), a.size());
  result.append(b.data(), b.size());
  result.append(c.data(), c.size());
  return result;
}

std::string StrCatImpl(string_view a, string_view b, string_view c, string_view d) {
  std::string result;
  RIEGELI_CHECK_LE(b.size(), result.max_size() - a.size())
      << "Failed precondition of StrCat(): string size overflow";
  size_t size = a.size() + b.size();
  RIEGELI_CHECK_LE(c.size(), result.max_size() - size)
      << "Failed precondition of StrCat(): string size overflow";
  size += c.size();
  RIEGELI_CHECK_LE(d.size(), result.max_size() - size)
      << "Failed precondition of StrCat(): string size overflow";
  size += d.size();
  result.reserve(size);
  result.append(a.data(), a.size());
  result.append(b.data(), b.size());
  result.append(c.data(), c.size());
  result.append(d.data(), d.size());
  return result;
}

std::string StrCatImpl(std::initializer_list<string_view> values) {
  std::string result;
  size_t size = 0;
  for (string_view value : values) {
    RIEGELI_CHECK_LE(value.size(), result.max_size() - size)
        << "Failed precondition of StrCat(): string size overflow";
    size += value.size();
  }
  result.reserve(size);
  for (string_view value : values) result.append(value.data(), value.size());
  return result;
}

void StrAppendImpl(std::string* dest, string_view a) {
  RIEGELI_CHECK_LE(a.size(), dest->max_size() - dest->size())
      << "Failed precondition of StrAppend(): string size overflow";
  dest->append(a.data(), a.size());
}

void StrAppendImpl(std::string* dest, string_view a, string_view b) {
  RIEGELI_CHECK_LE(a.size(), dest->max_size() - dest->size())
      << "Failed precondition of StrAppend(): string size overflow";
  size_t size = dest->size() + a.size();
  RIEGELI_CHECK_LE(b.size(), dest->max_size() - size)
      << "Failed precondition of StrAppend(): string size overflow";
  size += b.size();
  if (dest->capacity() < size) dest->reserve(size);
  dest->append(a.data(), a.size());
  dest->append(b.data(), b.size());
}

void StrAppendImpl(std::string* dest, string_view a, string_view b, string_view c) {
  RIEGELI_CHECK_LE(a.size(), dest->max_size() - dest->size())
      << "Failed precondition of StrAppend(): string size overflow";
  size_t size = dest->size() + a.size();
  RIEGELI_CHECK_LE(b.size(), dest->max_size() - size)
      << "Failed precondition of StrAppend(): string size overflow";
  size += b.size();
  RIEGELI_CHECK_LE(c.size(), dest->max_size() - size)
      << "Failed precondition of StrAppend(): string size overflow";
  size += c.size();
  if (dest->capacity() < size) dest->reserve(size);
  dest->append(a.data(), a.size());
  dest->append(b.data(), b.size());
  dest->append(c.data(), c.size());
}

void StrAppendImpl(std::string* dest, string_view a, string_view b, string_view c,
                   string_view d) {
  RIEGELI_CHECK_LE(a.size(), dest->max_size() - dest->size())
      << "Failed precondition of StrAppend(): string size overflow";
  size_t size = dest->size() + a.size();
  RIEGELI_CHECK_LE(b.size(), dest->max_size() - size)
      << "Failed precondition of StrAppend(): string size overflow";
  size += b.size();
  RIEGELI_CHECK_LE(c.size(), dest->max_size() - size)
      << "Failed precondition of StrAppend(): string size overflow";
  size += c.size();
  RIEGELI_CHECK_LE(d.size(), dest->max_size() - size)
      << "Failed precondition of StrAppend(): string size overflow";
  size += d.size();
  if (dest->capacity() < size) dest->reserve(size);
  dest->append(a.data(), a.size());
  dest->append(b.data(), b.size());
  dest->append(c.data(), c.size());
  dest->append(d.data(), d.size());
}

void StrAppendImpl(std::string* dest, std::initializer_list<string_view> values) {
  size_t size = dest->size();
  for (string_view value : values) {
    RIEGELI_CHECK_LE(value.size(), dest->max_size() - size)
        << "Failed precondition of StrAppend(): string size overflow";
    size += value.size();
  }
  if (dest->capacity() < size) dest->reserve(size);
  for (string_view value : values) dest->append(value.data(), value.size());
}

}  // namespace internal
}  // namespace riegeli
