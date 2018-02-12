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

#ifndef RIEGELI_BASE_STR_CAT_H_
#define RIEGELI_BASE_STR_CAT_H_

#include <stddef.h>
#include <initializer_list>
#include <limits>
#include <string>

#include "riegeli/base/string_view.h"

namespace riegeli {

namespace internal {

constexpr size_t kIntToBufferSize() {
  return size_t{std::numeric_limits<long long>::digits10 + 2};
}

string_view IntToBuffer(int value, char (&buffer)[kIntToBufferSize()]);
string_view IntToBuffer(unsigned value, char (&buffer)[kIntToBufferSize()]);
string_view IntToBuffer(long value, char (&buffer)[kIntToBufferSize()]);
string_view IntToBuffer(unsigned long value,
                        char (&buffer)[kIntToBufferSize()]);
string_view IntToBuffer(long long value, char (&buffer)[kIntToBufferSize()]);
string_view IntToBuffer(unsigned long long value,
                        char (&buffer)[kIntToBufferSize()]);

std::string StrCatImpl(string_view a, string_view b);
std::string StrCatImpl(string_view a, string_view b, string_view c);
std::string StrCatImpl(string_view a, string_view b, string_view c, string_view d);
std::string StrCatImpl(std::initializer_list<string_view> values);

void StrAppendImpl(std::string* dest, string_view a);
void StrAppendImpl(std::string* dest, string_view a, string_view b);
void StrAppendImpl(std::string* dest, string_view a, string_view b, string_view c);
void StrAppendImpl(std::string* dest, string_view a, string_view b, string_view c,
                   string_view d);
void StrAppendImpl(std::string* dest, std::initializer_list<string_view> values);

}  // namespace internal

class AlphaNum {
 public:
  AlphaNum(string_view value) : data_(value) {}
  AlphaNum(const char* value) : data_(value) {}
  AlphaNum(const std::string& value) : data_(value) {}

  AlphaNum(int value) : data_(internal::IntToBuffer(value, digits_)) {}
  AlphaNum(unsigned value) : data_(internal::IntToBuffer(value, digits_)) {}
  AlphaNum(long value) : data_(internal::IntToBuffer(value, digits_)) {}
  AlphaNum(unsigned long value)
      : data_(internal::IntToBuffer(value, digits_)) {}
  AlphaNum(long long value) : data_(internal::IntToBuffer(value, digits_)) {}
  AlphaNum(unsigned long long value)
      : data_(internal::IntToBuffer(value, digits_)) {}

  string_view data() const { return data_; }

  // This would be ambiguous between a character and a number.
  AlphaNum(char) = delete;

 private:
  string_view data_;
  char digits_[internal::kIntToBufferSize()];
};

inline std::string StrCat() { return std::string(); }

inline std::string StrCat(const AlphaNum& a) { return std::string(a.data()); }

inline std::string StrCat(const AlphaNum& a, const AlphaNum& b) {
  return internal::StrCatImpl(a.data(), b.data());
}

inline std::string StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c) {
  return internal::StrCatImpl(a.data(), b.data(), c.data());
}

inline std::string StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c,
                     const AlphaNum& d) {
  return internal::StrCatImpl(a.data(), b.data(), c.data(), d.data());
}

template <typename... Args>
std::string StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c,
              const AlphaNum& d, const AlphaNum& e, const Args&... args) {
  return internal::StrCatImpl({a.data(), b.data(), c.data(), d.data(), e.data(),
                               static_cast<const AlphaNum&>(args).data()...});
}

inline void StrAppend(std::string* dest) {}

inline void StrAppend(std::string* dest, const AlphaNum& a) {
  return internal::StrAppendImpl(dest, a.data());
}

inline void StrAppend(std::string* dest, const AlphaNum& a, const AlphaNum& b) {
  return internal::StrAppendImpl(dest, a.data(), b.data());
}

inline void StrAppend(std::string* dest, const AlphaNum& a, const AlphaNum& b,
                      const AlphaNum& c) {
  return internal::StrAppendImpl(dest, a.data(), b.data(), c.data());
}

inline void StrAppend(std::string* dest, const AlphaNum& a, const AlphaNum& b,
                      const AlphaNum& c, const AlphaNum& d) {
  return internal::StrAppendImpl(dest, a.data(), b.data(), c.data(), d.data());
}

template <typename... Args>
void StrAppend(std::string* dest, const AlphaNum& a, const AlphaNum& b,
               const AlphaNum& c, const AlphaNum& d, const AlphaNum& e,
               const Args&... args) {
  return internal::StrAppendImpl(
      dest, {a.data(), b.data(), c.data(), d.data(), e.data(),
             static_cast<const AlphaNum&>(args).data()...});
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_STR_CAT_H_
