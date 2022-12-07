// Copyright 2022 Google LLC
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

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#endif

#include "riegeli/base/unicode.h"

#ifdef _WIN32

#include <stddef.h>
#include <windows.h>

#include <limits>
#include <string>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/arithmetic.h"

namespace riegeli {

bool Utf8ToWide(absl::string_view src, std::wstring& dest) {
  dest.clear();
  if (src.empty()) return true;
  if (ABSL_PREDICT_FALSE(src.size() >
                         unsigned{std::numeric_limits<int>::max()})) {
    return false;
  }
  const int dest_size =
      MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, src.data(),
                          IntCast<int>(src.size()), nullptr, 0);
  if (ABSL_PREDICT_FALSE(dest_size == 0)) return false;
  dest.resize(IntCast<size_t>(dest_size));
  MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, src.data(),
                      IntCast<int>(src.size()), &dest[0], dest_size);
  return true;
}

bool WideToUtf8(absl::Span<const wchar_t> src, std::string& dest) {
  dest.clear();
  if (src.empty()) return true;
  if (ABSL_PREDICT_FALSE(src.size() >
                         unsigned{std::numeric_limits<int>::max()})) {
    return false;
  }
  const int dest_size = WideCharToMultiByte(
      CP_UTF8, WC_ERR_INVALID_CHARS, src.data(), IntCast<int>(src.size()),
      nullptr, 0, nullptr, nullptr);
  if (ABSL_PREDICT_FALSE(dest_size == 0)) return false;
  dest.resize(IntCast<size_t>(dest_size));
  WideCharToMultiByte(CP_UTF8, WC_ERR_INVALID_CHARS, src.data(),
                      IntCast<int>(src.size()), &dest[0], dest_size, nullptr,
                      nullptr);
  return true;
}

std::string WideToUtf8Lossy(absl::Span<const wchar_t> src) {
  std::string dest;
  if (src.empty()) return dest;
  const int dest_size = WideCharToMultiByte(CP_UTF8, 0, src.data(),
                                            SaturatingIntCast<int>(src.size()),
                                            nullptr, 0, nullptr, nullptr);
  dest.resize(IntCast<size_t>(dest_size));
  WideCharToMultiByte(CP_UTF8, 0, src.data(),
                      SaturatingIntCast<int>(src.size()), &dest[0], dest_size,
                      nullptr, nullptr);
  return dest;
}

}  // namespace riegeli

#endif
