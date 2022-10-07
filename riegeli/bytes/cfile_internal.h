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

#ifndef RIEGELI_BYTES_CFILE_INTERNAL_H_
#define RIEGELI_BYTES_CFILE_INTERNAL_H_

// Warning: Do not include this header in other headers, because the definition
// of `off_t` depends on `_FILE_OFFSET_BITS` which can reliably be set only
// in a standalone compilation unit.

#include <stdio.h>
#include <sys/types.h>

#include <type_traits>
#include <utility>

#include "absl/meta/type_traits.h"
#include "absl/strings/string_view.h"

namespace riegeli {
namespace cfile_internal {

// Use `fseeko()` and `ftello()` when available, otherwise `fseek()` and
// `ftell()`.

template <typename File, typename Enable = void>
struct HaveFSeekO : std::false_type {};

template <typename File>
struct HaveFSeekO<File, absl::void_t<decltype(fseeko(std::declval<File*>(),
                                                     std::declval<off_t>(),
                                                     std::declval<int>())),
                                     decltype(ftello(std::declval<File*>()))>>
    : std::true_type {};

using Offset = absl::conditional_t<HaveFSeekO<FILE>::value, off_t, long>;

template <typename File, std::enable_if_t<HaveFSeekO<File>::value, int> = 0>
inline int FSeek(File* file, off_t offset, int whence) {
  return fseeko(file, offset, whence);
}

template <typename File, std::enable_if_t<!HaveFSeekO<File>::value, int> = 0>
inline int FSeek(File* file, long offset, int whence) {
  return fseek(file, offset, whence);
}

constexpr absl::string_view kFSeekFunctionName =
    HaveFSeekO<FILE>::value ? "fseeko()" : "fseek()";

template <typename File, std::enable_if_t<HaveFSeekO<File>::value, int> = 0>
inline off_t FTell(File* file) {
  return ftello(file);
}

template <typename File, std::enable_if_t<!HaveFSeekO<File>::value, int> = 0>
inline long FTell(File* file) {
  return ftell(file);
}

constexpr absl::string_view kFTellFunctionName =
    HaveFSeekO<FILE>::value ? "ftello()" : "ftell()";

}  // namespace cfile_internal
}  // namespace riegeli

#endif  // RIEGELI_BYTES_CFILE_INTERNAL_H_
