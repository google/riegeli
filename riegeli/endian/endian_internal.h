// Copyright 2021 Google LLC
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

#ifndef RIEGELI_ENDIAN_ENDIAN_INTERNAL_H_
#define RIEGELI_ENDIAN_ENDIAN_INTERNAL_H_

#include <stdint.h>

namespace riegeli {
namespace endian_internal {

// TODO: Use `std::endian` instead when C++20 is available.
//
// With the following implementations the result is not known at preprocessing
// time nor during constexpr evaluation, but these functions are in practice
// optimized out to constants.

inline bool IsLittleEndian() {
  const uint32_t value = 0x00000001;
  return reinterpret_cast<const unsigned char*>(&value)[0] == 1;
}

inline bool IsBigEndian() {
  const uint32_t value = 0x00000001;
  return reinterpret_cast<const unsigned char*>(&value)[3] == 1;
}

}  // namespace endian_internal
}  // namespace riegeli

#endif  // RIEGELI_ENDIAN_ENDIAN_INTERNAL_H_
