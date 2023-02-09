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

#ifndef RIEGELI_VARINT_VARINT_INTERNAL_H_
#define RIEGELI_VARINT_VARINT_INTERNAL_H_

// IWYU pragma: private, include "riegeli/varint/varint_reading.h"
// IWYU pragma: private, include "riegeli/varint/varint_writing.h"

#include <stddef.h>

#include "riegeli/base/constexpr.h"

namespace riegeli {

RIEGELI_INLINE_CONSTEXPR(size_t, kMaxLengthVarint32, 5);
RIEGELI_INLINE_CONSTEXPR(size_t, kMaxLengthVarint64, 10);

}  // namespace riegeli

#endif  // RIEGELI_VARINT_VARINT_INTERNAL_H_
