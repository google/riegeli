// Copyright 2024 Google LLC
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

#include "riegeli/digests/highwayhash_digester.h"

#include "highwayhash/hh_types.h"

namespace riegeli {

template <>
alignas(32) const HighwayHashKey
    HighwayHashDigester<highwayhash::HHResult64>::kDefaultKey = {
        0x4ea9929a25d561c6,
        0x98470d187b523e8f,
        0x592040a2da3c4b53,
        0xbff8b246e3c587a2,
};

template <>
alignas(32) const HighwayHashKey
    HighwayHashDigester<highwayhash::HHResult128>::kDefaultKey = {
        0x025ed8a16fb5f783,
        0xb44bc74d89d26c86,
        0x111ea964039fa769,
        0x6f7d7159e15612b6,
};

template <>
alignas(32) const HighwayHashKey
    HighwayHashDigester<highwayhash::HHResult256>::kDefaultKey = {
        0x93fee04321119357,
        0x21e397ea62c264b6,
        0x9d856914f2ad0e15,
        0x64dca6f86247f384,
};

}  // namespace riegeli
