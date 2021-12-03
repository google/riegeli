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

#include "riegeli/base/base.h"

#include <cstring>
#include <exception>
#include <iostream>
#include <new>
#include <sstream>

#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"

namespace riegeli {

namespace internal {

CheckFailed::CheckFailed(const char* file, int line, const char* function,
                         const char* message) {
  stream_ << "Check failed at " << file << ":" << line << " in " << function
          << ": " << message << " ";
}

CheckFailed::~CheckFailed() {
  std::cerr << stream_.str() << std::endl;
  std::terminate();
}

}  // namespace internal

void ResizeStringAmortized(std::string& dest, size_t new_size) {
  if (new_size > dest.capacity()) {
    dest.reserve(UnsignedMin(
        UnsignedMax(new_size, dest.capacity() + dest.capacity() / 2),
        dest.max_size()));
  }
  dest.resize(new_size);
}

absl::Cord MakeFlatCord(absl::string_view src) {
  if (src.size() <= 4096 - 13 /* `kMaxFlatSize` from cord.cc */) {
    // `absl::Cord(absl::string_view)` allocates a single node of that length.
    return absl::Cord(src);
  }
  char* const ptr = static_cast<char*>(operator new(src.size()));
  std::memcpy(ptr, src.data(), src.size());
  return absl::MakeCordFromExternal(
      absl::string_view(ptr, src.size()), [](absl::string_view data) {
#if __cpp_sized_deallocation || __GXX_DELETE_WITH_SIZE__
        operator delete(const_cast<char*>(data.data()), data.size());
#else
        operator delete(const_cast<char*>(data.data()));
#endif
      });
}

}  // namespace riegeli
