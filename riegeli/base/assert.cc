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

#include "riegeli/base/assert.h"

#include <cstdlib>
#include <iostream>

namespace riegeli {
namespace internal {

FailedAssert::FailedAssert(const char* file, int line, const char* function,
                           const char* message) {
  FailedAssertMessage(file, line, function, message);
}

FailedAssert::FailedAssert(const char* file, int line, const char* function) {
  std::cerr << "Impossible happened at " << file << ":" << line << " in "
            << function << " ";
}

std::ostream& FailedAssert::stream() { return std::cerr; }

FailedAssert::~FailedAssert() {
  stream() << std::endl;
  std::abort();
}

std::ostream& FailedAssertMessage(const char* file, int line,
                                  const char* function, const char* message) {
  return std::cerr << "Assertion failed at " << file << ":" << line << " in "
                   << function << ": " << message << " ";
}

}  // namespace internal
}  // namespace riegeli
