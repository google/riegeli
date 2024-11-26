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

#include <sstream>
#include <string>
#include <utility>

#include "absl/log/absl_log.h"

namespace riegeli {
namespace assert_internal {

CheckResult::CheckResult(const char* function, const char* prefix)
    : header_(new std::ostringstream()) {
  header() << "Check failed in " << function << ": " << prefix;
}

CheckFailed::CheckFailed(const char* file, int line, CheckResult check_result)
    : file_(file),
      line_(line),
      check_result_(check_result),
      details_(new std::ostringstream()) {}

CheckFailed::~CheckFailed() {
  std::ostringstream& message = check_result_.header();
  const std::string details = std::move(*details_).str();
  if (!details.empty()) message << "; " << details;
  ABSL_LOG(FATAL).AtLocation(file_, line_) << std::move(message).str();
}

void CheckNotNullFailed(const char* file, int line, const char* function,
                        const char* expression) {
  CheckResult check_result(function, expression);
  check_result.header() << " != nullptr";
  CheckFailed check_failed(file, line, check_result);
}

CheckResult CheckImpossibleResult(const char* function) {
  return CheckResult(function, "Impossible");
}

}  // namespace assert_internal
}  // namespace riegeli
