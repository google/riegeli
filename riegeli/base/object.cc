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

#include "riegeli/base/object.h"

#include <string>
#include <utility>

#include "riegeli/base/assert.h"
#include "riegeli/base/memory.h"
#include "riegeli/base/string_view.h"

namespace riegeli {

bool Object::Fail(string_view message) {
  RIEGELI_ASSERT(healthy());
  state_ = State::kFailed;
  message_ = std::string(message);
  return false;
}

const std::string& Object::Message() const {
  switch (state_) {
    case State::kHealthy: {
      static const NoDestructor<std::string> kHealthy("Healthy");
      return *kHealthy;
    }
    case State::kClosed: {
      static const NoDestructor<std::string> kClosed("Closed");
      return *kClosed;
    }
    case State::kCancelling:
    case State::kCancelled: {
      static const NoDestructor<std::string> kCancelled("Cancelled");
      return *kCancelled;
    }
    case State::kFailed:
    case State::kFailedAndClosed:
      return message_;
  }
  RIEGELI_UNREACHABLE() << "Unknown state: " << static_cast<int>(state_);
}

TypeId Object::GetTypeId() const { return TypeId(); }

}  // namespace riegeli
