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

#include <stddef.h>
#include <atomic>
#include <cstring>

#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/memory.h"

namespace riegeli {

inline Object::FailedStatus::FailedStatus(absl::string_view message)
    : message_size(message.size()) {
  std::memcpy(message_data, message.data(), message.size());
}

bool Object::Fail(absl::string_view message) {
  RIEGELI_ASSERT(!closed())
      << "Failed precondition of Object::Fail(): Object closed";
  const uintptr_t new_status =
      reinterpret_cast<uintptr_t>(NewAligned<FailedStatus>(
          offsetof(FailedStatus, message_data) + message.size(), message));
  uintptr_t old_status = kHealthy();
  if (RIEGELI_UNLIKELY(!status_.compare_exchange_strong(
          old_status, new_status, std::memory_order_release))) {
    // status_ was already set, new_status loses.
    DeleteStatus(new_status);
  }
  return false;
}

bool Object::Fail(absl::string_view message, const Object& src) {
  return Fail(src.healthy() ? message
                            : absl::StrCat(message, ": ", src.Message()));
}

bool Object::Fail(const Object& src) {
  RIEGELI_ASSERT(!src.healthy())
      << "Failed precondition of Object::Fail(Object): "
         "source Object is healthy";
  return Fail(src.Message());
}

absl::string_view Object::Message() const {
  const uintptr_t status = status_.load(std::memory_order_acquire);
  switch (status) {
    case kHealthy():
      return "Healthy";
    case kClosedSuccessfully():
      return "Closed";
    default:
      return absl::string_view(
          reinterpret_cast<const FailedStatus*>(status)->message_data,
          reinterpret_cast<const FailedStatus*>(status)->message_size);
  }
}

TypeId Object::GetTypeId() const { return TypeId(); }

}  // namespace riegeli
