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

#include <stdint.h>

#include <utility>

#include "absl/status/status.h"
#include "riegeli/base/base.h"
#include "riegeli/base/status.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if __cplusplus < 201703
constexpr uintptr_t ObjectState::kHealthy;
constexpr uintptr_t ObjectState::kClosedSuccessfully;
#endif

absl::Status ObjectState::status() const {
  if (status_ptr_ == kHealthy) return absl::OkStatus();
  if (status_ptr_ == kClosedSuccessfully) {
    return absl::FailedPreconditionError("Object closed");
  }
  return reinterpret_cast<const FailedStatus*>(status_ptr_)->status;
}

bool ObjectState::Fail(absl::Status status) {
  RIEGELI_ASSERT(!status.ok())
      << "Failed precondition of ObjectState::Fail(): status not failed";
  if (status_ptr_ == kHealthy || status_ptr_ == kClosedSuccessfully) {
    status_ptr_ = reinterpret_cast<uintptr_t>(new FailedStatus{
        status_ptr_ == kClosedSuccessfully, std::move(status)});
  }
  return false;
}

bool ObjectState::AnnotateStatus(absl::string_view detail) {
  RIEGELI_ASSERT(!not_failed())
      << "Failed precondition of ObjectState::AnnotateStatus(): "
         "ObjectState not failed";
  absl::Status& status = reinterpret_cast<FailedStatus*>(status_ptr_)->status;
  status = Annotate(status, detail);
  return false;
}

void Object::Done() {}

bool Object::Fail(absl::Status status) {
  RIEGELI_ASSERT(!status.ok())
      << "Failed precondition of Object::Fail(): status not failed";
  OnFail();
  if (ABSL_PREDICT_FALSE(!not_failed())) return false;
  state_.Fail(std::move(status));
  DefaultAnnotateStatus();
  return false;
}

bool Object::Fail(const Object& dependency) {
  RIEGELI_ASSERT(!dependency.healthy())
      << "Failed precondition of Object::Fail(): dependency healthy";
  return Fail(dependency.status());
}

void Object::OnFail() {}

void Object::DefaultAnnotateStatus() {
  RIEGELI_ASSERT(!not_failed())
      << "Failed precondition of Object::DefaultAnnotateStatus(): "
         "Object not failed";
}

bool Object::AnnotateStatus(absl::string_view detail) {
  RIEGELI_ASSERT(!not_failed())
      << "Failed precondition of Object::AnnotateStatus(): Object not failed";
  return state_.AnnotateStatus(detail);
}

bool Object::FailWithoutAnnotation(absl::Status status) {
  RIEGELI_ASSERT(!status.ok())
      << "Failed precondition of Object::FailWithoutAnnotation(): "
         "status not failed";
  OnFail();
  if (ABSL_PREDICT_FALSE(!not_failed())) return false;
  state_.Fail(std::move(status));
  return false;
}

bool Object::FailWithoutAnnotation(const Object& dependency) {
  RIEGELI_ASSERT(!dependency.healthy())
      << "Failed precondition of Object::FailWithoutAnnotation(): "
         "dependency healthy";
  return FailWithoutAnnotation(dependency.status());
}

TypeId Object::GetTypeId() const { return TypeId(); }

}  // namespace riegeli
