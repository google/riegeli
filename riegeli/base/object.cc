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

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/type_id.h"

namespace riegeli {

ObjectState& ObjectState::operator=(ObjectState&& that) noexcept {
  DeleteStatus(std::exchange(
      status_ptr_, std::exchange(that.status_ptr_, kClosedSuccessfully)));
  return *this;
}

ObjectState::~ObjectState() { DeleteStatus(status_ptr_); }

void ObjectState::Reset(Closed) {
  DeleteStatus(std::exchange(status_ptr_, kClosedSuccessfully));
}

void ObjectState::Reset() { DeleteStatus(std::exchange(status_ptr_, kOk)); }

bool ObjectState::MarkClosed() {
  if (ABSL_PREDICT_TRUE(not_failed())) {
    status_ptr_ = kClosedSuccessfully;
    return true;
  }
  reinterpret_cast<FailedStatus*>(status_ptr_)->closed = true;
  return false;
}

void ObjectState::MarkNotFailed() {
  DeleteStatus(
      std::exchange(status_ptr_, is_open() ? kOk : kClosedSuccessfully));
}

inline void ObjectState::DeleteStatus(uintptr_t status_ptr) {
  if (ABSL_PREDICT_FALSE(status_ptr != kOk &&
                         status_ptr != kClosedSuccessfully)) {
    delete reinterpret_cast<FailedStatus*>(status_ptr);
  }
}

absl::Status ObjectState::status() const {
  if (status_ptr_ == kOk) return absl::OkStatus();
  if (status_ptr_ == kClosedSuccessfully) {
    return absl::FailedPreconditionError("Object closed");
  }
  return reinterpret_cast<const FailedStatus*>(status_ptr_)->status;
}

bool ObjectState::Fail(absl::Status status) {
  RIEGELI_ASSERT(!status.ok())
      << "Failed precondition of ObjectState::Fail(): status not failed";
  if (status_ptr_ == kOk || status_ptr_ == kClosedSuccessfully) {
    status_ptr_ = reinterpret_cast<uintptr_t>(new FailedStatus{
        status_ptr_ == kClosedSuccessfully, std::move(status)});
  }
  return false;
}

void ObjectState::SetStatus(absl::Status status) {
  RIEGELI_ASSERT(!status.ok())
      << "Failed precondition of ObjectState::SetStatus(): status not failed";
  RIEGELI_ASSERT(!not_failed())
      << "Failed precondition of ObjectState::SetStatus(): "
         "ObjectState not failed";
  reinterpret_cast<FailedStatus*>(status_ptr_)->status = std::move(status);
}

bool Object::Close() {
  if (ABSL_PREDICT_FALSE(!state_.is_open())) return state_.not_failed();
  Done();
  return state_.MarkClosed();
}

void Object::Done() {}

bool Object::Fail(absl::Status status) {
  RIEGELI_ASSERT(!status.ok())
      << "Failed precondition of Object::Fail(): status not failed";
  if (ABSL_PREDICT_FALSE(!not_failed())) return false;
  state_.Fail(std::move(status));
  state_.SetStatus(AnnotateStatus(state_.status()));
  OnFail();
  return false;
}

void Object::SetStatus(absl::Status status) {
  RIEGELI_ASSERT(!status.ok())
      << "Failed precondition of Object::SetStatus(): status not failed";
  RIEGELI_ASSERT(!not_failed())
      << "Failed precondition of Object::SetStatus(): Object not failed";
  state_.SetStatus(std::move(status));
}

void Object::OnFail() {}

absl::Status Object::AnnotateStatusImpl(absl::Status status) { return status; }

bool Object::FailWithoutAnnotation(absl::Status status) {
  RIEGELI_ASSERT(!status.ok())
      << "Failed precondition of Object::FailWithoutAnnotation(): "
         "status not failed";
  if (ABSL_PREDICT_FALSE(!not_failed())) return false;
  state_.Fail(std::move(status));
  OnFail();
  return false;
}

absl::Status Object::StatusOrAnnotate(absl::Status other_status) {
  if (ABSL_PREDICT_FALSE(!ok())) return status();
  return AnnotateStatus(std::move(other_status));
}

TypeId Object::GetTypeId() const { return TypeId(); }

}  // namespace riegeli
