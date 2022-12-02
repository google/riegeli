// Copyright 2018 Google LLC
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

#include "riegeli/base/errno_mapping.h"

#include <cerrno>

#include "absl/status/status.h"

namespace riegeli {

int StatusCodeToErrno(absl::StatusCode status_code) {
  switch (status_code) {
    case absl::StatusCode::kOk:
      return 0;
    case absl::StatusCode::kCancelled:
      return ECANCELED;
    case absl::StatusCode::kUnknown:
      return EIO;
    case absl::StatusCode::kInvalidArgument:
      return EINVAL;
    case absl::StatusCode::kDeadlineExceeded:
      return ETIMEDOUT;
    case absl::StatusCode::kNotFound:
      return ENOENT;
    case absl::StatusCode::kAlreadyExists:
      return EEXIST;
    case absl::StatusCode::kPermissionDenied:
      return EACCES;
    case absl::StatusCode::kResourceExhausted:
      return ENOSPC;
    case absl::StatusCode::kFailedPrecondition:
      // Does not round trip:
      // `absl::ErrnoToStatusCode(EINVAL) == absl::StatusCode::kInvalidArgument`
      return EINVAL;
    case absl::StatusCode::kAborted:
      return EDEADLK;
    case absl::StatusCode::kOutOfRange:
      return ERANGE;
    case absl::StatusCode::kUnimplemented:
      return ENOTSUP;
    case absl::StatusCode::kInternal:
      // Does not round trip:
      // `absl::ErrnoToStatusCode(EIO) == absl::StatusCode::kUnknown`
      return EIO;
    case absl::StatusCode::kUnavailable:
      return EAGAIN;
    case absl::StatusCode::kDataLoss:
      // Does not round trip:
      // `absl::ErrnoToStatusCode(EIO) == absl::StatusCode::kUnknown`
      return EIO;
    case absl::StatusCode::kUnauthenticated:
      // Does not round trip:
      // `absl::ErrnoToStatusCode(EACCES) ==
      //      absl::StatusCode::kPermissionDenied`
      return EACCES;
    default:
      // Does not round trip:
      // `absl::ErrnoToStatusCode(EIO) == absl::StatusCode::kUnknown`
      return EIO;
  }
}

}  // namespace riegeli
