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

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#endif

#include "riegeli/base/errno_mapping.h"

#ifdef _WIN32
#include <stdint.h>
#include <windows.h>
#endif

#include <cerrno>

#include "absl/status/status.h"
#ifdef _WIN32
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/unicode.h"
#endif

namespace riegeli {

#ifdef _WIN32

namespace {

absl::StatusCode WindowsErrorToStatusCode(DWORD error_number) {
  switch (error_number) {
    case ERROR_SUCCESS:
      return absl::StatusCode::kOk;
    case ERROR_OPERATION_ABORTED:
      return absl::StatusCode::kCancelled;
    case ERROR_INVALID_HANDLE:
    case ERROR_INVALID_PARAMETER:
    case ERROR_BUFFER_OVERFLOW:
    case ERROR_INVALID_NAME:
    case ERROR_NEGATIVE_SEEK:
    case ERROR_DIRECTORY:
    case ERROR_REPARSE_TAG_INVALID:
    case WSAEFAULT:
    case WSAEINVAL:
    case WSAENAMETOOLONG:
      return absl::StatusCode::kInvalidArgument;
    case ERROR_FILE_NOT_FOUND:
    case ERROR_PATH_NOT_FOUND:
    case ERROR_INVALID_DRIVE:
    case ERROR_BAD_UNIT:
    case ERROR_BAD_NETPATH:
    case ERROR_DEV_NOT_EXIST:
    case ERROR_BAD_PATHNAME:
      return absl::StatusCode::kNotFound;
    case ERROR_FILE_EXISTS:
    case ERROR_ALREADY_EXISTS:
      return absl::StatusCode::kAlreadyExists;
    case ERROR_ACCESS_DENIED:
    case ERROR_INVALID_ACCESS:
    case ERROR_CURRENT_DIRECTORY:
    case ERROR_WRITE_PROTECT:
    case ERROR_SHARING_VIOLATION:
    case ERROR_CANNOT_MAKE:
    case ERROR_NOACCESS:
    case WSAEACCES:
      return absl::StatusCode::kPermissionDenied;
    case ERROR_TOO_MANY_OPEN_FILES:
    case ERROR_NOT_ENOUGH_MEMORY:
    case ERROR_OUTOFMEMORY:
    case ERROR_HANDLE_DISK_FULL:
    case ERROR_DISK_FULL:
    case WSAEMFILE:
      return absl::StatusCode::kResourceExhausted;
    case ERROR_BROKEN_PIPE:
    case ERROR_BUSY_DRIVE:
    case ERROR_DIR_NOT_EMPTY:
    case ERROR_BUSY:
    case ERROR_OPEN_FILES:
    case ERROR_DEVICE_IN_USE:
    case WSAEBADF:
      return absl::StatusCode::kFailedPrecondition;
    case ERROR_HANDLE_EOF:
      return absl::StatusCode::kOutOfRange;
    case ERROR_INVALID_FUNCTION:
    case ERROR_NOT_SUPPORTED:
      return absl::StatusCode::kUnimplemented;
    case ERROR_NOT_READY:
    case ERROR_LOCK_VIOLATION:
    case ERROR_LOCKED:
    case ERROR_RETRY:
    case WSAEINTR:
      return absl::StatusCode::kUnavailable;
    default:
      return absl::StatusCode::kUnknown;
  }
}

}  // namespace

#endif  // _WIN32

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

#ifdef _WIN32
absl::Status WindowsErrorToStatus(uint32_t error_number,
                                  absl::string_view message) {
  LPWSTR os_message;
  const DWORD length = FormatMessageW(
      FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM |
          FORMAT_MESSAGE_IGNORE_INSERTS,
      nullptr, IntCast<DWORD>(error_number), 0,
      reinterpret_cast<LPWSTR>(&os_message), 0, nullptr);
  const absl::Status status(
      WindowsErrorToStatusCode(IntCast<DWORD>(error_number)),
      absl::StrCat(message, ": ",
                   WideToUtf8Lossy(absl::MakeConstSpan(
                       os_message, IntCast<size_t>(length)))));
  LocalFree(os_message);
  return status;
}
#endif  // _WIN32

}  // namespace riegeli
