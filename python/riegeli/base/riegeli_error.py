# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

__all__ = ('RiegeliError', 'CancelledError', 'UnknownError',
           'InvalidArgumentError', 'DeadlineExceededError', 'NotFoundError',
           'AlreadyExistsError', 'PermissionDeniedError',
           'UnauthenticatedError', 'ResourceExhaustedError',
           'FailedPreconditionError', 'AbortedError', 'OutOfRangeError',
           'UnimplementedError', 'InternalError', 'UnavailableError',
           'DataLossError')


class RiegeliError(Exception):
  """Base class of errors reported by Google APIs.

  Sometimes multiple error codes may apply. Services should return the most
  specific error code that applies. For example, prefer `OutOfRangeError` over
  `FailedPreconditionError` if both codes apply. Similarly prefer
  `NotFoundError` or `AlreadyExistsError` over `FailedPreconditionError`.

  Attributes:
    code: Error code classifying the error, matching C++ StatusCode.
  """


class CancelledError(RiegeliError):
  """The operation was cancelled, typically by the caller."""
  code = 1


class UnknownError(RiegeliError):
  """Unknown error.

  For example, this error may be returned when a Status value received from
  another address space belongs to an error-space that is not known in this
  address space. Also errors raised by APIs that do not return enough error
  information may be converted to this error.
  """
  code = 2


class InvalidArgumentError(RiegeliError):
  """The client specified an invalid argument.

  Note that this differs from `FailedPreconditionError`. `InvalidArgumentError`
  indicates arguments that are problematic regardless of the state of the system
  (e.g., a malformed file name).
  """
  code = 3


class DeadlineExceededError(RiegeliError):
  """The deadline expired before the operation could complete.

  For operations that change the state of the system, this error may be returned
  even if the operation has completed successfully. or example, a successful
  response from a server could have been delayed long enough for the deadline to
  expire.
  """
  code = 4


class NotFoundError(RiegeliError):
  """Some requested entity (e.g., file or directory) was not found.

  Note to server developers: if a request is denied for an entire class of
  users, such as gradual feature rollout or undocumented allowlist,
  `NotFoundError` may be used. If a request is denied for some users within a
  class of users, such as user-based access control, `PermissionDeniedError`
  must be used.
  """
  code = 5


class AlreadyExistsError(RiegeliError):
  """The entity that a client attempted to create already exists."""
  code = 6


class PermissionDeniedError(RiegeliError):
  """The caller does not have permission to execute the specified operation.

  `PermissionDeniedError` must not be used for rejections caused by exhausting
  some resource (use `ResourceExhaustedError` instead for those errors).
  `PermissionDeniedError` must not be used if the caller can not be identified
  (use `UnauthenticatedError` instead for those errors). This error code does
  not imply the request is valid or the requested entity exists or satisfies
  other pre-conditions.
  """
  code = 7


class UnauthenticatedError(RiegeliError):
  """No valid authentication credentials for the operation."""
  code = 16


class ResourceExhaustedError(RiegeliError):
  """Some resource has been exhausted.

  Perhaps a per-user quota, or perhaps the entire file system is out of
  space.
  """
  code = 8


class FailedPreconditionError(RiegeliError):
  """Failed precondition.

  The operation was rejected because the system is not in a state required for
  the operation's execution. For example, the directory to be deleted is
  non-empty, an rmdir operation is applied to a non-directory, etc.

  A litmus test that may help a service implementor in deciding between
  `FailedPreconditionError`, `AbortedError`, and `UnavailableError`:
   (a) Use `UnavailableError` if the client can retry just the failing call.
   (b) Use `AbortedError` if the client should retry at a higher-level (e.g.,
       when a client-specified test-and-set fails, indicating the client should
       restart a read-modify-write sequence).
   (c) Use `FailedPreconditionError` if the client should not retry until the
       system state has been explicitly fixed. E.g., if an "rmdir" fails because
       the directory is non-empty, `FailedPreconditionError` should be returned
       since the client should not retry unless the files are deleted from the
       directory.
  """
  code = 9


class AbortedError(RiegeliError):
  """The operation was aborted.

  Typically due to a concurrency issue such as a sequencer check failure or
  transaction abort.

  See litmus test at `FailedPreconditionError` for deciding between
  `FailedPreconditionError`, `AbortedError`, and `UnavailableError`.
  """
  code = 10


class OutOfRangeError(RiegeliError):
  """The operation was attempted past the valid range.

  E.g., seeking or reading past end-of-file.

  Unlike `InvalidArgumentError`, this error indicates a problem that may be
  fixed if the system state changes. For example, a 32-bit file system will
  generate `InvalidArgumentError` if asked to read at an offset that is not in
  the range [0,2^32-1], but it will generate `OutOfRangeError` if asked to read
  from an offset past the current file size.

  There is a fair bit of overlap between `FailedPreconditionError` and
  `OutOfRangeError`. We recommend using `OutOfRangeError` (the more specific
  error) when it applies so that callers who are iterating through a space can
  easily look for an `OutOfRangeError` error to detect when they are done.
  """
  code = 11


class UnimplementedError(RiegeliError):
  """The operation is not implemented.

  Or is not supported/enabled in this service.
  """
  code = 12


class InternalError(RiegeliError):
  """Internal errors.

  This means that some invariants expected by the underlying system have been
  broken. This error code is reserved for serious errors.
  """
  code = 13


class UnavailableError(RiegeliError):
  """The service is currently unavailable.

  This is most likely a transient condition, which can be corrected by retrying
  with a backoff.

  See litmus test at `FailedPreconditionError` for deciding between
  `FailedPreconditionError`, `AbortedError`, and `UnavailableError`.
  """
  code = 14


class DataLossError(RiegeliError):
  """Unrecoverable data loss or corruption."""
  code = 15
