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

#ifndef _WIN32

// Make `pwrite()` and `ftruncate()` available.
#if !defined(_XOPEN_SOURCE) || _XOPEN_SOURCE < 500
#undef _XOPEN_SOURCE
#define _XOPEN_SOURCE 500
#endif

// Make `off_t` 64-bit even on 32-bit systems.
#undef _FILE_OFFSET_BITS
#define _FILE_OFFSET_BITS 64

#else

#define WIN32_LEAN_AND_MEAN

#endif

#include "riegeli/bytes/fd_writer.h"

#ifdef _WIN32
#include <io.h>
#endif
#include <fcntl.h>
#include <stddef.h>
#include <stdio.h>
#include <sys/types.h>
#ifndef _WIN32
#include <unistd.h>
#else
#include <windows.h>
#endif

#include <cerrno>
#include <limits>
#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#ifndef _WIN32
#include "absl/strings/match.h"
#endif
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#ifdef _WIN32
#include "riegeli/base/errno_mapping.h"
#endif
#include "riegeli/base/no_destructor.h"
#include "riegeli/base/object.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"
#ifdef _WIN32
#include "riegeli/base/unicode.h"
#endif
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/fd_internal.h"
#include "riegeli/bytes/fd_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void FdWriterBase::Initialize(int dest,
                              absl::optional<std::string>&& assumed_filename,
#ifdef _WIN32
                              int mode,
#endif
                              absl::optional<Position> assumed_pos,
                              absl::optional<Position> independent_pos) {
  RIEGELI_ASSERT_GE(dest, 0)
      << "Failed precondition of FdWriter: negative file descriptor";
  filename_ = fd_internal::ResolveFilename(dest, std::move(assumed_filename));
  InitializePos(dest,
#ifdef _WIN32
                mode, /*mode_was_passed_to_open=*/false,
#endif
                assumed_pos, independent_pos);
}

int FdWriterBase::OpenFd(absl::string_view filename, int mode,
                         Options::Permissions permissions) {
#ifndef _WIN32
  RIEGELI_ASSERT((mode & O_ACCMODE) == O_WRONLY || (mode & O_ACCMODE) == O_RDWR)
      << "Failed precondition of FdWriter: "
         "mode must include either O_WRONLY or O_RDWR";
#else
  RIEGELI_ASSERT((mode & (_O_RDONLY | _O_WRONLY | _O_RDWR)) == _O_WRONLY ||
                 (mode & (_O_RDONLY | _O_WRONLY | _O_RDWR)) == _O_RDWR)
      << "Failed precondition of FdWriter: "
         "mode must include either _O_WRONLY or _O_RDWR";
#endif
  // TODO: When `absl::string_view` becomes C++17 `std::string_view`:
  // `filename_ = filename`
  filename_.assign(filename.data(), filename.size());
#ifndef _WIN32
again:
  const int dest = open(filename_.c_str(), mode, permissions);
  if (ABSL_PREDICT_FALSE(dest < 0)) {
    if (errno == EINTR) goto again;
    BufferedWriter::Reset(kClosed);
    FailOperation("open()");
    return -1;
  }
#else
  std::wstring filename_wide;
  if (ABSL_PREDICT_FALSE(!Utf8ToWide(filename_, filename_wide))) {
    BufferedWriter::Reset(kClosed);
    Fail(absl::InvalidArgumentError("Filename not valid UTF-8"));
    return -1;
  }
  int dest;
  if (ABSL_PREDICT_FALSE(_wsopen_s(&dest, filename_wide.c_str(), mode,
                                   _SH_DENYNO, permissions) != 0)) {
    BufferedWriter::Reset(kClosed);
    FailOperation("_wsopen_s()");
    return -1;
  }
#endif
  return dest;
}

#ifndef _WIN32

void FdWriterBase::InitializePos(int dest, absl::optional<Position> assumed_pos,
                                 absl::optional<Position> independent_pos) {
  int mode = fcntl(dest, F_GETFL);
  if (ABSL_PREDICT_FALSE(mode < 0)) {
    FailOperation("fcntl()");
    return;
  }
  return InitializePos(dest, mode, assumed_pos, independent_pos);
}

#endif

void FdWriterBase::InitializePos(int dest, int mode,
#ifdef _WIN32
                                 bool mode_was_passed_to_open,
#endif
                                 absl::optional<Position> assumed_pos,
                                 absl::optional<Position> independent_pos) {
  RIEGELI_ASSERT(!has_independent_pos_)
      << "Failed precondition of FdWriterBase::InitializePos(): "
         "has_independent_pos_ not reset";
  RIEGELI_ASSERT(supports_random_access_ == LazyBoolState::kUnknown)
      << "Failed precondition of FdWriterBase::InitializePos(): "
         "supports_random_access_ not reset";
  RIEGELI_ASSERT(supports_read_mode_ == LazyBoolState::kUnknown)
      << "Failed precondition of FdWriterBase::InitializePos(): "
         "supports_read_mode_ not reset";
  RIEGELI_ASSERT_EQ(random_access_status_, absl::OkStatus())
      << "Failed precondition of FdWriterBase::InitializePos(): "
         "random_access_status_ not reset";
  RIEGELI_ASSERT_EQ(read_mode_status_, absl::OkStatus())
      << "Failed precondition of FdWriterBase::InitializePos(): "
         "read_mode_status_ not reset";
#ifndef _WIN32
  if ((mode & O_ACCMODE) != O_RDWR) {
    supports_read_mode_ = LazyBoolState::kFalse;
    static const NoDestructor<absl::Status> status(
        absl::UnimplementedError("Mode does not include O_RDWR"));
    read_mode_status_ = *status;
  }
#else
  RIEGELI_ASSERT(original_mode_ == absl::nullopt)
      << "Failed precondition of FdWriterBase::InitializePos(): "
         "original_mode_ not reset";
  int text_mode =
      mode & (_O_BINARY | _O_TEXT | _O_WTEXT | _O_U16TEXT | _O_U8TEXT);
  if (mode_was_passed_to_open) {
    if ((mode & (_O_RDONLY | _O_WRONLY | _O_RDWR)) != _O_RDWR) {
      supports_read_mode_ = LazyBoolState::kFalse;
      static const NoDestructor<absl::Status> status(
          absl::UnimplementedError("Mode does not include _O_RDWR"));
      read_mode_status_ = *status;
    }
  } else if (text_mode != 0) {
    const int original_mode = _setmode(dest, text_mode);
    if (ABSL_PREDICT_FALSE(original_mode < 0)) {
      FailOperation("_setmode()");
      return;
    }
    original_mode_ = original_mode;
  }
  if (assumed_pos == absl::nullopt) {
    if (text_mode == 0) {
      // There is no `_getmode()`, but `_setmode()` returns the previous mode.
      text_mode = _setmode(dest, _O_BINARY);
      if (ABSL_PREDICT_FALSE(text_mode < 0)) {
        FailOperation("_setmode()");
        return;
      }
      if (ABSL_PREDICT_FALSE(_setmode(dest, text_mode) < 0)) {
        FailOperation("_setmode()");
        return;
      }
    }
    if (text_mode != _O_BINARY) {
      if (ABSL_PREDICT_FALSE(independent_pos != absl::nullopt)) {
        Fail(absl::InvalidArgumentError(
            "FdWriterBase::Options::independent_pos() requires binary mode"));
        return;
      }
      assumed_pos = 0;
    }
  }
#endif
  if (assumed_pos != absl::nullopt) {
    if (ABSL_PREDICT_FALSE(independent_pos != absl::nullopt)) {
      Fail(absl::InvalidArgumentError(
          "FdWriterBase::Options::assumed_pos() and independent_pos() "
          "must not be both set"));
      return;
    }
    if (ABSL_PREDICT_FALSE(
            *assumed_pos >
            Position{std::numeric_limits<fd_internal::Offset>::max()})) {
      FailOverflow();
      return;
    }
    set_start_pos(*assumed_pos);
    supports_random_access_ = LazyBoolState::kFalse;
    supports_read_mode_ = LazyBoolState::kFalse;
    static const NoDestructor<absl::Status> status(absl::UnimplementedError(
        "FileWriterBase::Options::assumed_pos() excludes random access"));
    random_access_status_ = *status;
    read_mode_status_.Update(random_access_status_);
  } else if (independent_pos != absl::nullopt) {
    if (ABSL_PREDICT_FALSE((mode & O_APPEND) != 0)) {
      Fail(
          absl::InvalidArgumentError("FdWriterBase::Options::independent_pos() "
                                     "is incompatible with append mode"));
      return;
    }
    has_independent_pos_ = true;
    if (ABSL_PREDICT_FALSE(
            *independent_pos >
            Position{std::numeric_limits<fd_internal::Offset>::max()})) {
      FailOverflow();
      return;
    }
    set_start_pos(*independent_pos);
    supports_random_access_ = LazyBoolState::kTrue;
    if (
#ifdef _WIN32
        mode_was_passed_to_open &&
#endif
        supports_read_mode_ == LazyBoolState::kUnknown) {
      supports_read_mode_ = LazyBoolState::kTrue;
    }
  } else {
    const fd_internal::Offset file_pos = fd_internal::LSeek(
        dest, 0, (mode & O_APPEND) != 0 ? SEEK_END : SEEK_CUR);
    if (file_pos < 0) {
      // Random access is not supported. Assume the current position as 0.
      supports_random_access_ = LazyBoolState::kFalse;
      supports_read_mode_ = LazyBoolState::kFalse;
      random_access_status_ =
          FailedOperationStatus(fd_internal::kLSeekFunctionName);
      read_mode_status_.Update(random_access_status_);
      return;
    }
    set_start_pos(IntCast<Position>(file_pos));
    if ((mode & O_APPEND) != 0) {
      // `fd_internal::LSeek(SEEK_END)` succeeded.
      supports_random_access_ = LazyBoolState::kFalse;
      if (
#ifdef _WIN32
          mode_was_passed_to_open &&
#endif
          supports_read_mode_ == LazyBoolState::kUnknown) {
        supports_read_mode_ = LazyBoolState::kTrue;
      }
      static const NoDestructor<absl::Status> status(
          absl::UnimplementedError("Append mode excludes random access"));
      random_access_status_ = *status;
    } else {
      // `fd_internal::LSeek(SEEK_CUR)` succeeded, and
      // `fd_internal::LSeek(SEEK_END)` will be checked later.
      //  `supports_random_access_` and `supports_read_mode_` are left as
      // `LazyBoolState::kUnknown`.
    }
  }
  BeginRun();
}

void FdWriterBase::Done() {
  BufferedWriter::Done();
#ifdef _WIN32
  if (original_mode_ != absl::nullopt) {
    const int dest = DestFd();
    if (ABSL_PREDICT_FALSE(_setmode(dest, *original_mode_) < 0)) {
      FailOperation("_setmode()");
    }
  }
#endif
  random_access_status_ = absl::OkStatus();
  read_mode_status_ = absl::OkStatus();
  associated_reader_.Reset();
}

inline absl::Status FdWriterBase::FailedOperationStatus(
    absl::string_view operation) {
  const int error_number = errno;
  RIEGELI_ASSERT_NE(error_number, 0)
      << "Failed precondition of FdWriterBase::FailedOperationStatus(): "
         "zero errno";
  return absl::ErrnoToStatus(error_number, absl::StrCat(operation, " failed"));
}

bool FdWriterBase::FailOperation(absl::string_view operation) {
  return Fail(FailedOperationStatus(operation));
}

#ifdef _WIN32

absl::Status FdWriterBase::FailedWindowsOperationStatus(
    absl::string_view operation) {
  const DWORD error_number = GetLastError();
  RIEGELI_ASSERT_NE(error_number, 0)
      << "Failed precondition of FdWriterBase::FailedWindowsOperationStatus(): "
         "zero error code";
  return WindowsErrorToStatus(IntCast<uint32_t>(error_number),
                              absl::StrCat(operation, " failed"));
}

bool FdWriterBase::FailWindowsOperation(absl::string_view operation) {
  return Fail(FailedWindowsOperationStatus(operation));
}

#endif

absl::Status FdWriterBase::AnnotateStatusImpl(absl::Status status) {
  if (!filename_.empty()) {
    status = Annotate(status, absl::StrCat("writing ", filename_));
  }
  return BufferedWriter::AnnotateStatusImpl(std::move(status));
}

inline absl::Status FdWriterBase::SizeStatus() {
  RIEGELI_ASSERT(ok()) << "Failed precondition of FdWriterBase::SizeStatus(): "
                       << status();
#ifndef _WIN32
  if (ABSL_PREDICT_FALSE(absl::StartsWith(filename(), "/sys/"))) {
    // "/sys" files do not support random access. It is hard to reliably
    // recognize them, so `FdWriter` checks the filename.
    //
    // Some "/proc" files also do not support random access, but they are
    // recognized by a failing `fd_internal::LSeek(SEEK_END)`.
    return absl::UnimplementedError("/sys files do not support random access");
  }
#endif
  const int dest = DestFd();
  if (fd_internal::LSeek(dest, 0, SEEK_END) < 0) {
    // Not supported.
    return FailedOperationStatus(fd_internal::kLSeekFunctionName);
  }
  // Supported.
  if (ABSL_PREDICT_FALSE(
          fd_internal::LSeek(dest, IntCast<fd_internal::Offset>(start_pos()),
                             SEEK_SET) < 0)) {
    FailOperation(fd_internal::kLSeekFunctionName);
    return status();
  }
  return absl::OkStatus();
}

bool FdWriterBase::SupportsRandomAccess() {
  if (ABSL_PREDICT_TRUE(supports_random_access_ != LazyBoolState::kUnknown)) {
    return supports_random_access_ == LazyBoolState::kTrue;
  }
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  absl::Status status = SizeStatus();
  if (!status.ok()) {
    // Not supported.
    supports_random_access_ = LazyBoolState::kFalse;
    supports_read_mode_ = LazyBoolState::kFalse;
    random_access_status_ = std::move(status);
    read_mode_status_.Update(random_access_status_);
    return false;
  }
  // Supported.
  supports_random_access_ = LazyBoolState::kTrue;
#ifndef _WIN32
  if (supports_read_mode_ == LazyBoolState::kUnknown) {
    supports_read_mode_ = LazyBoolState::kTrue;
  }
#endif
  return true;
}

bool FdWriterBase::SupportsReadMode() {
  if (ABSL_PREDICT_TRUE(supports_read_mode_ != LazyBoolState::kUnknown)) {
    return supports_read_mode_ == LazyBoolState::kTrue;
  }
#ifndef _WIN32
  RIEGELI_ASSERT(supports_random_access_ == LazyBoolState::kUnknown)
      << "Failed invariant of FdWriterBase: "
         "supports_random_access_ is resolved but supports_read_mode_ is not";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  absl::Status status = SizeStatus();
  if (!status.ok()) {
    // Not supported.
    supports_random_access_ = LazyBoolState::kFalse;
    supports_read_mode_ = LazyBoolState::kFalse;
    random_access_status_ = std::move(status);
    read_mode_status_ = random_access_status_;
    return false;
  }
  // Supported.
  supports_random_access_ = LazyBoolState::kTrue;
  supports_read_mode_ = LazyBoolState::kTrue;
  return true;
#else
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (supports_random_access_ == LazyBoolState::kUnknown) {
    // It is unknown whether even size is supported.
    absl::Status status = SizeStatus();
    if (!status.ok()) {
      // Not supported.
      supports_random_access_ = LazyBoolState::kFalse;
      supports_read_mode_ = LazyBoolState::kFalse;
      random_access_status_ = std::move(status);
      read_mode_status_ = random_access_status_;
      return false;
    }
    // Size is supported.
    supports_random_access_ = LazyBoolState::kTrue;
  }

  const int dest = DestFd();
  if (has_independent_pos_) {
    const HANDLE file_handle = reinterpret_cast<HANDLE>(_get_osfhandle(dest));
    if (file_handle == INVALID_HANDLE_VALUE ||
        file_handle == reinterpret_cast<HANDLE>(-2)) {
      // Not supported.
      supports_read_mode_ = LazyBoolState::kFalse;
      read_mode_status_ = absl::UnimplementedError("Invalid _get_osfhandle()");
      return false;
    }
    char buf[1];
    DWORD length_read;
    OVERLAPPED overlapped{};
    overlapped.Offset = IntCast<DWORD>(start_pos() & 0xffffffff);
    overlapped.OffsetHigh = IntCast<DWORD>(start_pos() >> 32);
    if (!ReadFile(file_handle, &buf, 1, &length_read, &overlapped) &&
        GetLastError() != ERROR_HANDLE_EOF) {
      supports_read_mode_ = LazyBoolState::kFalse;
      read_mode_status_ = FailedWindowsOperationStatus("ReadFile()");
      return false;
    }
    // Supported.
    supports_read_mode_ = LazyBoolState::kTrue;
    return true;
  } else {
    if (fd_internal::LSeek(dest, 0, SEEK_END) < 0) {
      // Not supported.
      supports_read_mode_ = LazyBoolState::kFalse;
      read_mode_status_ =
          FailedOperationStatus(fd_internal::kLSeekFunctionName);
      return false;
    }
    char buf[1];
    if (_read(dest, buf, 1) < 0) {
      // Not supported.
      supports_read_mode_ = LazyBoolState::kFalse;
      read_mode_status_ = FailedOperationStatus("_read()");
    } else {
      // Supported.
      supports_read_mode_ = LazyBoolState::kTrue;
    }
    if (ABSL_PREDICT_FALSE(
            fd_internal::LSeek(dest, IntCast<fd_internal::Offset>(start_pos()),
                               SEEK_SET) < 0)) {
      return FailOperation(fd_internal::kLSeekFunctionName);
    }
    return supports_read_mode_ == LazyBoolState::kTrue;
  }
#endif
}

inline bool FdWriterBase::WriteMode() {
  if (ABSL_PREDICT_TRUE(!read_mode_)) return true;
  read_mode_ = false;
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  const int dest = DestFd();
  return SeekInternal(dest, start_pos());
}

bool FdWriterBase::WriteInternal(absl::string_view src) {
  RIEGELI_ASSERT(!src.empty())
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "nothing to write";
  RIEGELI_ASSERT(ok())
      << "Failed precondition of BufferedWriter::WriteInternal(): " << status();
  if (ABSL_PREDICT_FALSE(!WriteMode())) return false;
  const int dest = DestFd();
  if (ABSL_PREDICT_FALSE(
          src.size() >
          Position{std::numeric_limits<fd_internal::Offset>::max()} -
              start_pos())) {
    return FailOverflow();
  }
  do {
#ifndef _WIN32
  again:
    const size_t length_to_write =
        UnsignedMin(src.size(), size_t{std::numeric_limits<ssize_t>::max()});
    const ssize_t length_written =
        has_independent_pos_ ? pwrite(dest, src.data(), length_to_write,
                                      IntCast<fd_internal::Offset>(start_pos()))
                             : write(dest, src.data(), length_to_write);
    if (ABSL_PREDICT_FALSE(length_written < 0)) {
      if (errno == EINTR) goto again;
      return FailOperation(has_independent_pos_ ? "pwrite()" : "write()");
    }
#else
    DWORD length_written;
    if (has_independent_pos_) {
      const HANDLE file_handle = reinterpret_cast<HANDLE>(_get_osfhandle(dest));
      if (ABSL_PREDICT_FALSE(file_handle == INVALID_HANDLE_VALUE ||
                             file_handle == reinterpret_cast<HANDLE>(-2))) {
        return FailWindowsOperation("_get_osfhandle()");
      }
      const DWORD length_to_write =
          UnsignedMin(src.size(), std::numeric_limits<DWORD>::max());
      OVERLAPPED overlapped{};
      overlapped.Offset = IntCast<DWORD>(start_pos() & 0xffffffff);
      overlapped.OffsetHigh = IntCast<DWORD>(start_pos() >> 32);
      if (ABSL_PREDICT_FALSE(!WriteFile(file_handle, src.data(),
                                        length_to_write, &length_written,
                                        &overlapped))) {
        return FailWindowsOperation("WriteFile()");
      }
    } else {
      const unsigned length_to_write =
          UnsignedMin(src.size(), unsigned{std::numeric_limits<int>::max()});
      const int length_written_int = _write(dest, src.data(), length_to_write);
      if (ABSL_PREDICT_FALSE(length_written_int < 0)) {
        return FailOperation("_write()");
      }
      length_written = IntCast<DWORD>(length_written_int);
    }
#endif
    RIEGELI_ASSERT_GT(length_written, 0)
#ifndef _WIN32
        << (has_independent_pos_ ? "pwrite()" : "write()")
#else
        << (has_independent_pos_ ? "WriteFile()" : "_write()")
#endif
        << " returned 0";
    RIEGELI_ASSERT_LE(IntCast<size_t>(length_written), src.size())
#ifndef _WIN32
        << (has_independent_pos_ ? "pwrite()" : "write()")
#else
        << (has_independent_pos_ ? "WriteFile()" : "_write()")
#endif
        << " wrote more than requested";
    move_start_pos(IntCast<size_t>(length_written));
    src.remove_prefix(IntCast<size_t>(length_written));
  } while (!src.empty());
  return true;
}

bool FdWriterBase::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!BufferedWriter::FlushImpl(flush_type))) return false;
  switch (flush_type) {
    case FlushType::kFromObject:
    case FlushType::kFromProcess:
      return true;
    case FlushType::kFromMachine: {
      const int dest = DestFd();
#ifndef _WIN32
      if (ABSL_PREDICT_FALSE(fsync(dest) < 0)) {
        return FailOperation("fsync()");
      }
#else
      if (ABSL_PREDICT_FALSE(_commit(dest) < 0)) {
        return FailOperation("_commit()");
      }
#endif
      return true;
    }
  }
  RIEGELI_ASSERT_UNREACHABLE()
      << "Unknown flush type: " << static_cast<int>(flush_type);
}

bool FdWriterBase::FlushBehindBuffer(absl::string_view src,
                                     FlushType flush_type) {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::FlushBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!WriteMode())) return false;
  return BufferedWriter::FlushBehindBuffer(src, flush_type);
}

inline bool FdWriterBase::SeekInternal(int dest, Position new_pos) {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of FdWriterBase::SeekInternal(): "
         "buffer not empty";
  RIEGELI_ASSERT(ok())
      << "Failed precondition of FdWriterBase::SeekInternal(): " << status();
  if (!has_independent_pos_) {
    if (ABSL_PREDICT_FALSE(
            fd_internal::LSeek(dest, IntCast<fd_internal::Offset>(new_pos),
                               SEEK_SET) < 0)) {
      return FailOperation(fd_internal::kLSeekFunctionName);
    }
  }
  set_start_pos(new_pos);
  return true;
}

bool FdWriterBase::SeekBehindBuffer(Position new_pos) {
  RIEGELI_ASSERT_NE(new_pos, pos())
      << "Failed precondition of BufferedWriter::SeekBehindBuffer(): "
         "position unchanged, use Seek() instead";
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::SeekBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!FdWriterBase::SupportsRandomAccess())) {
    if (ok()) Fail(random_access_status_);
    return false;
  }
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  read_mode_ = false;
  const int dest = DestFd();
  if (new_pos > start_pos()) {
    // Seeking forwards.
    fd_internal::StatInfo stat_info;
    if (ABSL_PREDICT_FALSE(fd_internal::FStat(dest, &stat_info) < 0)) {
      return FailOperation(fd_internal::kFStatFunctionName);
    }
    if (ABSL_PREDICT_FALSE(new_pos > IntCast<Position>(stat_info.st_size))) {
      // File ends.
      SeekInternal(dest, IntCast<Position>(stat_info.st_size));
      return false;
    }
  }
  return SeekInternal(dest, new_pos);
}

absl::optional<Position> FdWriterBase::SizeBehindBuffer() {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::SizeBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!FdWriterBase::SupportsRandomAccess())) {
    if (ok()) Fail(random_access_status_);
    return absl::nullopt;
  }
  if (ABSL_PREDICT_FALSE(!ok())) return absl::nullopt;
  const int dest = DestFd();
  fd_internal::StatInfo stat_info;
  if (ABSL_PREDICT_FALSE(fd_internal::FStat(dest, &stat_info) < 0)) {
    FailOperation(fd_internal::kFStatFunctionName);
    return absl::nullopt;
  }
  return IntCast<Position>(stat_info.st_size);
}

bool FdWriterBase::TruncateBehindBuffer(Position new_size) {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::TruncateBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  read_mode_ = false;
  const int dest = DestFd();
  if (new_size >= start_pos()) {
    // Seeking forwards.
    fd_internal::StatInfo stat_info;
    if (ABSL_PREDICT_FALSE(fd_internal::FStat(dest, &stat_info) < 0)) {
      return FailOperation(fd_internal::kFStatFunctionName);
    }
    if (ABSL_PREDICT_FALSE(new_size > IntCast<Position>(stat_info.st_size))) {
      // File ends.
      SeekInternal(dest, IntCast<Position>(stat_info.st_size));
      return false;
    }
  }
#ifndef _WIN32
again:
  if (ABSL_PREDICT_FALSE(
          ftruncate(dest, IntCast<fd_internal::Offset>(new_size)) < 0)) {
    if (errno == EINTR) goto again;
    return FailOperation("ftruncate()");
  }
#else
  if (ABSL_PREDICT_FALSE(
          _chsize_s(dest, IntCast<fd_internal::Offset>(new_size)) != 0)) {
    return FailOperation("_chsize_s()");
  }
#endif
  return SeekInternal(dest, new_size);
}

Reader* FdWriterBase::ReadModeBehindBuffer(Position initial_pos) {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::ReadModeBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!FdWriterBase::SupportsReadMode())) {
    if (ok()) Fail(read_mode_status_);
    return nullptr;
  }
  if (ABSL_PREDICT_FALSE(!ok())) return nullptr;
  const int dest = DestFd();
  FdReader<UnownedFd>* const reader = associated_reader_.ResetReader(
      dest, FdReaderBase::Options()
                .set_assumed_filename(filename())
                .set_independent_pos(has_independent_pos_
                                         ? absl::make_optional(initial_pos)
                                         : absl::nullopt)
                .set_buffer_options(buffer_options()));
  if (!has_independent_pos_) reader->Seek(initial_pos);
  read_mode_ = true;
  return reader;
}

}  // namespace riegeli
