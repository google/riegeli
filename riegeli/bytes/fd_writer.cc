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

#include <fcntl.h>
#ifdef _WIN32
#include <io.h>
#endif
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
#include <optional>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/numeric/bits.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#ifdef _WIN32
#include "riegeli/base/errno_mapping.h"
#endif
#include "riegeli/base/byte_fill.h"
#include "riegeli/base/global.h"
#include "riegeli/base/status.h"
#include "riegeli/base/type_id.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/fd_handle.h"
#include "riegeli/bytes/fd_internal_for_cc.h"
#include "riegeli/bytes/fd_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

TypeId FdWriterBase::GetTypeId() const { return TypeId::For<FdWriterBase>(); }

void FdWriterBase::Initialize(int dest, Options&& options) {
  RIEGELI_ASSERT_GE(dest, 0)
      << "Failed precondition of FdWriter: negative file descriptor";
  InitializePos(dest, std::move(options), /*mode_was_passed_to_open=*/false);
}

void FdWriterBase::InitializePos(int dest, Options&& options,
                                 bool mode_was_passed_to_open) {
  RIEGELI_ASSERT(!has_independent_pos_)
      << "Failed precondition of FdWriterBase::InitializePos(): "
         "has_independent_pos_ not reset";
  RIEGELI_ASSERT_EQ(supports_random_access_, LazyBoolState::kUnknown)
      << "Failed precondition of FdWriterBase::InitializePos(): "
         "supports_random_access_ not reset";
  RIEGELI_ASSERT_EQ(supports_read_mode_, LazyBoolState::kUnknown)
      << "Failed precondition of FdWriterBase::InitializePos(): "
         "supports_read_mode_ not reset";
  RIEGELI_ASSERT_OK(random_access_status_)
      << "Failed precondition of FdWriterBase::InitializePos(): "
         "random_access_status_ not reset";
  RIEGELI_ASSERT_OK(read_mode_status_)
      << "Failed precondition of FdWriterBase::InitializePos(): "
         "read_mode_status_ not reset";
#ifndef _WIN32
  if (!mode_was_passed_to_open) {
    const int mode = fcntl(dest, F_GETFL);
    if (ABSL_PREDICT_FALSE(mode < 0)) {
      FailOperation("fcntl()");
      return;
    }
    options.set_mode(mode);
  }
  if ((options.mode() & O_ACCMODE) != O_RDWR) {
    supports_read_mode_ = LazyBoolState::kFalse;
    read_mode_status_ = Global([] {
      return absl::UnimplementedError("Mode does not include O_RDWR");
    });
  }
#else   // _WIN32
  RIEGELI_ASSERT_EQ(original_mode_, std::nullopt)
      << "Failed precondition of FdWriterBase::InitializePos(): "
         "original_mode_ not reset";
  int text_mode = options.mode() &
                  (_O_BINARY | _O_TEXT | _O_WTEXT | _O_U16TEXT | _O_U8TEXT);
  if (mode_was_passed_to_open) {
    if ((options.mode() & (_O_RDONLY | _O_WRONLY | _O_RDWR)) != _O_RDWR) {
      supports_read_mode_ = LazyBoolState::kFalse;
      read_mode_status_ = Global([] {
        return absl::UnimplementedError("Mode does not include _O_RDWR");
      });
    }
  } else if (text_mode != 0) {
    const int original_mode = _setmode(dest, text_mode);
    if (ABSL_PREDICT_FALSE(original_mode < 0)) {
      FailOperation("_setmode()");
      return;
    }
    original_mode_ = original_mode;
  }
  if (options.assumed_pos() == std::nullopt) {
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
      if (ABSL_PREDICT_FALSE(options.independent_pos() != std::nullopt)) {
        Fail(absl::InvalidArgumentError(
            "FdWriterBase::Options::independent_pos() requires binary mode"));
        return;
      }
      options.set_assumed_pos(0);
    }
  }
#endif  // _WIN32
  if (options.assumed_pos() != std::nullopt) {
    if (ABSL_PREDICT_FALSE(options.independent_pos() != std::nullopt)) {
      Fail(absl::InvalidArgumentError(
          "FdWriterBase::Options::assumed_pos() and independent_pos() "
          "must not be both set"));
      return;
    }
    if (ABSL_PREDICT_FALSE(
            *options.assumed_pos() >
            Position{std::numeric_limits<fd_internal::Offset>::max()})) {
      FailOverflow();
      return;
    }
    set_start_pos(*options.assumed_pos());
    supports_random_access_ = LazyBoolState::kFalse;
    supports_read_mode_ = LazyBoolState::kFalse;
    random_access_status_ = Global([] {
      return absl::UnimplementedError(
          "FileWriterBase::Options::assumed_pos() excludes random access");
    });
    read_mode_status_.Update(random_access_status_);
  } else if (options.independent_pos() != std::nullopt) {
    if (ABSL_PREDICT_FALSE((options.mode() & O_APPEND) != 0)) {
      Fail(
          absl::InvalidArgumentError("FdWriterBase::Options::independent_pos() "
                                     "is incompatible with append mode"));
      return;
    }
    has_independent_pos_ = true;
    if (ABSL_PREDICT_FALSE(
            *options.independent_pos() >
            Position{std::numeric_limits<fd_internal::Offset>::max()})) {
      FailOverflow();
      return;
    }
    set_start_pos(*options.independent_pos());
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
        dest, 0, (options.mode() & O_APPEND) != 0 ? SEEK_END : SEEK_CUR);
    if (file_pos < 0) {
      // Random access is not supported. Assume 0 as the initial position.
      supports_random_access_ = LazyBoolState::kFalse;
      supports_read_mode_ = LazyBoolState::kFalse;
      random_access_status_ =
          FailedOperationStatus(fd_internal::kLSeekFunctionName);
      read_mode_status_.Update(random_access_status_);
      return;
    }
    set_start_pos(IntCast<Position>(file_pos));
    if ((options.mode() & O_APPEND) != 0) {
      // `fd_internal::LSeek(SEEK_END)` succeeded.
      supports_random_access_ = LazyBoolState::kFalse;
      if (
#ifdef _WIN32
          mode_was_passed_to_open &&
#endif
          supports_read_mode_ == LazyBoolState::kUnknown) {
        supports_read_mode_ = LazyBoolState::kTrue;
      }
      random_access_status_ = Global([] {
        return absl::UnimplementedError("Append mode excludes random access");
      });
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
  if (original_mode_ != std::nullopt) {
    const int dest = DestFd();
    if (ABSL_PREDICT_FALSE(_setmode(dest, *original_mode_) < 0)) {
      FailOperation("_setmode()");
    }
  }
#endif  // _WIN32
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

#endif  // _WIN32

absl::Status FdWriterBase::AnnotateStatusImpl(absl::Status status) {
  return BufferedWriter::AnnotateStatusImpl(
      Annotate(status, absl::StrCat("writing ", filename())));
}

inline absl::Status FdWriterBase::SizeStatus() {
  RIEGELI_ASSERT_OK(*this)
      << "Failed precondition of FdWriterBase::SizeStatus()";
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
  RIEGELI_ASSERT_EQ(supports_random_access_, LazyBoolState::kUnknown)
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
#else   // _WIN32
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
#endif  // _WIN32
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
  RIEGELI_ASSERT_OK(*this)
      << "Failed precondition of BufferedWriter::WriteInternal()";
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
    const size_t length_to_write = UnsignedMin(
        src.size(),
        absl::bit_floor(size_t{std::numeric_limits<ssize_t>::max()}),
        // Darwin and FreeBSD cannot write more than 2 GB - 1 at a time.
        // Limit to 1 GB for better alignment of writes.
        // https://codereview.appspot.com/89900044#msg9
        size_t{1} << 30);
    const ssize_t length_written =
        has_independent_pos_ ? pwrite(dest, src.data(), length_to_write,
                                      IntCast<fd_internal::Offset>(start_pos()))
                             : write(dest, src.data(), length_to_write);
    if (ABSL_PREDICT_FALSE(length_written < 0)) {
      if (errno == EINTR) goto again;
      return FailOperation(has_independent_pos_ ? "pwrite()" : "write()");
    }
#else   // _WIN32
    DWORD length_to_write;
    DWORD length_written;
    if (has_independent_pos_) {
      const HANDLE file_handle = reinterpret_cast<HANDLE>(_get_osfhandle(dest));
      if (ABSL_PREDICT_FALSE(file_handle == INVALID_HANDLE_VALUE ||
                             file_handle == reinterpret_cast<HANDLE>(-2))) {
        return FailWindowsOperation("_get_osfhandle()");
      }
      length_to_write = UnsignedMin(
          src.size(), absl::bit_floor(std::numeric_limits<DWORD>::max()));
      OVERLAPPED overlapped{};
      overlapped.Offset = IntCast<DWORD>(start_pos() & 0xffffffff);
      overlapped.OffsetHigh = IntCast<DWORD>(start_pos() >> 32);
      if (ABSL_PREDICT_FALSE(!WriteFile(file_handle, src.data(),
                                        length_to_write, &length_written,
                                        &overlapped))) {
        return FailWindowsOperation("WriteFile()");
      }
    } else {
      length_to_write = UnsignedMin(
          src.size(),
          absl::bit_floor(unsigned{std::numeric_limits<int>::max()}));
      const int length_written_int =
          _write(dest, src.data(), IntCast<unsigned>(length_to_write));
      if (ABSL_PREDICT_FALSE(length_written_int < 0)) {
        return FailOperation("_write()");
      }
      length_written = IntCast<DWORD>(length_written_int);
    }
#endif  // _WIN32
    RIEGELI_ASSERT_GT(length_written, 0)
#ifndef _WIN32
        << (has_independent_pos_ ? "pwrite()" : "write()")
#else
        << (has_independent_pos_ ? "WriteFile()" : "_write()")
#endif
        << " returned 0";
    RIEGELI_ASSERT_LE(UnsignedCast(length_written), length_to_write)
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

bool FdWriterBase::WriteSlow(ByteFill src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(ByteFill): "
         "enough space available, use Write(ByteFill) instead";
  if (src.fill() != '\0' || !FdWriterBase::SupportsRandomAccess()) {
    return BufferedWriter::WriteSlow(src);
  }
  const std::optional<Position> size = SizeImpl();
  if (ABSL_PREDICT_FALSE(size == std::nullopt)) return false;
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "BufferedWriter::SizeImpl() flushes the buffer";
  if (ABSL_PREDICT_FALSE(
          src.size() >
          Position{std::numeric_limits<fd_internal::Offset>::max()} -
              start_pos())) {
    return FailOverflow();
  }
  const Position new_pos = start_pos() + src.size();
  if (new_pos < *size) {
    // Existing data after zeros must be preserved. Optimization below is not
    // feasible.
    return BufferedWriter::WriteSlow(src);
  }

  // Optimize extending with zeros by calling `ftruncate()` (`_chsize_s()` on
  // Windows).
  const int dest = DestFd();
  if (start_pos() < *size) {
    // Remove the part to be overwritten with zeros.
    if (ABSL_PREDICT_FALSE(!TruncateInternal(dest, start_pos()))) return false;
  }
  // Extend with zeros.
  if (ABSL_PREDICT_FALSE(!TruncateInternal(dest, new_pos))) return false;
  return SeekInternal(dest, new_pos);
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
#else   // _WIN32
      if (ABSL_PREDICT_FALSE(_commit(dest) < 0)) {
        return FailOperation("_commit()");
      }
#endif  // _WIN32
      return true;
    }
  }
  RIEGELI_ASSUME_UNREACHABLE()
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
  RIEGELI_ASSERT_OK(*this)
      << "Failed precondition of FdWriterBase::SeekInternal()";
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

std::optional<Position> FdWriterBase::SizeBehindBuffer() {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::SizeBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!FdWriterBase::SupportsRandomAccess())) {
    if (ok()) Fail(random_access_status_);
    return std::nullopt;
  }
  if (ABSL_PREDICT_FALSE(!ok())) return std::nullopt;
  const int dest = DestFd();
  fd_internal::StatInfo stat_info;
  if (ABSL_PREDICT_FALSE(fd_internal::FStat(dest, &stat_info) < 0)) {
    FailOperation(fd_internal::kFStatFunctionName);
    return std::nullopt;
  }
  return IntCast<Position>(stat_info.st_size);
}

inline bool FdWriterBase::TruncateInternal(int dest, Position new_size) {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of FdWriterBase::TruncateInternal(): "
         "buffer not empty";
  RIEGELI_ASSERT_OK(*this)
      << "Failed precondition of FdWriterBase::TruncateInternal()";
#ifndef _WIN32
again:
  if (ABSL_PREDICT_FALSE(
          ftruncate(dest, IntCast<fd_internal::Offset>(new_size)) < 0)) {
    if (errno == EINTR) goto again;
    return FailOperation("ftruncate()");
  }
#else   // _WIN32
  if (ABSL_PREDICT_FALSE(
          _chsize_s(dest, IntCast<fd_internal::Offset>(new_size)) != 0)) {
    return FailOperation("_chsize_s()");
  }
#endif  // _WIN32
  return true;
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
  if (ABSL_PREDICT_FALSE(!TruncateInternal(dest, new_size))) return false;
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
  FdReader<UnownedFd>* const reader = associated_reader_.ResetReader(
      UnownedFd(DestFdHandle()),
      FdReaderBase::Options()
          .set_independent_pos(has_independent_pos_
                                   ? std::make_optional(initial_pos)
                                   : std::nullopt)
          .set_buffer_options(buffer_options()));
  if (!has_independent_pos_) reader->Seek(initial_pos);
  read_mode_ = true;
  return reader;
}

}  // namespace riegeli
