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

// Make `pread()` available.
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

#include "riegeli/bytes/fd_reader.h"

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
#include <memory>
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
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/fd_internal.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

void FdReaderBase::Initialize(int src,
#ifdef _WIN32
                              int mode,
#endif
                              absl::optional<std::string>&& assumed_filename,
                              absl::optional<Position> assumed_pos,
                              absl::optional<Position> independent_pos) {
  RIEGELI_ASSERT_GE(src, 0)
      << "Failed precondition of FdReader: negative file descriptor";
  filename_ = fd_internal::ResolveFilename(src, std::move(assumed_filename));
  InitializePos(src,
#ifdef _WIN32
                mode, /*mode_was_passed_to_open=*/false,
#endif
                assumed_pos, independent_pos);
}

int FdReaderBase::OpenFd(absl::string_view filename, int mode) {
#ifndef _WIN32
  RIEGELI_ASSERT((mode & O_ACCMODE) == O_RDONLY || (mode & O_ACCMODE) == O_RDWR)
      << "Failed precondition of FdReader: "
         "mode must include either O_RDONLY or O_RDWR";
#else
  RIEGELI_ASSERT((mode & (_O_RDONLY | _O_WRONLY | _O_RDWR)) == _O_RDONLY ||
                 (mode & (_O_RDONLY | _O_WRONLY | _O_RDWR)) == _O_RDWR)
      << "Failed precondition of FdReader: "
         "mode must include either _O_RDONLY or _O_RDWR";
#endif
  // TODO: When `absl::string_view` becomes C++17 `std::string_view`:
  // `filename_ = filename`
  filename_.assign(filename.data(), filename.size());
#ifndef _WIN32
again:
  const int src = open(filename_.c_str(), mode, 0666);
  if (ABSL_PREDICT_FALSE(src < 0)) {
    if (errno == EINTR) goto again;
    BufferedReader::Reset(kClosed);
    FailOperation("open()");
    return -1;
  }
#else
  std::wstring filename_wide;
  if (ABSL_PREDICT_FALSE(!Utf8ToWide(filename_, filename_wide))) {
    BufferedReader::Reset(kClosed);
    Fail(absl::InvalidArgumentError("Filename not valid UTF-8"));
    return -1;
  }
  int src;
  if (ABSL_PREDICT_FALSE(_wsopen_s(&src, filename_wide.c_str(), mode,
                                   _SH_DENYNO, _S_IREAD) != 0)) {
    BufferedReader::Reset(kClosed);
    FailOperation("_wsopen_s()");
    return -1;
  }
#endif
  return src;
}

void FdReaderBase::InitializePos(int src,
#ifdef _WIN32
                                 int mode, bool mode_was_passed_to_open,
#endif
                                 absl::optional<Position> assumed_pos,
                                 absl::optional<Position> independent_pos) {
  RIEGELI_ASSERT(!has_independent_pos_)
      << "Failed precondition of FdReaderBase::InitializePos(): "
         "has_independent_pos_ not reset";
  RIEGELI_ASSERT(!supports_random_access_)
      << "Failed precondition of FdReaderBase::InitializePos(): "
         "supports_random_access_ not reset";
  RIEGELI_ASSERT_EQ(random_access_status_, absl::OkStatus())
      << "Failed precondition of FdReaderBase::InitializePos(): "
         "random_access_status_ not reset";
#ifdef _WIN32
  RIEGELI_ASSERT(original_mode_ == absl::nullopt)
      << "Failed precondition of FdWriterBase::InitializePos(): "
         "original_mode_ not reset";
  int text_mode =
      mode & (_O_BINARY | _O_TEXT | _O_WTEXT | _O_U16TEXT | _O_U8TEXT);
  if (!mode_was_passed_to_open && text_mode != 0) {
    const int original_mode = _setmode(src, text_mode);
    if (ABSL_PREDICT_FALSE(original_mode < 0)) {
      FailOperation("_setmode()");
      return;
    }
    original_mode_ = original_mode;
  }
  if (assumed_pos == absl::nullopt) {
    if (text_mode == 0) {
      // There is no `_getmode()`, but `_setmode()` returns the previous mode.
      text_mode = _setmode(src, _O_BINARY);
      if (ABSL_PREDICT_FALSE(text_mode < 0)) {
        FailOperation("_setmode()");
        return;
      }
      if (ABSL_PREDICT_FALSE(_setmode(src, text_mode) < 0)) {
        FailOperation("_setmode()");
        return;
      }
    }
    if (text_mode != _O_BINARY) {
      if (ABSL_PREDICT_FALSE(independent_pos != absl::nullopt)) {
        Fail(absl::InvalidArgumentError(
            "FdReaderBase::Options::independent_pos() requires binary mode"));
        return;
      }
      assumed_pos = 0;
    }
  }
#endif
  if (assumed_pos != absl::nullopt) {
    if (ABSL_PREDICT_FALSE(independent_pos != absl::nullopt)) {
      Fail(absl::InvalidArgumentError(
          "FdReaderBase::Options::assumed_pos() and independent_pos() "
          "must not be both set"));
      return;
    }
    if (ABSL_PREDICT_FALSE(
            *assumed_pos >
            Position{std::numeric_limits<fd_internal::Offset>::max()})) {
      FailOverflow();
      return;
    }
    set_limit_pos(*assumed_pos);
    // `supports_random_access_` is left as `false`.
    static const NoDestructor<absl::Status> status(absl::UnimplementedError(
        "FdReaderBase::Options::assumed_pos() excludes random access"));
    random_access_status_ = *status;
  } else if (independent_pos != absl::nullopt) {
    has_independent_pos_ = true;
    if (ABSL_PREDICT_FALSE(
            *independent_pos >
            Position{std::numeric_limits<fd_internal::Offset>::max()})) {
      FailOverflow();
      return;
    }
    set_limit_pos(*independent_pos);
    supports_random_access_ = true;
  } else {
    const fd_internal::Offset file_pos = fd_internal::LSeek(src, 0, SEEK_CUR);
    if (file_pos < 0) {
      // Random access is not supported. Assume 0 as the initial position.
      // `supports_random_access_` is left as `false`.
      random_access_status_ =
          FailedOperationStatus(fd_internal::kLSeekFunctionName);
      return;
    }
    set_limit_pos(IntCast<Position>(file_pos));

    // Check if random access is supported.
#ifndef _WIN32
    if (ABSL_PREDICT_FALSE(absl::StartsWith(filename(), "/sys/"))) {
      // "/sys" files do not support random access. It is hard to reliably
      // recognize them, so `FdReader` checks the filename.
      //
      // Some "/proc" files also do not support random access, but they are
      // recognized by a failing `fd_internal::LSeek(SEEK_END)`.
      //
      // `supports_random_access_` is left as `false`.
      random_access_status_ =
          absl::UnimplementedError("/sys files do not support random access");
    } else
#endif
    {
      const fd_internal::Offset file_size =
          fd_internal::LSeek(src, 0, SEEK_END);
      if (file_size < 0) {
        // Not supported. `supports_random_access_` is left as `false`.
        random_access_status_ =
            FailedOperationStatus(fd_internal::kLSeekFunctionName);
      } else {
        // Supported.
        supports_random_access_ = true;
        if (ABSL_PREDICT_FALSE(
                fd_internal::LSeek(src,
                                   IntCast<fd_internal::Offset>(limit_pos()),
                                   SEEK_SET) < 0)) {
          FailOperation(fd_internal::kLSeekFunctionName);
          return;
        }
        if (!growing_source_) set_exact_size(IntCast<Position>(file_size));
      }
    }
  }
  BeginRun();
}

void FdReaderBase::Done() {
  BufferedReader::Done();
#ifdef _WIN32
  if (original_mode_ != absl::nullopt) {
    const int src = SrcFd();
    if (ABSL_PREDICT_FALSE(_setmode(src, *original_mode_) < 0)) {
      FailOperation("_setmode()");
    }
  }
#endif
  random_access_status_ = absl::OkStatus();
}

inline absl::Status FdReaderBase::FailedOperationStatus(
    absl::string_view operation) {
  const int error_number = errno;
  RIEGELI_ASSERT_NE(error_number, 0)
      << "Failed precondition of FdReaderBase::FailedOperationStatus(): "
         "zero errno";
  return absl::ErrnoToStatus(error_number, absl::StrCat(operation, " failed"));
}

bool FdReaderBase::FailOperation(absl::string_view operation) {
  return Fail(FailedOperationStatus(operation));
}

#ifdef _WIN32

bool FdReaderBase::FailWindowsOperation(absl::string_view operation) {
  const DWORD error_number = GetLastError();
  RIEGELI_ASSERT_NE(error_number, 0)
      << "Failed precondition of FdReaderBase::FailWindowsOperation(): "
         "zero error code";
  return Fail(WindowsErrorToStatus(IntCast<uint32_t>(error_number),
                                   absl::StrCat(operation, " failed")));
}

#endif

absl::Status FdReaderBase::AnnotateStatusImpl(absl::Status status) {
  if (!filename_.empty()) {
    status = Annotate(status, absl::StrCat("reading ", filename_));
  }
  return BufferedReader::AnnotateStatusImpl(std::move(status));
}

bool FdReaderBase::ReadInternal(size_t min_length, size_t max_length,
                                char* dest) {
  RIEGELI_ASSERT_GT(min_length, 0u)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "nothing to read";
  RIEGELI_ASSERT_GE(max_length, min_length)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "max_length < min_length";
  RIEGELI_ASSERT(ok())
      << "Failed precondition of BufferedReader::ReadInternal(): " << status();
  const int src = SrcFd();
  for (;;) {
    if (ABSL_PREDICT_FALSE(
            limit_pos() >=
            Position{std::numeric_limits<fd_internal::Offset>::max()})) {
      return FailOverflow();
    }
#ifndef _WIN32
    const size_t length_to_read = UnsignedMin(
        max_length,
        Position{std::numeric_limits<fd_internal::Offset>::max()} - limit_pos(),
        size_t{std::numeric_limits<ssize_t>::max()});
  again:
    const ssize_t length_read =
        has_independent_pos_ ? pread(src, dest, length_to_read,
                                     IntCast<fd_internal::Offset>(limit_pos()))
                             : read(src, dest, length_to_read);
    if (ABSL_PREDICT_FALSE(length_read < 0)) {
      if (errno == EINTR) goto again;
      return FailOperation(has_independent_pos_ ? "pread()" : "read()");
    }
#else
    DWORD length_read;
    if (has_independent_pos_) {
      const HANDLE file_handle = reinterpret_cast<HANDLE>(_get_osfhandle(src));
      if (ABSL_PREDICT_FALSE(file_handle == INVALID_HANDLE_VALUE ||
                             file_handle == reinterpret_cast<HANDLE>(-2))) {
        return FailWindowsOperation("_get_osfhandle()");
      }
      const DWORD length_to_read = IntCast<DWORD>(UnsignedMin(
          max_length,
          Position{std::numeric_limits<fd_internal::Offset>::max()} -
              limit_pos(),
          std::numeric_limits<DWORD>::max()));
      OVERLAPPED overlapped{};
      overlapped.Offset = IntCast<DWORD>(limit_pos() & 0xffffffff);
      overlapped.OffsetHigh = IntCast<DWORD>(limit_pos() >> 32);
      if (ABSL_PREDICT_FALSE(!ReadFile(file_handle, dest, length_to_read,
                                       &length_read, &overlapped)) &&
          ABSL_PREDICT_FALSE(GetLastError() != ERROR_HANDLE_EOF)) {
        return FailWindowsOperation("ReadFile()");
      }
    } else {
      const unsigned length_to_read = UnsignedMin(
          max_length,
          Position{std::numeric_limits<fd_internal::Offset>::max()} -
              limit_pos(),
          unsigned{std::numeric_limits<int>::max()});
      const int length_read_int = _read(src, dest, length_to_read);
      if (ABSL_PREDICT_FALSE(length_read_int < 0)) {
        return FailOperation("_read()");
      }
      length_read = IntCast<DWORD>(length_read_int);
    }
#endif
    if (ABSL_PREDICT_FALSE(length_read == 0)) {
      if (!growing_source_) set_exact_size(limit_pos());
      return false;
    }
    RIEGELI_ASSERT_LE(IntCast<size_t>(length_read), max_length)
#ifndef _WIN32
        << (has_independent_pos_ ? "pread()" : "read()")
#else
        << (has_independent_pos_ ? "ReadFile()" : "_read()")
#endif
        << " read more than requested";
    move_limit_pos(IntCast<size_t>(length_read));
    if (IntCast<size_t>(length_read) >= min_length) return true;
    dest += length_read;
    min_length -= IntCast<size_t>(length_read);
    max_length -= IntCast<size_t>(length_read);
  }
}

inline bool FdReaderBase::SeekInternal(int src, Position new_pos) {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of FdReaderBase::SeekInternal(): "
         "buffer not empty";
  RIEGELI_ASSERT(FdReaderBase::SupportsRandomAccess())
      << "Failed precondition of FdReaderBase::SeekInternal(): "
         "random access not supported";
  if (!has_independent_pos_) {
    if (ABSL_PREDICT_FALSE(
            fd_internal::LSeek(src, IntCast<fd_internal::Offset>(new_pos),
                               SEEK_SET) < 0)) {
      return FailOperation(fd_internal::kLSeekFunctionName);
    }
  }
  set_limit_pos(new_pos);
  return true;
}

bool FdReaderBase::SeekBehindBuffer(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of BufferedReader::SeekBehindBuffer(): "
         "position in the buffer, use Seek() instead";
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedReader::SeekBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!FdReaderBase::SupportsRandomAccess())) {
    if (ABSL_PREDICT_FALSE(new_pos < start_pos())) {
      if (ok()) Fail(random_access_status_);
      return false;
    }
    return BufferedReader::SeekBehindBuffer(new_pos);
  }
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  const int src = SrcFd();
  if (new_pos > limit_pos()) {
    // Seeking forwards.
    Position file_size;
    if (exact_size() != absl::nullopt) {
      file_size = *exact_size();
    } else {
      fd_internal::StatInfo stat_info;
      if (ABSL_PREDICT_FALSE(fd_internal::FStat(src, &stat_info) < 0)) {
        return FailOperation(fd_internal::kFStatFunctionName);
      }
      file_size = IntCast<Position>(stat_info.st_size);
      if (!growing_source_) set_exact_size(file_size);
    }
    if (ABSL_PREDICT_FALSE(new_pos > file_size)) {
      // File ends.
      SeekInternal(src, file_size);
      return false;
    }
  }
  return SeekInternal(src, new_pos);
}

absl::optional<Position> FdReaderBase::SizeImpl() {
  if (ABSL_PREDICT_FALSE(!FdReaderBase::SupportsRandomAccess())) {
    if (ok()) Fail(random_access_status_);
    return absl::nullopt;
  }
  if (ABSL_PREDICT_FALSE(!ok())) return absl::nullopt;
  if (exact_size() != absl::nullopt) return *exact_size();
  const int src = SrcFd();
  fd_internal::StatInfo stat_info;
  if (ABSL_PREDICT_FALSE(fd_internal::FStat(src, &stat_info) < 0)) {
    FailOperation(fd_internal::kFStatFunctionName);
    return absl::nullopt;
  }
  if (!growing_source_) set_exact_size(IntCast<Position>(stat_info.st_size));
  return IntCast<Position>(stat_info.st_size);
}

std::unique_ptr<Reader> FdReaderBase::NewReaderImpl(Position initial_pos) {
  if (ABSL_PREDICT_FALSE(!FdReaderBase::SupportsNewReader())) {
    if (ok()) {
      Fail(
#ifdef _WIN32
          !has_independent_pos_
              ? absl::UnimplementedError(
                    "FdReaderBase::Options::independent_pos() "
                    "required for read mode")
              :
#endif
              random_access_status_);
    }
    return nullptr;
  }
  if (ABSL_PREDICT_FALSE(!ok())) return nullptr;
  // `NewReaderImpl()` is thread-safe from this point.
  const int src = SrcFd();
  std::unique_ptr<FdReader<UnownedFd>> reader =
      std::make_unique<FdReader<UnownedFd>>(
          src, FdReaderBase::Options()
                   .set_assumed_filename(filename())
                   .set_independent_pos(initial_pos)
                   .set_growing_source(growing_source_)
                   .set_buffer_options(buffer_options()));
  reader->set_exact_size(exact_size());
  ShareBufferTo(*reader);
  return reader;
}

}  // namespace riegeli
