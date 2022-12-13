// Copyright 2022 Google LLC
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

// Make `fseeko()` and `ftello()` available.
#if !defined(_XOPEN_SOURCE) || _XOPEN_SOURCE < 500
#undef _XOPEN_SOURCE
#define _XOPEN_SOURCE 500
#endif

// Make `off_t` 64-bit even on 32-bit systems.
#undef _FILE_OFFSET_BITS
#define _FILE_OFFSET_BITS 64

#endif

#include "riegeli/bytes/cfile_writer.h"

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#endif
#include <stddef.h>
#include <stdio.h>

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
#include "riegeli/base/no_destructor.h"
#include "riegeli/base/object.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"
#ifdef _WIN32
#include "riegeli/base/unicode.h"
#endif
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/cfile_internal.h"
#include "riegeli/bytes/cfile_reader.h"
#include "riegeli/bytes/file_mode_string.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void CFileWriterBase::Initialize(FILE* dest, std::string&& assumed_filename,
                                 absl::string_view mode,
                                 absl::optional<Position> assumed_pos) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of CFileReader: null FILE pointer";
  filename_ = std::move(assumed_filename);
  InitializePos(dest, mode, /*mode_was_passed_to_fopen=*/false, assumed_pos);
}

FILE* CFileWriterBase::OpenFile(absl::string_view filename,
                                const std::string& mode) {
  // TODO: When `absl::string_view` becomes C++17 `std::string_view`:
  // `filename_ = filename`
  filename_.assign(filename.data(), filename.size());
#ifndef _WIN32
  FILE* const dest = fopen(filename_.c_str(), mode.c_str());
  if (ABSL_PREDICT_FALSE(dest == nullptr)) {
    BufferedWriter::Reset(kClosed);
    FailOperation("fopen()");
    return nullptr;
  }
#else
  std::wstring filename_wide;
  if (ABSL_PREDICT_FALSE(!Utf8ToWide(filename_, filename_wide))) {
    BufferedWriter::Reset(kClosed);
    Fail(absl::InvalidArgumentError("Filename not valid UTF-8"));
    return nullptr;
  }
  std::wstring mode_wide;
  if (ABSL_PREDICT_FALSE(!Utf8ToWide(mode, mode_wide))) {
    BufferedWriter::Reset(kClosed);
    Fail(absl::InvalidArgumentError(
        absl::StrCat("Mode not valid UTF-8: ", mode)));
    return nullptr;
  }
  FILE* const dest = _wfopen(filename_wide.c_str(), mode_wide.c_str());
  if (ABSL_PREDICT_FALSE(dest == nullptr)) {
    BufferedWriter::Reset(kClosed);
    FailOperation("_wfopen()");
    return nullptr;
  }
#endif
  return dest;
}

void CFileWriterBase::InitializePos(FILE* dest, absl::string_view mode,
                                    bool mode_was_passed_to_fopen,
                                    absl::optional<Position> assumed_pos) {
  RIEGELI_ASSERT(supports_random_access_ == LazyBoolState::kUnknown)
      << "Failed precondition of CFileWriterBase::InitializePos(): "
         "supports_random_access_ not reset";
  RIEGELI_ASSERT(supports_read_mode_ == LazyBoolState::kUnknown)
      << "Failed precondition of CFileWriterBase::InitializePos(): "
         "supports_read_mode_ not reset";
  RIEGELI_ASSERT_EQ(random_access_status_, absl::OkStatus())
      << "Failed precondition of CFileWriterBase::InitializePos(): "
         "random_access_status_ not reset";
  RIEGELI_ASSERT_EQ(read_mode_status_, absl::OkStatus())
      << "Failed precondition of CFileWriterBase::InitializePos(): "
         "read_mode_status_ not reset";
#ifdef _WIN32
  RIEGELI_ASSERT(original_mode_ == absl::nullopt)
      << "Failed precondition of CFileWriterBase::InitializePos(): "
         "original_mode_ not reset";
#endif
  if (ABSL_PREDICT_FALSE(ferror(dest))) {
    FailOperation("FILE");
    return;
  }
#ifdef _WIN32
  int text_mode = file_internal::GetTextAsFlags(mode);
#endif
  if (mode_was_passed_to_fopen) {
    if (!file_internal::GetRead(mode)) {
      supports_read_mode_ = LazyBoolState::kFalse;
      static const NoDestructor<absl::Status> status(
          absl::UnimplementedError("Mode does not include '+'"));
      read_mode_status_ = *status;
    }
  }
#ifdef _WIN32
  else if (text_mode != 0) {
    const int fd = _fileno(dest);
    if (ABSL_PREDICT_FALSE(fd < 0)) {
      FailOperation("_fileno()");
      return;
    }
    const int original_mode = _setmode(fd, text_mode);
    if (ABSL_PREDICT_FALSE(original_mode < 0)) {
      FailOperation("_setmode()");
      return;
    }
    original_mode_ = original_mode;
  }
  if (assumed_pos == absl::nullopt) {
    if (text_mode == 0) {
      const int fd = _fileno(dest);
      if (ABSL_PREDICT_FALSE(fd < 0)) {
        FailOperation("_fileno()");
        return;
      }
      // There is no `_getmode()`, but `_setmode()` returns the previous mode.
      text_mode = _setmode(fd, _O_BINARY);
      if (ABSL_PREDICT_FALSE(text_mode < 0)) {
        FailOperation("_setmode()");
        return;
      }
      if (ABSL_PREDICT_FALSE(_setmode(fd, text_mode) < 0)) {
        FailOperation("_setmode()");
        return;
      }
    }
    if (text_mode != _O_BINARY) assumed_pos = 0;
  }
#endif
  if (assumed_pos != absl::nullopt) {
    if (ABSL_PREDICT_FALSE(
            *assumed_pos >
            Position{std::numeric_limits<cfile_internal::Offset>::max()})) {
      FailOverflow();
      return;
    }
    set_start_pos(*assumed_pos);
    supports_random_access_ = LazyBoolState::kFalse;
    supports_read_mode_ = LazyBoolState::kFalse;
    static const NoDestructor<absl::Status> status(absl::UnimplementedError(
        "CFileWriterBase::Options::assumed_pos() excludes random access"));
    random_access_status_ = *status;
    read_mode_status_.Update(*status);
  } else {
    if (file_internal::GetAppend(mode)) {
      if (cfile_internal::FSeek(dest, 0, SEEK_END) != 0) {
        // Random access is not supported. Assume the current position as 0.
        supports_random_access_ = LazyBoolState::kFalse;
        supports_read_mode_ = LazyBoolState::kFalse;
        random_access_status_ =
            FailedOperationStatus(cfile_internal::kFSeekFunctionName);
        read_mode_status_.Update(random_access_status_);
        clearerr(dest);
        return;
      }
    }
    const cfile_internal::Offset file_pos = cfile_internal::FTell(dest);
    if (file_pos < 0) {
      // Random access is not supported. Assume the current position as 0.
      supports_random_access_ = LazyBoolState::kFalse;
      supports_read_mode_ = LazyBoolState::kFalse;
      random_access_status_ =
          FailedOperationStatus(cfile_internal::kFTellFunctionName);
      read_mode_status_.Update(random_access_status_);
      clearerr(dest);
      return;
    }
    set_start_pos(IntCast<Position>(file_pos));
    if (file_internal::GetAppend(mode)) {
      // `cfile_internal::FSeek(SEEK_END)` succeeded.
      supports_random_access_ = LazyBoolState::kFalse;
      if (mode_was_passed_to_fopen &&
          supports_read_mode_ == LazyBoolState::kUnknown) {
        supports_read_mode_ = LazyBoolState::kTrue;
      }
      static const NoDestructor<absl::Status> status(
          absl::UnimplementedError("Append mode excludes random access"));
      random_access_status_ = *status;
    } else {
      // `ftell()` succeeded, and `fseek(SEEK_END)` will be checked later.
      // `supports_random_access_` and `supports_read_mode_` are left as
      // `LazyBoolState::kUnknown`.
    }
  }
  BeginRun();
}

void CFileWriterBase::Done() {
  BufferedWriter::Done();
#ifdef _WIN32
  if (original_mode_ != absl::nullopt) {
    FILE* const dest = DestFile();
    const int fd = _fileno(dest);
    if (ABSL_PREDICT_FALSE(fd < 0)) {
      FailOperation("_fileno()");
    } else if (ABSL_PREDICT_FALSE(_setmode(fd, *original_mode_) < 0)) {
      FailOperation("_setmode()");
    }
  }
#endif
  random_access_status_ = absl::OkStatus();
  read_mode_status_.Update(absl::OkStatus());
  associated_reader_.Reset();
}

inline absl::Status CFileWriterBase::FailedOperationStatus(
    absl::string_view operation) {
  const int error_number = errno;
  RIEGELI_ASSERT_NE(error_number, 0)
      << "Failed precondition of CFileWriterBase::FailedOperationStatus(): "
         "zero errno";
  return absl::ErrnoToStatus(error_number, absl::StrCat(operation, " failed"));
}

bool CFileWriterBase::FailOperation(absl::string_view operation) {
  return Fail(FailedOperationStatus(operation));
}

absl::Status CFileWriterBase::AnnotateStatusImpl(absl::Status status) {
  if (!filename_.empty()) {
    status = Annotate(status, absl::StrCat("writing ", filename_));
  }
  return BufferedWriter::AnnotateStatusImpl(std::move(status));
}

inline absl::Status CFileWriterBase::SizeStatus() {
  RIEGELI_ASSERT(ok())
      << "Failed precondition of CFileWriterBase::SizeStatus(): " << status();
#ifndef _WIN32
  if (ABSL_PREDICT_FALSE(absl::StartsWith(filename(), "/sys/"))) {
    // "/sys" files do not support random access. It is hard to reliably
    // recognize them, so `CFileWriter` checks the filename.
    return absl::UnimplementedError("/sys files do not support random access");
  }
#endif
  FILE* const dest = DestFile();
  if (cfile_internal::FSeek(dest, 0, SEEK_END) != 0) {
    // Not supported.
    const absl::Status status =
        FailedOperationStatus(cfile_internal::kFSeekFunctionName);
    clearerr(dest);
    return status;
  }
  const cfile_internal::Offset file_size = cfile_internal::FTell(dest);
  if (ABSL_PREDICT_FALSE(file_size < 0)) {
    FailOperation(cfile_internal::kFTellFunctionName);
    return status();
  }
  if (ABSL_PREDICT_FALSE(cfile_internal::FSeek(
                             dest, IntCast<cfile_internal::Offset>(limit_pos()),
                             SEEK_SET) != 0)) {
    FailOperation(cfile_internal::kFSeekFunctionName);
    return status();
  }
#ifndef _WIN32
  if (file_size == 0 &&
      ABSL_PREDICT_FALSE(absl::StartsWith(filename(), "/proc/"))) {
    // Some "/proc" files do not support random access. It is hard to
    // reliably recognize them using the `FILE` API, so `CFileWriter`
    // checks the filename. Random access is assumed to be unsupported if
    // they claim to have a zero size.
    return absl::UnimplementedError(
        "/proc files claiming zero size do not support random access");
  }
#endif
  // Supported.
  return absl::OkStatus();
}

bool CFileWriterBase::SupportsRandomAccess() {
  if (ABSL_PREDICT_TRUE(supports_random_access_ != LazyBoolState::kUnknown)) {
    return supports_random_access_ == LazyBoolState::kTrue;
  }
  RIEGELI_ASSERT(supports_read_mode_ != LazyBoolState::kTrue)
      << "Failed invariant of CFileWriterBase: "
         "supports_random_access_ is unknown but supports_read_mode_ is true";
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
  return true;
}

bool CFileWriterBase::SupportsReadMode() {
  if (ABSL_PREDICT_TRUE(supports_read_mode_ != LazyBoolState::kUnknown)) {
    return supports_read_mode_ == LazyBoolState::kTrue;
  }
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

  FILE* const dest = DestFile();
  if (cfile_internal::FSeek(dest, 0, SEEK_END) != 0) {
    // Read mode is not supported.
    supports_read_mode_ = LazyBoolState::kFalse;
    read_mode_status_ =
        FailedOperationStatus(cfile_internal::kFSeekFunctionName);
    clearerr(dest);
    return false;
  }
  char buf[1];
  fread(buf, 1, 1, dest);
  if (ferror(dest)) {
    // Not supported.
    supports_read_mode_ = LazyBoolState::kFalse;
    read_mode_status_ = FailedOperationStatus("fread()");
    clearerr(dest);
  } else {
    // Supported.
    supports_read_mode_ = LazyBoolState::kTrue;
  }
  if (ABSL_PREDICT_FALSE(cfile_internal::FSeek(
                             dest, IntCast<cfile_internal::Offset>(start_pos()),
                             SEEK_SET) != 0)) {
    return FailOperation(cfile_internal::kFSeekFunctionName);
  }
  return supports_read_mode_ == LazyBoolState::kTrue;
}

inline bool CFileWriterBase::WriteMode() {
  if (ABSL_PREDICT_TRUE(!read_mode_)) return true;
  read_mode_ = false;
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  FILE* const dest = DestFile();
  if (ABSL_PREDICT_FALSE(cfile_internal::FSeek(
                             dest, IntCast<cfile_internal::Offset>(start_pos()),
                             SEEK_SET) != 0)) {
    return FailOperation(cfile_internal::kFSeekFunctionName);
  }
  return true;
}

bool CFileWriterBase::WriteInternal(absl::string_view src) {
  RIEGELI_ASSERT(!src.empty())
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "nothing to write";
  RIEGELI_ASSERT(ok())
      << "Failed precondition of BufferedWriter::WriteInternal(): " << status();
  if (ABSL_PREDICT_FALSE(!WriteMode())) return false;
  FILE* const dest = DestFile();
  if (ABSL_PREDICT_FALSE(
          src.size() >
          Position{std::numeric_limits<cfile_internal::Offset>::max()} -
              start_pos())) {
    return FailOverflow();
  }
  const size_t length_written = fwrite(src.data(), 1, src.size(), dest);
  RIEGELI_ASSERT_LE(length_written, src.size())
      << "fwrite() wrote more than requested";
  move_start_pos(length_written);
  if (ABSL_PREDICT_FALSE(length_written < src.size())) {
    RIEGELI_ASSERT(ferror(dest))
        << "fwrite() succeeded but wrote less than requested";
    return FailOperation("fwrite()");
  }
  return true;
}

bool CFileWriterBase::FlushBehindBuffer(absl::string_view src,
                                        FlushType flush_type) {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::FlushBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!WriteMode())) return false;
  return BufferedWriter::FlushBehindBuffer(src, flush_type);
}

bool CFileWriterBase::SeekBehindBuffer(Position new_pos) {
  RIEGELI_ASSERT_NE(new_pos, pos())
      << "Failed precondition of BufferedWriter::SeekBehindBuffer(): "
         "position unchanged, use Seek() instead";
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::SeekBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!CFileWriterBase::SupportsRandomAccess())) {
    if (ok()) Fail(random_access_status_);
    return false;
  }
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  read_mode_ = false;
  FILE* const dest = DestFile();
  if (new_pos > start_pos()) {
    // Seeking forwards.
    if (ABSL_PREDICT_FALSE(cfile_internal::FSeek(dest, 0, SEEK_END) != 0)) {
      return FailOperation(cfile_internal::kFSeekFunctionName);
    }
    const cfile_internal::Offset file_size = cfile_internal::FTell(dest);
    if (ABSL_PREDICT_FALSE(file_size < 0)) {
      return FailOperation(cfile_internal::kFTellFunctionName);
    }
    if (ABSL_PREDICT_FALSE(new_pos > IntCast<Position>(file_size))) {
      // File ends.
      set_start_pos(IntCast<Position>(file_size));
      return false;
    }
  }
  if (ABSL_PREDICT_FALSE(
          cfile_internal::FSeek(dest, IntCast<cfile_internal::Offset>(new_pos),
                                SEEK_SET) != 0)) {
    return FailOperation(cfile_internal::kFSeekFunctionName);
  }
  set_start_pos(new_pos);
  return true;
}

absl::optional<Position> CFileWriterBase::SizeBehindBuffer() {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::SizeBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!CFileWriterBase::SupportsRandomAccess())) {
    if (ok()) Fail(random_access_status_);
    return absl::nullopt;
  }
  if (ABSL_PREDICT_FALSE(!ok())) return absl::nullopt;
  read_mode_ = false;
  FILE* const dest = DestFile();
  if (ABSL_PREDICT_FALSE(cfile_internal::FSeek(dest, 0, SEEK_END) != 0)) {
    FailOperation(cfile_internal::kFSeekFunctionName);
    return absl::nullopt;
  }
  const cfile_internal::Offset file_size = cfile_internal::FTell(dest);
  if (ABSL_PREDICT_FALSE(file_size < 0)) {
    FailOperation(cfile_internal::kFTellFunctionName);
    return absl::nullopt;
  }
  if (ABSL_PREDICT_FALSE(cfile_internal::FSeek(
                             dest, IntCast<cfile_internal::Offset>(start_pos()),
                             SEEK_SET) != 0)) {
    FailOperation(cfile_internal::kFSeekFunctionName);
    return absl::nullopt;
  }
  return IntCast<Position>(file_size);
}

Reader* CFileWriterBase::ReadModeBehindBuffer(Position initial_pos) {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::ReadModeBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!CFileWriterBase::SupportsReadMode())) {
    if (ok()) Fail(read_mode_status_);
    return nullptr;
  }
  if (ABSL_PREDICT_FALSE(!ok())) return nullptr;
  FILE* const dest = DestFile();
  // Synchronize the writing aspect and the reading aspect of the `FILE`.
  if (ABSL_PREDICT_FALSE(cfile_internal::FSeek(dest, 0, SEEK_CUR) != 0)) {
    FailOperation(cfile_internal::kFSeekFunctionName);
    return nullptr;
  }
  CFileReader<UnownedCFile>* const reader = associated_reader_.ResetReader(
      dest, CFileReaderBase::Options()
                .set_assumed_filename(filename())
                .set_buffer_options(buffer_options()));
  read_mode_ = true;
  reader->Seek(initial_pos);
  return reader;
}

}  // namespace riegeli
