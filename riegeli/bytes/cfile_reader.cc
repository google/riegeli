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

#include "riegeli/bytes/cfile_reader.h"

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#endif
#include <stddef.h>
#include <stdio.h>

#include <cerrno>
#include <limits>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/dynamic_annotations.h"
#include "absl/base/optimization.h"
#include "absl/meta/type_traits.h"
#include "absl/status/status.h"
#ifndef _WIN32
#include "absl/strings/match.h"
#endif
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/global.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/cfile_internal.h"

namespace riegeli {

namespace {

// There is no portable way to apply efficient buffering over `FILE` while
// avoiding reading ahead more than eventually needed, because `fread()`
// performs multiple low level reads until the whole requested length is read.
//
// To solve this, `AvailableLength()` tries to determine how much data is
// available in the buffer of the `FILE` in a glibc-specific way, with a
// portable fallback.

template <typename DependentFILE, typename Enable = void>
struct HasAvailableLength : std::false_type {};

template <typename DependentFILE>
struct HasAvailableLength<
    DependentFILE,
    absl::void_t<decltype(std::declval<DependentFILE>()._IO_read_end -
                          std::declval<DependentFILE>()._IO_read_ptr)>>
    : std::true_type {};

template <typename DependentFILE,
          std::enable_if_t<HasAvailableLength<DependentFILE>::value, int> = 0>
inline size_t AvailableLength(DependentFILE* src) {
  // Msan does not properly track initialization performed by precompiled
  // libraries.
  ABSL_ANNOTATE_MEMORY_IS_INITIALIZED(src, sizeof(DependentFILE));
  return PtrDistance(src->_IO_read_ptr, src->_IO_read_end);
}

template <typename DependentFILE,
          std::enable_if_t<!HasAvailableLength<DependentFILE>::value, int> = 0>
inline size_t AvailableLength(DependentFILE* src) {
  return 0;
}

}  // namespace

void CFileReaderBase::Initialize(FILE* src, Options&& options) {
  RIEGELI_ASSERT(src != nullptr)
      << "Failed precondition of CFileReader: null FILE pointer";
  if (!InitializeAssumedFilename(options)) {
    cfile_internal::FilenameForCFile(src, filename_);
  }
  InitializePos(src, std::move(options)
#ifdef _WIN32
                         ,
                /*mode_was_passed_to_fopen=*/false
#endif
  );
}

void CFileReaderBase::InitializePos(FILE* src, Options&& options
#ifdef _WIN32
                                    ,
                                    bool mode_was_passed_to_fopen
#endif
) {
  RIEGELI_ASSERT(!supports_random_access_)
      << "Failed precondition of CFileReaderBase::InitializePos(): "
         "supports_random_access_ not reset";
  RIEGELI_ASSERT_EQ(random_access_status_, absl::OkStatus())
      << "Failed precondition of CFileReaderBase::InitializePos(): "
         "random_access_status_ not reset";
#ifdef _WIN32
  RIEGELI_ASSERT(original_mode_ == absl::nullopt)
      << "Failed precondition of CFileReaderBase::InitializePos(): "
         "original_mode_ not reset";
#endif
  if (ABSL_PREDICT_FALSE(ferror(src))) {
    FailOperation("FILE");
    return;
  }
#ifdef _WIN32
  int text_mode = file_internal::GetTextAsFlags(options.mode());
  if (!mode_was_passed_to_fopen && text_mode != 0) {
    const int fd = _fileno(src);
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
  if (options.assumed_pos() == absl::nullopt) {
    if (text_mode == 0) {
      const int fd = _fileno(src);
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
    if (text_mode != _O_BINARY) options.set_assumed_pos(0);
  }
#endif  // _WIN32
  if (options.assumed_pos() != absl::nullopt) {
    if (ABSL_PREDICT_FALSE(
            *options.assumed_pos() >
            Position{std::numeric_limits<cfile_internal::Offset>::max()})) {
      FailOverflow();
      return;
    }
    set_limit_pos(*options.assumed_pos());
    // `supports_random_access_` is left as `false`.
    random_access_status_ = Global([] {
      return absl::UnimplementedError(
          "CFileReaderBase::Options::assumed_pos() excludes random access");
    });
  } else {
    const cfile_internal::Offset file_pos = cfile_internal::FTell(src);
    if (file_pos < 0) {
      // Random access is not supported. Assume 0 as the initial position.
      // `supports_random_access_` is left as `false`.
      random_access_status_ =
          FailedOperationStatus(cfile_internal::kFTellFunctionName);
      clearerr(src);
      return;
    }
    set_limit_pos(IntCast<Position>(file_pos));
    CheckRandomAccess(src);
  }
  BeginRun();
}

void CFileReaderBase::Done() {
  BufferedReader::Done();
#ifdef _WIN32
  if (original_mode_ != absl::nullopt) {
    FILE* const src = SrcFile();
    const int fd = _fileno(src);
    if (ABSL_PREDICT_FALSE(fd < 0)) {
      FailOperation("_fileno()");
    } else if (ABSL_PREDICT_FALSE(_setmode(fd, *original_mode_) < 0)) {
      FailOperation("_setmode()");
    }
  }
#endif  // !_WIN32
  random_access_status_ = absl::OkStatus();
}

inline absl::Status CFileReaderBase::FailedOperationStatus(
    absl::string_view operation) {
  const int error_number = errno;
  RIEGELI_ASSERT_NE(error_number, 0)
      << "Failed precondition of CFileReaderBase::FailedOperationStatus(): "
         "zero errno";
  return absl::ErrnoToStatus(error_number, absl::StrCat(operation, " failed"));
}

inline void CFileReaderBase::CheckRandomAccess(FILE* src) {
#ifndef _WIN32
  if (ABSL_PREDICT_FALSE(absl::StartsWith(filename(), "/sys/"))) {
    // "/sys" files do not support random access. It is hard to reliably
    // recognize them, so `CFileReader` checks the filename.
    //
    // `supports_random_access_` is left as `false`.
    random_access_status_ =
        absl::UnimplementedError("/sys files do not support random access");
    return;
  }
#endif  // !_WIN32
  if (cfile_internal::FSeek(src, 0, SEEK_END) != 0) {
    // Not supported. `supports_random_access_` is left as `false`.
    random_access_status_ =
        FailedOperationStatus(cfile_internal::kFSeekFunctionName);
    clearerr(src);
    return;
  }
  const cfile_internal::Offset file_size = cfile_internal::FTell(src);
  if (ABSL_PREDICT_FALSE(file_size < 0)) {
    FailOperation(cfile_internal::kFTellFunctionName);
    return;
  }
  if (ABSL_PREDICT_FALSE(cfile_internal::FSeek(
                             src, IntCast<cfile_internal::Offset>(limit_pos()),
                             SEEK_SET) != 0)) {
    FailOperation(cfile_internal::kFSeekFunctionName);
    return;
  }
#ifndef _WIN32
  if (file_size == 0 &&
      ABSL_PREDICT_FALSE(absl::StartsWith(filename(), "/proc/"))) {
    // Some "/proc" files do not support random access. It is hard to reliably
    // recognize them using the `FILE` API, so `CFileReader` checks the
    // filename. Random access is assumed to be unsupported if they claim to
    // have a zero size, except for "/proc/*/fd" files.
    const absl::string_view after_proc = filename().substr(6);
    const size_t slash = after_proc.find('/');
    if (slash == absl::string_view::npos ||
        !absl::StartsWith(after_proc.substr(slash), "/fd/")) {
      // `supports_random_access_` is left as `false`.
      random_access_status_ = absl::UnimplementedError(
          "/proc files claiming zero size do not support random access");
      return;
    }
  }
#endif  // !_WIN32
  // Supported.
  supports_random_access_ = true;
  if (!growing_source_) set_exact_size(IntCast<Position>(file_size));
}

bool CFileReaderBase::FailOperation(absl::string_view operation) {
  return Fail(FailedOperationStatus(operation));
}

absl::Status CFileReaderBase::AnnotateStatusImpl(absl::Status status) {
  if (!filename_.empty()) {
    status = Annotate(status, absl::StrCat("reading ", filename_));
  }
  return BufferedReader::AnnotateStatusImpl(std::move(status));
}

bool CFileReaderBase::ReadInternal(size_t min_length, size_t max_length,
                                   char* dest) {
  RIEGELI_ASSERT_GT(min_length, 0u)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "nothing to read";
  RIEGELI_ASSERT_GE(max_length, min_length)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "max_length < min_length";
  RIEGELI_ASSERT(ok())
      << "Failed precondition of BufferedReader::ReadInternal(): " << status();
  FILE* const src = SrcFile();
  for (;;) {
    if (ABSL_PREDICT_FALSE(
            limit_pos() >=
            Position{std::numeric_limits<cfile_internal::Offset>::max()})) {
      return FailOverflow();
    }
    const size_t length_to_read = UnsignedMin(
        UnsignedMax(min_length, AvailableLength(src)), max_length,
        Position{std::numeric_limits<cfile_internal::Offset>::max()} -
            limit_pos());
    const size_t length_read = fread(dest, 1, length_to_read, src);
    RIEGELI_ASSERT_LE(length_read, length_to_read)
        << "fread() read more than requested";
    move_limit_pos(length_read);
    if (ABSL_PREDICT_FALSE(length_read < length_to_read)) {
      RIEGELI_ASSERT_LT(length_read, min_length)
          << "fread() read less than was available";
      if (ABSL_PREDICT_FALSE(ferror(src))) return FailOperation("fread()");
      RIEGELI_ASSERT(feof(src))
          << "fread() succeeded but read less than requested";
      clearerr(src);
      if (!growing_source_) set_exact_size(limit_pos());
      return length_read >= min_length;
    }
    if (length_read >= min_length) return true;
    dest += length_read;
    min_length -= length_read;
    max_length -= length_read;
  }
}

bool CFileReaderBase::SeekBehindBuffer(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of BufferedReader::SeekBehindBuffer(): "
         "position in the buffer, use Seek() instead";
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedReader::SeekBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!CFileReaderBase::SupportsRandomAccess())) {
    if (ABSL_PREDICT_FALSE(new_pos < start_pos())) {
      if (ok()) Fail(random_access_status_);
      return false;
    }
    return BufferedReader::SeekBehindBuffer(new_pos);
  }
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  FILE* const src = SrcFile();
  if (new_pos > limit_pos()) {
    // Seeking forwards.
    if (exact_size() != absl::nullopt) {
      if (ABSL_PREDICT_FALSE(new_pos > *exact_size())) {
        // File ends.
        if (ABSL_PREDICT_FALSE(cfile_internal::FSeek(
                src, IntCast<cfile_internal::Offset>(*exact_size()),
                SEEK_SET)) != 0) {
          return FailOperation(cfile_internal::kFSeekFunctionName);
        }
        set_limit_pos(*exact_size());
        return false;
      }
    } else {
      if (ABSL_PREDICT_FALSE(cfile_internal::FSeek(src, 0, SEEK_END) != 0)) {
        return FailOperation(cfile_internal::kFSeekFunctionName);
      }
      const cfile_internal::Offset file_size = cfile_internal::FTell(src);
      if (ABSL_PREDICT_FALSE(file_size < 0)) {
        return FailOperation(cfile_internal::kFTellFunctionName);
      }
      if (!growing_source_) set_exact_size(IntCast<Position>(file_size));
      if (ABSL_PREDICT_FALSE(new_pos > IntCast<Position>(file_size))) {
        // File ends.
        set_limit_pos(IntCast<Position>(file_size));
        return false;
      }
    }
  }
  if (ABSL_PREDICT_FALSE(cfile_internal::FSeek(
          src, IntCast<cfile_internal::Offset>(new_pos), SEEK_SET)) != 0) {
    return FailOperation(cfile_internal::kFSeekFunctionName);
  }
  set_limit_pos(new_pos);
  return true;
}

absl::optional<Position> CFileReaderBase::SizeImpl() {
  if (ABSL_PREDICT_FALSE(!ok())) return absl::nullopt;
  if (exact_size() != absl::nullopt) return *exact_size();
  if (ABSL_PREDICT_FALSE(!CFileReaderBase::SupportsRandomAccess())) {
    Fail(random_access_status_);
    return absl::nullopt;
  }
  FILE* const src = SrcFile();
  if (ABSL_PREDICT_FALSE(cfile_internal::FSeek(src, 0, SEEK_END)) != 0) {
    FailOperation(cfile_internal::kFSeekFunctionName);
    return absl::nullopt;
  }
  const cfile_internal::Offset file_size = cfile_internal::FTell(src);
  if (ABSL_PREDICT_FALSE(file_size < 0)) {
    FailOperation(cfile_internal::kFTellFunctionName);
    return absl::nullopt;
  }
  if (ABSL_PREDICT_FALSE(cfile_internal::FSeek(
                             src, IntCast<cfile_internal::Offset>(limit_pos()),
                             SEEK_SET) != 0)) {
    FailOperation(cfile_internal::kFSeekFunctionName);
    return absl::nullopt;
  }
  if (!growing_source_) set_exact_size(IntCast<Position>(file_size));
  return IntCast<Position>(file_size);
}

}  // namespace riegeli
