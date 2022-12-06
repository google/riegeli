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

// Make `fseeko()` and `ftello()` available.
#if !defined(_XOPEN_SOURCE) || _XOPEN_SOURCE < 500
#undef _XOPEN_SOURCE
#define _XOPEN_SOURCE 500
#endif

// Make `off_t` 64-bit even on 32-bit systems.
#undef _FILE_OFFSET_BITS
#define _FILE_OFFSET_BITS 64

#include "riegeli/bytes/cfile_reader.h"

#include <stddef.h>
#include <stdio.h>
#include <sys/types.h>

#include <cerrno>
#include <limits>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/dynamic_annotations.h"
#include "absl/base/optimization.h"
#include "absl/meta/type_traits.h"
#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/no_destructor.h"
#include "riegeli/base/object.h"
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

void CFileReaderBase::Initialize(FILE* src, std::string&& assumed_filename,
                                 absl::optional<Position> assumed_pos) {
  RIEGELI_ASSERT(src != nullptr)
      << "Failed precondition of CFileReader: null FILE pointer";
  filename_ = std::move(assumed_filename);
  InitializePos(src, assumed_pos);
}

FILE* CFileReaderBase::OpenFile(absl::string_view filename,
                                const std::string& mode) {
  // TODO: When `absl::string_view` becomes C++17 `std::string_view`:
  // `filename_ = filename`
  filename_.assign(filename.data(), filename.size());
  FILE* const src = fopen(filename_.c_str(), mode.c_str());
  if (ABSL_PREDICT_FALSE(src == nullptr)) {
    BufferedReader::Reset(kClosed);
    FailOperation("fopen()");
    return nullptr;
  }
  return src;
}

void CFileReaderBase::InitializePos(FILE* src,
                                    absl::optional<Position> assumed_pos) {
  RIEGELI_ASSERT(!supports_random_access_)
      << "Failed precondition of CFileReaderBase::InitializePos(): "
         "supports_random_access_ not reset";
  RIEGELI_ASSERT_EQ(random_access_status_, absl::OkStatus())
      << "Failed precondition of CFileReaderBase::InitializePos(): "
         "random_access_status_ not reset";
  if (ABSL_PREDICT_FALSE(ferror(src))) {
    FailOperation("FILE");
    return;
  }
  if (assumed_pos != absl::nullopt) {
    if (ABSL_PREDICT_FALSE(
            *assumed_pos >
            Position{std::numeric_limits<cfile_internal::Offset>::max()})) {
      FailOverflow();
      return;
    }
    set_limit_pos(*assumed_pos);
    // `supports_random_access_` is left as `false`.
    static const NoDestructor<absl::Status> status(absl::UnimplementedError(
        "CFileReaderBase::Options::assumed_pos() excludes random access"));
    random_access_status_ = *status;
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

    // Check if random access is supported.
    if (ABSL_PREDICT_FALSE(absl::StartsWith(filename(), "/sys/"))) {
      // "/sys" files do not support random access. It is hard to reliably
      // recognize them, so `CFileReader` checks the filename.
      //
      // `supports_random_access_` is left as `false`.
      random_access_status_ =
          absl::UnimplementedError("/sys files do not support random access");
    } else {
      FILE* const src = SrcFile();
      if (cfile_internal::FSeek(src, 0, SEEK_END) != 0) {
        // Not supported. `supports_random_access_` is left as `false`.
        random_access_status_ =
            FailedOperationStatus(cfile_internal::kFSeekFunctionName);
        clearerr(src);
      } else {
        const cfile_internal::Offset file_size = cfile_internal::FTell(src);
        if (ABSL_PREDICT_FALSE(file_size < 0)) {
          FailOperation(cfile_internal::kFTellFunctionName);
          return;
        }
        if (ABSL_PREDICT_FALSE(cfile_internal::FSeek(
                                   src,
                                   IntCast<cfile_internal::Offset>(limit_pos()),
                                   SEEK_SET) != 0)) {
          FailOperation(cfile_internal::kFSeekFunctionName);
          return;
        }
        if (file_size == 0 &&
            ABSL_PREDICT_FALSE(absl::StartsWith(filename(), "/proc/"))) {
          // Some "/proc" files do not support random access. It is hard to
          // reliably recognize them using the `FILE` API, so `CFileReader`
          // checks the filename. Random access is assumed to be unsupported if
          // they claim to have a zero size.
          //
          // `supports_random_access_` is left as `false`.
          random_access_status_ = absl::UnimplementedError(
              "/proc files claiming zero size do not support random access");
        } else {
          // Supported.
          supports_random_access_ = true;
          if (!growing_source_) set_exact_size(IntCast<Position>(file_size));
        }
      }
    }
  }
  BeginRun();
}

void CFileReaderBase::Done() {
  BufferedReader::Done();
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
    Position max_pos;
    if (exact_size() != absl::nullopt) {
      max_pos = *exact_size();
      if (ABSL_PREDICT_FALSE(limit_pos() >= max_pos)) return false;
    } else {
      max_pos = Position{std::numeric_limits<cfile_internal::Offset>::max()};
      if (ABSL_PREDICT_FALSE(limit_pos() >= max_pos)) return FailOverflow();
    }
    const size_t length_to_read =
        UnsignedMin(UnsignedMax(min_length, AvailableLength(src)), max_length);
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
        // Stream ends.
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
        // Stream ends.
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
  if (ABSL_PREDICT_FALSE(!CFileReaderBase::SupportsRandomAccess())) {
    if (ok()) Fail(random_access_status_);
    return absl::nullopt;
  }
  if (ABSL_PREDICT_FALSE(!ok())) return absl::nullopt;
  if (exact_size() != absl::nullopt) return *exact_size();
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
