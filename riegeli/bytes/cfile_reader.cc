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
#include "riegeli/base/base.h"
#include "riegeli/base/errno_mapping.h"
#include "riegeli/base/status.h"
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

FILE* CFileReaderBase::OpenFile(absl::string_view filename, const char* mode) {
  // TODO: When `absl::string_view` becomes C++17 `std::string_view`:
  // `filename_ = filename`
  filename_.assign(filename.data(), filename.size());
  FILE* const src = fopen(filename_.c_str(), mode);
  if (ABSL_PREDICT_FALSE(src == nullptr)) {
    FailOperation("fopen()");
    return nullptr;
  }
  return src;
}

void CFileReaderBase::InitializePos(FILE* src,
                                    absl::optional<Position> assumed_pos) {
  RIEGELI_ASSERT(supports_random_access_ == LazyBoolState::kFalse)
      << "Failed precondition of CFileReaderBase::InitializePos(): "
         "supports_random_access_ not reset";
  if (ABSL_PREDICT_FALSE(ferror(src))) {
    FailOperation("FILE");
    return;
  }
  if (assumed_pos != absl::nullopt) {
    if (ABSL_PREDICT_FALSE(*assumed_pos >
                           Position{std::numeric_limits<off_t>::max()})) {
      FailOverflow();
      return;
    }
    set_limit_pos(*assumed_pos);
  } else {
    const off_t file_pos = cfile_internal::FTell(src);
    if (file_pos < 0) {
      // Random access is not supported. Assume 0 as the initial position.
      clearerr(src);
      return;
    }
    set_limit_pos(IntCast<Position>(file_pos));
    // `ftell()` succeeded, and `fseek(SEEK_END)` will be checked later.
    supports_random_access_ = LazyBoolState::kUnknown;
  }
  BeginRun();
}

void CFileReaderBase::Done() {
  BufferedReader::Done();
  // If `supports_random_access_` is still `LazyBoolState::kUnknown`, change it
  // to `LazyBoolState::kFalse`, because trying to resolve it later might access
  // a closed stream. The resolution is no longer interesting anyway.
  if (supports_random_access_ == LazyBoolState::kUnknown) {
    supports_random_access_ = LazyBoolState::kFalse;
  }
}

bool CFileReaderBase::FailOperation(absl::string_view operation) {
  const int error_number = errno;
  RIEGELI_ASSERT_NE(error_number, 0)
      << "Failed precondition of CFileReaderBase::FailOperation(): "
         "zero errno";
  return Fail(
      ErrnoToCanonicalStatus(error_number, absl::StrCat(operation, " failed")));
}

absl::Status CFileReaderBase::AnnotateStatusImpl(absl::Status status) {
  if (!filename_.empty()) {
    status = Annotate(status, absl::StrCat("reading ", filename_));
  }
  return BufferedReader::AnnotateStatusImpl(std::move(status));
}

bool CFileReaderBase::supports_random_access() {
  switch (supports_random_access_) {
    case LazyBoolState::kFalse:
      return false;
    case LazyBoolState::kTrue:
      return true;
    case LazyBoolState::kUnknown:
      break;
  }
  RIEGELI_ASSERT(is_open())
      << "Failed invariant of CFileReaderBase: "
         "unresolved supports_random_access_ but object closed";
  bool supported = false;
  if (ABSL_PREDICT_FALSE(absl::StartsWith(filename(), "/sys/"))) {
    // "/sys" files do not support random access. It is hard to reliably
    // recognize them, so `CFileReader` checks the filename.
  } else {
    FILE* const src = src_file();
    if (cfile_internal::FSeek(src, 0, SEEK_END) != 0) {
      clearerr(src);
    } else {
      const off_t file_size = cfile_internal::FTell(src);
      if (ABSL_PREDICT_FALSE(file_size < 0)) {
        FailOperation(cfile_internal::kFTellFunctionName);
      } else if (ABSL_PREDICT_FALSE(
                     cfile_internal::FSeek(src, IntCast<off_t>(limit_pos()),
                                           SEEK_SET) != 0)) {
        FailOperation(cfile_internal::kFSeekFunctionName);
      } else if (file_size > 0 ||
                 ABSL_PREDICT_TRUE(!absl::StartsWith(filename(), "/proc/"))) {
        // Some "/proc" files do not support random access. It is hard to
        // reliably recognize them using the `FILE` API, so `CFileReader` checks
        // the filename. Random access is assumed to be supported only if they
        // claim to have a non-zero size.
        StoreSize(IntCast<Position>(file_size));
        supported = true;
      }
    }
  }
  supports_random_access_ =
      supported ? LazyBoolState::kTrue : LazyBoolState::kFalse;
  return supported;
}

inline void CFileReaderBase::StoreSize(Position size) {
  set_size_hint(size);
  if (!growing_source_) size_hint_is_exact_ = true;
}

inline absl::optional<Position> CFileReaderBase::exact_size() const {
  if (size_hint_is_exact_) return size_hint();
  return absl::nullopt;
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
  if (exact_size() != absl::nullopt &&
      ABSL_PREDICT_FALSE(limit_pos() >= exact_size())) {
    return false;
  }
  FILE* const src = src_file();
  if (ABSL_PREDICT_FALSE(max_length >
                         Position{std::numeric_limits<off_t>::max()} -
                             limit_pos())) {
    max_length = Position{std::numeric_limits<off_t>::max()} - limit_pos();
    if (ABSL_PREDICT_FALSE(max_length < min_length)) return FailOverflow();
  }
  for (;;) {
    size_t length_to_read = UnsignedMax(min_length, AvailableLength(src));
    if (length_to_read < max_length && supports_random_access() &&
        exact_size() != absl::nullopt) {
      // Increase `length_to_read` to cover the rest of the file.
      length_to_read = UnsignedMax(
          length_to_read,
          SaturatingIntCast<size_t>(SaturatingSub(*exact_size(), limit_pos())));
    }
    length_to_read = UnsignedMin(length_to_read, max_length);
    const size_t length_read = fread(dest, 1, length_to_read, src);
    RIEGELI_ASSERT_LE(length_read, length_to_read)
        << "fread() read more than requested";
    move_limit_pos(length_read);
    if (ABSL_PREDICT_FALSE(length_read < length_to_read)) {
      RIEGELI_ASSERT_LT(length_read, min_length)
          << "fread() read less than was available";
      if (ABSL_PREDICT_FALSE(ferror(src))) return FailOperation("fread()");
      RIEGELI_ASSERT(feof(src))
          << "fread() read less than requested "
             "but did not indicate failure nor end of file";
      clearerr(src);
      StoreSize(limit_pos());
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
  if (ABSL_PREDICT_FALSE(!supports_random_access())) {
    return BufferedReader::SeekBehindBuffer(new_pos);
  }
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  FILE* const src = src_file();
  if (new_pos > limit_pos()) {
    // Seeking forwards.
    if (exact_size() != absl::nullopt) {
      if (ABSL_PREDICT_FALSE(new_pos > *exact_size())) {
        // Stream ends.
        if (ABSL_PREDICT_FALSE(cfile_internal::FSeek(
                src, IntCast<off_t>(*exact_size()), SEEK_SET)) != 0) {
          return FailOperation(cfile_internal::kFSeekFunctionName);
        }
        set_limit_pos(*exact_size());
        return false;
      }
    } else {
      if (ABSL_PREDICT_FALSE(cfile_internal::FSeek(src, 0, SEEK_END) != 0)) {
        return FailOperation(cfile_internal::kFSeekFunctionName);
      }
      const off_t file_size = cfile_internal::FTell(src);
      if (ABSL_PREDICT_FALSE(file_size < 0)) {
        return FailOperation(cfile_internal::kFTellFunctionName);
      }
      StoreSize(IntCast<Position>(file_size));
      if (ABSL_PREDICT_FALSE(new_pos > IntCast<Position>(file_size))) {
        // Stream ends.
        set_limit_pos(IntCast<Position>(file_size));
        return false;
      }
    }
  }
  if (ABSL_PREDICT_FALSE(
          cfile_internal::FSeek(src, IntCast<off_t>(new_pos), SEEK_SET)) != 0) {
    return FailOperation(cfile_internal::kFSeekFunctionName);
  }
  set_limit_pos(new_pos);
  return true;
}

absl::optional<Position> CFileReaderBase::SizeImpl() {
  if (ABSL_PREDICT_FALSE(!supports_random_access())) {
    // Delegate to base class version which fails, to avoid duplicating the
    // failure message here.
    return BufferedReader::SizeImpl();
  }
  if (ABSL_PREDICT_FALSE(!ok())) return absl::nullopt;
  if (exact_size() != absl::nullopt) return *exact_size();
  FILE* const src = src_file();
  if (ABSL_PREDICT_FALSE(cfile_internal::FSeek(src, 0, SEEK_END)) != 0) {
    FailOperation(cfile_internal::kFSeekFunctionName);
    return absl::nullopt;
  }
  const off_t file_size = cfile_internal::FTell(src);
  if (ABSL_PREDICT_FALSE(file_size < 0)) {
    FailOperation(cfile_internal::kFTellFunctionName);
    return absl::nullopt;
  }
  if (ABSL_PREDICT_FALSE(cfile_internal::FSeek(src, IntCast<off_t>(limit_pos()),
                                               SEEK_SET) != 0)) {
    FailOperation(cfile_internal::kFSeekFunctionName);
    return absl::nullopt;
  }
  StoreSize(IntCast<Position>(file_size));
  return IntCast<Position>(file_size);
}

}  // namespace riegeli
