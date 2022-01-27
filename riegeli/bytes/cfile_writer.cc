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

#include "riegeli/bytes/cfile_writer.h"

#include <stddef.h>
#include <stdio.h>

#include <cerrno>
#include <limits>
#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/errno_mapping.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/cfile_internal.h"
#include "riegeli/bytes/cfile_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void CFileWriterBase::Initialize(FILE* dest, std::string&& assumed_filename,
                                 absl::optional<Position> assumed_pos,
                                 bool append) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of CFileReader: null FILE pointer";
  filename_ = std::move(assumed_filename);
  InitializePos(dest, assumed_pos, append);
}

FILE* CFileWriterBase::OpenFile(absl::string_view filename, const char* mode) {
  // TODO: When `absl::string_view` becomes C++17 `std::string_view`:
  // `filename_ = filename`
  filename_.assign(filename.data(), filename.size());
  FILE* const dest = fopen(filename_.c_str(), mode);
  if (ABSL_PREDICT_FALSE(dest == nullptr)) {
    FailOperation("fopen()");
    return nullptr;
  }
  return dest;
}

inline void CFileWriterBase::InitializePos(FILE* dest,
                                           absl::optional<Position> assumed_pos,
                                           bool append) {
  RIEGELI_ASSERT(supports_random_access_ == LazyBoolState::kFalse)
      << "Failed precondition of CFileWriterBase::InitializePos(): "
         "supports_random_access_ not reset";
  RIEGELI_ASSERT(supports_read_mode_ == LazyBoolState::kFalse)
      << "Failed precondition of CFileWriterBase::InitializePos(): "
         "supports_read_mode_ not reset";
  if (ABSL_PREDICT_FALSE(ferror(dest))) {
    FailOperation("FILE");
    return;
  }
  if (assumed_pos != absl::nullopt) {
    if (ABSL_PREDICT_FALSE(*assumed_pos >
                           Position{std::numeric_limits<off_t>::max()})) {
      FailOverflow();
      return;
    }
    set_start_pos(*assumed_pos);
  } else {
    if (append) {
      if (cfile_internal::FSeek(dest, 0, SEEK_END) != 0) {
        // Random access is not supported. Assume the current position as 0.
        clearerr(dest);
        return;
      }
    }
    const off_t file_pos = cfile_internal::FTell(dest);
    if (file_pos < 0) {
      // Random access is not supported. Assume the current position as 0.
      clearerr(dest);
      return;
    }
    set_start_pos(IntCast<Position>(file_pos));
    // If `!append` then `fseek(SEEK_CUR)` succeeded, and `fseek(SEEK_END)` will
    // be checked later.
    supports_random_access_ =
        append ? LazyBoolState::kTrue : LazyBoolState::kUnknown;
    supports_read_mode_ = LazyBoolState::kUnknown;
  }
}

void CFileWriterBase::Done() {
  BufferedWriter::Done();
  // If `supports_random_access_` is still `LazyBoolState::kUnknown`, change it
  // to `LazyBoolState::kFalse`, because trying to resolve it later might access
  // a closed stream. The resolution is no longer interesting anyway.
  if (supports_random_access_ == LazyBoolState::kUnknown) {
    supports_random_access_ = LazyBoolState::kFalse;
  }
  // Same for `supports_read_mode_`.
  if (supports_read_mode_ == LazyBoolState::kUnknown) {
    supports_read_mode_ = LazyBoolState::kFalse;
  }
  associated_reader_.Reset();
}

bool CFileWriterBase::FailOperation(absl::string_view operation) {
  const int error_number = errno;
  RIEGELI_ASSERT_NE(error_number, 0)
      << "Failed precondition of CFileWriterBase::FailOperation(): "
         "zero errno";
  return Fail(
      ErrnoToCanonicalStatus(error_number, absl::StrCat(operation, " failed")));
}

absl::Status CFileWriterBase::AnnotateStatusImpl(absl::Status status) {
  if (!filename_.empty()) {
    status = Annotate(status, absl::StrCat("writing ", filename_));
  }
  return BufferedWriter::AnnotateStatusImpl(std::move(status));
}

bool CFileWriterBase::supports_random_access() {
  switch (supports_random_access_) {
    case LazyBoolState::kFalse:
      return false;
    case LazyBoolState::kTrue:
      return true;
    case LazyBoolState::kUnknown:
      break;
  }
  RIEGELI_ASSERT(is_open())
      << "Failed invariant of CFileWriterBase: "
         "unresolved supports_random_access_ but object closed";
  bool supported = false;
  if (absl::StartsWith(filename(), "/sys/")) {
    // "/sys" files do not support random access. It is hard to reliably
    // recognize them, so `CFileWriter` checks the filename.
  } else {
    FILE* const dest = dest_file();
    if (cfile_internal::FSeek(dest, 0, SEEK_END) != 0) {
      clearerr(dest);
    } else if (absl::StartsWith(filename(), "/proc/")) {
      // Some "/proc" files do not support random access. It is hard to reliably
      // recognize them using the `FILE` API, so `CFileWriter` checks the
      // filename. Random access is assumed to be supported only if they claim
      // to have a non-zero size.
      const off_t file_size = cfile_internal::FTell(dest);
      if (ABSL_PREDICT_FALSE(file_size < 0)) {
        FailOperation(cfile_internal::kFTellFunctionName);
      } else if (ABSL_PREDICT_FALSE(
                     cfile_internal::FSeek(dest, IntCast<off_t>(limit_pos()),
                                           SEEK_SET) != 0)) {
        FailOperation(cfile_internal::kFSeekFunctionName);
      } else {
        supported = file_size > 0;
      }
    } else {
      if (ABSL_PREDICT_FALSE(cfile_internal::FSeek(dest,
                                                   IntCast<off_t>(limit_pos()),
                                                   SEEK_SET) != 0)) {
        FailOperation(cfile_internal::kFSeekFunctionName);
      } else {
        supported = true;
      }
    }
  }
  supports_random_access_ =
      supported ? LazyBoolState::kTrue : LazyBoolState::kFalse;
  return supported;
}

bool CFileWriterBase::supports_read_mode() {
  switch (supports_read_mode_) {
    case LazyBoolState::kFalse:
      return false;
    case LazyBoolState::kTrue:
      return true;
    case LazyBoolState::kUnknown:
      break;
  }
  RIEGELI_ASSERT(is_open())
      << "Failed invariant of CFileWriterBase: "
         "unresolved supports_read_mode_ but object closed";
  if (!supports_random_access()) {
    supports_read_mode_ = LazyBoolState::kFalse;
    return false;
  }
  FILE* const dest = dest_file();
  bool supported = false;
  if (cfile_internal::FSeek(dest, 0, SEEK_END) != 0) {
    clearerr(dest);
  } else {
    char buf[1];
    fread(buf, 1, 1, dest);
    supported = !ferror(dest);
    clearerr(dest);
    if (ABSL_PREDICT_FALSE(cfile_internal::FSeek(dest,
                                                 IntCast<off_t>(start_pos()),
                                                 SEEK_SET) != 0)) {
      return FailOperation(cfile_internal::kFSeekFunctionName);
    }
  }
  supports_read_mode_ =
      supported ? LazyBoolState::kTrue : LazyBoolState::kFalse;
  return supported;
}

inline bool CFileWriterBase::WriteMode() {
  if (ABSL_PREDICT_TRUE(!read_mode_)) return true;
  read_mode_ = false;
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  FILE* const dest = dest_file();
  if (ABSL_PREDICT_FALSE(cfile_internal::FSeek(dest,
                                               IntCast<off_t>(start_pos()),
                                               SEEK_SET) != 0)) {
    return FailOperation(cfile_internal::kFSeekFunctionName);
  }
  return true;
}

bool CFileWriterBase::WriteInternal(absl::string_view src) {
  RIEGELI_ASSERT(!src.empty())
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "nothing to write";
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of BufferedWriter::WriteInternal(): " << status();
  if (ABSL_PREDICT_FALSE(!WriteMode())) return false;
  FILE* const dest = dest_file();
  if (ABSL_PREDICT_FALSE(src.size() >
                         Position{std::numeric_limits<off_t>::max()} -
                             start_pos())) {
    return FailOverflow();
  }
  const size_t length_written = fwrite(src.data(), 1, src.size(), dest);
  RIEGELI_ASSERT_LE(length_written, src.size())
      << "fwrite() wrote more than requested";
  move_start_pos(length_written);
  if (ABSL_PREDICT_FALSE(length_written < src.size())) {
    RIEGELI_ASSERT(ferror(dest)) << "fwrite() wrote less than requested "
                                    "but did not indicate failure";
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
  if (ABSL_PREDICT_FALSE(!supports_random_access())) {
    // Delegate to base class version which fails, to avoid duplicating the
    // failure message here.
    return BufferedWriter::SeekBehindBuffer(new_pos);
  }
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  read_mode_ = false;
  FILE* const dest = dest_file();
  if (new_pos > start_pos()) {
    // Seeking forwards.
    if (ABSL_PREDICT_FALSE(cfile_internal::FSeek(dest, 0, SEEK_END) != 0)) {
      return FailOperation(cfile_internal::kFSeekFunctionName);
    }
    const off_t file_size = cfile_internal::FTell(dest);
    if (ABSL_PREDICT_FALSE(file_size < 0)) {
      return FailOperation(cfile_internal::kFTellFunctionName);
    }
    if (ABSL_PREDICT_FALSE(new_pos > IntCast<Position>(file_size))) {
      // Stream ends.
      set_start_pos(IntCast<Position>(file_size));
      return false;
    }
  }
  if (ABSL_PREDICT_FALSE(cfile_internal::FSeek(dest, IntCast<off_t>(new_pos),
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
  if (ABSL_PREDICT_FALSE(!supports_random_access())) {
    // Delegate to base class version which fails, to avoid duplicating the
    // failure message here.
    return BufferedWriter::SizeBehindBuffer();
  }
  if (ABSL_PREDICT_FALSE(!healthy())) return absl::nullopt;
  read_mode_ = false;
  FILE* const dest = dest_file();
  if (ABSL_PREDICT_FALSE(cfile_internal::FSeek(dest, 0, SEEK_END) != 0)) {
    FailOperation(cfile_internal::kFSeekFunctionName);
    return absl::nullopt;
  }
  const off_t file_size = cfile_internal::FTell(dest);
  if (ABSL_PREDICT_FALSE(file_size < 0)) {
    FailOperation(cfile_internal::kFTellFunctionName);
    return absl::nullopt;
  }
  if (ABSL_PREDICT_FALSE(cfile_internal::FSeek(dest,
                                               IntCast<off_t>(start_pos()),
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
  if (ABSL_PREDICT_FALSE(!supports_read_mode())) {
    // Delegate to base class version which fails, to avoid duplicating the
    // failure message here.
    return BufferedWriter::ReadModeBehindBuffer(initial_pos);
  }
  if (ABSL_PREDICT_FALSE(!healthy())) return nullptr;
  FILE* const dest = dest_file();
  CFileReader<UnownedCFile>* const reader =
      associated_reader_.ResetReader(dest, CFileReaderBase::Options()
                                               .set_assumed_filename(filename())
                                               .set_buffer_size(buffer_size()));
  read_mode_ = true;
  reader->Seek(initial_pos);
  return reader;
}

}  // namespace riegeli
