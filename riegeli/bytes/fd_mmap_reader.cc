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

// Make `pread()` available.
#if !defined(_XOPEN_SOURCE) || _XOPEN_SOURCE < 500
#undef _XOPEN_SOURCE
#define _XOPEN_SOURCE 500
#endif

// Make `off_t` 64-bit even on 32-bit systems.
#undef _FILE_OFFSET_BITS
#define _FILE_OFFSET_BITS 64

#include "riegeli/bytes/fd_mmap_reader.h"

#include <fcntl.h>
#include <stddef.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <limits>
#include <memory>
#include <ostream>
#include <string>
#include <tuple>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/errno_mapping.h"
#include "riegeli/base/memory_estimator.h"
#include "riegeli/base/object.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/fd_internal.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

namespace {

class MMapRef {
 public:
  MMapRef() noexcept {}

  MMapRef(const MMapRef&) = delete;
  MMapRef& operator=(const MMapRef&) = delete;

  void operator()(absl::string_view data) const;
  void RegisterSubobjects(MemoryEstimator& memory_estimator) const;
  void DumpStructure(std::ostream& out) const;
};

void MMapRef::operator()(absl::string_view data) const {
  RIEGELI_CHECK_EQ(munmap(const_cast<char*>(data.data()), data.size()), 0)
      << ErrnoToCanonicalStatus(errno, "munmap() failed").message();
}

void MMapRef::RegisterSubobjects(MemoryEstimator& memory_estimator) const {}

void MMapRef::DumpStructure(std::ostream& out) const { out << "[mmap] { }"; }

}  // namespace

void FdMMapReaderBase::Initialize(
    int src, absl::optional<std::string>&& assumed_filename,
    absl::optional<Position> independent_pos) {
  RIEGELI_ASSERT_GE(src, 0)
      << "Failed precondition of FdMMapReader: negative file descriptor";
  filename_ = fd_internal::ResolveFilename(src, std::move(assumed_filename));
  InitializePos(src, independent_pos);
}

int FdMMapReaderBase::OpenFd(absl::string_view filename, int mode) {
  RIEGELI_ASSERT((mode & O_ACCMODE) == O_RDONLY || (mode & O_ACCMODE) == O_RDWR)
      << "Failed precondition of FdMMapReader: "
         "mode must include either O_RDONLY or O_RDWR";
  // TODO: When `absl::string_view` becomes C++17 `std::string_view`:
  // `filename_ = filename`
  filename_.assign(filename.data(), filename.size());
again:
  const int src = open(filename_.c_str(), mode, 0666);
  if (ABSL_PREDICT_FALSE(src < 0)) {
    if (errno == EINTR) goto again;
    FailOperation("open()");
    return -1;
  }
  return src;
}

void FdMMapReaderBase::InitializePos(int src,
                                     absl::optional<Position> independent_pos) {
  struct stat stat_info;
  if (ABSL_PREDICT_FALSE(fstat(src, &stat_info) < 0)) {
    FailOperation("fstat()");
    return;
  }
  if (ABSL_PREDICT_FALSE(IntCast<Position>(stat_info.st_size) >
                         std::numeric_limits<size_t>::max())) {
    Fail(absl::OutOfRangeError(absl::StrCat("mmap() cannot be used reading ",
                                            filename_, ": File too large")));
    return;
  }
  if (stat_info.st_size == 0) return;
  void* const data = mmap(nullptr, IntCast<size_t>(stat_info.st_size),
                          PROT_READ, MAP_SHARED, src, 0);
  if (ABSL_PREDICT_FALSE(data == MAP_FAILED)) {
    FailOperation("mmap()");
    return;
  }
  // The `Chain` to read from was not known in `FdMMapReaderBase` constructor.
  // Set it now.
  ChainReader::Reset(std::forward_as_tuple(ChainBlock::FromExternal<MMapRef>(
      std::forward_as_tuple(),
      absl::string_view(static_cast<const char*>(data),
                        IntCast<size_t>(stat_info.st_size)))));
  if (independent_pos != absl::nullopt) {
    move_cursor(UnsignedMin(*independent_pos, available()));
  } else {
    const off_t file_pos = lseek(src, 0, SEEK_CUR);
    if (ABSL_PREDICT_FALSE(file_pos < 0)) {
      FailOperation("lseek()");
      return;
    }
    move_cursor(UnsignedMin(IntCast<Position>(file_pos), available()));
  }
}

void FdMMapReaderBase::InitializeWithExistingData(int src,
                                                  absl::string_view filename,
                                                  Position independent_pos,
                                                  const Chain& data) {
  // TODO: When `absl::string_view` becomes C++17 `std::string_view`:
  // `filename_ = filename`.
  filename_.assign(filename.data(), filename.size());
  ChainReader::Reset(data);
  move_cursor(independent_pos);
}

void FdMMapReaderBase::Done() {
  FdMMapReaderBase::SyncImpl(SyncType::kFromObject);
  ChainReader::Done();
  ChainReader::src().Clear();
}

bool FdMMapReaderBase::FailOperation(absl::string_view operation) {
  const int error_number = errno;
  RIEGELI_ASSERT_NE(error_number, 0)
      << "Failed precondition of FdMMapReaderBase::FailOperation(): "
         "zero errno";
  return Fail(
      ErrnoToCanonicalStatus(error_number, absl::StrCat(operation, " failed")));
}

absl::Status FdMMapReaderBase::AnnotateStatusImpl(absl::Status status) {
  if (!filename_.empty()) {
    status = Annotate(status, absl::StrCat("reading ", filename_));
  }
  return ChainReader::AnnotateStatusImpl(std::move(status));
}

bool FdMMapReaderBase::SyncImpl(SyncType sync_type) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  const int src = SrcFd();
  if (!has_independent_pos_) {
    if (ABSL_PREDICT_FALSE(lseek(src, IntCast<off_t>(pos()), SEEK_SET) < 0)) {
      return FailOperation("lseek()");
    }
  }
  return true;
}

std::unique_ptr<Reader> FdMMapReaderBase::NewReaderImpl(Position initial_pos) {
  if (ABSL_PREDICT_FALSE(!ok())) return nullptr;
  // `NewReaderImpl()` is thread-safe from this point.
  const int src = SrcFd();
  std::unique_ptr<FdMMapReader<UnownedFd>> reader =
      std::make_unique<FdMMapReader<UnownedFd>>(kClosed);
  reader->InitializeWithExistingData(src, filename(), initial_pos,
                                     ChainReader::src());
  return reader;
}

}  // namespace riegeli
