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

// Make `posix_fadvise()` available.
#if !defined(_XOPEN_SOURCE) || _XOPEN_SOURCE < 600
#undef _XOPEN_SOURCE
#define _XOPEN_SOURCE 600
#endif

// Make `off_t` 64-bit even on 32-bit systems.
#undef _FILE_OFFSET_BITS
#define _FILE_OFFSET_BITS 64

#else

#define WIN32_LEAN_AND_MEAN

#endif

#include "riegeli/bytes/fd_mmap_reader.h"

#include <fcntl.h>
#ifdef _WIN32
#include <io.h>
#endif
#include <stddef.h>
#ifdef _WIN32
#include <stdint.h>
#endif
#include <stdio.h>
#ifndef _WIN32
#include <sys/mman.h>
#endif
#include <sys/types.h>
#ifndef _WIN32
#include <unistd.h>
#else
#include <windows.h>
#endif

#include <cerrno>
#include <limits>
#include <memory>
#include <optional>
#include <ostream>
#ifndef _WIN32
#include <type_traits>
#endif
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#ifndef _WIN32
#include "absl/status/statusor.h"
#endif
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#ifdef _WIN32
#include "riegeli/base/errno_mapping.h"
#endif
#include "riegeli/base/external_ref.h"
#ifndef _WIN32
#include "riegeli/base/global.h"
#endif
#include "riegeli/base/maker.h"
#include "riegeli/base/object.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/fd_handle.h"
#include "riegeli/bytes/fd_internal_for_cc.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

namespace {

#ifdef _WIN32

struct HandleDeleter {
  void operator()(void* handle) const {
    RIEGELI_CHECK(CloseHandle(reinterpret_cast<HANDLE>(handle)))
        << WindowsErrorToStatus(IntCast<uint32_t>(GetLastError()),
                                "CloseHandle() failed")
               .message();
  }
};

using UniqueHandle = std::unique_ptr<void, HandleDeleter>;

#endif  // _WIN32

#ifndef _WIN32

inline absl::StatusOr<Position> GetPageSize() {
  const long page_size = sysconf(_SC_PAGE_SIZE);
  if (ABSL_PREDICT_FALSE(page_size < 0)) {
    return absl::ErrnoToStatus(errno, "sysconf() failed");
  }
  return IntCast<Position>(page_size);
}

#else  // _WIN32

inline Position GetPageSize() {
  SYSTEM_INFO system_info;
  GetSystemInfo(&system_info);
  return IntCast<Position>(system_info.dwAllocationGranularity);
}

#endif  // _WIN32

#ifndef _WIN32

// `posix_fadvise()` is supported by POSIX systems but not MacOS.

template <typename DependentInt, typename Enable = void>
struct HavePosixFadvise : std::false_type {};

template <typename DependentInt>
struct HavePosixFadvise<
    DependentInt,
    std::void_t<decltype(posix_fadvise(
        std::declval<DependentInt>(), std::declval<fd_internal::Offset>(),
        std::declval<fd_internal::Offset>(), std::declval<int>()))>>
    : std::true_type {};

template <typename DependentInt>
inline void FdSetReadAllHint(ABSL_ATTRIBUTE_UNUSED DependentInt src,
                             ABSL_ATTRIBUTE_UNUSED bool read_all_hint) {
  if constexpr (HavePosixFadvise<DependentInt>::value) {
#ifdef POSIX_FADV_SEQUENTIAL
    posix_fadvise(src, 0, 0,
                  read_all_hint ? POSIX_FADV_SEQUENTIAL : POSIX_FADV_NORMAL);
#endif
  }
}

#endif  // !_WIN32

class MMapBlock {
 public:
  explicit MMapBlock(const char* addr) : addr_(addr) {}

  MMapBlock(MMapBlock&& that) = default;
  MMapBlock& operator=(MMapBlock&& that) = default;

  // Indicates support for `ExternalRef(MMapBlock&&, substr)`.
  friend void RiegeliSupportsExternalRef(MMapBlock*) {}

  // Supports `ExternalRef`, `Chain::Block`, and `absl::MakeCordFromExternal()`.
  void operator()(absl::string_view data) const;

  // Supports `ExternalRef` and `Chain::Block`.
  friend void RiegeliDumpStructure(ABSL_ATTRIBUTE_UNUSED const MMapBlock* self,
                                   std::ostream& dest) {
    dest << "[mmap] { }";
  }

 private:
  const char* addr_;
};

void MMapBlock::operator()(absl::string_view data) const {
#ifndef _WIN32
  RIEGELI_CHECK_EQ(munmap(const_cast<char*>(addr_),
                          data.size() + PtrDistance(addr_, data.data())),
                   0)
      << absl::ErrnoToStatus(errno, "munmap() failed").message();
#else   // _WIN32
  RIEGELI_CHECK(UnmapViewOfFile(addr_))
      << WindowsErrorToStatus(IntCast<uint32_t>(GetLastError()),
                              "UnmapViewOfFile() failed")
             .message();
#endif  // _WIN32
}

}  // namespace

void FdMMapReaderBase::Initialize(int src, Options&& options) {
  RIEGELI_ASSERT_GE(src, 0)
      << "Failed precondition of FdMMapReader: negative file descriptor";
  InitializePos(src, std::move(options));
}

void FdMMapReaderBase::InitializePos(int src, Options&& options) {
  Position initial_pos;
  if (options.independent_pos() != std::nullopt) {
    initial_pos = *options.independent_pos();
  } else {
    const fd_internal::Offset file_pos = fd_internal::LSeek(src, 0, SEEK_CUR);
    if (ABSL_PREDICT_FALSE(file_pos < 0)) {
      FailOperation(fd_internal::kLSeekFunctionName);
      return;
    }
    initial_pos = IntCast<Position>(file_pos);
  }

  fd_internal::StatInfo stat_info;
  if (ABSL_PREDICT_FALSE(fd_internal::FStat(src, &stat_info) < 0)) {
    FailOperation(fd_internal::kFStatFunctionName);
    return;
  }
  Position base_pos = 0;
  Position length = IntCast<Position>(stat_info.st_size);
  if (options.max_length() != std::nullopt) {
    base_pos = initial_pos;
    length =
        UnsignedMin(SaturatingSub(length, initial_pos), *options.max_length());
  }
  if (options.independent_pos() == std::nullopt) base_pos_to_sync_ = base_pos;
  if (length == 0) {
    // The `Chain` to read from was not known in `FdMMapReaderBase` constructor.
    // Set it now to empty.
    ChainReader::Reset(riegeli::Maker());
    return;
  }

  Position rounded_base_pos = base_pos;
  if (rounded_base_pos > 0) {
#ifndef _WIN32
    const absl::StatusOr<Position>& page_size =
        Global([] { return GetPageSize(); });
    if (ABSL_PREDICT_FALSE(!page_size.ok())) {
      Fail(page_size.status());
      return;
    }
    rounded_base_pos &= ~(*page_size - 1);
#else   // _WIN32
    static const Position kPageSize = GetPageSize();
    rounded_base_pos &= ~(kPageSize - 1);
#endif  // _WIN32
  }
  const Position rounding = base_pos - rounded_base_pos;
  const Position rounded_length = length + rounding;
  if (ABSL_PREDICT_FALSE(rounded_length > std::numeric_limits<size_t>::max())) {
    Fail(absl::OutOfRangeError("File too large for memory mapping"));
    return;
  }
#ifndef _WIN32
  void* const addr = mmap(nullptr, IntCast<size_t>(rounded_length), PROT_READ,
                          MAP_SHARED, src, IntCast<off_t>(rounded_base_pos));
  if (ABSL_PREDICT_FALSE(addr == MAP_FAILED)) {
    FailOperation("mmap()");
    return;
  }
#else   // _WIN32
  const HANDLE file_handle = reinterpret_cast<HANDLE>(_get_osfhandle(src));
  if (ABSL_PREDICT_FALSE(file_handle == INVALID_HANDLE_VALUE ||
                         file_handle == reinterpret_cast<HANDLE>(-2))) {
    FailWindowsOperation("_get_osfhandle()");
    return;
  }
  UniqueHandle memory_handle(reinterpret_cast<void*>(
      CreateFileMappingW(file_handle, nullptr, PAGE_READONLY, 0, 0, nullptr)));
  if (ABSL_PREDICT_FALSE(memory_handle == nullptr)) {
    FailWindowsOperation("CreateFileMappingW()");
    return;
  }
  void* const addr =
      MapViewOfFile(reinterpret_cast<HANDLE>(memory_handle.get()),
                    FILE_MAP_READ, IntCast<DWORD>(rounded_base_pos >> 32),
                    IntCast<DWORD>(rounded_base_pos & 0xffffffff),
                    IntCast<size_t>(rounded_length));
  if (ABSL_PREDICT_FALSE(addr == nullptr)) {
    FailWindowsOperation("MapViewOfFile()");
    return;
  }
#endif  // _WIN32

  // The `Chain` to read from was not known in `FdMMapReaderBase` constructor.
  // Set it now.
  ChainReader::Reset(riegeli::Maker(
      ExternalRef(riegeli::Maker<MMapBlock>(static_cast<const char*>(addr)),
                  absl::string_view(static_cast<const char*>(addr) + rounding,
                                    IntCast<size_t>(length)))));
  if (options.max_length() == std::nullopt) Seek(initial_pos);
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
      absl::ErrnoToStatus(error_number, absl::StrCat(operation, " failed")));
}

#ifdef _WIN32

bool FdMMapReaderBase::FailWindowsOperation(absl::string_view operation) {
  const DWORD error_number = GetLastError();
  RIEGELI_ASSERT_NE(error_number, 0)
      << "Failed precondition of FdMMapReaderBase::FailWindowsOperation(): "
         "zero error code";
  return Fail(WindowsErrorToStatus(IntCast<uint32_t>(error_number),
                                   absl::StrCat(operation, " failed")));
}

#endif  // _WIN32

absl::Status FdMMapReaderBase::AnnotateStatusImpl(absl::Status status) {
  return ChainReader::AnnotateStatusImpl(
      Annotate(status, absl::StrCat("reading ", filename())));
}

#ifndef _WIN32

void FdMMapReaderBase::SetReadAllHintImpl(bool read_all_hint) {
  ChainReader::SetReadAllHintImpl(read_all_hint);
  if (ABSL_PREDICT_FALSE(!ok())) return;
  const int src = SrcFd();
  FdSetReadAllHint(src, read_all_hint);
}

#endif  // !_WIN32

bool FdMMapReaderBase::SyncImpl(SyncType sync_type) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  const int src = SrcFd();
  if (base_pos_to_sync_ != std::nullopt) {
    if (ABSL_PREDICT_FALSE(
            fd_internal::LSeek(
                src, IntCast<fd_internal::Offset>(*base_pos_to_sync_ + pos()),
                SEEK_SET) < 0)) {
      return FailOperation(fd_internal::kLSeekFunctionName);
    }
  }
  return true;
}

std::unique_ptr<Reader> FdMMapReaderBase::NewReaderImpl(Position initial_pos) {
  if (ABSL_PREDICT_FALSE(!ok())) return nullptr;
  // `NewReaderImpl()` is thread-safe from this point.
  std::unique_ptr<FdMMapReader<UnownedFd>> reader =
      std::make_unique<FdMMapReader<UnownedFd>>(kClosed);
  reader->InitializeWithExistingData(UnownedFd(SrcFdHandle()),
                                     ChainReader::src());
  reader->Seek(initial_pos);
  return reader;
}

}  // namespace riegeli
