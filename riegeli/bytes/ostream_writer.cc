// Copyright 2019 Google LLC
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

#include "riegeli/bytes/ostream_writer.h"

#include <stddef.h>

#include <cerrno>
#include <ios>
#include <istream>
#include <limits>
#include <ostream>
#include <string>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/global.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/istream_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void OStreamWriterBase::Initialize(std::ostream* dest,
                                   absl::optional<Position> assumed_pos,
                                   bool assumed_append) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of OStreamWriter: null stream pointer";
  RIEGELI_ASSERT(supports_random_access_ == LazyBoolState::kUnknown)
      << "Failed precondition of OStreamWriterBase::Initialize(): "
         "supports_random_access_ not reset";
  RIEGELI_ASSERT(supports_read_mode_ == LazyBoolState::kUnknown)
      << "Failed precondition of OStreamWriterBase::Initialize(): "
         "supports_read_mode_ not reset";
  RIEGELI_ASSERT_EQ(random_access_status_, absl::OkStatus())
      << "Failed precondition of OStreamWriterBase::Initialize(): "
         "random_access_status_ not reset";
  RIEGELI_ASSERT_EQ(read_mode_status_, absl::OkStatus())
      << "Failed precondition of OStreamWriterBase::Initialize(): "
         "read_mode_status_ not reset";
  if (ABSL_PREDICT_FALSE(dest->fail())) {
    // Either constructing the stream failed or the stream was already in a
    // failed state. In any case `OStreamWriterBase` should fail.
    FailOperation("ostream::ostream()");
    return;
  }
  if (assumed_pos != absl::nullopt) {
    if (ABSL_PREDICT_FALSE(
            *assumed_pos >
            Position{std::numeric_limits<std::streamoff>::max()})) {
      FailOverflow();
      return;
    }
    set_start_pos(*assumed_pos);
    supports_random_access_ = LazyBoolState::kFalse;
    supports_read_mode_ = LazyBoolState::kFalse;
    random_access_status_ = Global([] {
      return absl::UnimplementedError(
          "OStreamWriterBase::Options::assumed_pos() excludes random access");
    });
    read_mode_status_ = random_access_status_;
  } else {
    errno = 0;
    const std::streamoff stream_pos = dest->tellp();
    if (stream_pos < 0) {
      // Random access is not supported. Assume 0 as the initial position.
      supports_random_access_ = LazyBoolState::kFalse;
      supports_read_mode_ = LazyBoolState::kFalse;
      random_access_status_ = FailedOperationStatus("ostream::tellp()");
      read_mode_status_ = random_access_status_;
      return;
    }
    set_start_pos(IntCast<Position>(stream_pos));
    if (assumed_append) {
      supports_random_access_ = LazyBoolState::kFalse;
      // `supports_read_mode_` is left as `LazyBoolState::kUnknown`.
      random_access_status_ = Global([] {
        return absl::UnimplementedError("Append mode excludes random access");
      });
    } else {
      // `std::ostream::tellp()` succeeded, and `std::ostream::seekp()` will be
      // checked later. `supports_random_access_` and `supports_read_mode_` are
      // left as `LazyBoolState::kUnknown`.
    }
  }
  BeginRun();
}

void OStreamWriterBase::Done() {
  BufferedWriter::Done();
  random_access_status_ = absl::OkStatus();
  read_mode_status_ = absl::OkStatus();
  associated_reader_.Reset();
}

inline absl::Status OStreamWriterBase::FailedOperationStatus(
    absl::string_view operation) {
  // There is no way to get details why a stream operation failed without
  // letting the stream throw exceptions. Hopefully low level failures have set
  // `errno` as a side effect.
  //
  // This requires resetting `errno` to 0 before the stream operation because
  // the operation may fail without setting `errno`.
  const int error_number = errno;
  const std::string message = absl::StrCat(operation, " failed");
  return error_number == 0 ? absl::UnknownError(message)
                           : absl::ErrnoToStatus(error_number, message);
}

bool OStreamWriterBase::FailOperation(absl::string_view operation) {
  return Fail(FailedOperationStatus(operation));
}

bool OStreamWriterBase::SupportsRandomAccess() {
  if (ABSL_PREDICT_TRUE(supports_random_access_ != LazyBoolState::kUnknown)) {
    return supports_random_access_ == LazyBoolState::kTrue;
  }
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  std::ostream& dest = *DestStream();
  errno = 0;
  dest.seekp(0, std::ios_base::end);
  if (dest.fail()) {
    // Not supported.
    supports_random_access_ = LazyBoolState::kFalse;
    random_access_status_ = FailedOperationStatus("ostream::seekp()");
    dest.clear(dest.rdstate() & ~std::ios_base::failbit);
    return false;
  }
  // Supported.
  dest.seekp(IntCast<std::streamoff>(start_pos()), std::ios_base::beg);
  if (ABSL_PREDICT_FALSE(dest.fail())) return FailOperation("ostream::seekp()");
  supports_random_access_ = LazyBoolState::kTrue;
  return true;
}

bool OStreamWriterBase::SupportsReadMode() {
  if (ABSL_PREDICT_TRUE(supports_read_mode_ != LazyBoolState::kUnknown)) {
    return supports_read_mode_ == LazyBoolState::kTrue;
  }
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  std::istream* const src = SrcStream();
  if (src == nullptr) {
    supports_read_mode_ = LazyBoolState::kFalse;
    read_mode_status_ = Global([] {
      return absl::UnimplementedError(
          "Read mode requires the static type of the destination "
          "deriving from std::istream");
    });
    return false;
  }
  errno = 0;
  const std::streamoff stream_pos = src->tellg();
  if (stream_pos < 0) {
    // Not supported.
    supports_read_mode_ = LazyBoolState::kFalse;
    read_mode_status_ = FailedOperationStatus("istream::tellg()");
    return false;
  }
  src->seekg(0, std::ios_base::end);
  if (src->fail()) {
    // Not supported.
    supports_read_mode_ = LazyBoolState::kFalse;
    read_mode_status_ = FailedOperationStatus("istream::seekg()");
    src->clear(src->rdstate() & ~std::ios_base::failbit);
    return false;
  }
  // Supported.
  supports_read_mode_ = LazyBoolState::kTrue;
  std::ostream& dest = *DestStream();
  dest.seekp(IntCast<std::streamoff>(start_pos()), std::ios_base::beg);
  if (ABSL_PREDICT_FALSE(dest.fail())) return FailOperation("ostream::seekp()");
  return true;
}

inline bool OStreamWriterBase::WriteMode() {
  if (ABSL_PREDICT_TRUE(!read_mode_)) return true;
  read_mode_ = false;
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  std::ostream& dest = *DestStream();
  errno = 0;
  dest.seekp(IntCast<std::streamoff>(start_pos()), std::ios_base::beg);
  if (ABSL_PREDICT_FALSE(dest.fail())) return FailOperation("ostream::seekp()");
  return true;
}

bool OStreamWriterBase::WriteInternal(absl::string_view src) {
  RIEGELI_ASSERT(!src.empty())
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "nothing to write";
  RIEGELI_ASSERT(ok())
      << "Failed precondition of BufferedWriter::WriteInternal(): " << status();
  if (ABSL_PREDICT_FALSE(!WriteMode())) return false;
  std::ostream& dest = *DestStream();
  if (ABSL_PREDICT_FALSE(src.size() >
                         Position{std::numeric_limits<std::streamoff>::max()} -
                             start_pos())) {
    return FailOverflow();
  }
  errno = 0;
  do {
    dest.write(src.data(), IntCast<std::streamsize>(src.size()));
    if (ABSL_PREDICT_FALSE(dest.fail())) {
      return FailOperation("ostream::write()");
    }
    move_start_pos(src.size());
    src.remove_prefix(src.size());
  } while (!src.empty());
  return true;
}

bool OStreamWriterBase::FlushBehindBuffer(absl::string_view src,
                                          FlushType flush_type) {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::FlushBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!WriteMode())) return false;
  return BufferedWriter::FlushBehindBuffer(src, flush_type);
}

bool OStreamWriterBase::SeekBehindBuffer(Position new_pos) {
  RIEGELI_ASSERT_NE(new_pos, pos())
      << "Failed precondition of BufferedWriter::SeekBehindBuffer(): "
         "position unchanged, use Seek() instead";
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::SeekBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!OStreamWriterBase::SupportsRandomAccess())) {
    if (ok()) Fail(random_access_status_);
    return false;
  }
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  read_mode_ = false;
  std::ostream& dest = *DestStream();
  errno = 0;
  if (new_pos > start_pos()) {
    // Seeking forwards.
    dest.seekp(0, std::ios_base::end);
    if (ABSL_PREDICT_FALSE(dest.fail())) {
      return FailOperation("ostream::seekp()");
    }
    const std::streamoff stream_size = dest.tellp();
    if (ABSL_PREDICT_FALSE(stream_size < 0)) {
      return FailOperation("ostream::tellp()");
    }
    if (ABSL_PREDICT_FALSE(new_pos > IntCast<Position>(stream_size))) {
      // Stream ends.
      set_start_pos(IntCast<Position>(stream_size));
      return false;
    }
  }
  dest.seekp(IntCast<std::streamoff>(new_pos), std::ios_base::beg);
  if (ABSL_PREDICT_FALSE(dest.fail())) return FailOperation("ostream::seekp()");
  set_start_pos(new_pos);
  return true;
}

absl::optional<Position> OStreamWriterBase::SizeBehindBuffer() {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::SizeBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!OStreamWriterBase::SupportsRandomAccess())) {
    if (ok()) Fail(random_access_status_);
    return absl::nullopt;
  }
  if (ABSL_PREDICT_FALSE(!ok())) return absl::nullopt;
  read_mode_ = false;
  std::ostream& dest = *DestStream();
  errno = 0;
  dest.seekp(0, std::ios_base::end);
  if (ABSL_PREDICT_FALSE(dest.fail())) {
    FailOperation("ostream::seekp()");
    return absl::nullopt;
  }
  const std::streamoff stream_size = dest.tellp();
  if (ABSL_PREDICT_FALSE(stream_size < 0)) {
    FailOperation("ostream::tellp()");
    return absl::nullopt;
  }
  dest.seekp(IntCast<std::streamoff>(start_pos()), std::ios_base::beg);
  if (ABSL_PREDICT_FALSE(dest.fail())) {
    FailOperation("ostream::seekp()");
    return absl::nullopt;
  }
  return IntCast<Position>(stream_size);
}

Reader* OStreamWriterBase::ReadModeBehindBuffer(Position initial_pos) {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::ReadModeBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!OStreamWriterBase::SupportsReadMode())) {
    if (ok()) Fail(read_mode_status_);
    return nullptr;
  }
  if (ABSL_PREDICT_FALSE(!ok())) return nullptr;
  std::istream& src = *SrcStream();
  IStreamReader<>* const reader = associated_reader_.ResetReader(
      &src, IStreamReaderBase::Options().set_buffer_options(buffer_options()));
  reader->Seek(initial_pos);
  read_mode_ = true;
  return reader;
}

}  // namespace riegeli
