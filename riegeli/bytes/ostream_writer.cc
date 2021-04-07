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
#include <limits>
#include <ostream>
#include <string>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/errno_mapping.h"

namespace riegeli {

bool OstreamWriterBase::FailOperation(absl::string_view operation) {
  RIEGELI_ASSERT(is_open())
      << "Failed precondition of OstreamWriterBase::FailOperation(): "
         "Object closed";
  // There is no way to get details why a stream operation failed without
  // letting the stream throw exceptions. Hopefully low level failures have set
  // `errno` as a side effect.
  //
  // This requires resetting `errno` to 0 before the stream operation because
  // the operation may fail without setting `errno`.
  const int error_number = errno;
  const std::string message = absl::StrCat(operation, " failed");
  return Fail(error_number == 0
                  ? absl::UnknownError(message)
                  : ErrnoToCanonicalStatus(error_number, message));
}

void OstreamWriterBase::Initialize(std::ostream* dest,
                                   absl::optional<Position> assumed_pos) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of OstreamWriter: null stream pointer";
  if (ABSL_PREDICT_FALSE(dest->fail())) {
    // Either constructing the stream failed or the stream was already in a
    // failed state. In any case `OstreamWriterBase` should fail.
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
  } else {
    const std::streamoff stream_pos = dest->tellp();
    if (ABSL_PREDICT_FALSE(stream_pos < 0)) {
      FailOperation("ostream::tellp()");
      return;
    }
    set_start_pos(IntCast<Position>(stream_pos));
  }
}

void OstreamWriterBase::Done() {
  PushInternal();
  BufferedWriter::Done();
}

bool OstreamWriterBase::WriteInternal(absl::string_view src) {
  RIEGELI_ASSERT(!src.empty())
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "nothing to write";
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of BufferedWriter::WriteInternal(): " << status();
  RIEGELI_ASSERT_EQ(written_to_buffer(), 0u)
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "buffer not empty";
  std::ostream& dest = *dest_stream();
  if (ABSL_PREDICT_FALSE(src.size() >
                         Position{std::numeric_limits<std::streamoff>::max()} -
                             start_pos())) {
    return FailOverflow();
  }
  errno = 0;
  do {
    const size_t length_to_write = UnsignedMin(
        src.size(), size_t{std::numeric_limits<std::streamsize>::max()});
    dest.write(src.data(), IntCast<std::streamsize>(length_to_write));
    if (ABSL_PREDICT_FALSE(dest.fail())) {
      return FailOperation("ostream::write()");
    }
    move_start_pos(length_to_write);
    src.remove_prefix(length_to_write);
  } while (!src.empty());
  return true;
}

bool OstreamWriterBase::Flush(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!PushInternal())) return false;
  switch (flush_type) {
    case FlushType::kFromObject:
      return true;
    case FlushType::kFromProcess:
    case FlushType::kFromMachine: {
      std::ostream& dest = *dest_stream();
      errno = 0;
      dest.flush();
      if (ABSL_PREDICT_FALSE(dest.fail())) {
        return FailOperation("ostream::flush()");
      }
      return true;
    }
  }
  RIEGELI_ASSERT_UNREACHABLE()
      << "Unknown flush type: " << static_cast<int>(flush_type);
}

bool OstreamWriterBase::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > pos())
      << "Failed precondition of Writer::SeekSlow(): "
         "position in the buffer, use Seek() instead";
  if (ABSL_PREDICT_FALSE(!random_access_)) {
    return Fail(
        absl::UnimplementedError("OstreamWriterBase::Seek() not supported"));
  }
  if (ABSL_PREDICT_FALSE(!PushInternal())) return false;
  RIEGELI_ASSERT_EQ(written_to_buffer(), 0u)
      << "BufferedWriter::PushInternal() did not empty the buffer";
  std::ostream& dest = *dest_stream();
  errno = 0;
  if (new_pos >= start_pos()) {
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
  if (ABSL_PREDICT_FALSE(dest.fail())) {
    return FailOperation("ostream::seekp()");
  }
  set_start_pos(new_pos);
  return true;
}

absl::optional<Position> OstreamWriterBase::Size() {
  if (ABSL_PREDICT_FALSE(!healthy())) return absl::nullopt;
  if (ABSL_PREDICT_FALSE(!random_access_)) {
    Fail(absl::UnimplementedError("OstreamWriterBase::Size() not supported"));
    return absl::nullopt;
  }
  std::ostream& dest = *dest_stream();
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
  return UnsignedMax(IntCast<Position>(stream_size), pos());
}

}  // namespace riegeli
