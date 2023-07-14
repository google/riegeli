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

#include "riegeli/tensorflow/io/file_reader.h"

#include <stddef.h>
#include <stdint.h>

#include <cstring>
#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"
#include "tensorflow/core/platform/env.h"
#include "tensorflow/core/platform/errors.h"
#include "tensorflow/core/platform/file_system.h"
#include "tensorflow/core/platform/status.h"
#include "tensorflow/core/public/version.h"

namespace riegeli {
namespace tensorflow {

bool FileReaderBase::InitializeFilename(::tensorflow::RandomAccessFile* src) {
  absl::string_view filename;
  {
    const ::tensorflow::Status status = src->Name(&filename);
    if (ABSL_PREDICT_FALSE(!status.ok())) {
      if (!::tensorflow::errors::IsUnimplemented(status)) {
        return FailOperation(status, "RandomAccessFile::Name()");
      }
      return true;
    }
  }
  return InitializeFilename(filename);
}

bool FileReaderBase::InitializeFilename(absl::string_view filename) {
  // TODO: When `absl::string_view` becomes C++17 `std::string_view`:
  // `filename_ = filename`
  filename_.assign(filename.data(), filename.size());
  {
    const ::tensorflow::Status status =
        env_->GetFileSystemForFile(filename_, &file_system_);
    if (ABSL_PREDICT_FALSE(!status.ok())) {
      return FailOperation(status, "Env::GetFileSystemForFile()");
    }
  }
  return true;
}

std::unique_ptr<::tensorflow::RandomAccessFile> FileReaderBase::OpenFile() {
  std::unique_ptr<::tensorflow::RandomAccessFile> src;
  {
    const ::tensorflow::Status status =
        file_system_->NewRandomAccessFile(filename_, &src);
    if (ABSL_PREDICT_FALSE(!status.ok())) {
      Reader::Reset(kClosed);
      FailOperation(status, "FileSystem::NewRandomAccessFile()");
      return nullptr;
    }
  }
  return src;
}

void FileReaderBase::InitializePos(Position initial_pos) {
  if (ABSL_PREDICT_FALSE(initial_pos > std::numeric_limits<uint64_t>::max())) {
    FailOverflow();
    return;
  }
  set_limit_pos(initial_pos);
  buffer_sizer_.BeginRun(limit_pos());
}

void FileReaderBase::Done() {
  Reader::Done();
  buffer_ = SizedSharedBuffer();
}

inline bool FileReaderBase::FailOperation(const ::tensorflow::Status& status,
                                          absl::string_view operation) {
  RIEGELI_ASSERT(!status.ok())
      << "Failed precondition of FileReaderBase::FailOperation(): "
         "status not failed";
  return Fail(
      Annotate(absl::Status(static_cast<absl::StatusCode>(status.code()),
#if TF_GRAPH_DEF_VERSION < 1467
                            status.error_message()),
#else
                            status.message()),
#endif
               absl::StrCat(operation, " failed")));
}

inline absl::Status FileReaderBase::NoRandomAccessStatus() {
  return absl::UnimplementedError(
      "A non-empty filename required for random access");
}

absl::Status FileReaderBase::AnnotateStatusImpl(absl::Status status) {
  if (!filename_.empty()) {
    status = Annotate(status, absl::StrCat("reading ", filename_));
  }
  return Reader::AnnotateStatusImpl(std::move(status));
}

inline void FileReaderBase::SyncBuffer() {
  buffer_.Clear();
  set_buffer();
}

void FileReaderBase::SetReadAllHintImpl(bool read_all_hint) {
  buffer_sizer_.set_read_all_hint(read_all_hint);
}

bool FileReaderBase::PullSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Reader::PullSlow(): "
         "enough data available, use Pull() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  ::tensorflow::RandomAccessFile* const src = SrcFile();
  const size_t available_length = available();
  const size_t buffer_length = buffer_sizer_.BufferLength(
      limit_pos(), min_length - available_length,
      SaturatingSub(recommended_length, available_length));
  if (ABSL_PREDICT_FALSE(buffer_length == 0)) return false;
  size_t cursor_index;
  absl::Span<char> flat_buffer;
  if (buffer_.empty()) {
    // Copy available data to `buffer_` so that newly read data will be adjacent
    // to available data.
    cursor_index = 0;
    flat_buffer = buffer_.AppendFixedBuffer(available_length + buffer_length);
    // `std::memcpy(_, nullptr, 0)` is undefined.
    if (available_length > 0) {
      std::memcpy(flat_buffer.data(), cursor(), available_length);
      flat_buffer.remove_prefix(available_length);
    }
  } else {
    cursor_index = start_to_cursor();
    flat_buffer = buffer_.AppendBufferIfExisting(buffer_length);
    if (flat_buffer.empty()) {
      // Not enough space in `buffer_`. Resize `buffer_`, keeping available
      // data.
      buffer_.RemovePrefix(cursor_index);
      buffer_.Shrink(available_length + buffer_length);
      cursor_index = 0;
      flat_buffer = buffer_.AppendFixedBuffer(buffer_length);
    }
  }
  // Read more data, preferably into `buffer_`.
  ReadToBuffer(cursor_index, src, flat_buffer);
  return available() >= min_length;
}

inline bool FileReaderBase::ReadToDest(size_t length,
                                       ::tensorflow::RandomAccessFile* src,
                                       char* dest) {
  if (ABSL_PREDICT_FALSE(limit_pos() >= std::numeric_limits<uint64_t>::max())) {
    return FailOverflow();
  }
  const size_t length_to_read =
      UnsignedMin(length, std::numeric_limits<uint64_t>::max() - limit_pos());
  absl::string_view result;
  const ::tensorflow::Status status =
      src->Read(IntCast<uint64_t>(limit_pos()), length_to_read, &result, dest);
  RIEGELI_ASSERT_LE(result.size(), length_to_read)
      << "RandomAccessFile::Read() read more than requested";
  if (result.data() != dest) std::memcpy(dest, result.data(), result.size());
  move_limit_pos(result.size());
  if (ABSL_PREDICT_FALSE(!status.ok())) {
    if (ABSL_PREDICT_FALSE(!::tensorflow::errors::IsOutOfRange(status))) {
      return FailOperation(status, "RandomAccessFile::Read()");
    }
    if (!growing_source_) set_exact_size(limit_pos());
    return false;
  }
  RIEGELI_ASSERT_EQ(result.size(), length_to_read)
      << "RandomAccessFile::Read() succeeded but read less than requested";
  if (ABSL_PREDICT_FALSE(result.size() < length)) {
    // `result.size() == length_to_read < length`, which implies that
    // `std::numeric_limits<uint64_t>::max()` was reached.
    RIEGELI_ASSERT_EQ(limit_pos(), std::numeric_limits<uint64_t>::max())
        << "Maximum position must have been reached";
    return FailOverflow();
  }
  return true;
}

inline bool FileReaderBase::ReadToBuffer(size_t cursor_index,
                                         ::tensorflow::RandomAccessFile* src,
                                         absl::Span<char> flat_buffer) {
  RIEGELI_ASSERT(flat_buffer.data() + flat_buffer.size() ==
                 buffer_.data() + buffer_.size())
      << "Failed precondition of FileReaderBase::ReadToBuffer(): "
         "flat_buffer not a suffix of buffer_";
  if (ABSL_PREDICT_FALSE(limit_pos() >= std::numeric_limits<uint64_t>::max())) {
    buffer_.RemoveSuffix(flat_buffer.size());
    set_buffer(buffer_.data(), buffer_.size(), cursor_index);
    return FailOverflow();
  }
  const size_t length_to_read = UnsignedMin(
      flat_buffer.size(), std::numeric_limits<uint64_t>::max() - limit_pos());
  absl::string_view result;
  const ::tensorflow::Status status =
      src->Read(IntCast<uint64_t>(limit_pos()), length_to_read, &result,
                flat_buffer.data());
  RIEGELI_ASSERT_LE(result.size(), length_to_read)
      << "RandomAccessFile::Read() read more than requested";
  if (result.data() == flat_buffer.data()) {
    buffer_.RemoveSuffix(flat_buffer.size() - result.size());
    set_buffer(buffer_.data(), buffer_.size(), cursor_index);
  } else if (buffer_.size() > cursor_index + flat_buffer.size()) {
    // Copy newly read data to `buffer_` so that they are adjacent to previously
    // available data.
    std::memcpy(flat_buffer.data(), result.data(), result.size());
    buffer_.RemoveSuffix(flat_buffer.size() - result.size());
    set_buffer(buffer_.data(), buffer_.size(), cursor_index);
  } else {
    buffer_.Clear();
    set_buffer(result.data(), result.size());
  }
  move_limit_pos(result.size());
  if (ABSL_PREDICT_FALSE(!status.ok())) {
    if (ABSL_PREDICT_FALSE(!::tensorflow::errors::IsOutOfRange(status))) {
      return FailOperation(status, "RandomAccessFile::Read()");
    }
    if (!growing_source_) set_exact_size(limit_pos());
    return false;
  }
  RIEGELI_ASSERT_EQ(result.size(), length_to_read)
      << "RandomAccessFile::Read() succeeded but read less than requested";
  if (ABSL_PREDICT_FALSE(result.size() < flat_buffer.size())) {
    // `result.size() == length_to_read < flat_buffer.size()`, which implies
    // that `std::numeric_limits<uint64_t>::max()` was reached.
    RIEGELI_ASSERT_EQ(limit_pos(), std::numeric_limits<uint64_t>::max())
        << "Maximum position must have been reached";
    return FailOverflow();
  }
  return true;
}

bool FileReaderBase::ReadSlow(size_t length, char* dest) {
  RIEGELI_ASSERT_LT(available(), length)
      << "Failed precondition of Reader::ReadSlow(char*): "
         "enough data available, use Read(char*) instead";
  if (length >= buffer_sizer_.BufferLength(pos())) {
    // Read directly to `dest`.
    const size_t available_length = available();
    // `std::memcpy(_, nullptr, 0)` is undefined.
    if (available_length > 0) {
      std::memcpy(dest, cursor(), available_length);
      dest += available_length;
      length -= available_length;
    }
    SyncBuffer();
    if (ABSL_PREDICT_FALSE(!ok())) return false;
    ::tensorflow::RandomAccessFile* const src = SrcFile();
    size_t length_to_read = length;
    if (exact_size() != absl::nullopt) {
      if (ABSL_PREDICT_FALSE(limit_pos() >= *exact_size())) return false;
      length_to_read = UnsignedMin(length_to_read, *exact_size() - limit_pos());
    }
    if (ABSL_PREDICT_FALSE(!ReadToDest(length_to_read, src, dest))) {
      return false;
    }
    return length_to_read >= length;
  }
  return Reader::ReadSlow(length, dest);
}

bool FileReaderBase::ReadSlow(size_t length, Chain& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::ReadSlow(Chain&): "
         "enough data available, use Read(Chain&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadSlow(Chain&): "
         "Chain size overflow";
  ::tensorflow::RandomAccessFile* const src = SrcFile();
  bool enough_read = true;
  while (length > available()) {
    const size_t available_length = available();
    if (ABSL_PREDICT_FALSE(!ok())) {
      // Read as much as is available.
      enough_read = false;
      length = available_length;
      break;
    }
    const size_t buffer_length =
        buffer_sizer_.BufferLength(limit_pos(), 1, length - available_length);
    size_t cursor_index;
    absl::Span<char> flat_buffer;
    if (buffer_.empty()) {
      // Do not extend `buffer_` if available data are outside of `buffer_`,
      // because available data would be lost.
      dest.Append(absl::string_view(cursor(), available_length));
      length -= available_length;
      if (ABSL_PREDICT_FALSE(buffer_length == 0)) {
        set_buffer();
        return false;
      }
      cursor_index = 0;
      flat_buffer = buffer_.AppendFixedBuffer(buffer_length);
    } else {
      cursor_index = start_to_cursor();
      flat_buffer = buffer_.AppendBufferIfExisting(buffer_length);
      if (flat_buffer.empty()) {
        // Not enough space in `buffer_`. Append available data to `dest` and
        // make a new buffer.
        dest.Append(std::move(buffer_).Substr(cursor(), available_length));
        length -= available_length;
        buffer_.ClearAndShrink(buffer_length);
        if (ABSL_PREDICT_FALSE(buffer_length == 0)) {
          set_buffer();
          return false;
        }
        cursor_index = 0;
        flat_buffer = buffer_.AppendFixedBuffer(buffer_length);
      }
    }
    // Read more data, preferably into `buffer_`.
    if (ABSL_PREDICT_FALSE(!ReadToBuffer(cursor_index, src, flat_buffer))) {
      // Read as much as is available.
      enough_read = available() >= length;
      if (ABSL_PREDICT_FALSE(!enough_read)) length = available();
      break;
    }
  }
  if (buffer_.empty()) {
    dest.Append(absl::string_view(cursor(), length));
  } else {
    dest.Append(buffer_.Substr(cursor(), length));
  }
  move_cursor(length);
  return enough_read;
}

bool FileReaderBase::ReadSlow(size_t length, absl::Cord& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::ReadSlow(Cord&): "
         "enough data available, use Read(Cord&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadSlow(Cord&): "
         "Cord size overflow";
  ::tensorflow::RandomAccessFile* const src = SrcFile();
  bool enough_read = true;
  while (length > available()) {
    const size_t available_length = available();
    if (ABSL_PREDICT_FALSE(!ok())) {
      // Read as much as is available.
      enough_read = false;
      length = available_length;
      break;
    }
    const size_t buffer_length =
        buffer_sizer_.BufferLength(limit_pos(), 1, length - available_length);
    size_t cursor_index;
    absl::Span<char> flat_buffer;
    if (buffer_.empty()) {
      // Do not extend `buffer_` if available data are outside of `buffer_`,
      // because available data would be lost.
      dest.Append(absl::string_view(cursor(), available_length));
      length -= available_length;
      if (ABSL_PREDICT_FALSE(buffer_length == 0)) {
        set_buffer();
        return false;
      }
      cursor_index = 0;
      flat_buffer = buffer_.AppendFixedBuffer(buffer_length);
    } else {
      cursor_index = start_to_cursor();
      flat_buffer = buffer_.AppendBufferIfExisting(buffer_length);
      if (flat_buffer.empty()) {
        // Not enough space in `buffer_`. Append available data to `dest` and
        // make a new buffer.
        std::move(buffer_).Substr(cursor(), available_length).AppendTo(dest);
        length -= available_length;
        buffer_.ClearAndShrink(buffer_length);
        if (ABSL_PREDICT_FALSE(buffer_length == 0)) {
          set_buffer();
          return false;
        }
        cursor_index = 0;
        flat_buffer = buffer_.AppendFixedBuffer(buffer_length);
      }
    }
    // Read more data, preferably into `buffer_`.
    if (ABSL_PREDICT_FALSE(!ReadToBuffer(cursor_index, src, flat_buffer))) {
      // Read as much as is available.
      enough_read = available() >= length;
      if (ABSL_PREDICT_FALSE(!enough_read)) length = available();
      break;
    }
  }
  if (buffer_.empty()) {
    dest.Append(absl::string_view(cursor(), length));
  } else {
    buffer_.Substr(cursor(), length).AppendTo(dest);
  }
  move_cursor(length);
  return enough_read;
}

bool FileReaderBase::CopySlow(Position length, Writer& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::CopySlow(Writer&): "
         "enough data available, use Copy(Writer&) instead";
  ::tensorflow::RandomAccessFile* const src = SrcFile();
  bool enough_read = true;
  while (length > available()) {
    const size_t available_length = available();
    if (ABSL_PREDICT_FALSE(!ok())) {
      // Copy as much as is available.
      length = available_length;
      enough_read = false;
      break;
    }
    const bool read_directly = length >= buffer_sizer_.BufferLength(pos());
    if (read_directly) {
      if (buffer_.empty() || available_length <= kMaxBytesToCopy ||
          dest.PrefersCopying()) {
        if (ABSL_PREDICT_FALSE(
                !dest.Write(absl::string_view(cursor(), available_length)))) {
          move_cursor(available_length);
          return false;
        }
        length -= available_length;
        SyncBuffer();
        return CopyUsingPush(length, src, dest);
      }
      // It is better to write available data from `buffer_` as a `Chain` before
      // reading directly to `dest`. Before that, `buffer_` might need to be
      // filled more to avoid attaching a wasteful `Chain`.
    }
    const size_t buffer_length =
        buffer_sizer_.BufferLength(limit_pos(), 1, length - available_length);
    size_t cursor_index;
    absl::Span<char> flat_buffer;
    if (buffer_.empty()) {
      // Do not extend `buffer_` if available data are outside of `buffer_`,
      // because available data would be lost.
      if (ABSL_PREDICT_FALSE(
              !dest.Write(absl::string_view(cursor(), available_length)))) {
        move_cursor(available_length);
        return false;
      }
      length -= available_length;
      if (ABSL_PREDICT_FALSE(buffer_length == 0)) {
        set_buffer();
        return false;
      }
      cursor_index = 0;
      flat_buffer = buffer_.AppendFixedBuffer(buffer_length);
    } else {
      cursor_index = start_to_cursor();
      flat_buffer = buffer_.AppendBufferIfExisting(buffer_length);
      if (flat_buffer.empty()) {
        // Not enough space in `buffer_`. Append available data to `dest` and
        // make a new buffer.
        if (available_length > 0) {
          const bool write_ok =
              available_length <= kMaxBytesToCopy || dest.PrefersCopying()
                  ? dest.Write(absl::string_view(cursor(), available_length))
                  : dest.Write(Chain(
                        std::move(buffer_).Substr(cursor(), available_length)));
          if (ABSL_PREDICT_FALSE(!write_ok)) {
            buffer_.ClearAndShrink(buffer_length);
            set_buffer();
            return false;
          }
          length -= available_length;
        }
        buffer_.ClearAndShrink(buffer_length);
        if (ABSL_PREDICT_FALSE(buffer_length == 0)) {
          set_buffer();
          return false;
        }
        if (read_directly) {
          set_buffer();
          return CopyUsingPush(length, src, dest);
        }
        cursor_index = 0;
        flat_buffer = buffer_.AppendFixedBuffer(buffer_length);
      }
    }
    // Read more data, preferably into `buffer_`.
    if (ABSL_PREDICT_FALSE(!ReadToBuffer(cursor_index, src, flat_buffer))) {
      // Copy as much as is available.
      enough_read = available() >= length;
      if (ABSL_PREDICT_FALSE(!enough_read)) length = available();
      break;
    }
  }
  const bool write_ok =
      buffer_.empty() || IntCast<size_t>(length) <= kMaxBytesToCopy ||
              dest.PrefersCopying()
          ? dest.Write(absl::string_view(cursor(), IntCast<size_t>(length)))
          : dest.Write(
                Chain(buffer_.Substr(cursor(), IntCast<size_t>(length))));
  move_cursor(IntCast<size_t>(length));
  return write_ok && enough_read;
}

inline bool FileReaderBase::CopyUsingPush(Position length,
                                          ::tensorflow::RandomAccessFile* src,
                                          Writer& dest) {
  RIEGELI_ASSERT_GT(length, 0u)
      << "Failed precondition of FileReaderBase::CopyUsingPush(): "
         "nothing to copy";
  do {
    size_t length_to_read = SaturatingIntCast<size_t>(length);
    if (exact_size() != absl::nullopt) {
      if (ABSL_PREDICT_FALSE(limit_pos() >= *exact_size())) return false;
      length_to_read = UnsignedMin(length_to_read, *exact_size() - limit_pos());
    }
    if (ABSL_PREDICT_FALSE(!dest.Push(1, length_to_read))) return false;
    const size_t length_to_copy = UnsignedMin(length_to_read, dest.available());
    const Position pos_before = limit_pos();
    const bool read_ok = ReadToDest(length_to_copy, src, dest.cursor());
    const Position length_read = limit_pos() - pos_before;
    dest.move_cursor(IntCast<size_t>(length_read));
    if (ABSL_PREDICT_FALSE(!read_ok)) return false;
    length -= length_read;
  } while (length > 0);
  return true;
}

bool FileReaderBase::CopySlow(size_t length, BackwardWriter& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::CopySlow(BackwardWriter&): "
         "enough data available, use Copy(BackwardWriter&) instead";
  if (length <= available() && buffer_.empty()) {
    // Avoid writing an `absl::string_view` if available data are in `buffer_`,
    // because in this case it is better to write a `Chain`.
    const absl::string_view data(cursor(), length);
    move_cursor(length);
    return dest.Write(data);
  }
  if (length <= kMaxBytesToCopy) {
    if (ABSL_PREDICT_FALSE(!dest.Push(length))) return false;
    dest.move_cursor(length);
    if (ABSL_PREDICT_FALSE(!ReadSlow(length, dest.cursor()))) {
      dest.set_cursor(dest.cursor() + length);
      return false;
    }
    return true;
  }
  Chain data;
  if (ABSL_PREDICT_FALSE(!ReadSlow(length, data))) return false;
  return dest.Write(std::move(data));
}

bool FileReaderBase::ReadSomeDirectlySlow(
    size_t max_length, absl::FunctionRef<char*(size_t&)> get_dest) {
  RIEGELI_ASSERT_GT(max_length, 0u)
      << "Failed precondition of Reader::ReadSomeDirectlySlow(): "
         "nothing to read, use ReadSomeDirectly() instead";
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of Reader::ReadSomeDirectlySlow(): "
         "some data available, use ReadSomeDirectly() instead";
  if (max_length >= buffer_sizer_.BufferLength(limit_pos())) {
    // Read directly to `get_dest(max_length)`.
    SyncBuffer();
    if (ABSL_PREDICT_FALSE(!ok())) return false;
    ::tensorflow::RandomAccessFile* const src = SrcFile();
    if (exact_size() != absl::nullopt) {
      if (ABSL_PREDICT_FALSE(limit_pos() >= *exact_size())) return false;
      max_length = UnsignedMin(max_length, *exact_size() - limit_pos());
    }
    char* const dest = get_dest(max_length);
    if (ABSL_PREDICT_FALSE(max_length == 0)) return true;
    ReadToDest(max_length, src, dest);
    return true;
  }
  PullSlow(1, max_length);
  return false;
}

bool FileReaderBase::SyncImpl(SyncType sync_type) {
  const Position new_pos = pos();
  buffer_sizer_.EndRun(new_pos);
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  SyncBuffer();
  set_limit_pos(new_pos);
  buffer_sizer_.BeginRun(limit_pos());
  return true;
}

bool FileReaderBase::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of Reader::SeekSlow(): "
         "position in the buffer, use Seek() instead";
  if (ABSL_PREDICT_FALSE(!FileReaderBase::SupportsRandomAccess())) {
    if (ABSL_PREDICT_FALSE(new_pos < start_pos())) {
      if (ok()) Fail(NoRandomAccessStatus());
      return false;
    }
    return Reader::SeekSlow(new_pos);
  }
  buffer_sizer_.EndRun(pos());
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  SyncBuffer();
  if (new_pos > limit_pos()) {
    // Seeking forwards.
    uint64_t file_size;
    if (exact_size() != absl::nullopt) {
      file_size = IntCast<uint64_t>(*exact_size());
    } else {
      {
        const ::tensorflow::Status status =
            file_system_->GetFileSize(filename_, &file_size);
        if (ABSL_PREDICT_FALSE(!status.ok())) {
          return FailOperation(status, "FileSystem::GetFileSize()");
        }
      }
      if (!growing_source_) set_exact_size(Position{file_size});
    }
    if (ABSL_PREDICT_FALSE(new_pos > file_size)) {
      // File ends.
      set_limit_pos(Position{file_size});
      buffer_sizer_.BeginRun(limit_pos());
      return false;
    }
  }
  set_limit_pos(new_pos);
  buffer_sizer_.BeginRun(limit_pos());
  return true;
}

absl::optional<Position> FileReaderBase::SizeImpl() {
  if (ABSL_PREDICT_FALSE(!ok())) return absl::nullopt;
  if (exact_size() != absl::nullopt) return *exact_size();
  if (ABSL_PREDICT_FALSE(!FileReaderBase::SupportsRandomAccess())) {
    Fail(NoRandomAccessStatus());
    return absl::nullopt;
  }
  uint64_t file_size;
  {
    const ::tensorflow::Status status =
        file_system_->GetFileSize(filename_, &file_size);
    if (ABSL_PREDICT_FALSE(!status.ok())) {
      FailOperation(status, "FileSystem::GetFileSize()");
      return absl::nullopt;
    }
  }
  if (!growing_source_) set_exact_size(Position{file_size});
  return Position{file_size};
}

std::unique_ptr<Reader> FileReaderBase::NewReaderImpl(Position initial_pos) {
  if (ABSL_PREDICT_FALSE(!FileReaderBase::SupportsRandomAccess())) {
    if (ok()) Fail(NoRandomAccessStatus());
    return nullptr;
  }
  if (ABSL_PREDICT_FALSE(!ok())) return nullptr;
  // `NewReaderImpl()` is thread-safe from this point.
  ::tensorflow::RandomAccessFile* const src = SrcFile();
  std::unique_ptr<FileReader<::tensorflow::RandomAccessFile*>> reader =
      std::make_unique<FileReader<::tensorflow::RandomAccessFile*>>(
          src, FileReaderBase::Options()
                   .set_env(env_)
                   .set_initial_pos(initial_pos)
                   .set_growing_source(growing_source_)
                   .set_buffer_options(buffer_sizer_.buffer_options()));
  reader->set_exact_size(exact_size());
  if (initial_pos >= start_pos() && initial_pos < limit_pos()) {
    // Share `buffer_` with `*reader`.
    reader->buffer_ = buffer_;
    reader->set_buffer(start(), start_to_limit(),
                       IntCast<size_t>(initial_pos - start_pos()));
    reader->set_limit_pos(limit_pos());
  }
  return reader;
}

}  // namespace tensorflow
}  // namespace riegeli
