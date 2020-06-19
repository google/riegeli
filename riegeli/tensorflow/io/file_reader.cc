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

#include <cstring>
#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "riegeli/base/base.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/status.h"
#include "tensorflow/core/platform/env.h"
#include "tensorflow/core/platform/file_system.h"
#include "tensorflow/core/platform/types.h"

namespace riegeli {
namespace tensorflow {

bool FileReaderBase::InitializeFilename(::tensorflow::RandomAccessFile* src,
                                        ::tensorflow::Env* env) {
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
  return InitializeFilename(filename, env);
}

bool FileReaderBase::InitializeFilename(absl::string_view filename,
                                        ::tensorflow::Env* env) {
  // TODO: When `absl::string_view` becomes C++17 `std::string_view`:
  // filename_ = filename;
  filename_.assign(filename.data(), filename.size());
  if (env == nullptr) env = ::tensorflow::Env::Default();
  {
    const ::tensorflow::Status status =
        env->GetFileSystemForFile(filename_, &file_system_);
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
      FailOperation(status, "FileSystem::NewRandomAccessFile()");
      return nullptr;
    }
  }
  return src;
}

void FileReaderBase::InitializePos(Position initial_pos) {
  if (ABSL_PREDICT_FALSE(initial_pos >
                         std::numeric_limits<::tensorflow::uint64>::max())) {
    FailOverflow();
    return;
  }
  set_limit_pos(initial_pos);
}

bool FileReaderBase::FailOperation(const ::tensorflow::Status& status,
                                   absl::string_view operation) {
  RIEGELI_ASSERT(!status.ok())
      << "Failed precondition of FileReaderBase::FailOperation(): "
         "status not failed";
  RIEGELI_ASSERT(!closed())
      << "Failed precondition of FileReaderBase::FailOperation(): "
         "Object closed";
  return Fail(
      Annotate(absl::Status(static_cast<absl::StatusCode>(status.code()),
                            status.error_message()),
               absl::StrCat(operation, " failed")));
}

bool FileReaderBase::Fail(absl::Status status) {
  RIEGELI_ASSERT(!status.ok())
      << "Failed precondition of Object::Fail(): status not failed";
  return Reader::Fail(
      filename_.empty()
          ? std::move(status)
          : Annotate(status, absl::StrCat("reading ", filename_)));
}

inline size_t FileReaderBase::LengthToReadDirectly() const {
  // Read directly if reading through `buffer_` would need more than one read,
  // or if `buffer_` would be full.
  return SaturatingAdd(available(), buffer_size_);
}

bool FileReaderBase::PullSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_GT(min_length, available())
      << "Failed precondition of Reader::PullSlow(): "
         "length too small, use Pull() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  ::tensorflow::RandomAccessFile* const src = src_file();
  size_t buffer_length =
      UnsignedMax(buffer_size_, min_length, recommended_length);
  const size_t available_length = available();
  size_t cursor_index;
  absl::Span<char> flat_buffer;
  if (buffer_.empty()) {
    // Copy available data to `buffer_` so that newly read data will be adjacent
    // to available data.
    cursor_index = 0;
    flat_buffer = buffer_.AppendFixedBuffer(buffer_length);
    if (
        // `std::memcpy(_, nullptr, 0)` is undefined.
        available() > 0) {
      std::memcpy(flat_buffer.data(), cursor(), available());
      flat_buffer.remove_prefix(available());
    }
  } else {
    cursor_index = read_from_buffer();
    flat_buffer = buffer_.AppendBuffer(0, 0, buffer_length);
    if (flat_buffer.size() < min_length - available_length) {
      // Resize `buffer_`, keeping available data.
      buffer_.RemoveSuffix(flat_buffer.size());
      buffer_.RemovePrefix(cursor_index);
      cursor_index = 0;
      flat_buffer = buffer_.AppendFixedBuffer(buffer_length - available_length);
    }
  }
  // Read more data, preferably into `buffer_`.
  if (ABSL_PREDICT_FALSE(!ReadToBuffer(cursor_index, src, flat_buffer))) {
    return available() >= min_length;
  }
  return true;
}

bool FileReaderBase::ReadSlow(size_t length, char* dest) {
  RIEGELI_ASSERT_GT(length, available())
      << "Failed precondition of Reader::ReadSlow(char*): "
         "length too small, use Read(char*) instead";
  if (length >= LengthToReadDirectly()) {
    ::tensorflow::RandomAccessFile* const src = src_file();
    const size_t available_length = available();
    if (
        // `std::memcpy(_, nullptr, 0)` is undefined.
        available_length > 0) {
      std::memcpy(dest, cursor(), available_length);
      dest += available_length;
      length -= available_length;
    }
    if (ABSL_PREDICT_FALSE(!healthy())) {
      ClearBuffer();
      return false;
    }
    size_t length_read;
    return ReadToDest(length, src, dest, length_read);
  }
  return Reader::ReadSlow(length, dest);
}

bool FileReaderBase::ReadSlow(size_t length, Chain& dest) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::ReadSlow(Chain&): "
         "length too small, use Read(Chain&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadSlow(Chain&): "
         "Chain size overflow";
  ::tensorflow::RandomAccessFile* const src = src_file();
  bool enough_read = true;
  bool ok = healthy();
  while (length > available()) {
    if (ABSL_PREDICT_FALSE(!ok)) {
      // Read as much as is available.
      length = available();
      enough_read = false;
      break;
    }
    if (available() == 0 && length >= LengthToReadDirectly()) {
      const absl::Span<char> flat_buffer = dest.AppendFixedBuffer(length);
      size_t length_read;
      if (ABSL_PREDICT_FALSE(!ReadToDest(flat_buffer.size(), src,
                                         flat_buffer.data(), length_read))) {
        dest.RemoveSuffix(flat_buffer.size() - length_read);
        return false;
      }
      return true;
    }
    size_t cursor_index;
    absl::Span<char> flat_buffer;
    if (buffer_.empty()) {
      // Do not extend `buffer_` if available data are outside of `buffer_`,
      // because available data would be lost.
      length -= available();
      dest.Append(absl::string_view(cursor(), available()));
      cursor_index = 0;
      flat_buffer = buffer_.AppendFixedBuffer(buffer_size_);
    } else {
      cursor_index = read_from_buffer();
      flat_buffer = buffer_.AppendBuffer(0, 0, buffer_size_);
      if (flat_buffer.empty()) {
        // Append available data to `*dest` and make a new buffer.
        length -= available();
        buffer_.AppendSubstrTo(absl::string_view(cursor(), available()), dest);
        buffer_.Clear();
        cursor_index = 0;
        flat_buffer = buffer_.AppendFixedBuffer(buffer_size_);
      }
    }
    // Read more data, preferably into `buffer_`.
    ok = ReadToBuffer(cursor_index, src, flat_buffer);
  }
  if (buffer_.empty()) {
    dest.Append(absl::string_view(cursor(), length));
  } else {
    buffer_.AppendSubstrTo(absl::string_view(cursor(), length), dest);
  }
  move_cursor(length);
  return enough_read;
}

bool FileReaderBase::ReadSlow(size_t length, absl::Cord& dest) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::ReadSlow(Cord&): "
         "length too small, use Read(Cord&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadSlow(Cord&): "
         "Cord size overflow";
  ::tensorflow::RandomAccessFile* const src = src_file();
  bool enough_read = true;
  bool ok = healthy();
  while (length > available()) {
    if (ABSL_PREDICT_FALSE(!ok)) {
      // Read as much as is available.
      length = available();
      enough_read = false;
      break;
    }
    if (available() == 0 && length >= LengthToReadDirectly()) {
      Buffer flat_buffer(length);
      char* const ptr = flat_buffer.GetData();
      size_t length_read;
      ok = ReadToDest(length, src, ptr, length_read);
      dest.Append(
          BufferToCord(absl::string_view(ptr, length_read), &flat_buffer));
      return ok;
    }
    size_t cursor_index;
    absl::Span<char> flat_buffer;
    if (buffer_.empty()) {
      // Do not extend `buffer_` if available data are outside of `buffer_`,
      // because available data would be lost.
      length -= available();
      dest.Append(absl::string_view(cursor(), available()));
      cursor_index = 0;
      flat_buffer = buffer_.AppendFixedBuffer(buffer_size_);
    } else {
      cursor_index = read_from_buffer();
      flat_buffer = buffer_.AppendBuffer(0, 0, buffer_size_);
      if (flat_buffer.empty()) {
        // Append available data to `*dest` and make a new buffer.
        length -= available();
        buffer_.AppendSubstrTo(absl::string_view(cursor(), available()), dest);
        buffer_.Clear();
        cursor_index = 0;
        flat_buffer = buffer_.AppendFixedBuffer(buffer_size_);
      }
    }
    // Read more data, preferably into `buffer_`.
    ok = ReadToBuffer(cursor_index, src, flat_buffer);
  }
  if (buffer_.empty()) {
    dest.Append(absl::string_view(cursor(), length));
  } else {
    buffer_.AppendSubstrTo(absl::string_view(cursor(), length), dest);
  }
  move_cursor(length);
  return enough_read;
}

bool FileReaderBase::CopyToSlow(Position length, Writer& dest) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::CopyToSlow(Writer&): "
         "length too small, use CopyTo(Writer&) instead";
  ::tensorflow::RandomAccessFile* const src = src_file();
  bool enough_read = true;
  bool read_ok = healthy();
  while (length > available()) {
    if (ABSL_PREDICT_FALSE(!read_ok)) {
      // Copy as much as is available.
      length = available();
      enough_read = false;
      break;
    }
    size_t cursor_index;
    absl::Span<char> flat_buffer;
    if (buffer_.empty()) {
      // Do not extend `buffer_` if available data are outside of `buffer_`,
      // because available data would be lost.
      length -= available();
      if (ABSL_PREDICT_FALSE(
              !dest.Write(absl::string_view(cursor(), available())))) {
        move_cursor(available());
        return false;
      }
      cursor_index = 0;
      flat_buffer = buffer_.AppendFixedBuffer(buffer_size_);
    } else {
      cursor_index = read_from_buffer();
      flat_buffer = buffer_.AppendBuffer(0, 0, buffer_size_);
      if (flat_buffer.empty()) {
        // Append available data to `dest` and make a new buffer.
        if (available() > 0) {
          length -= available();
          bool write_ok;
          if (dest.PrefersCopying()) {
            write_ok = dest.Write(absl::string_view(cursor(), available()));
          } else {
            Chain data;
            buffer_.AppendSubstrTo(absl::string_view(cursor(), available()),
                                   data,
                                   Chain::Options().set_size_hint(available()));
            write_ok = dest.Write(std::move(data));
          }
          if (ABSL_PREDICT_FALSE(!write_ok)) {
            move_cursor(available());
            return false;
          }
        }
        buffer_.Clear();
        cursor_index = 0;
        flat_buffer = buffer_.AppendFixedBuffer(buffer_size_);
      }
    }
    // Read more data, preferably into `buffer_`.
    read_ok = ReadToBuffer(cursor_index, src, flat_buffer);
  }
  bool write_ok = true;
  if (length > 0) {
    if (buffer_.empty() || dest.PrefersCopying()) {
      write_ok =
          dest.Write(absl::string_view(cursor(), IntCast<size_t>(length)));
    } else {
      Chain data;
      buffer_.AppendSubstrTo(
          absl::string_view(cursor(), IntCast<size_t>(length)), data,
          Chain::Options().set_size_hint(IntCast<size_t>(length)));
      write_ok = dest.Write(std::move(data));
    }
    move_cursor(IntCast<size_t>(length));
  }
  return write_ok && enough_read;
}

bool FileReaderBase::CopyToSlow(size_t length, BackwardWriter& dest) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::CopyToSlow(BackwardWriter&): "
         "length too small, use CopyTo(BackwardWriter&) instead";
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

inline bool FileReaderBase::ReadToDest(size_t length,
                                       ::tensorflow::RandomAccessFile* src,
                                       char* dest, size_t& length_read) {
  ClearBuffer();
  if (ABSL_PREDICT_FALSE(length >
                         std::numeric_limits<::tensorflow::uint64>::max() -
                             limit_pos())) {
    length_read = 0;
    return FailOverflow();
  }
  absl::string_view result;
  const ::tensorflow::Status status = src->Read(
      IntCast<::tensorflow::uint64>(limit_pos()), length, &result, dest);
  RIEGELI_ASSERT_LE(result.size(), length)
      << "RandomAccessFile::Read() read more than requested";
  if (result.data() != dest) std::memcpy(dest, result.data(), result.size());
  move_limit_pos(result.size());
  length_read = result.size();
  if (ABSL_PREDICT_FALSE(!status.ok())) {
    if (ABSL_PREDICT_FALSE(!::tensorflow::errors::IsOutOfRange(status))) {
      return FailOperation(status, "RandomAccessFile::Read()");
    }
    return false;
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
  if (ABSL_PREDICT_FALSE(flat_buffer.size() >
                         std::numeric_limits<::tensorflow::uint64>::max() -
                             limit_pos())) {
    buffer_.RemoveSuffix(flat_buffer.size());
    set_buffer(buffer_.data(), buffer_.size(), cursor_index);
    return FailOverflow();
  }
  absl::string_view result;
  const ::tensorflow::Status status =
      src->Read(IntCast<::tensorflow::uint64>(limit_pos()), flat_buffer.size(),
                &result, flat_buffer.data());
  RIEGELI_ASSERT_LE(result.size(), flat_buffer.size())
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
    return false;
  }
  return true;
}

bool FileReaderBase::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of Reader::SeekSlow(): "
         "position in the buffer, use Seek() instead";
  if (ABSL_PREDICT_FALSE(filename_.empty())) return Reader::SeekSlow(new_pos);
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  ClearBuffer();
  if (new_pos > limit_pos()) {
    // Seeking forwards.
    ::tensorflow::uint64 file_size;
    {
      const ::tensorflow::Status status =
          file_system_->GetFileSize(filename_, &file_size);
      if (ABSL_PREDICT_FALSE(!status.ok())) {
        return FailOperation(status, "FileSystem::GetFileSize()");
      }
    }
    if (ABSL_PREDICT_FALSE(new_pos > file_size)) {
      // File ends.
      set_limit_pos(Position{file_size});
      return false;
    }
  }
  set_limit_pos(new_pos);
  return true;
}

absl::optional<Position> FileReaderBase::Size() {
  if (ABSL_PREDICT_FALSE(filename_.empty())) return Reader::Size();
  if (ABSL_PREDICT_FALSE(!healthy())) return absl::nullopt;
  ::tensorflow::uint64 file_size;
  {
    const ::tensorflow::Status status =
        file_system_->GetFileSize(filename_, &file_size);
    if (ABSL_PREDICT_FALSE(!status.ok())) {
      FailOperation(status, "FileSystem::GetFileSize()");
      return absl::nullopt;
    }
  }
  return Position{file_size};
}

void FileReaderBase::ClearBuffer() {
  buffer_.Clear();
  set_buffer();
}

}  // namespace tensorflow
}  // namespace riegeli
