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
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/writer.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/status.h"
#include "tensorflow/core/platform/env.h"
#include "tensorflow/core/platform/file_system.h"
#include "tensorflow/core/platform/types.h"

namespace riegeli {
namespace tensorflow {

bool FileReaderBase::InitializeFilename(::tensorflow::Env* env,
                                        ::tensorflow::RandomAccessFile* src) {
  absl::string_view filename;
  const ::tensorflow::Status name_status = src->Name(&filename);
  if (ABSL_PREDICT_FALSE(!name_status.ok())) {
    if (!::tensorflow::errors::IsUnimplemented(name_status)) {
      return FailOperation(name_status, "RandomAccessFile::Name()");
    }
    return true;
  }
  return InitializeFilename(env, filename);
}

bool FileReaderBase::InitializeFilename(::tensorflow::Env* env,
                                        absl::string_view filename) {
  // TODO: When absl::string_view becomes C++17 std::string_view:
  // filename_ = filename;
  filename_.assign(filename.data(), filename.size());
  if (env == nullptr) env = ::tensorflow::Env::Default();
  const ::tensorflow::Status get_file_system_status =
      env->GetFileSystemForFile(filename_, &file_system_);
  if (ABSL_PREDICT_FALSE(!get_file_system_status.ok())) {
    return FailOperation(get_file_system_status, "Env::GetFileSystemForFile()");
  }
  return true;
}

std::unique_ptr<::tensorflow::RandomAccessFile> FileReaderBase::OpenFile() {
  std::unique_ptr<::tensorflow::RandomAccessFile> src;
  const ::tensorflow::Status new_file_status =
      file_system_->NewRandomAccessFile(filename_, &src);
  if (ABSL_PREDICT_FALSE(!new_file_status.ok())) {
    FailOperation(new_file_status, "FileSystem::NewRandomAccessFile()");
    return nullptr;
  }
  return src;
}

void FileReaderBase::InitializePos(Position initial_pos) {
  if (ABSL_PREDICT_FALSE(initial_pos >
                         std::numeric_limits<::tensorflow::uint64>::max())) {
    FailOverflow();
    return;
  }
  limit_pos_ = initial_pos;
}

bool FileReaderBase::FailOperation(const ::tensorflow::Status& status,
                                   absl::string_view operation) {
  RIEGELI_ASSERT(!status.ok())
      << "Failed precondition of FileReaderBase::FailOperation(): "
         "status not failed";
  std::string context = absl::StrCat(operation, " failed");
  if (!filename_.empty()) absl::StrAppend(&context, " reading ", filename_);
  return Fail(Annotate(
      Status(static_cast<StatusCode>(status.code()), status.error_message()),
      context));
}

inline size_t FileReaderBase::BufferLength(size_t min_length) const {
  RIEGELI_ASSERT_GT(buffer_size_, 0u)
      << "Failed invariant of FileReaderBase: no buffer size specified";
  return UnsignedMax(min_length, buffer_size_);
}

inline size_t FileReaderBase::LengthToReadDirectly() const {
  // Read directly if reading through buffer_ would need more than one read,
  // or if buffer_ would be full.
  return SaturatingAdd(available(), BufferLength());
}

bool FileReaderBase::PullSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_GT(min_length, available())
      << "Failed precondition of Reader::PullSlow(): "
         "length too small, use Pull() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  ::tensorflow::RandomAccessFile* const src = src_file();
  const size_t buffer_length = BufferLength(min_length);
  if (available() > 0 && buffer_.empty()) {
    // Copy available data to buffer_ so that newly read data will be adjacent
    // to available data.
    absl::Span<char> flat_buffer = buffer_.AppendFixedBuffer(
        available(), SaturatingAdd(available(), buffer_length));
    std::memcpy(flat_buffer.data(), cursor_, available());
    start_ = flat_buffer.data();
    cursor_ = start_;
    limit_ = start_ + flat_buffer.size();
  }
  absl::Span<char> flat_buffer = buffer_.AppendBuffer(0);
  if (flat_buffer.size() < min_length - available()) {
    // Make a new buffer, preserving available data.
    const size_t available_length = available();
    buffer_.RemoveSuffix(flat_buffer.size());
    buffer_.RemovePrefix(buffer_.size() - available_length);
    flat_buffer = buffer_.AppendBuffer(buffer_length);
    start_ = buffer_.data();
    cursor_ = start_;
    limit_ = flat_buffer.data();
  } else if (flat_buffer.size() == buffer_.size()) {
    // buffer_ was empty.
    start_ = buffer_.data();
    cursor_ = start_;
    limit_ = start_;
  }
  // Read more data, preferably into buffer_.
  if (ABSL_PREDICT_FALSE(!ReadToBuffer(flat_buffer, src))) {
    return available() >= min_length;
  }
  return true;
}

bool FileReaderBase::ReadSlow(char* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, available())
      << "Failed precondition of Reader::ReadSlow(char*): "
         "length too small, use Read(char*) instead";
  if (length >= LengthToReadDirectly()) {
    if (ABSL_PREDICT_FALSE(!healthy())) return false;
    ::tensorflow::RandomAccessFile* const src = src_file();
    const size_t available_length = available();
    if (available_length > 0) {  // memcpy(_, nullptr, 0) is undefined.
      std::memcpy(dest, cursor_, available_length);
      dest += available_length;
      length -= available_length;
    }
    size_t length_read;
    return ReadToDest(dest, length, src, &length_read);
  }
  return Reader::ReadSlow(dest, length);
}

bool FileReaderBase::ReadSlow(Chain* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::ReadSlow(Chain*): "
         "length too small, use Read(Chain*) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Reader::ReadSlow(Chain*): "
         "Chain size overflow";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  ::tensorflow::RandomAccessFile* const src = src_file();
  bool ok = true;
  while (length > available()) {
    if (available() == 0 && length >= LengthToReadDirectly()) {
      const absl::Span<char> flat_buffer = dest->AppendFixedBuffer(length);
      size_t length_read;
      if (ABSL_PREDICT_FALSE(!ReadToDest(flat_buffer.data(), flat_buffer.size(),
                                         src, &length_read))) {
        dest->RemoveSuffix(flat_buffer.size() - length_read);
        return false;
      }
      return true;
    }
    absl::Span<char> flat_buffer;
    if (available() == 0 || !buffer_.empty()) {
      // Do not extend buffer_ if available data are outside of buffer_, because
      // available data would be lost.
      flat_buffer = buffer_.AppendBuffer(0);
    }
    if (flat_buffer.empty()) {
      // Append available data to dest and make a new buffer.
      const size_t available_length = available();
      if (buffer_.empty()) {
        dest->Append(absl::string_view(cursor_, available_length));
      } else {
        buffer_.AppendSubstrTo(absl::string_view(cursor_, available_length),
                               dest);
        buffer_.Clear();
      }
      length -= available_length;
      flat_buffer = buffer_.AppendBuffer(BufferLength());
      start_ = flat_buffer.data();
      cursor_ = start_;
      limit_ = start_;
    } else if (flat_buffer.size() == buffer_.size()) {
      // buffer_ was empty.
      start_ = buffer_.data();
      cursor_ = start_;
      limit_ = start_;
    }
    // Read more data, preferably into buffer_.
    if (ABSL_PREDICT_FALSE(!ReadToBuffer(flat_buffer, src))) {
      if (length > available()) {
        length = available();
        ok = false;
      }
      break;
    }
  }
  RIEGELI_ASSERT_LE(length, available())
      << "Bug in FileReaderBase::ReadSlow(Chain*): "
         "remaining length larger than available data";
  if (buffer_.empty()) {
    dest->Append(absl::string_view(cursor_, length));
  } else {
    buffer_.AppendSubstrTo(absl::string_view(cursor_, length), dest);
  }
  cursor_ += length;
  return ok;
}

bool FileReaderBase::CopyToSlow(Writer* dest, Position length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::CopyToSlow(Writer*): "
         "length too small, use CopyTo(Writer*) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  ::tensorflow::RandomAccessFile* const src = src_file();
  bool read_ok = true;
  while (length > available()) {
    absl::Span<char> flat_buffer;
    if (available() == 0 || !buffer_.empty()) {
      // Do not extend buffer_ if available data are outside of buffer_, because
      // available data would be lost.
      flat_buffer = buffer_.AppendBuffer(0);
    }
    if (flat_buffer.empty()) {
      // Write available data to dest and make a new buffer.
      const size_t available_length = available();
      if (available_length > 0) {
        bool write_ok;
        if (buffer_.empty()) {
          write_ok = dest->Write(absl::string_view(cursor_, available_length));
        } else if (available_length == buffer_.size()) {
          write_ok = dest->Write(Chain(buffer_));
        } else {
          Chain data;
          buffer_.AppendSubstrTo(absl::string_view(cursor_, available_length),
                                 &data, available_length);
          write_ok = dest->Write(std::move(data));
        }
        if (ABSL_PREDICT_FALSE(!write_ok)) {
          cursor_ = limit_;
          return false;
        }
        length -= available_length;
      }
      buffer_.Clear();
      flat_buffer = buffer_.AppendBuffer(BufferLength());
      start_ = flat_buffer.data();
      cursor_ = start_;
      limit_ = start_;
    } else if (flat_buffer.size() == buffer_.size()) {
      // buffer_ was empty.
      start_ = buffer_.data();
      cursor_ = start_;
      limit_ = start_;
    }
    // Read more data, preferably into buffer_.
    if (ABSL_PREDICT_FALSE(!ReadToBuffer(flat_buffer, src))) {
      if (length > available()) {
        length = available();
        read_ok = false;
      }
      break;
    }
  }
  RIEGELI_ASSERT_LE(length, available())
      << "Bug in FileReaderBase::CopyToSlow(Writer*): "
         "remaining length larger than available data";
  bool write_ok = true;
  if (length > 0) {
    if (buffer_.empty()) {
      write_ok = dest->Write(absl::string_view(cursor_, length));
    } else if (length == buffer_.size()) {
      write_ok = dest->Write(Chain(buffer_));
    } else {
      Chain data;
      buffer_.AppendSubstrTo(absl::string_view(cursor_, length), &data, length);
      write_ok = dest->Write(std::move(data));
    }
    cursor_ += length;
  }
  return write_ok && read_ok;
}

bool FileReaderBase::CopyToSlow(BackwardWriter* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::CopyToSlow(BackwardWriter*): "
         "length too small, use CopyTo(BackwardWriter*) instead";
  if (length <= available() && buffer_.empty()) {
    // Avoid writing a string_view if available data are in buffer_, because in
    // this case it is better to write a Chain.
    const absl::string_view data(cursor_, length);
    cursor_ += length;
    return dest->Write(data);
  }
  if (length <= kMaxBytesToCopy) {
    if (ABSL_PREDICT_FALSE(!dest->Push(length))) return false;
    dest->set_cursor(dest->cursor() - length);
    if (ABSL_PREDICT_FALSE(!ReadSlow(dest->cursor(), length))) {
      dest->set_cursor(dest->cursor() + length);
      return false;
    }
    return true;
  }
  Chain data;
  if (ABSL_PREDICT_FALSE(!ReadSlow(&data, length))) return false;
  return dest->Write(std::move(data));
}

inline bool FileReaderBase::ReadToDest(char* dest, size_t length,
                                       ::tensorflow::RandomAccessFile* src,
                                       size_t* length_read) {
  ClearBuffer();
  if (ABSL_PREDICT_FALSE(length >
                         std::numeric_limits<::tensorflow::uint64>::max() -
                             limit_pos_)) {
    *length_read = 0;
    return FailOverflow();
  }
  absl::string_view result;
  const ::tensorflow::Status read_status = src->Read(
      IntCast<::tensorflow::uint64>(limit_pos_), length, &result, dest);
  RIEGELI_ASSERT_LE(result.size(), length)
      << "RandomAccessFile::Read() read more than requested";
  if (result.data() != dest) std::memcpy(dest, result.data(), result.size());
  limit_pos_ += result.size();
  *length_read = result.size();
  if (ABSL_PREDICT_FALSE(!read_status.ok())) {
    if (ABSL_PREDICT_FALSE(!::tensorflow::errors::IsOutOfRange(read_status))) {
      return FailOperation(read_status, "RandomAccessFile::Read()");
    }
    return false;
  }
  return true;
}

inline bool FileReaderBase::ReadToBuffer(absl::Span<char> flat_buffer,
                                         ::tensorflow::RandomAccessFile* src) {
  RIEGELI_ASSERT(start_ == buffer_.data())
      << "Failed precondition of FileReaderBase::ReadToBuffer(): "
         "start_ does not point to buffer_";
  RIEGELI_ASSERT(limit_ == flat_buffer.data())
      << "Failed precondition of FileReaderBase::ReadToBuffer(): "
         "limit_ does not point to flat_buffer";
  if (ABSL_PREDICT_FALSE(flat_buffer.size() >
                         std::numeric_limits<::tensorflow::uint64>::max() -
                             limit_pos_)) {
    return FailOverflow();
  }
  absl::string_view result;
  const ::tensorflow::Status read_status =
      src->Read(IntCast<::tensorflow::uint64>(limit_pos_), flat_buffer.size(),
                &result, flat_buffer.data());
  RIEGELI_ASSERT_LE(result.size(), flat_buffer.size())
      << "RandomAccessFile::Read() read more than requested";
  if (result.data() != flat_buffer.data()) {
    if (available() > 0) {
      // Copy newly read data to buffer_ so that they are adjacent to previously
      // available data.
      std::memcpy(flat_buffer.data(), result.data(), result.size());
    } else {
      buffer_.Clear();
      start_ = result.data();
      cursor_ = start_;
      limit_ = start_;
    }
  }
  limit_ += result.size();
  limit_pos_ += result.size();
  if (ABSL_PREDICT_FALSE(!read_status.ok())) {
    if (!buffer_.empty()) {
      buffer_.RemoveSuffix(flat_buffer.size() - result.size());
    }
    if (ABSL_PREDICT_FALSE(!::tensorflow::errors::IsOutOfRange(read_status))) {
      return FailOperation(read_status, "RandomAccessFile::Read()");
    }
    return false;
  }
  return true;
}

bool FileReaderBase::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos_)
      << "Failed precondition of Reader::SeekSlow(): "
         "position in the buffer, use Seek() instead";
  if (ABSL_PREDICT_FALSE(filename_.empty())) return Reader::SeekSlow(new_pos);
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  ClearBuffer();
  if (new_pos > limit_pos_) {
    // Seeking forwards.
    ::tensorflow::uint64 file_size;
    const ::tensorflow::Status get_file_size_status =
        file_system_->GetFileSize(filename_, &file_size);
    if (ABSL_PREDICT_FALSE(!get_file_size_status.ok())) {
      return FailOperation(get_file_size_status, "FileSystem::GetFileSize()");
    }
    if (ABSL_PREDICT_FALSE(new_pos > file_size)) {
      // File ends.
      limit_pos_ = Position{file_size};
      return false;
    }
  }
  limit_pos_ = new_pos;
  return true;
}

bool FileReaderBase::Size(Position* size) {
  if (ABSL_PREDICT_FALSE(filename_.empty())) return Reader::Size(size);
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  ::tensorflow::uint64 file_size;
  const ::tensorflow::Status get_file_size_status =
      file_system_->GetFileSize(filename_, &file_size);
  if (ABSL_PREDICT_FALSE(!get_file_size_status.ok())) {
    return FailOperation(get_file_size_status, "FileSystem::GetFileSize()");
  }
  *size = Position{file_size};
  return true;
}

void FileReaderBase::ClearBuffer() {
  buffer_.Clear();
  start_ = nullptr;
  cursor_ = nullptr;
  limit_ = nullptr;
}

template class FileReader<std::unique_ptr<::tensorflow::RandomAccessFile>>;
template class FileReader<::tensorflow::RandomAccessFile*>;

}  // namespace tensorflow
}  // namespace riegeli
