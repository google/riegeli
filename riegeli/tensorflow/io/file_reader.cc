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

#include <cstring>
#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
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
  const ::tensorflow::Status status = src->Name(&filename);
  if (ABSL_PREDICT_FALSE(!status.ok())) {
    if (!::tensorflow::errors::IsUnimplemented(status)) {
      return FailOperation("RandomAccessFile::Name()", status);
    }
    return true;
  }
  return InitializeFilename(env, filename);
}

bool FileReaderBase::InitializeFilename(::tensorflow::Env* env,
                                        absl::string_view filename) {
  filename_.assign(filename.data(), filename.size());
  if (env == nullptr) env = ::tensorflow::Env::Default();
  const ::tensorflow::Status status =
      env->GetFileSystemForFile(filename_, &file_system_);
  if (ABSL_PREDICT_FALSE(!status.ok())) {
    return FailOperation("Env::GetFileSystemForFile()", status);
  }
  return true;
}

std::unique_ptr<::tensorflow::RandomAccessFile> FileReaderBase::OpenFile() {
  std::unique_ptr<::tensorflow::RandomAccessFile> src;
  const ::tensorflow::Status status =
      file_system_->NewRandomAccessFile(filename_, &src);
  if (ABSL_PREDICT_FALSE(!status.ok())) {
    FailOperation("FileSystem::NewRandomAccessFile()", status);
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

bool FileReaderBase::FailOperation(absl::string_view operation,
                                   const ::tensorflow::Status& status) {
  status_ = status;
  std::string message =
      absl::StrCat(operation, " failed: ", status_.ToString());
  if (!filename_.empty()) absl::StrAppend(&message, ", reading ", filename_);
  return Fail(message);
}

bool FileReaderBase::PullSlow() {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of Reader::PullSlow(): "
         "data available, use Pull() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  ::tensorflow::RandomAccessFile* const src = src_file();
  char* const ptr = buffer_.GetData();
  if (ABSL_PREDICT_FALSE(buffer_.size() >
                         std::numeric_limits<::tensorflow::uint64>::max() -
                             limit_pos_)) {
    return FailOverflow();
  }
  absl::string_view result;
  const ::tensorflow::Status status = src->Read(
      IntCast<::tensorflow::uint64>(limit_pos_), buffer_.size(), &result, ptr);
  RIEGELI_ASSERT_LE(result.size(), buffer_.size())
      << "RandomAccessFile::Read() read more than requested";
  start_ = result.data();
  cursor_ = start_;
  limit_ = start_ + result.size();
  limit_pos_ += result.size();
  if (ABSL_PREDICT_FALSE(!status.ok())) {
    if (ABSL_PREDICT_FALSE(!::tensorflow::errors::IsOutOfRange(status))) {
      return FailOperation("RandomAccessFile::Read()", status);
    }
    return !result.empty();
  }
  return true;
}

bool FileReaderBase::ReadSlow(char* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, available())
      << "Failed precondition of Reader::ReadSlow(char*): "
         "length too small, use Read(char*) instead";
  if (length - available() > buffer_.size()) {
    // If reading through buffer_ would need multiple Read() calls, it is faster
    // to copy current contents of buffer_ and read the remaining data directly
    // into dest.
    if (ABSL_PREDICT_FALSE(!healthy())) return false;
    ::tensorflow::RandomAccessFile* const src = src_file();
    const size_t available_length = available();
    if (available_length > 0) {  // memcpy(_, nullptr, 0) is undefined.
      std::memcpy(dest, cursor_, available_length);
      dest += available_length;
      length -= available_length;
    }
    start_ = nullptr;
    cursor_ = nullptr;
    limit_ = nullptr;
    if (ABSL_PREDICT_FALSE(length >
                           std::numeric_limits<::tensorflow::uint64>::max() -
                               limit_pos_)) {
      return FailOverflow();
    }
    absl::string_view result;
    const ::tensorflow::Status status = src->Read(
        IntCast<::tensorflow::uint64>(limit_pos_), length, &result, dest);
    RIEGELI_ASSERT_LE(result.size(), length)
        << "RandomAccessFile::Read() read more than requested";
    if (result.data() != dest) std::memcpy(dest, result.data(), result.size());
    limit_pos_ += result.size();
    if (ABSL_PREDICT_FALSE(!status.ok())) {
      if (ABSL_PREDICT_FALSE(!::tensorflow::errors::IsOutOfRange(status))) {
        return FailOperation("RandomAccessFile::Read()", status);
      }
      return false;
    }
    return true;
  }
  return Reader::ReadSlow(dest, length);
}

bool FileReaderBase::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos_)
      << "Failed precondition of Reader::SeekSlow(): "
         "position in the buffer, use Seek() instead";
  if (ABSL_PREDICT_FALSE(filename_.empty())) return Reader::SeekSlow(new_pos);
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  start_ = nullptr;
  cursor_ = nullptr;
  limit_ = nullptr;
  if (new_pos > limit_pos_) {
    // Seeking forwards.
    ::tensorflow::uint64 file_size;
    const ::tensorflow::Status status =
        file_system_->GetFileSize(filename_, &file_size);
    if (ABSL_PREDICT_FALSE(!status.ok())) {
      return FailOperation("FileSystem::GetFileSize()", status);
    }
    if (ABSL_PREDICT_FALSE(new_pos > file_size)) {
      // File ends.
      limit_pos_ = Position{file_size};
      return false;
    }
  }
  limit_pos_ = new_pos;
  PullSlow();
  return true;
}

bool FileReaderBase::Size(Position* size) {
  if (ABSL_PREDICT_FALSE(filename_.empty())) return Reader::Size(size);
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  ::tensorflow::uint64 file_size;
  const ::tensorflow::Status status =
      file_system_->GetFileSize(filename_, &file_size);
  if (ABSL_PREDICT_FALSE(!status.ok())) {
    return FailOperation("FileSystem::GetFileSize()", status);
  }
  *size = Position{file_size};
  return true;
}

template class FileReader<std::unique_ptr<::tensorflow::RandomAccessFile>>;
template class FileReader<::tensorflow::RandomAccessFile*>;

}  // namespace tensorflow
}  // namespace riegeli
