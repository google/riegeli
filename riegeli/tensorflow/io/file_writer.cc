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

#include "riegeli/tensorflow/io/file_writer.h"

#include <stddef.h>

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
#include "riegeli/base/base.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/memory.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/writer.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/platform/env.h"
#include "tensorflow/core/platform/file_system.h"
#include "tensorflow/core/platform/status.h"
#include "tensorflow/core/platform/types.h"

namespace riegeli {
namespace tensorflow {

bool FileWriterBase::InitializeFilename(::tensorflow::WritableFile* dest) {
  absl::string_view filename;
  {
    const ::tensorflow::Status status = dest->Name(&filename);
    if (ABSL_PREDICT_FALSE(!status.ok())) {
      if (!::tensorflow::errors::IsUnimplemented(status)) {
        return FailOperation(status, "WritableFile::Name()");
      }
      return true;
    }
  }
  return InitializeFilename(filename, env_);
}

bool FileWriterBase::InitializeFilename(absl::string_view filename,
                                        ::tensorflow::Env* env) {
  // TODO: When `absl::string_view` becomes C++17 `std::string_view`:
  // `filename_ = filename`
  filename_.assign(filename.data(), filename.size());
  {
    const ::tensorflow::Status status =
        env->GetFileSystemForFile(filename_, &file_system_);
    if (ABSL_PREDICT_FALSE(!status.ok())) {
      return FailOperation(status, "Env::GetFileSystemForFile()");
    }
  }
  return true;
}

std::unique_ptr<::tensorflow::WritableFile> FileWriterBase::OpenFile(
    bool append) {
  std::unique_ptr<::tensorflow::WritableFile> dest;
  {
    const ::tensorflow::Status status =
        append ? file_system_->NewAppendableFile(filename_, &dest)
               : file_system_->NewWritableFile(filename_, &dest);
    if (ABSL_PREDICT_FALSE(!status.ok())) {
      FailOperation(
          status, append ? absl::string_view("FileSystem::NewAppendableFile()")
                         : absl::string_view("FileSystem::NewWritableFile()"));
      return nullptr;
    }
  }
  return dest;
}

void FileWriterBase::InitializePos(::tensorflow::WritableFile* dest) {
  ::tensorflow::int64 file_pos;
  {
    const ::tensorflow::Status status = dest->Tell(&file_pos);
    if (ABSL_PREDICT_FALSE(!status.ok())) {
      FailOperation(status, "WritableFile::Tell()");
      return;
    }
  }
  set_start_pos(IntCast<Position>(file_pos));
}

void FileWriterBase::Done() {
  SyncBuffer();
  Writer::Done();
  buffer_ = Buffer();
}

bool FileWriterBase::FailOperation(const ::tensorflow::Status& status,
                                   absl::string_view operation) {
  RIEGELI_ASSERT(!status.ok())
      << "Failed precondition of FileWriterBase::FailOperation(): "
         "status not failed";
  return Fail(
      Annotate(absl::Status(static_cast<absl::StatusCode>(status.code()),
                            status.error_message()),
               absl::StrCat(operation, " failed")));
}

void FileWriterBase::DefaultAnnotateStatus() {
  RIEGELI_ASSERT(!not_failed())
      << "Failed precondition of Object::DefaultAnnotateStatus(): "
         "Object not failed";
  if (!filename_.empty()) AnnotateStatus(absl::StrCat("writing ", filename_));
  Writer::DefaultAnnotateStatus();
}

bool FileWriterBase::SyncBuffer() {
  const absl::string_view data(start(), start_to_cursor());
  set_buffer();
  if (data.empty()) return true;
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  return WriteInternal(data);
}

inline size_t FileWriterBase::LengthToWriteDirectly() const {
  // Write directly at least `buffer_size_` of data. Even if the buffer is
  // partially full, this ensures that at least every other write has length at
  // least `buffer_size_`.
  return buffer_size_;
}

bool FileWriterBase::PushSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Writer::PushSlow(): "
         "enough space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(!SyncBuffer())) return false;
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(min_length >
                         std::numeric_limits<Position>::max() - start_pos())) {
    return FailOverflow();
  }
  const size_t buffer_length = UnsignedMax(buffer_size_, min_length);
  buffer_.Reset(buffer_length);
  set_buffer(buffer_.data(),
             UnsignedMin(buffer_.capacity(),
                         SaturatingAdd(buffer_length, buffer_length),
                         std::numeric_limits<Position>::max() - start_pos()));
  return true;
}

bool FileWriterBase::WriteInternal(absl::string_view src) {
  RIEGELI_ASSERT(!src.empty())
      << "Failed precondition of FileWriterBase::WriteInternal(): "
         "nothing to write";
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of FileWriterBase::WriteInternal(): " << status();
  ::tensorflow::WritableFile* const dest = dest_file();
  if (ABSL_PREDICT_FALSE(src.size() >
                         std::numeric_limits<Position>::max() - start_pos())) {
    return FailOverflow();
  }
  {
    const ::tensorflow::Status status = dest->Append(src);
    if (ABSL_PREDICT_FALSE(!status.ok())) {
      return FailOperation(status, "WritableFile::Append(string_view)");
    }
  }
  move_start_pos(src.size());
  return true;
}

bool FileWriterBase::WriteSlow(absl::string_view src) {
  RIEGELI_ASSERT_LT(available(), src.size())
      << "Failed precondition of Writer::WriteSlow(string_view): "
         "enough space available, use Write(string_view) instead";
  if (src.size() >= LengthToWriteDirectly()) {
    if (ABSL_PREDICT_FALSE(!SyncBuffer())) return false;
    if (ABSL_PREDICT_FALSE(!healthy())) return false;
    return WriteInternal(src);
  }
  return Writer::WriteSlow(src);
}

bool FileWriterBase::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!SyncBuffer())) return false;
  return healthy();
}

absl::optional<Position> FileWriterBase::SizeImpl() {
  if (ABSL_PREDICT_FALSE(filename_.empty())) {
    return Writer::SizeImpl();  // Fail.
  }
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

}  // namespace tensorflow
}  // namespace riegeli
