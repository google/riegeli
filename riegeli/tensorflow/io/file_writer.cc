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

#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/writer.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/status.h"
#include "tensorflow/core/platform/env.h"
#include "tensorflow/core/platform/file_system.h"
#include "tensorflow/core/platform/types.h"

namespace riegeli {
namespace tensorflow {

void FileWriterBase::InitializeFilename(::tensorflow::WritableFile* dest) {
  absl::string_view filename;
  const ::tensorflow::Status status = dest->Name(&filename);
  if (ABSL_PREDICT_FALSE(!status.ok())) {
    if (!::tensorflow::errors::IsUnimplemented(status)) {
      FailOperation("WritableFile::Name()", status);
    }
    return;
  }
  filename_.assign(filename.data(), filename.size());
}

std::unique_ptr<::tensorflow::WritableFile> FileWriterBase::OpenFile(
    ::tensorflow::Env* env, absl::string_view filename, bool append) {
  filename_.assign(filename.data(), filename.size());
  if (env == nullptr) env = ::tensorflow::Env::Default();
  std::unique_ptr<::tensorflow::WritableFile> dest;
  const ::tensorflow::Status status =
      append ? env->NewAppendableFile(filename_, &dest)
             : env->NewWritableFile(filename_, &dest);
  if (ABSL_PREDICT_FALSE(!status.ok())) {
    FailOperation(append ? absl::string_view("Env::NewAppendableFile()")
                         : absl::string_view("Env::NewWritableFile()"),
                  status);
    return nullptr;
  }
  return dest;
}

void FileWriterBase::InitializePos(::tensorflow::WritableFile* dest) {
  ::tensorflow::int64 file_pos;
  const ::tensorflow::Status status = dest->Tell(&file_pos);
  if (ABSL_PREDICT_FALSE(!status.ok())) {
    FailOperation("WritableFile::Tell()", status);
    return;
  }
  start_pos_ = IntCast<Position>(file_pos);
}

bool FileWriterBase::FailOperation(absl::string_view operation,
                                   const ::tensorflow::Status& status) {
  status_ = status;
  std::string message =
      absl::StrCat(operation, " failed: ", status_.ToString());
  if (!filename_.empty()) absl::StrAppend(&message, ", writing ", filename_);
  return Fail(message);
}

bool FileWriterBase::PushSlow() {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of Writer::PushSlow(): "
         "space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(!PushInternal())) return false;
  if (start_ == nullptr) {
    start_ = buffer_.GetData();
    if (ABSL_PREDICT_FALSE(buffer_.size() >
                           std::numeric_limits<Position>::max() - start_pos_)) {
      return FailOverflow();
    }
    cursor_ = start_;
    limit_ = start_ + buffer_.size();
  }
  return true;
}

bool FileWriterBase::PushInternal() {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  const size_t buffered_length = written_to_buffer();
  if (buffered_length == 0) return true;
  cursor_ = start_;
  return WriteInternal(absl::string_view(start_, buffered_length));
}

bool FileWriterBase::WriteSlow(absl::string_view src) {
  RIEGELI_ASSERT_GT(src.size(), available())
      << "Failed precondition of Writer::WriteSlow(string_view): "
         "length too small, use Write(string_view) instead";
  if (written_to_buffer() == 0 ? src.size() >= buffer_.size()
                               : src.size() - available() >= buffer_.size()) {
    // If writing through the buffer would need multiple WriteInternal() calls,
    // it is faster to push current contents of the buffer and write the
    // remaining data directly from src.
    if (ABSL_PREDICT_FALSE(!PushInternal())) return false;
    return WriteInternal(src);
  }
  return Writer::WriteSlow(src);
}

bool FileWriterBase::WriteInternal(absl::string_view src) {
  RIEGELI_ASSERT(!src.empty())
      << "Failed precondition of FileWriterBase::WriteInternal(): "
         "nothing to write";
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of FileWriterBase::WriteInternal(): "
      << message();
  RIEGELI_ASSERT_EQ(written_to_buffer(), 0u)
      << "Failed precondition of FileWriterBase::WriteInternal(): "
         "buffer not empty";
  ::tensorflow::WritableFile* const dest = dest_file();
  if (ABSL_PREDICT_FALSE(src.size() >
                         std::numeric_limits<Position>::max() - start_pos_)) {
    return FailOverflow();
  }
  const ::tensorflow::Status status = dest->Append(src);
  if (ABSL_PREDICT_FALSE(!status.ok())) {
    return FailOperation("WritableFile::Append(string_view)", status);
  }
  start_pos_ += src.size();
  return true;
}

bool FileWriterBase::Flush(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!PushInternal())) return false;
  ::tensorflow::WritableFile* const dest = dest_file();
  switch (flush_type) {
    case FlushType::kFromObject:
      return true;
    case FlushType::kFromProcess: {
      const ::tensorflow::Status status = dest->Flush();
      if (ABSL_PREDICT_FALSE(!status.ok())) {
        return FailOperation("WritableFile::Flush()", status);
      }
      return true;
    }
    case FlushType::kFromMachine: {
      const ::tensorflow::Status status = dest->Sync();
      if (ABSL_PREDICT_FALSE(!status.ok())) {
        return FailOperation("WritableFile::Sync()", status);
      }
      return true;
    }
  }
  RIEGELI_ASSERT_UNREACHABLE()
      << "Unknown flush type: " << static_cast<int>(flush_type);
}

template class FileWriter<std::unique_ptr<::tensorflow::WritableFile>>;
template class FileWriter<::tensorflow::WritableFile*>;

}  // namespace tensorflow
}  // namespace riegeli
