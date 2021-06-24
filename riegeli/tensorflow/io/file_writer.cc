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

void FileWriterBase::InitializeFilename(::tensorflow::WritableFile* dest) {
  absl::string_view filename;
  {
    const ::tensorflow::Status status = dest->Name(&filename);
    if (ABSL_PREDICT_FALSE(!status.ok())) {
      if (!::tensorflow::errors::IsUnimplemented(status)) {
        FailOperation(status, "WritableFile::Name()");
      }
      return;
    }
  }
  // TODO: When `absl::string_view` becomes C++17 `std::string_view`:
  // `filename_ = filename`
  filename_.assign(filename.data(), filename.size());
}

std::unique_ptr<::tensorflow::WritableFile> FileWriterBase::OpenFile(
    ::tensorflow::Env* env, absl::string_view filename, bool append) {
  // TODO: When `absl::string_view` becomes C++17 `std::string_view`:
  // `filename_ = filename`
  filename_.assign(filename.data(), filename.size());
  if (env == nullptr) env = ::tensorflow::Env::Default();
  std::unique_ptr<::tensorflow::WritableFile> dest;
  {
    const ::tensorflow::Status status =
        append ? env->NewAppendableFile(filename_, &dest)
               : env->NewWritableFile(filename_, &dest);
    if (ABSL_PREDICT_FALSE(!status.ok())) {
      FailOperation(status, append
                                ? absl::string_view("Env::NewAppendableFile()")
                                : absl::string_view("Env::NewWritableFile()"));
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
  buffer_ = Buffer();
  Writer::Done();
}

bool FileWriterBase::FailOperation(const ::tensorflow::Status& status,
                                   absl::string_view operation) {
  RIEGELI_ASSERT(!status.ok())
      << "Failed precondition of FileWriterBase::FailOperation(): "
         "status not failed";
  RIEGELI_ASSERT(is_open())
      << "Failed precondition of FileWriterBase::FailOperation(): "
         "Object closed";
  return Fail(
      Annotate(absl::Status(static_cast<absl::StatusCode>(status.code()),
                            status.error_message()),
               absl::StrCat(operation, " failed")));
}

void FileWriterBase::AnnotateFailure(absl::Status& status) {
  RIEGELI_ASSERT(!status.ok())
      << "Failed precondition of Object::AnnotateFailure(): status not failed";
  if (!filename_.empty()) {
    status = Annotate(status, absl::StrCat("writing ", filename_));
  }
  Writer::AnnotateFailure(status);
}

bool FileWriterBase::SyncBuffer() {
  const absl::string_view data(start(), written_to_buffer());
  set_buffer();
  if (data.empty()) return true;
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  return WriteInternal(data);
}

inline size_t FileWriterBase::LengthToWriteDirectly() const {
  size_t length = buffer_size_;
  if (written_to_buffer() > 0) {
    // Two writes are needed because current contents of `buffer_` must be
    // pushed. Write directly if writing through `buffer_` would need more than
    // two writes, or if `buffer_` would be full for the second write.
    length = SaturatingAdd(available(), length);
  } else {
    // Write directly if writing through `buffer_` would need more than one
    // write, or if `buffer_` would be full.
  }
  return length;
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
  const size_t buffer_length =
      UnsignedMin(UnsignedMax(buffer_size_, min_length, recommended_length),
                  std::numeric_limits<Position>::max() - start_pos());
  buffer_.Reset(buffer_length);
  set_buffer(buffer_.data(), buffer_length);
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

void FileWriterBase::WriteHintSlow(size_t length) {
  RIEGELI_ASSERT_LT(available(), length)
      << "Failed precondition of Writer::WriteHintSlow(): "
         "enough space available, use WriteHint() instead";
  if (ABSL_PREDICT_FALSE(!SyncBuffer())) return;
  if (ABSL_PREDICT_FALSE(!healthy())) return;
  const size_t buffer_length =
      UnsignedMin(UnsignedMax(buffer_size_, length),
                  std::numeric_limits<Position>::max() - start_pos());
  buffer_.Reset(buffer_length);
  set_buffer(buffer_.data(), buffer_length);
}

bool FileWriterBase::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!SyncBuffer())) return false;
  return healthy();
}

}  // namespace tensorflow
}  // namespace riegeli
