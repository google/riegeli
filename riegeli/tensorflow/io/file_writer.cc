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
#include <stdint.h>

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
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/byte_fill.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/external_ref.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/object.h"
#include "riegeli/base/reset.h"
#include "riegeli/base/shared_buffer.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/tensorflow/io/file_reader.h"
#include "tensorflow/core/platform/env.h"
#include "tensorflow/core/platform/file_system.h"
#include "tensorflow/core/public/version.h"

namespace riegeli::tensorflow {

bool FileWriterBase::InitializeFilename(::tensorflow::WritableFile* dest) {
  absl::string_view filename;
  if (const absl::Status status = dest->Name(&filename);
      ABSL_PREDICT_FALSE(!status.ok())) {
    filename_ = "<unknown>";
    if (!absl::IsUnimplemented(status)) {
      return FailOperation(status, "WritableFile::Name()");
    }
    return true;
  }
  return InitializeFilename(filename);
}

bool FileWriterBase::InitializeFilename(
    Initializer<std::string>::AllowingExplicit filename) {
  riegeli::Reset(filename_, std::move(filename));
  if (const absl::Status status =
          env_->GetFileSystemForFile(filename_, &file_system_);
      ABSL_PREDICT_FALSE(!status.ok())) {
    return FailOperation(status, "Env::GetFileSystemForFile()");
  }
  return true;
}

std::unique_ptr<::tensorflow::WritableFile> FileWriterBase::OpenFile(
    bool append) {
  std::unique_ptr<::tensorflow::WritableFile> dest;
  if (const absl::Status status =
          append ? file_system_->NewAppendableFile(filename_, &dest)
                 : file_system_->NewWritableFile(filename_, &dest);
      ABSL_PREDICT_FALSE(!status.ok())) {
    Writer::Reset(kClosed);
    FailOperation(status,
                  append ? absl::string_view("FileSystem::NewAppendableFile()")
                         : absl::string_view("FileSystem::NewWritableFile()"));
    return nullptr;
  }
  return dest;
}

void FileWriterBase::InitializePos(::tensorflow::WritableFile* dest) {
  int64_t file_pos;
  if (const absl::Status status = dest->Tell(&file_pos);
      ABSL_PREDICT_FALSE(!status.ok())) {
    FailOperation(status, "WritableFile::Tell()");
    return;
  }
  set_start_pos(IntCast<Position>(file_pos));
  buffer_sizer_.BeginRun(start_pos());
}

void FileWriterBase::Done() {
  SyncBuffer();
  Writer::Done();
  buffer_ = SharedBuffer();
}

bool FileWriterBase::FailOperation(const absl::Status& status,
                                   absl::string_view operation) {
  RIEGELI_ASSERT(!status.ok())
      << "Failed precondition of FileWriterBase::FailOperation(): "
         "status not failed";
  return Fail(
      Annotate(absl::Status(static_cast<absl::StatusCode>(status.code()),
#if TF_GRAPH_DEF_VERSION < 1467
                            status.error_message()
#else
                            status.message()
#endif
                                ),
               absl::StrCat(operation, " failed")));
}

absl::Status FileWriterBase::AnnotateStatusImpl(absl::Status status) {
  return Writer::AnnotateStatusImpl(
      Annotate(status, absl::StrCat("writing ", filename_)));
}

inline bool FileWriterBase::SyncBuffer() {
  if (start_to_cursor() > kMaxBytesToCopy) {
    if (ABSL_PREDICT_FALSE(!ok())) return false;
    const absl::Cord data(
        ExternalRef(buffer_, absl::string_view(start(), start_to_cursor())));
    set_buffer();
    return WriteInternal(data);
  }
  const absl::string_view data(start(), start_to_cursor());
  set_buffer();
  if (data.empty()) return true;
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  return WriteInternal(data);
}

void FileWriterBase::SetWriteSizeHintImpl(
    absl::optional<Position> write_size_hint) {
  buffer_sizer_.set_write_size_hint(pos(), write_size_hint);
}

bool FileWriterBase::PushSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Writer::PushSlow(): "
         "enough space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(!SyncBuffer())) return false;
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (ABSL_PREDICT_FALSE(min_length >
                         std::numeric_limits<Position>::max() - start_pos())) {
    return FailOverflow();
  }
  const size_t buffer_length = UnsignedMin(
      buffer_sizer_.BufferLength(start_pos(), min_length, recommended_length),
      std::numeric_limits<Position>::max() - start_pos());
  buffer_.Reset(buffer_length);
  set_buffer(buffer_.mutable_data(), buffer_length);
  return true;
}

bool FileWriterBase::WriteInternal(absl::string_view src) {
  RIEGELI_ASSERT(!src.empty())
      << "Failed precondition of FileWriterBase::WriteInternal(): "
         "nothing to write";
  RIEGELI_ASSERT_OK(*this)
      << "Failed precondition of FileWriterBase::WriteInternal()";
  ::tensorflow::WritableFile* const dest = DestFile();
  if (ABSL_PREDICT_FALSE(src.size() >
                         std::numeric_limits<Position>::max() - start_pos())) {
    return FailOverflow();
  }
  if (const absl::Status status = dest->Append(src);
      ABSL_PREDICT_FALSE(!status.ok())) {
    return FailOperation(status, "WritableFile::Append(string_view)");
  }
  move_start_pos(src.size());
  return true;
}

bool FileWriterBase::WriteSlow(absl::string_view src) {
  RIEGELI_ASSERT_LT(available(), src.size())
      << "Failed precondition of Writer::WriteSlow(string_view): "
         "enough space available, use Write(string_view) instead";
  if (src.size() >= buffer_sizer_.BufferLength(pos())) {
    // Write directly from `src`.
    if (ABSL_PREDICT_FALSE(!SyncBuffer())) return false;
    if (ABSL_PREDICT_FALSE(!ok())) return false;
    return WriteInternal(src);
  }
  return Writer::WriteSlow(src);
}

bool FileWriterBase::WriteSlow(ExternalRef src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(ExternalRef): "
         "enough space available, use Write(ExternalRef) instead";
  if (src.size() >= buffer_sizer_.BufferLength(pos())) {
    // Write directly from `src`.
    if (ABSL_PREDICT_FALSE(!SyncBuffer())) return false;
    if (ABSL_PREDICT_FALSE(!ok())) return false;
    return WriteInternal(absl::Cord(std::move(src)));
  }
  return Writer::WriteSlow(std::move(src));
}

bool FileWriterBase::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain): "
         "enough space available, use Write(Chain) instead";
  if (src.size() >= buffer_sizer_.BufferLength(pos())) {
    // Write directly from `src`.
    if (ABSL_PREDICT_FALSE(!SyncBuffer())) return false;
    if (ABSL_PREDICT_FALSE(!ok())) return false;
    return WriteInternal(absl::Cord(src));
  }
  return Writer::WriteSlow(src);
}

bool FileWriterBase::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain&&): "
         "enough space available, use Write(Chain&&) instead";
  if (src.size() >= buffer_sizer_.BufferLength(pos())) {
    // Write directly from `src`.
    if (ABSL_PREDICT_FALSE(!SyncBuffer())) return false;
    if (ABSL_PREDICT_FALSE(!ok())) return false;
    return WriteInternal(absl::Cord(std::move(src)));
  }
  // Not `std::move(src)`: forward to `Writer::WriteSlow(const Chain&)`,
  // because `Writer::WriteSlow(Chain&&)` would forward to
  // `FileWriterBase::WriteSlow(const Chain&)`.
  return Writer::WriteSlow(src);
}

bool FileWriterBase::WriteSlow(const absl::Cord& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Cord): "
         "enough space available, use Write(Cord) instead";
  if (src.size() >= buffer_sizer_.BufferLength(pos())) {
    // Write directly from `src`.
    if (ABSL_PREDICT_FALSE(!SyncBuffer())) return false;
    if (ABSL_PREDICT_FALSE(!ok())) return false;
    return WriteInternal(src);
  }
  return Writer::WriteSlow(src);
}

bool FileWriterBase::WriteSlow(ByteFill src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(ByteFill): "
         "enough space available, use Write(ByteFill) instead";
  if (src.size() >= buffer_sizer_.BufferLength(pos())) {
    // Write directly from `Cord(ByteFill)`.
    if (ABSL_PREDICT_FALSE(!SyncBuffer())) return false;
    if (ABSL_PREDICT_FALSE(!ok())) return false;
    while (
        ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max())) {
      if (ABSL_PREDICT_FALSE(!WriteInternal(
              absl::Cord(src.Extract(std::numeric_limits<size_t>::max()))))) {
        return false;
      }
    }
    return WriteInternal(absl::Cord(src));
  }
  return Writer::WriteSlow(src);
}

bool FileWriterBase::WriteInternal(const absl::Cord& src) {
  RIEGELI_ASSERT(!src.empty())
      << "Failed precondition of FileWriterBase::WriteInternal(): "
         "nothing to write";
  RIEGELI_ASSERT_OK(*this)
      << "Failed precondition of FileWriterBase::WriteInternal()";
  ::tensorflow::WritableFile* const dest = DestFile();
  if (ABSL_PREDICT_FALSE(src.size() >
                         std::numeric_limits<Position>::max() - start_pos())) {
    return FailOverflow();
  }
  if (absl::Status status = dest->Append(src);
      ABSL_PREDICT_FALSE(!status.ok())) {
    return FailOperation(status, "WritableFile::Append(Cord)");
  }
  move_start_pos(src.size());
  return true;
}

bool FileWriterBase::FlushImpl(FlushType flush_type) {
  buffer_sizer_.EndRun(pos());
  if (ABSL_PREDICT_FALSE(!SyncBuffer())) return false;
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  buffer_sizer_.BeginRun(start_pos());
  return true;
}

Reader* FileWriterBase::ReadModeImpl(Position initial_pos) {
  if (ABSL_PREDICT_FALSE(!FileWriterBase::SupportsReadMode())) {
    Fail(absl::UnimplementedError("A filename required for read mode"));
    return nullptr;
  }
  if (ABSL_PREDICT_FALSE(!Flush())) return nullptr;
  return associated_reader_.ResetReader(
      filename_, FileReaderBase::Options()
                     .set_env(env_)
                     .set_initial_pos(initial_pos)
                     .set_buffer_options(buffer_sizer_.buffer_options()));
}

}  // namespace riegeli::tensorflow
