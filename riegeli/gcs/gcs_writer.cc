// Copyright 2023 Google LLC
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

#include "riegeli/gcs/gcs_writer.h"

#include <stdint.h>

#include <functional>
#include <optional>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "google/cloud/storage/object_metadata.h"
#include "google/cloud/storage/object_write_stream.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/object.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/ostream_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/gcs/gcs_internal.h"
#include "riegeli/gcs/gcs_object.h"
#include "riegeli/gcs/gcs_reader.h"

namespace riegeli {

void GcsWriter::Reset(Closed) {
  OStreamWriter::Reset(kClosed);
  client_ = std::nullopt;
  object_ = GcsObject();
  temp_object_name_ = std::nullopt;
  read_mode_buffer_options_ = BufferOptions();
  write_temp_object_ = nullptr;
  compose_object_ = nullptr;
  delete_temp_object_ = nullptr;
  get_object_size_ = nullptr;
  make_gcs_reader_ = nullptr;
  writing_temp_object_ = false;
  initial_temp_object_pos_ = 0;
  metadata_ = std::nullopt;
  associated_reader_.Reset();
}

bool GcsWriter::CheckPreconditions(bool append, bool resumable_upload_session) {
  if (ABSL_PREDICT_FALSE(object_.generation() != std::nullopt && !append)) {
    return Fail(absl::FailedPreconditionError(
        "Specifying a generation requires GcsWriter::Options::append()"));
  }
  if (temp_object_name_ == std::nullopt) {
    if (ABSL_PREDICT_FALSE(append)) {
      return Fail(absl::FailedPreconditionError(
          "GcsWriter::Options::append() requires "
          "GcsWriter::Options::temp_object_name()"));
    }
  } else {
    if (ABSL_PREDICT_FALSE(*temp_object_name_ == object_.object_name())) {
      return Fail(absl::FailedPreconditionError(
          absl::StrCat("GcsWriter::Options::temp_object_name() must differ "
                       "from object name: \"",
                       object_.object_name(), "\"")));
    }
    if (ABSL_PREDICT_FALSE(resumable_upload_session)) {
      return Fail(absl::FailedPreconditionError(
          "UseResumableUploadSession() is incompatible with "
          "GcsWriter::Options::temp_object_name()"));
    }
  }
  return true;
}

void GcsWriter::Initialize(google::cloud::storage::ObjectWriteStream stream,
                           BufferOptions buffer_options, bool append) {
  uint64_t initial_pos;
  if (append) {
    stream.Close();
    if (stream.last_status().code() ==
        google::cloud::StatusCode::kFailedPrecondition) {
      // Precondition `IfGenerationMatch(0)` failed: object already exists.
    } else if (ABSL_PREDICT_FALSE(stream.fail())) {
      FailOperation("ObjectWriteStream::Close()");
      PropagateStatus();
      return;
    }
    const google::cloud::StatusOr<google::cloud::storage::ObjectMetadata>
        metadata = get_object_size_(*this);
    if (ABSL_PREDICT_FALSE(!metadata.ok())) {
      Fail(gcs_internal::FromCloudStatus(metadata.status()));
      return;
    }
    initial_pos = metadata->size();
    stream = write_temp_object_(*this);
    writing_temp_object_ = true;
    initial_temp_object_pos_ = initial_pos;
  } else {
    initial_pos = stream.next_expected_byte();
  }
  OStreamWriter::Reset(std::move(stream),
                       OStreamWriterBase::Options()
                           .set_assumed_pos(initial_pos)
                           .set_buffer_options(buffer_options));
  PropagateStatus();
  if (ABSL_PREDICT_FALSE(!dest().IsOpen()) && ABSL_PREDICT_TRUE(ok())) {
    // A resumable upload is already finished. Mark `GcsWriter` closed without
    // clearing `dest()`.
    OStreamWriterBase::Reset(kClosed);
  }
}

void GcsWriter::Done() {
  OStreamWriter::Done();
  dest().Close();
  if (ABSL_PREDICT_FALSE(dest().fail())) {
    FailOperation("ObjectWriteStream::Close()");
    PropagateStatus();
  }
  if (writing_temp_object_ && ABSL_PREDICT_TRUE(ok())) {
    if (start_pos() > initial_temp_object_pos_) {
      google::cloud::StatusOr<google::cloud::storage::ObjectMetadata> metadata =
          compose_object_(*this);
      if (ABSL_PREDICT_FALSE(!metadata.ok())) {
        Fail(gcs_internal::FromCloudStatus(metadata.status()));
        goto failed;
      }
      metadata_ = *std::move(metadata);
    }
    if (const google::cloud::Status status = delete_temp_object_(*this);
        ABSL_PREDICT_FALSE(!status.ok())) {
      Fail(gcs_internal::FromCloudStatus(status));
    }
  }
failed:
  client_ = std::nullopt;
  write_temp_object_ = nullptr;
  compose_object_ = nullptr;
  delete_temp_object_ = nullptr;
  get_object_size_ = nullptr;
  make_gcs_reader_ = nullptr;
  associated_reader_.Reset();
}

absl::Status GcsWriter::AnnotateStatusImpl(absl::Status status) {
  if (object_.ok()) {
    status = Annotate(status, absl::StrCat("writing ", object_));
  }
  return OStreamWriter::AnnotateStatusImpl(std::move(status));
}

inline void GcsWriter::PropagateStatus() {
  const google::cloud::Status status = dest().last_status();
  if (ABSL_PREDICT_FALSE(!status.ok())) PropagateStatusSlow(status);
}

void GcsWriter::PropagateStatusSlow(const google::cloud::Status& status) {
  RIEGELI_ASSERT(!status.ok())
      << "Failed precondition of GcsWriter::PropagateStatusSlow(): "
         "ObjectWriteStream not failed";
  MarkNotFailed();
  Fail(gcs_internal::FromCloudStatus(status));
}

bool GcsWriter::WriteInternal(absl::string_view src) {
  RIEGELI_ASSERT(!src.empty())
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "nothing to write";
  RIEGELI_ASSERT_OK(*this)
      << "Failed precondition of BufferedWriter::WriteInternal()";
  const bool result = OStreamWriter::WriteInternal(src);
  PropagateStatus();
  return result;
}

bool GcsWriter::FlushImpl(FlushType flush_type) {
  const bool flush_ok = OStreamWriter::FlushImpl(flush_type);
  PropagateStatus();
  if (ABSL_PREDICT_FALSE(!flush_ok)) return false;
  if (temp_object_name_ == std::nullopt ||
      (writing_temp_object_ && start_pos() == initial_temp_object_pos_)) {
    return true;
  }
  dest().Close();
  if (ABSL_PREDICT_FALSE(dest().fail())) {
    FailOperation("ObjectWriteStream::Close()");
    PropagateStatus();
    return false;
  }
  if (writing_temp_object_) {
    google::cloud::StatusOr<google::cloud::storage::ObjectMetadata> metadata =
        compose_object_(*this);
    if (ABSL_PREDICT_FALSE(!metadata.ok())) {
      return Fail(gcs_internal::FromCloudStatus(metadata.status()));
    }
    metadata_ = *std::move(metadata);
  }
  dest() = write_temp_object_(*this);
  writing_temp_object_ = true;
  initial_temp_object_pos_ = start_pos();
  if (ABSL_PREDICT_FALSE(dest().fail())) {
    FailOperation("Client::WriteObject()");
    PropagateStatus();
    return false;
  }
  return true;
}

bool GcsWriter::SupportsReadMode() { return temp_object_name_ != std::nullopt; }

Reader* GcsWriter::ReadModeBehindBuffer(Position initial_pos) {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::ReadModeBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!GcsWriter::SupportsReadMode())) {
    // Delegate to the base class to avoid repeating the error message.
    return OStreamWriter::ReadModeBehindBuffer(initial_pos);
  }
  if (ABSL_PREDICT_FALSE(!GcsWriter::FlushImpl(FlushType::kFromObject))) {
    return nullptr;
  }
  // `ObjectReadStream` does not support seeking to the very end.
  const Position read_from_offset =
      UnsignedMin(initial_pos, SaturatingSub(start_pos(), Position{1}));
  GcsReader* const reader = make_gcs_reader_(*this, read_from_offset);
  reader->set_exact_size(start_pos());
  if (initial_pos > read_from_offset) {
    if (ABSL_PREDICT_TRUE(reader->Pull())) reader->move_cursor(1);
  }
  return reader;
}

}  // namespace riegeli
