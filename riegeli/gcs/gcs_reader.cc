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

#include "riegeli/gcs/gcs_reader.h"

#include <stddef.h>
#include <stdint.h>

#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "google/cloud/status.h"
#include "google/cloud/storage/client.h"
#include "google/cloud/storage/object_read_stream.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/object.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/istream_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/gcs/gcs_internal.h"
#include "riegeli/gcs/gcs_object.h"

namespace riegeli {

GcsReader::GcsReader(
    NewReaderTag, const google::cloud::storage::Client& client,
    const GcsObject& object,
    const std::function<google::cloud::storage::ObjectReadStream(
        GcsReader&, int64_t)>& read_object,
    BufferOptions buffer_options, Position read_from_offset)
    : IStreamReader(kClosed),
      client_(client),
      object_(object),
      read_object_(read_object) {
  IStreamReader::Reset(read_object(*this, IntCast<int64_t>(read_from_offset)),
                       IStreamReaderBase::Options()
                           .set_assumed_pos(read_from_offset)
                           .set_buffer_options(buffer_options));
  PropagateStatus();
  set_limit_pos(read_from_offset);
}

void GcsReader::Reset(Closed) {
  IStreamReader::Reset(kClosed);
  client_ = std::nullopt;
  object_ = GcsObject();
  read_object_ = nullptr;
}

void GcsReader::Initialize(google::cloud::storage::ObjectReadStream stream,
                           BufferOptions buffer_options,
                           const RangeOptions& range_options) {
  uint64_t initial_pos = 0;
  if (ABSL_PREDICT_TRUE(range_options.initial_pos >= 0)) {
    switch (range_options.origin) {
      case Origin::kBegin:
        initial_pos = IntCast<uint64_t>(range_options.initial_pos);
        break;
      case Origin::kEnd:
        if (stream.size() != std::nullopt &&
            ABSL_PREDICT_TRUE(*stream.size() <=
                              uint64_t{std::numeric_limits<int64_t>::max()})) {
          initial_pos = SaturatingSub(
              *stream.size(), IntCast<uint64_t>(range_options.initial_pos));
        }
        break;
    }
  }
  IStreamReader::Reset(std::move(stream),
                       IStreamReaderBase::Options()
                           .set_assumed_pos(initial_pos)
                           .set_buffer_options(buffer_options));
  PropagateStatus();
  if (src().size() != std::nullopt &&
      ABSL_PREDICT_TRUE(*src().size() <=
                        uint64_t{std::numeric_limits<int64_t>::max()})) {
    set_exact_size(*src().size());
    if (range_options.max_size != std::nullopt &&
        ABSL_PREDICT_TRUE(*range_options.max_size >= 0)) {
      set_exact_size(UnsignedMin(*exact_size(),
                                 IntCast<uint64_t>(*range_options.max_size)));
    }
  }
}

void GcsReader::Done() {
  IStreamReader::Done();
  src().Close();
  PropagateStatus();
  client_ = std::nullopt;
  read_object_ = nullptr;
}

absl::Status GcsReader::AnnotateStatusImpl(absl::Status status) {
  if (object_.ok()) {
    status = Annotate(status, absl::StrCat("reading ", object_));
  }
  return IStreamReader::AnnotateStatusImpl(std::move(status));
}

inline void GcsReader::PropagateStatus() {
  if (ABSL_PREDICT_FALSE(!src().status().ok())) PropagateStatusSlow();
}

void GcsReader::PropagateStatusSlow() {
  RIEGELI_ASSERT(!src().status().ok())
      << "Failed precondition of GcsReader::PropagateStatusSlow(): "
         "ObjectReadStream not failed";
  MarkNotFailed();
  Fail(gcs_internal::FromCloudStatus(src().status()));
}

bool GcsReader::ReadInternal(size_t min_length, size_t max_length, char* dest) {
  RIEGELI_ASSERT_GT(min_length, 0u)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "nothing to read";
  RIEGELI_ASSERT_GE(max_length, min_length)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "max_length < min_length";
  RIEGELI_ASSERT_OK(*this)
      << "Failed precondition of BufferedReader::ReadInternal()";
  const bool result = IStreamReader::ReadInternal(min_length, max_length, dest);
  PropagateStatus();
  return result;
}

bool GcsReader::SupportsRandomAccess() { return exact_size() != std::nullopt; }

bool GcsReader::SeekBehindBuffer(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of BufferedReader::SeekBehindBuffer(): "
         "position in the buffer, use Seek() instead";
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedReader::SeekBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(exact_size() == std::nullopt)) {
    return IStreamReader::SeekBehindBuffer(new_pos);
  }
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  // `ObjectReadStream` does not support seeking to the very end.
  const Position read_from_offset =
      UnsignedMin(new_pos, SaturatingSub(*exact_size(), Position{1}));
  src() = read_object_(*this, IntCast<int64_t>(read_from_offset));
  PropagateStatus();
  set_limit_pos(read_from_offset);
  if (new_pos > read_from_offset) {
    if (ABSL_PREDICT_FALSE(!Pull())) return false;
    move_cursor(1);
  } else {
    if (ABSL_PREDICT_FALSE(!ok())) return false;
  }
  return new_pos <= *exact_size();
}

bool GcsReader::SupportsNewReader() { return exact_size() != std::nullopt; }

std::unique_ptr<Reader> GcsReader::NewReaderImpl(Position initial_pos) {
  if (ABSL_PREDICT_FALSE(!GcsReader::SupportsNewReader())) {
    // Delegate to the base class to avoid repeating the error message.
    return IStreamReader::NewReaderImpl(initial_pos);
  }
  if (ABSL_PREDICT_FALSE(!ok())) return nullptr;
  // `NewReaderImpl()` is thread-safe from this point.

  // `ObjectReadStream` does not support seeking to the very end.
  const Position read_from_offset =
      UnsignedMin(initial_pos, SaturatingSub(*exact_size(), Position{1}));
  std::unique_ptr<GcsReader> reader(
      new GcsReader(NewReaderTag(), client(), object(), read_object_,
                    buffer_options(), read_from_offset));
  reader->set_exact_size(exact_size());
  if (initial_pos > read_from_offset) {
    if (ABSL_PREDICT_TRUE(reader->Pull())) reader->move_cursor(1);
  }
  return reader;
}

}  // namespace riegeli
