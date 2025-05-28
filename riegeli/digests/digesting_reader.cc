// Copyright 2021 Google LLC
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

#include "riegeli/digests/digesting_reader.h"

#include <stddef.h>

#include <limits>
#include <memory>
#include <optional>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/digests/digester_handle.h"

namespace riegeli {

namespace {

ABSL_ATTRIBUTE_COLD absl::Status FailedStatus(DigesterBaseHandle digester) {
  absl::Status status = digester.status();
  if (status.ok()) status = absl::UnknownError("Digester failed");
  return status;
}

}  // namespace

bool DigestingReaderBase::FailFromDigester() {
  const DigesterBaseHandle digester = GetDigester();
  return Fail(FailedStatus(digester));
}

void DigestingReaderBase::Done() {
  if (ABSL_PREDICT_TRUE(ok())) {
    Reader& src = *SrcReader();
    SyncBuffer(src);
  }
  Reader::Done();
}

absl::Status DigestingReaderBase::AnnotateStatusImpl(absl::Status status) {
  // Fully delegate annotations to `*SrcReader()`.
  if (is_open()) {
    Reader& src = *SrcReader();
    const bool sync_buffer_ok = SyncBuffer(src);
    status = src.AnnotateStatus(std::move(status));
    if (ABSL_PREDICT_TRUE(sync_buffer_ok)) MakeBuffer(src);
  }
  return status;
}

bool DigestingReaderBase::PullSlow(size_t min_length,
                                   size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Reader::PullSlow(): "
         "enough data available, use Pull() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader& src = *SrcReader();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(src))) return false;
  const bool pull_ok = src.Pull(min_length, recommended_length);
  MakeBuffer(src);
  return pull_ok;
}

bool DigestingReaderBase::ReadSlow(size_t length, char* dest) {
  RIEGELI_ASSERT_LT(available(), length)
      << "Failed precondition of Reader::ReadSlow(char*): "
         "enough data available, use Read(char*) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader& src = *SrcReader();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(src))) return false;
  size_t length_read;
  bool read_ok = src.Read(length, dest, &length_read);
  if (length_read > 0) {
    if (ABSL_PREDICT_FALSE(
            !WriteToDigester(absl::string_view(dest, length_read)))) {
      RIEGELI_EVAL_ASSERT(!FailFromDigester());
      read_ok = false;
    }
  }
  MakeBuffer(src);
  return read_ok;
}

bool DigestingReaderBase::ReadSlow(size_t length, Chain& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::ReadSlow(Chain&): "
         "enough data available, use Read(Chain&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadSlow(Chain&): "
         "Chain size overflow";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader& src = *SrcReader();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(src))) return false;
  Chain data;
  bool read_ok = src.Read(length, data);
  if (!data.empty()) {
    DigesterBaseHandle digester = GetDigester();
    if (ABSL_PREDICT_FALSE(!digester.Write(data))) {
      RIEGELI_EVAL_ASSERT(!FailFromDigester());
      read_ok = false;
    } else {
      dest.Append(std::move(data));
    }
  }
  MakeBuffer(src);
  return read_ok;
}

bool DigestingReaderBase::ReadSlow(size_t length, absl::Cord& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::ReadSlow(Cord&): "
         "enough data available, use Read(Cord&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadSlow(Cord&): "
         "Cord size overflow";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader& src = *SrcReader();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(src))) return false;
  absl::Cord data;
  bool read_ok = src.Read(length, data);
  if (!data.empty()) {
    DigesterBaseHandle digester = GetDigester();
    if (ABSL_PREDICT_FALSE(!digester.Write(data))) {
      RIEGELI_EVAL_ASSERT(!FailFromDigester());
      read_ok = false;
    } else {
      dest.Append(std::move(data));
    }
  }
  MakeBuffer(src);
  return read_ok;
}

bool DigestingReaderBase::ReadOrPullSomeSlow(
    size_t max_length, absl::FunctionRef<char*(size_t&)> get_dest) {
  RIEGELI_ASSERT_GT(max_length, 0u)
      << "Failed precondition of Reader::ReadOrPullSomeSlow(): "
         "nothing to read, use ReadOrPullSome() instead";
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of Reader::ReadOrPullSomeSlow(): "
         "some data available, use ReadOrPullSome() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader& src = *SrcReader();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(src))) return false;
  char* dest;
  size_t length_read;
  const bool read_ok = src.ReadOrPullSome(
      max_length,
      [get_dest, &dest](size_t& length) {
        dest = get_dest(length);
        return dest;
      },
      &length_read);
  if (length_read > 0) {
    if (ABSL_PREDICT_FALSE(
            !WriteToDigester(absl::string_view(dest, length_read)))) {
      FailFromDigester();
    }
  }
  MakeBuffer(src);
  return read_ok;
}

void DigestingReaderBase::ReadHintSlow(size_t min_length,
                                       size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Reader::ReadHintSlow(): "
         "enough data available, use ReadHint() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return;
  Reader& src = *SrcReader();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(src))) return;
  src.ReadHint(min_length, recommended_length);
  MakeBuffer(src);
}

bool DigestingReaderBase::SupportsSize() {
  Reader* const src = SrcReader();
  return src != nullptr && src->SupportsSize();
}

std::optional<Position> DigestingReaderBase::SizeImpl() {
  if (ABSL_PREDICT_FALSE(!ok())) return std::nullopt;
  Reader& src = *SrcReader();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(src))) return std::nullopt;
  const std::optional<Position> size = src.Size();
  MakeBuffer(src);
  return size;
}

bool DigestingReaderBase::SupportsNewReader() {
  Reader* const src = SrcReader();
  return src != nullptr && src->SupportsNewReader();
}

std::unique_ptr<Reader> DigestingReaderBase::NewReaderImpl(
    Position initial_pos) {
  if (ABSL_PREDICT_FALSE(!ok())) return nullptr;
  // `NewReaderImpl()` is thread-safe from this point
  // if `SrcReader()->SupportsNewReader()`.
  Reader& src = *SrcReader();
  std::unique_ptr<Reader> reader = src.NewReader(initial_pos);
  if (ABSL_PREDICT_FALSE(reader == nullptr)) {
    FailWithoutAnnotation(src.status());
  }
  return reader;
}

}  // namespace riegeli
