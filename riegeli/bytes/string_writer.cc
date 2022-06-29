// Copyright 2017 Google LLC
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

#include "riegeli/bytes/string_writer.h"

#include <stddef.h>

#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/string_reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void StringWriterBase::Done() {
  StringWriterBase::FlushImpl(FlushType::kFromObject);
  Writer::Done();
  secondary_buffer_ = Chain();
  associated_reader_.Reset();
}

void StringWriterBase::SetWriteSizeHint(
    absl::optional<Position> write_size_hint) {
  if (write_size_hint == absl::nullopt || ABSL_PREDICT_FALSE(!ok())) return;
  std::string& dest = *dest_string();
  RIEGELI_ASSERT_EQ(limit_pos(), dest.size() + secondary_buffer_.size())
      << "StringWriter destination changed unexpectedly";
  const size_t size_hint =
      UnsignedMin(SaturatingAdd(pos(), *write_size_hint), dest.max_size());
  if (secondary_buffer_.empty()) {
    SyncDestBuffer(dest);
    if (dest.capacity() < size_hint) dest.reserve(size_hint);
  } else {
    if (dest.capacity() < size_hint) dest.reserve(size_hint);
    SyncSecondaryBuffer();
    std::move(secondary_buffer_).AppendTo(dest);
    secondary_buffer_.Clear();
  }
}

bool StringWriterBase::PushSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Writer::PushSlow(): "
         "enough space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  std::string& dest = *dest_string();
  RIEGELI_ASSERT_EQ(limit_pos(), dest.size() + secondary_buffer_.size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(min_length >
                         dest.max_size() - IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  if (secondary_buffer_.empty()) {
    SyncDestBuffer(dest);
    if (dest.empty()) {
      // Allocate the first block directly in `dest`. It is possible that it
      // will not need to be copied if it turns out to be the only block,
      // although this decision might cause it to remain wasteful if less data
      // are written than space requested.
      dest.reserve(dest.size() + UnsignedMax(min_length, recommended_length,
                                             dest.size() / 2));
    }
    if (min_length <= dest.capacity() - dest.size()) {
      MakeDestBuffer(dest);
      return true;
    }
  } else {
    SyncSecondaryBuffer();
  }
  MakeSecondaryBuffer(min_length, recommended_length);
  return true;
}

bool StringWriterBase::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain): "
         "enough space available, use Write(Chain) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  std::string& dest = *dest_string();
  RIEGELI_ASSERT_EQ(limit_pos(), dest.size() + secondary_buffer_.size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() >
                         dest.max_size() - IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  if (secondary_buffer_.empty()) {
    SyncDestBuffer(dest);
    if (src.size() <= dest.capacity() - dest.size()) {
      src.AppendTo(dest);
      MakeDestBuffer(dest);
      return true;
    }
  } else {
    SyncSecondaryBuffer();
  }
  move_start_pos(src.size());
  secondary_buffer_.Append(src, options_);
  MakeSecondaryBuffer();
  return true;
}

bool StringWriterBase::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain&&): "
         "enough space available, use Write(Chain) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  std::string& dest = *dest_string();
  RIEGELI_ASSERT_EQ(limit_pos(), dest.size() + secondary_buffer_.size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() >
                         dest.max_size() - IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  if (secondary_buffer_.empty()) {
    SyncDestBuffer(dest);
    if (src.size() <= dest.capacity() - dest.size()) {
      std::move(src).AppendTo(dest);
      MakeDestBuffer(dest);
      return true;
    }
  } else {
    SyncSecondaryBuffer();
  }
  move_start_pos(src.size());
  secondary_buffer_.Append(std::move(src), options_);
  MakeSecondaryBuffer();
  return true;
}

bool StringWriterBase::WriteSlow(const absl::Cord& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Cord): "
         "enough space available, use Write(Cord) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  std::string& dest = *dest_string();
  RIEGELI_ASSERT_EQ(limit_pos(), dest.size() + secondary_buffer_.size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() >
                         dest.max_size() - IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  if (secondary_buffer_.empty()) {
    SyncDestBuffer(dest);
    if (src.size() <= dest.capacity() - dest.size()) {
      for (const absl::string_view fragment : src.Chunks()) {
        // TODO: When `absl::string_view` becomes C++17
        // `std::string_view`: `dest.append(fragment)`
        dest.append(fragment.data(), fragment.size());
      }
      MakeDestBuffer(dest);
      return true;
    }
  } else {
    SyncSecondaryBuffer();
  }
  move_start_pos(src.size());
  secondary_buffer_.Append(src, options_);
  MakeSecondaryBuffer();
  return true;
}

bool StringWriterBase::WriteSlow(absl::Cord&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Cord&&): "
         "enough space available, use Write(Cord&&) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  std::string& dest = *dest_string();
  RIEGELI_ASSERT_EQ(limit_pos(), dest.size() + secondary_buffer_.size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() >
                         dest.max_size() - IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  if (secondary_buffer_.empty()) {
    SyncDestBuffer(dest);
    if (src.size() <= dest.capacity() - dest.size()) {
      for (const absl::string_view fragment : src.Chunks()) {
        // TODO: When `absl::string_view` becomes C++17
        // `std::string_view`: `dest.append(fragment)`
        dest.append(fragment.data(), fragment.size());
      }
      MakeDestBuffer(dest);
      return true;
    }
  } else {
    SyncSecondaryBuffer();
  }
  move_start_pos(src.size());
  secondary_buffer_.Append(std::move(src), options_);
  MakeSecondaryBuffer();
  return true;
}

bool StringWriterBase::WriteZerosSlow(Position length) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Writer::WriteZerosSlow(): "
         "enough space available, use WriteZeros() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  std::string& dest = *dest_string();
  RIEGELI_ASSERT_EQ(limit_pos(), dest.size() + secondary_buffer_.size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(length > dest.max_size() - IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  if (secondary_buffer_.empty()) {
    SyncDestBuffer(dest);
    if (length <= dest.capacity() - dest.size()) {
      dest.append(length, '\0');
      MakeDestBuffer(dest);
      return true;
    }
  } else {
    SyncSecondaryBuffer();
  }
  move_start_pos(length);
  secondary_buffer_.Append(ChainOfZeros(IntCast<size_t>(length)), options_);
  MakeSecondaryBuffer();
  return true;
}

bool StringWriterBase::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  std::string& dest = *dest_string();
  RIEGELI_ASSERT_EQ(limit_pos(), dest.size() + secondary_buffer_.size())
      << "StringWriter destination changed unexpectedly";
  if (secondary_buffer_.empty()) {
    SyncDestBuffer(dest);
  } else {
    SyncSecondaryBuffer();
    std::move(secondary_buffer_).AppendTo(dest);
    secondary_buffer_.Clear();
  }
  return true;
}

absl::optional<Position> StringWriterBase::SizeImpl() {
  if (ABSL_PREDICT_FALSE(!ok())) return absl::nullopt;
  return pos();
}

bool StringWriterBase::TruncateImpl(Position new_size) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  std::string& dest = *dest_string();
  RIEGELI_ASSERT_EQ(limit_pos(), dest.size() + secondary_buffer_.size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(new_size > pos())) return false;
  if (new_size < dest.size()) {
    secondary_buffer_.Clear();
    set_buffer(&dest[0], dest.size(), IntCast<size_t>(new_size));
    set_start_pos(0);
  } else {
    secondary_buffer_.RemoveSuffix(
        dest.size() + secondary_buffer_.size() - IntCast<size_t>(new_size),
        options_);
    set_start_pos(new_size);
    set_buffer();
  }
  return true;
}

Reader* StringWriterBase::ReadModeImpl(Position initial_pos) {
  if (ABSL_PREDICT_FALSE(
          !StringWriterBase::FlushImpl(FlushType::kFromObject))) {
    return nullptr;
  }
  std::string& dest = *dest_string();
  StringReader<>* const reader = associated_reader_.ResetReader(dest);
  reader->Seek(initial_pos);
  return reader;
}

inline void StringWriterBase::SyncSecondaryBuffer() {
  set_start_pos(pos());
  secondary_buffer_.RemoveSuffix(available(), options_);
  set_buffer();
}

inline void StringWriterBase::MakeSecondaryBuffer(size_t min_length,
                                                  size_t recommended_length) {
  const absl::Span<char> buffer = secondary_buffer_.AppendBuffer(
      min_length, recommended_length, Chain::kAnyLength, options_);
  set_buffer(buffer.data(), buffer.size());
}

}  // namespace riegeli
