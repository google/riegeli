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
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void StringWriterBase::Done() {
  StringWriterBase::FlushImpl(FlushType::kFromObject);
  Writer::Done();
}

bool StringWriterBase::PushSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Writer::PushSlow(): "
         "enough space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  std::string& dest = *dest_string();
  RIEGELI_ASSERT_EQ(start_to_limit(), dest.size())
      << "StringWriter destination changed unexpectedly";
  SyncBuffer(dest);
  if (min_length > dest.capacity() - dest.size()) {
    if (ABSL_PREDICT_FALSE(min_length > dest.max_size() - dest.size())) {
      return FailOverflow();
    }
    dest.reserve(UnsignedMin(
        UnsignedMax(SaturatingAdd(dest.size(),
                                  UnsignedMax(min_length, recommended_length)),
                    // Ensure amortized constant time of a reallocation.
                    SaturatingAdd(dest.capacity(), dest.capacity() / 2)),
        dest.max_size()));
  }
  MakeBuffer(dest);
  return true;
}

bool StringWriterBase::WriteSlow(absl::string_view src) {
  RIEGELI_ASSERT_LT(available(), src.size())
      << "Failed precondition of Writer::WriteSlow(string_view): "
         "enough space available, use Write(string_view) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  std::string& dest = *dest_string();
  RIEGELI_ASSERT_EQ(start_to_limit(), dest.size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > dest.max_size() - start_to_cursor())) {
    return FailOverflow();
  }
  SyncBuffer(dest);
  // TODO: When `absl::string_view` becomes C++17 `std::string_view`:
  // `dest.append(src)`
  dest.append(src.data(), src.size());
  MakeBuffer(dest);
  return true;
}

bool StringWriterBase::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain): "
         "enough space available, use Write(Chain) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  std::string& dest = *dest_string();
  RIEGELI_ASSERT_EQ(start_to_limit(), dest.size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > dest.max_size() - start_to_cursor())) {
    return FailOverflow();
  }
  SyncBuffer(dest);
  src.AppendTo(dest);
  MakeBuffer(dest);
  return true;
}

bool StringWriterBase::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain&&): "
         "enough space available, use Write(Chain) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  std::string& dest = *dest_string();
  RIEGELI_ASSERT_EQ(start_to_limit(), dest.size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > dest.max_size() - start_to_cursor())) {
    return FailOverflow();
  }
  SyncBuffer(dest);
  std::move(src).AppendTo(dest);
  MakeBuffer(dest);
  return true;
}

bool StringWriterBase::WriteSlow(const absl::Cord& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Cord): "
         "enough space available, use Write(Cord) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  std::string& dest = *dest_string();
  RIEGELI_ASSERT_EQ(start_to_limit(), dest.size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > dest.max_size() - start_to_cursor())) {
    return FailOverflow();
  }
  SyncBuffer(dest);
  for (absl::string_view fragment : src.Chunks()) {
    // TODO: When `absl::string_view` becomes C++17 `std::string_view`:
    // `dest.append(fragment)`
    dest.append(fragment.data(), fragment.size());
  }
  MakeBuffer(dest);
  return true;
}

bool StringWriterBase::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  std::string& dest = *dest_string();
  RIEGELI_ASSERT_EQ(start_to_limit(), dest.size())
      << "StringWriter destination changed unexpectedly";
  SyncBuffer(dest);
  return true;
}

absl::optional<Position> StringWriterBase::SizeImpl() {
  if (ABSL_PREDICT_FALSE(!healthy())) return absl::nullopt;
  return pos();
}

bool StringWriterBase::TruncateImpl(Position new_size) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  std::string& dest = *dest_string();
  RIEGELI_ASSERT_EQ(start_to_limit(), dest.size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(new_size > start_to_cursor())) return false;
  set_cursor(start() + new_size);
  return true;
}

inline void StringWriterBase::SyncBuffer(std::string& dest) {
  dest.erase(start_to_cursor());
  set_buffer(&dest[0], dest.size(), dest.size());
}

}  // namespace riegeli
