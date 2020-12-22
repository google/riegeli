// Copyright 2020 Google LLC
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

#include "riegeli/csv/csv_writer.h"

#include <stddef.h>

#include <array>
#include <cstring>

#include "absl/base/optimization.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void CsvWriterBase::Initialize(Writer* dest, Options&& options) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of CsvWriter: null Writer pointer";
  if (ABSL_PREDICT_FALSE(!dest->healthy())) {
    Fail(*dest);
    return;
  }

  quotes_needed_['\n'] = true;
  quotes_needed_['\r'] = true;
  quotes_needed_[static_cast<unsigned char>(options.field_separator())] = true;
  quotes_needed_['"'] = true;
  standalone_record_ = options.standalone_record();
  newline_ = options.newline();
  field_separator_ = options.field_separator();
  record_index_ = 0;
}

bool CsvWriterBase::Fail(absl::Status status) {
  RIEGELI_ASSERT(!status.ok())
      << "Failed precondition of Object::Fail(): status not failed";
  return FailWithoutAnnotation(
      Annotate(status, absl::StrCat("at record ", record_index())));
}

bool CsvWriterBase::FailWithoutAnnotation(absl::Status status) {
  RIEGELI_ASSERT(!status.ok())
      << "Failed precondition of CsvWriterBase::FailWithoutAnnotation(): "
         "status not failed";
  return Object::Fail(std::move(status));
}

bool CsvWriterBase::FailWithoutAnnotation(const Object& dependency) {
  RIEGELI_ASSERT(!dependency.healthy())
      << "Failed precondition of CsvWriterBase::FailWithoutAnnotation(): "
         "dependency healthy";
  return FailWithoutAnnotation(dependency.status());
}

inline bool CsvWriterBase::WriteQuoted(Writer& dest, absl::string_view field,
                                       size_t already_scanned) {
  if (ABSL_PREDICT_FALSE(!dest.WriteChar('"'))) return Fail(dest);
  const char* start = field.data();
  const char* next_to_check = field.data() + already_scanned;
  const char* const limit = field.data() + field.size();
  // Write characters [start, limit), except that if quotes are found in
  // [next_to_check, limit), write them twice.
  while (const char* const next_quote = static_cast<const char*>(std::memchr(
             next_to_check, '"', PtrDistance(next_to_check, limit)))) {
    if (ABSL_PREDICT_FALSE(!dest.Write(
            absl::string_view(start, PtrDistance(start, next_quote + 1))))) {
      return Fail(dest);
    }
    start = next_quote;
    next_to_check = next_quote + 1;
  }
  if (ABSL_PREDICT_FALSE(
          !dest.Write(absl::string_view(start, PtrDistance(start, limit))))) {
    return Fail(dest);
  }
  if (ABSL_PREDICT_FALSE(!dest.WriteChar('"'))) return Fail(dest);
  return true;
}

bool CsvWriterBase::WriteField(Writer& dest, absl::string_view field) {
  for (size_t i = 0; i < field.size(); ++i) {
    if (quotes_needed_[static_cast<unsigned char>(field[i])]) {
      return WriteQuoted(dest, field, i);
    }
  }
  if (ABSL_PREDICT_FALSE(!dest.Write(field))) return Fail(dest);
  return true;
}

}  // namespace riegeli
