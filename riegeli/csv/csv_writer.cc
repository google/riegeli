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
#include <string>
#include <utility>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/object.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/csv/csv_record.h"

namespace riegeli {

void CsvWriterBase::Initialize(Writer* dest, Options&& options) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of CsvWriter: null Writer pointer";
  RIEGELI_ASSERT(options.field_separator() != options.comment())
      << "Field separator conflicts with comment character";
  if (options.quote() != absl::nullopt) {
    RIEGELI_ASSERT(*options.quote() != options.comment())
        << "Quote character conflicts with comment character";
    RIEGELI_ASSERT(*options.quote() != options.field_separator())
        << "Quote character conflicts with field separator";
  }
  if (ABSL_PREDICT_FALSE(!dest->healthy())) {
    Fail(*dest);
    return;
  }

  quotes_needed_['\n'] = true;
  quotes_needed_['\r'] = true;
  if (options.comment() != absl::nullopt) {
    quotes_needed_[static_cast<unsigned char>(*options.comment())] = true;
  }
  quotes_needed_[static_cast<unsigned char>(options.field_separator())] = true;
  if (options.quote() != absl::nullopt) {
    quotes_needed_[static_cast<unsigned char>(*options.quote())] = true;
  }
  newline_ = options.newline();
  field_separator_ = options.field_separator();
  quote_ = options.quote();

  if (!options.header().empty()) {
    header_ = std::move(options.header());
    if (ABSL_PREDICT_TRUE(WriteRecord(header_.names()))) {
      --record_index_;
    }
  }
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
  if (ABSL_PREDICT_FALSE(quote_ == absl::nullopt)) {
    return Fail(absl::InvalidArgumentError(absl::StrCat(
        "If quoting is turned off, special characters inside fields are not "
        "expressible: '",
        absl::CHexEscape(absl::string_view(&field[already_scanned], 1)), "'")));
  }
  if (ABSL_PREDICT_FALSE(!dest.WriteChar(*quote_))) return Fail(dest);
  const char* start = field.data();
  const char* next_to_check = field.data() + already_scanned;
  const char* const limit = field.data() + field.size();
  // Write characters [start, limit), except that if quotes are found in
  // [next_to_check, limit), write them twice.
  while (const char* const next_quote = static_cast<const char*>(std::memchr(
             next_to_check, *quote_, PtrDistance(next_to_check, limit)))) {
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
  if (ABSL_PREDICT_FALSE(!dest.WriteChar(*quote_))) return Fail(dest);
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

bool CsvWriterBase::WriteRecord(const CsvRecord& record) {
  if (healthy()) {
    RIEGELI_CHECK(!header_.empty())
        << "Failed precondition of CsvWriterBase::WriteRecord(CsvRecord): "
           "CsvWriterBase::Options::set_header() is required";
    RIEGELI_CHECK_EQ(record.header(), header_)
        << "Failed precondition of CsvWriterBase::WriteRecord(CsvRecord): "
        << "mismatched CSV header and record";
  }
  return WriteRecord(record.fields());
}

}  // namespace riegeli
