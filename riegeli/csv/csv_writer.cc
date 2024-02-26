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

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/csv/csv_record.h"
#include "riegeli/lines/line_writing.h"
#include "riegeli/lines/newline.h"

namespace riegeli {

namespace {

std::string ShowEscaped(char ch) {
  return absl::StrCat("'", absl::CHexEscape(absl::string_view(&ch, 1)), "'");
}

}  // namespace

void CsvWriterBase::Initialize(Writer* dest, Options&& options) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of CsvWriter: null Writer pointer";
  // Set `header_` early, so that `header()` is valid even in case of a failure,
  // and because `WriteRecord(const CsvRecord&)` uses this as a precondition.
  bool write_header = false;
  if (options.header() != absl::nullopt) {
    RIEGELI_ASSERT(options.assumed_header() == absl::nullopt)
        << "Failed precondition of CsvWriter: "
           "header() and assumed_header() both set";
    has_header_ = true;
    write_header = true;
    header_ = *std::move(options.header());
  } else if (options.assumed_header() != absl::nullopt) {
    has_header_ = true;
    header_ = *std::move(options.assumed_header());
  }

  if (options.comment() != absl::nullopt &&
      ABSL_PREDICT_FALSE(*options.comment() == '\n' ||
                         *options.comment() == '\r')) {
    Fail(absl::InvalidArgumentError(
        absl::StrCat("Comment character conflicts with record separator: ",
                     ShowEscaped(*options.comment()))));
    return;
  }
  if (ABSL_PREDICT_FALSE(options.field_separator() == '\n' ||
                         options.field_separator() == '\r')) {
    Fail(absl::InvalidArgumentError(
        absl::StrCat("Field separator conflicts with record separator: ",
                     ShowEscaped(options.field_separator()))));
    return;
  }
  if (ABSL_PREDICT_FALSE(options.field_separator() == options.comment())) {
    Fail(absl::InvalidArgumentError(
        absl::StrCat("Field separator conflicts with comment character: ",
                     ShowEscaped(options.field_separator()))));
    return;
  }
  if (options.quote() != absl::nullopt) {
    if (ABSL_PREDICT_FALSE(*options.quote() == '\n' ||
                           *options.quote() == '\r')) {
      Fail(absl::InvalidArgumentError(
          absl::StrCat("Quote character conflicts with record separator: ",
                       ShowEscaped(*options.quote()))));
      return;
    }
    if (ABSL_PREDICT_FALSE(*options.quote() == options.comment())) {
      Fail(absl::InvalidArgumentError(
          absl::StrCat("Quote character conflicts with comment character: ",
                       ShowEscaped(*options.quote()))));
      return;
    }
    if (ABSL_PREDICT_FALSE(*options.quote() == options.field_separator())) {
      Fail(absl::InvalidArgumentError(
          absl::StrCat("Quote character conflicts with field separator: ",
                       ShowEscaped(*options.quote()))));
      return;
    }
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

  if (ABSL_PREDICT_FALSE(!dest->ok())) {
    FailWithoutAnnotation(AnnotateOverDest(dest->status()));
    return;
  }
  if (options.write_utf8_bom()) WriteUtf8Bom(*dest);

  if (write_header) {
    if (ABSL_PREDICT_TRUE(WriteRecord(header_.names()))) --record_index_;
  }
}

absl::Status CsvWriterBase::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) {
    Writer& dest = *DestWriter();
    status = dest.AnnotateStatus(std::move(status));
  }
  return AnnotateOverDest(std::move(status));
}

absl::Status CsvWriterBase::AnnotateOverDest(absl::Status status) {
  if (!standalone_record_) {
    return Annotate(status, absl::StrCat("at record ", record_index()));
  }
  return status;
}

inline bool CsvWriterBase::WriteQuoted(Writer& dest, absl::string_view field,
                                       size_t already_scanned) {
  RIEGELI_ASSERT(quote_ != absl::nullopt)
      << "Failed precondition of CsvWriterBase::WriteQuoted(): "
         "quote character not available";
  if (ABSL_PREDICT_FALSE(!dest.Write(*quote_))) {
    return FailWithoutAnnotation(AnnotateOverDest(dest.status()));
  }
  const char* start = field.data();
  const char* next_to_check = field.data() + already_scanned;
  const char* const limit = field.data() + field.size();
  // Write characters in the range [`start`..`limit`), except that if quotes are
  // found in the range [`next_to_check`..`limit`), write them twice.
  while (const char* const next_quote = static_cast<const char*>(std::memchr(
             next_to_check, *quote_, PtrDistance(next_to_check, limit)))) {
    if (ABSL_PREDICT_FALSE(!dest.Write(
            absl::string_view(start, PtrDistance(start, next_quote + 1))))) {
      return FailWithoutAnnotation(AnnotateOverDest(dest.status()));
    }
    start = next_quote;
    next_to_check = next_quote + 1;
  }
  if (ABSL_PREDICT_FALSE(
          !dest.Write(absl::string_view(start, PtrDistance(start, limit))))) {
    return FailWithoutAnnotation(AnnotateOverDest(dest.status()));
  }
  if (ABSL_PREDICT_FALSE(!dest.Write(*quote_))) {
    return FailWithoutAnnotation(AnnotateOverDest(dest.status()));
  }
  return true;
}

bool CsvWriterBase::WriteQuotes(Writer& dest) {
  if (quote_ == absl::nullopt) return true;
  if (ABSL_PREDICT_FALSE(!dest.Write(*quote_) || !dest.Write(*quote_))) {
    return FailWithoutAnnotation(AnnotateOverDest(dest.status()));
  }
  return true;
}

bool CsvWriterBase::WriteFirstField(Writer& dest, absl::string_view field) {
  // Quote the first field if the field together with the field separator could
  // make the line starting with UTF-8 BOM.
  //
  // For simplicity other fields are not considered, at the cost of unnecessary
  // quoting in corner cases.
  if (ABSL_PREDICT_FALSE(
          field.empty() ? field_separator_ == kUtf8Bom[0]
                        : field[0] == kUtf8Bom[0] &&
                              (field.size() == 1
                                   ? field_separator_ == kUtf8Bom[1]
                                   : field[1] == kUtf8Bom[1] &&
                                         (field.size() == 2
                                              ? field_separator_ == kUtf8Bom[2]
                                              : field[2] == kUtf8Bom[2]))) &&
      quote_ != absl::nullopt) {
    return WriteQuoted(dest, field, 0);
  }
  return WriteField(dest, field);
}

bool CsvWriterBase::WriteField(Writer& dest, absl::string_view field) {
  for (size_t i = 0; i < field.size(); ++i) {
    if (quotes_needed_[static_cast<unsigned char>(field[i])]) {
      if (ABSL_PREDICT_FALSE(quote_ == absl::nullopt)) {
        return Fail(absl::InvalidArgumentError(
            absl::StrCat("If quoting is turned off, special characters inside "
                         "fields are not "
                         "expressible: ",
                         ShowEscaped(field[i]))));
      }
      return WriteQuoted(dest, field, i);
    }
  }
  if (ABSL_PREDICT_FALSE(!dest.Write(field))) {
    return FailWithoutAnnotation(AnnotateOverDest(dest.status()));
  }
  return true;
}

bool CsvWriterBase::WriteRecord(const CsvRecord& record) {
  RIEGELI_CHECK(has_header_)
      << "Failed precondition of CsvWriterBase::WriteRecord(CsvRecord): "
         "CsvWriterBase::Options::header() is required";
  if (ok()) {
    RIEGELI_CHECK_EQ(record.header(), header_)
        << "Failed precondition of CsvWriterBase::WriteRecord(CsvRecord): "
        << "mismatched CSV header and record";
  }
  return WriteRecord(record.fields());
}

}  // namespace riegeli
