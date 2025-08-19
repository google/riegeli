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

#include "riegeli/csv/csv_reader.h"

#include <stddef.h>

#include <array>
#include <functional>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/debug.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/string_reader.h"
#include "riegeli/bytes/string_writer.h"
#include "riegeli/csv/csv_record.h"
#include "riegeli/lines/line_reading.h"

namespace riegeli {

void CsvReaderBase::Initialize(Reader* src, Options&& options) {
  RIEGELI_ASSERT_NE(src, nullptr)
      << "Failed precondition of CsvReader: null Reader pointer";
  // Set `has_header_` before early returns because `ReadRecord(CsvRecord&)`
  // uses this as a precondition.
  if (options.required_header() != std::nullopt ||
      options.assumed_header() != std::nullopt) {
    RIEGELI_ASSERT(options.required_header() == std::nullopt ||
                   options.assumed_header() == std::nullopt)
        << "Failed precondition of CsvReader: "
           "required_header() and assumed_header() both set";
    has_header_ = true;
  }

  if (options.comment() != std::nullopt &&
      ABSL_PREDICT_FALSE(*options.comment() == '\n' ||
                         *options.comment() == '\r')) {
    Fail(absl::InvalidArgumentError(
        absl::StrCat("Comment character conflicts with record separator: ",
                     riegeli::Debug(*options.comment()))));
    return;
  }
  if (ABSL_PREDICT_FALSE(options.field_separator() == '\n' ||
                         options.field_separator() == '\r')) {
    Fail(absl::InvalidArgumentError(
        absl::StrCat("Field separator conflicts with record separator: ",
                     riegeli::Debug(options.field_separator()))));
    return;
  }
  if (ABSL_PREDICT_FALSE(options.field_separator() == options.comment())) {
    Fail(absl::InvalidArgumentError(
        absl::StrCat("Field separator conflicts with comment character: ",
                     riegeli::Debug(options.field_separator()))));
    return;
  }
  if (options.quote() != std::nullopt) {
    if (ABSL_PREDICT_FALSE(*options.quote() == '\n' ||
                           *options.quote() == '\r')) {
      Fail(absl::InvalidArgumentError(
          absl::StrCat("Quote character conflicts with record separator: ",
                       riegeli::Debug(*options.quote()))));
      return;
    }
    if (ABSL_PREDICT_FALSE(*options.quote() == options.comment())) {
      Fail(absl::InvalidArgumentError(
          absl::StrCat("Quote character conflicts with comment character: ",
                       riegeli::Debug(*options.quote()))));
      return;
    }
    if (ABSL_PREDICT_FALSE(*options.quote() == options.field_separator())) {
      Fail(absl::InvalidArgumentError(
          absl::StrCat("Quote character conflicts with field separator: ",
                       riegeli::Debug(*options.quote()))));
      return;
    }
  }
  if (options.escape() != std::nullopt) {
    if (ABSL_PREDICT_FALSE(*options.escape() == '\n' ||
                           *options.escape() == '\r')) {
      Fail(absl::InvalidArgumentError(
          absl::StrCat("Escape character conflicts with record separator: ",
                       riegeli::Debug(*options.escape()))));
      return;
    }
    if (ABSL_PREDICT_FALSE(*options.escape() == options.comment())) {
      Fail(absl::InvalidArgumentError(
          absl::StrCat("Escape character conflicts with comment character: ",
                       riegeli::Debug(*options.escape()))));
      return;
    }
    if (ABSL_PREDICT_FALSE(*options.escape() == options.field_separator())) {
      Fail(absl::InvalidArgumentError(
          absl::StrCat("Escape character conflicts with field separator: ",
                       riegeli::Debug(*options.escape()))));
      return;
    }
    if (ABSL_PREDICT_FALSE(*options.escape() == options.quote())) {
      Fail(absl::InvalidArgumentError(
          absl::StrCat("Escape character conflicts with quote character: ",
                       riegeli::Debug(*options.escape()))));
      return;
    }
  }

  char_classes_['\n'] = CharClass::kLf;
  char_classes_['\r'] = CharClass::kCr;
  if (options.comment() != std::nullopt) {
    char_classes_[static_cast<unsigned char>(*options.comment())] =
        CharClass::kComment;
  }
  char_classes_[static_cast<unsigned char>(options.field_separator())] =
      CharClass::kFieldSeparator;
  if (options.quote() != std::nullopt) {
    char_classes_[static_cast<unsigned char>(*options.quote())] =
        CharClass::kQuote;
  }
  if (options.escape() != std::nullopt) {
    char_classes_[static_cast<unsigned char>(*options.escape())] =
        CharClass::kEscape;
  }
  skip_empty_lines_ = options.skip_empty_lines();
  quote_ = options.quote().value_or('\0');
  max_num_fields_ = options.max_num_fields();
  max_field_length_ = options.max_field_length();

  if (ABSL_PREDICT_FALSE(!src->ok())) {
    FailWithoutAnnotation(AnnotateOverSrc(src->status()));
    return;
  }
  if (!options.preserve_utf8_bom()) SkipUtf8Bom(*src);

  // Recovery is not applicable to reading the header. Hence `recovery_` is set
  // after reading the header.
  if (options.required_header() != std::nullopt) {
    std::vector<std::string> header;
    if (ABSL_PREDICT_FALSE(!ReadRecord(header))) {
      Fail(absl::InvalidArgumentError("Empty CSV file"));
    } else {
      --record_index_;
      if (header == options.required_header()->names()) {
        header_ = *std::move(options.required_header());
      } else if (const absl::Status status =
                     header_.TryReset(options.required_header()->normalizer(),
                                      std::move(header));
                 ABSL_PREDICT_FALSE(!status.ok())) {
        FailAtPreviousRecord(absl::InvalidArgumentError(status.message()));
      } else {
        std::vector<absl::string_view> missing_names;
        for (const absl::string_view field :
             options.required_header()->names()) {
          if (ABSL_PREDICT_FALSE(!header_.contains(field))) {
            missing_names.push_back(field);
          }
        }
        if (ABSL_PREDICT_FALSE(!missing_names.empty())) {
          StringWriter<std::string> message;
          message.Write("Missing field names: ");
          for (std::vector<absl::string_view>::const_iterator iter =
                   missing_names.cbegin();
               iter != missing_names.cend(); ++iter) {
            if (iter != missing_names.cbegin()) message.Write(',');
            csv_internal::WriteDebugQuotedIfNeeded(*iter, message);
          }
          message.Write("; existing field names: ");
          for (CsvHeader::const_iterator iter = header_.cbegin();
               iter != header_.cend(); ++iter) {
            if (iter != header_.cbegin()) message.Write(',');
            csv_internal::WriteDebugQuotedIfNeeded(*iter, message);
          }
          message.Close();
          FailAtPreviousRecord(absl::InvalidArgumentError(message.dest()));
        }
      }
    }
  } else if (options.assumed_header() != std::nullopt) {
    header_ = *std::move(options.assumed_header());
  }

  recovery_ = std::move(options.recovery());
}

void CsvReaderBase::FailAtPreviousRecord(absl::Status status) {
  RIEGELI_ASSERT(!status.ok())
      << "Failed precondition of CsvReaderBase::FailAtPreviousRecord(): "
         "status not failed";
  RIEGELI_ASSERT(!standalone_record_)
      << "Failed precondition of CsvReaderBase::FailAtPreviousRecord(): "
         "should never happen in ReadCsvRecordFromString()";
  if (is_open()) {
    Reader& src = *SrcReader();
    status = src.AnnotateStatus(std::move(status));
  }
  FailWithoutAnnotation(
      Annotate(status, absl::StrCat("at line ", last_line_number())));
}

absl::Status CsvReaderBase::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) {
    Reader& src = *SrcReader();
    status = src.AnnotateStatus(std::move(status));
  }
  return AnnotateOverSrc(std::move(status));
}

absl::Status CsvReaderBase::AnnotateOverSrc(absl::Status status) {
  if (!standalone_record_) {
    return Annotate(status, absl::StrCat("at line ", line_number()));
  }
  return status;
}

bool CsvReaderBase::FailMaxFieldLengthExceeded() {
  recoverable_ = true;
  return Fail(absl::ResourceExhaustedError(
      absl::StrCat("Maximum field length exceeded: ", max_field_length_)));
}

inline void CsvReaderBase::SkipLine(Reader& src) {
  const char* ptr = src.cursor();
  for (;;) {
    if (ABSL_PREDICT_FALSE(ptr == src.limit())) {
      src.move_cursor(src.available());
      if (ABSL_PREDICT_FALSE(!src.Pull())) {
        // Set `line_number_` as if the last line was terminated by a newline.
        ++line_number_;
        return;
      }
      ptr = src.cursor();
    }
    if (*ptr == '\n') {
      ++line_number_;
      src.set_cursor(ptr + 1);
      return;
    }
    ++ptr;
  }
}

inline bool CsvReaderBase::ReadQuoted(Reader& src, std::string& field) {
  if (ABSL_PREDICT_FALSE(!field.empty())) {
    recoverable_ = true;
    return Fail(
        absl::InvalidArgumentError("Unquoted data before opening quote"));
  }

  // Data from `src.cursor()` to where `ptr` stops will be appended to `field`.
  const char* ptr = src.cursor();
  for (;;) {
    if (ABSL_PREDICT_FALSE(ptr == src.limit())) {
      if (ABSL_PREDICT_FALSE(src.available() >
                             max_field_length_ - field.size())) {
        return FailMaxFieldLengthExceeded();
      }
      field.append(src.cursor(), src.available());
      src.move_cursor(src.available());
      if (ABSL_PREDICT_FALSE(!src.Pull())) {
        if (ABSL_PREDICT_FALSE(!src.ok())) {
          return FailWithoutAnnotation(AnnotateOverSrc(src.status()));
        }
        recoverable_ = true;
        return Fail(absl::InvalidArgumentError("Missing closing quote"));
      }
      ptr = src.cursor();
    }
    const CharClass char_class =
        char_classes_[static_cast<unsigned char>(*ptr++)];
    if (ABSL_PREDICT_TRUE(char_class == CharClass::kOther)) continue;
    switch (char_class) {
      case CharClass::kLf:
        ++line_number_;
        continue;
      case CharClass::kCr:
      case CharClass::kComment:
      case CharClass::kFieldSeparator:
        continue;
      default:
        break;
    }
    const size_t length = PtrDistance(src.cursor(), ptr - 1);
    if (ABSL_PREDICT_FALSE(length > max_field_length_ - field.size())) {
      return FailMaxFieldLengthExceeded();
    }
    field.append(src.cursor(), length);
    src.set_cursor(ptr);
    switch (char_class) {
      case CharClass::kOther:
      case CharClass::kLf:
      case CharClass::kCr:
      case CharClass::kComment:
      case CharClass::kFieldSeparator:
        RIEGELI_ASSUME_UNREACHABLE() << "Handled before switch";
      case CharClass::kQuote:
        if (ABSL_PREDICT_FALSE(!src.Pull())) {
          if (ABSL_PREDICT_FALSE(!src.ok())) {
            return FailWithoutAnnotation(AnnotateOverSrc(src.status()));
          }
          return true;
        }
        if (*src.cursor() == quote_) {
          // Quote written twice.
          ptr = src.cursor() + 1;
          continue;
        }
        return true;
      case CharClass::kEscape:
        if (ABSL_PREDICT_FALSE(!src.Pull())) {
          if (ABSL_PREDICT_FALSE(!src.ok())) {
            return FailWithoutAnnotation(AnnotateOverSrc(src.status()));
          }
          recoverable_ = true;
          return Fail(
              absl::InvalidArgumentError("Missing character after escape"));
        }
        ptr = src.cursor() + 1;
        continue;
    }
    RIEGELI_ASSUME_UNREACHABLE()
        << "Unknown character class: " << static_cast<int>(char_class);
  }
}

inline bool CsvReaderBase::ReadFields(Reader& src,
                                      std::vector<std::string>& fields,
                                      size_t& field_index) {
  RIEGELI_ASSERT_EQ(field_index, 0u)
      << "Failed precondition of CsvReaderBase::ReadFields(): "
         "initial index must be 0";
next_record:
  last_line_number_ = line_number_;
  if (standalone_record_) {
    if (ABSL_PREDICT_FALSE(record_index_ > 0)) return false;
  } else {
    if (ABSL_PREDICT_FALSE(!src.Pull())) {
      // End of file at the beginning of a record.
      if (ABSL_PREDICT_FALSE(!src.ok())) {
        return FailWithoutAnnotation(AnnotateOverSrc(src.status()));
      }
      return false;
    }
  }

next_field:
  if (ABSL_PREDICT_FALSE(field_index == max_num_fields_)) {
    recoverable_ = true;
    return Fail(absl::ResourceExhaustedError(
        absl::StrCat("Maximum number of fields exceeded: ", max_num_fields_)));
  }
  if (fields.size() == field_index) {
    fields.emplace_back();
  } else {
    fields[field_index].clear();
  }
  std::string& field = fields[field_index];

  // Data from `src.cursor()` to where `ptr` stops will be appended to `field`.
  const char* ptr = src.cursor();
  for (;;) {
    if (ABSL_PREDICT_FALSE(ptr == src.limit())) {
      if (ABSL_PREDICT_FALSE(src.available() >
                             max_field_length_ - field.size())) {
        return FailMaxFieldLengthExceeded();
      }
      field.append(src.cursor(), src.available());
      src.move_cursor(src.available());
      if (ABSL_PREDICT_FALSE(!src.Pull())) {
        if (ABSL_PREDICT_FALSE(!src.ok())) {
          return FailWithoutAnnotation(AnnotateOverSrc(src.status()));
        }
        // Set `line_number_` as if the last line was terminated by a newline.
        ++line_number_;
        return true;
      }
      ptr = src.cursor();
    }
    const CharClass char_class =
        char_classes_[static_cast<unsigned char>(*ptr++)];
    if (ABSL_PREDICT_TRUE(char_class == CharClass::kOther)) continue;
    switch (char_class) {
      case CharClass::kComment:
        if (field_index == 0 && field.empty() && ptr - 1 == src.cursor()) {
          src.set_cursor(ptr);
          SkipLine(src);
          goto next_record;
        }
        continue;
      default:
        break;
    }
    const size_t length = PtrDistance(src.cursor(), ptr - 1);
    if (ABSL_PREDICT_FALSE(length > max_field_length_ - field.size())) {
      return FailMaxFieldLengthExceeded();
    }
    field.append(src.cursor(), length);
    src.set_cursor(ptr);
    switch (char_class) {
      case CharClass::kOther:
      case CharClass::kComment:
        RIEGELI_ASSUME_UNREACHABLE() << "Handled before switch";
      case CharClass::kCr:
        if (ABSL_PREDICT_FALSE(!src.Pull())) {
          if (ABSL_PREDICT_FALSE(!src.ok())) {
            return FailWithoutAnnotation(AnnotateOverSrc(src.status()));
          }
          recoverable_ = true;
          return Fail(absl::InvalidArgumentError("Missing LF after CR"));
        }
        if (ABSL_PREDICT_FALSE(*src.cursor() != '\n')) {
          recoverable_ = true;
          return Fail(absl::InvalidArgumentError("Missing LF after CR"));
        }
        src.move_cursor(1);
        ABSL_FALLTHROUGH_INTENDED;
      case CharClass::kLf:
        ++line_number_;
        if (skip_empty_lines_ && field_index == 0 && field.empty()) {
          goto next_record;
        }
        if (ABSL_PREDICT_FALSE(standalone_record_)) {
          return Fail(absl::InvalidArgumentError("Unexpected newline"));
        }
        return true;
      case CharClass::kFieldSeparator:
        ++field_index;
        goto next_field;
      case CharClass::kQuote: {
        if (ABSL_PREDICT_FALSE(!ReadQuoted(src, field))) return false;
        if (ABSL_PREDICT_FALSE(!src.Pull())) {
          if (ABSL_PREDICT_FALSE(!src.ok())) {
            return FailWithoutAnnotation(AnnotateOverSrc(src.status()));
          }
          // Set `line_number_` as if the last line was terminated by a newline.
          ++line_number_;
          return true;
        }
        const CharClass char_class_after_quoted =
            char_classes_[static_cast<unsigned char>(*src.cursor())];
        src.move_cursor(1);
        switch (char_class_after_quoted) {
          case CharClass::kOther:
          case CharClass::kComment:
          case CharClass::kEscape:
            recoverable_ = true;
            return Fail(absl::InvalidArgumentError(
                "Unquoted data after closing quote"));
          case CharClass::kFieldSeparator:
            ++field_index;
            goto next_field;
          case CharClass::kCr:
            if (ABSL_PREDICT_FALSE(!src.Pull())) {
              if (ABSL_PREDICT_FALSE(!src.ok())) {
                return FailWithoutAnnotation(AnnotateOverSrc(src.status()));
              }
              recoverable_ = true;
              return Fail(absl::InvalidArgumentError("Missing LF after CR"));
            }
            if (ABSL_PREDICT_FALSE(*src.cursor() != '\n')) {
              recoverable_ = true;
              return Fail(absl::InvalidArgumentError("Missing LF after CR"));
            }
            src.move_cursor(1);
            ABSL_FALLTHROUGH_INTENDED;
          case CharClass::kLf:
            ++line_number_;
            if (ABSL_PREDICT_FALSE(standalone_record_)) {
              return Fail(absl::InvalidArgumentError("Unexpected newline"));
            }
            return true;
          case CharClass::kQuote:
            RIEGELI_ASSUME_UNREACHABLE() << "Handled by ReadQuoted()";
        }
        RIEGELI_ASSUME_UNREACHABLE()
            << "Unknown character class: "
            << static_cast<int>(char_class_after_quoted);
      }
      case CharClass::kEscape:
        if (ABSL_PREDICT_FALSE(!src.Pull())) {
          if (ABSL_PREDICT_FALSE(!src.ok())) {
            return FailWithoutAnnotation(AnnotateOverSrc(src.status()));
          }
          recoverable_ = true;
          return Fail(
              absl::InvalidArgumentError("Missing character after escape"));
        }
        ptr = src.cursor() + 1;
        continue;
    }
    RIEGELI_ASSUME_UNREACHABLE()
        << "Unknown character class: " << static_cast<int>(char_class);
  }
}

bool CsvReaderBase::ReadRecord(CsvRecord& record) {
  RIEGELI_CHECK(has_header())
      << "Failed precondition of CsvReaderBase::ReadRecord(CsvRecord&): "
         "CsvReaderBase::Options::required_header() != nullopt or "
         "assumed_header() != nullopt is required";
  if (ABSL_PREDICT_FALSE(!ok())) {
    record.Reset();
    return false;
  }
try_again:
  record.Reset(header_);
  // Reading directly into `record.fields_` must be careful to maintain the
  // invariant that `record.header_.size() == record.fields_.size()`.
  if (ABSL_PREDICT_FALSE(!ReadRecord(record.fields_))) {
    record.Reset();
    return false;
  }
  if (ABSL_PREDICT_FALSE(record.fields_.size() != header_.size())) {
    --record_index_;
    const size_t record_size = record.fields_.size();
    record.Reset();
    FailAtPreviousRecord(absl::InvalidArgumentError(
        absl::StrCat("Mismatched number of CSV fields: header has ",
                     header_.size(), ", record has ", record_size)));
    if (recovery_ != nullptr) {
      absl::Status status = this->status();
      MarkNotFailed();
      if (recovery_(std::move(status), *this)) goto try_again;
    }
    return false;
  }
  return true;
}

namespace csv_internal {

inline bool ReadStandaloneRecord(CsvReaderBase& csv_reader,
                                 std::vector<std::string>& record) {
  csv_reader.standalone_record_ = true;
  return csv_reader.ReadRecordInternal(record);
}

}  // namespace csv_internal

bool CsvReaderBase::ReadRecord(std::vector<std::string>& record) {
  return ReadRecordInternal(record);
}

inline bool CsvReaderBase::ReadRecordInternal(
    std::vector<std::string>& record) {
  if (ABSL_PREDICT_FALSE(!ok())) {
    record.clear();
    return false;
  }
  if (standalone_record_) {
    RIEGELI_ASSERT_EQ(record_index_, 0u)
        << "Failed precondition of CsvReaderBase::ReadRecordInternal(): "
           "called more than once by ReadCsvRecordFromString()";
  }
  Reader& src = *SrcReader();
try_again:
  size_t field_index = 0;
  // Assign to existing elements of `record` when possible and then `erase()`
  // excess elements, instead of calling `record.clear()` upfront, to avoid
  // losing existing `std::string` allocations.
  if (ABSL_PREDICT_FALSE(!ReadFields(src, record, field_index))) {
    if (recovery_ != nullptr && recoverable_) {
      recoverable_ = false;
      absl::Status status = this->status();
      MarkNotFailed();
      SkipLine(src);
      if (recovery_(std::move(status), *this)) goto try_again;
      if (standalone_record_) {
        // Recovery was cancelled. Return the same result as for an empty input:
        // one empty field.
        if (record.empty()) {
          record.emplace_back();
        } else {
          record[0].clear();
        }
        record.erase(record.begin() + 1, record.end());
        ++record_index_;
        return true;
      }
    }
    record.clear();
    return false;
  }
  record.erase(record.begin() + field_index + 1, record.end());
  ++record_index_;
  return true;
}

bool CsvReaderBase::HasNextRecord() {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader& src = *SrcReader();
  for (;;) {
    last_line_number_ = line_number_;
    if (ABSL_PREDICT_FALSE(!src.Pull())) {
      // End of file at the beginning of a record.
      if (ABSL_PREDICT_FALSE(!src.ok())) {
        return FailWithoutAnnotation(AnnotateOverSrc(src.status()));
      }
      return false;
    }
    const CharClass char_class =
        char_classes_[static_cast<unsigned char>(*src.cursor())];
    switch (char_class) {
      case CharClass::kCr:
        src.move_cursor(1);
        if (ABSL_PREDICT_FALSE(!src.Pull())) {
          if (ABSL_PREDICT_FALSE(!src.ok())) {
            return FailWithoutAnnotation(AnnotateOverSrc(src.status()));
          }
          return false;
        }
        if (ABSL_PREDICT_FALSE(*src.cursor() != '\n')) return false;
        ABSL_FALLTHROUGH_INTENDED;
      case CharClass::kLf:
        if (skip_empty_lines_) {
          ++line_number_;
          src.move_cursor(1);
          continue;
        }
        return true;
      case CharClass::kComment:
        SkipLine(src);
        continue;
      default:
        return true;
    }
  }
}

absl::Status ReadCsvRecordFromString(absl::string_view src,
                                     std::vector<std::string>& record,
                                     CsvReaderBase::Options options) {
  RIEGELI_ASSERT(options.required_header() == std::nullopt)
      << "Failed precondition of ReadCsvRecordFromString(): "
         "CsvReaderBase::Options::required_header() != nullopt not applicable";
  CsvReader csv_reader(riegeli::Maker<StringReader>(src), std::move(options));
  if (ABSL_PREDICT_FALSE(
          !csv_internal::ReadStandaloneRecord(csv_reader, record))) {
    RIEGELI_ASSERT(!csv_reader.ok())
        << "ReadStandaloneRecord() returned false but ok() is true";
    return csv_reader.status();
  }
  return absl::OkStatus();
}

}  // namespace riegeli
