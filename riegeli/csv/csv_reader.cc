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
#include <string>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

void CsvReaderBase::Initialize(Reader* src, Options&& options) {
  RIEGELI_ASSERT(src != nullptr)
      << "Failed precondition of CsvReader: null Reader pointer";
  RIEGELI_ASSERT(options.field_separator() != options.comment())
      << "Field separator conflicts with comment character";
  if (options.escape() != absl::nullopt) {
    RIEGELI_ASSERT(*options.escape() != options.comment())
        << "Escape character conflicts with comment character";
    RIEGELI_ASSERT(*options.escape() != options.field_separator())
        << "Escape character conflicts with field separator";
  }
  if (ABSL_PREDICT_FALSE(!src->healthy())) {
    Fail(*src);
    return;
  }

  char_classes_['\n'] = CharClass::kLf;
  char_classes_['\r'] = CharClass::kCr;
  if (options.comment() != absl::nullopt) {
    char_classes_[static_cast<unsigned char>(*options.comment())] =
        CharClass::kComment;
  }
  char_classes_[static_cast<unsigned char>(options.field_separator())] =
      CharClass::kFieldSeparator;
  char_classes_['"'] = CharClass::kQuote;
  if (options.escape() != absl::nullopt) {
    char_classes_[static_cast<unsigned char>(*options.escape())] =
        CharClass::kEscape;
  }
  standalone_record_ = options.standalone_record();
  max_num_fields_ = UnsignedMin(options.max_num_fields(),
                                std::vector<std::string>().max_size());
  max_field_length_ =
      UnsignedMin(options.max_field_length(), std::string().max_size());
  recovery_ = std::move(options.recovery());
  record_index_ = 0;
  last_line_number_ = 0;
  line_number_ = 1;
  recoverable_ = false;
}

bool CsvReaderBase::Fail(absl::Status status) {
  RIEGELI_ASSERT(!status.ok())
      << "Failed precondition of Object::Fail(): status not failed";
  return FailWithoutAnnotation(
      Annotate(status, absl::StrCat("at line ", line_number())));
}

bool CsvReaderBase::FailWithoutAnnotation(absl::Status status) {
  RIEGELI_ASSERT(!status.ok())
      << "Failed precondition of CsvReaderBase::FailWithoutAnnotation(): "
         "status not failed";
  return Object::Fail(std::move(status));
}

bool CsvReaderBase::FailWithoutAnnotation(const Object& dependency) {
  RIEGELI_ASSERT(!dependency.healthy())
      << "Failed precondition of CsvReaderBase::FailWithoutAnnotation(): "
         "dependency healthy";
  return FailWithoutAnnotation(dependency.status());
}

bool CsvReaderBase::MaxFieldLengthExceeded() {
  recoverable_ = true;
  return Fail(absl::ResourceExhaustedError(
      absl::StrCat("Maximum field length exceeded: ", max_field_length_)));
}

inline void CsvReaderBase::SkipLine(Reader& src) {
  const char* ptr = src.cursor();
  for (;;) {
    if (ABSL_PREDICT_FALSE(ptr == src.limit())) {
      src.move_cursor(src.available());
      if (ABSL_PREDICT_FALSE(!src.Pull())) return;
      ptr = src.cursor();
    }
    if (*ptr == '\n') {
      ++line_number_;
      src.set_cursor(ptr + 1);
      return;
    }
    if (*ptr == '\r') {
      ++line_number_;
      src.set_cursor(ptr + 1);
      if (ABSL_PREDICT_FALSE(!src.Pull())) return;
      if (*src.cursor() == '\n') src.move_cursor(1);
      return;
    }
    ++ptr;
  }
}

inline bool CsvReaderBase::ReadQuoted(Reader& src, std::string& field) {
  if (ABSL_PREDICT_FALSE(!field.empty())) {
    recoverable_ = true;
    return Fail(absl::DataLossError("Unquoted data before opening quote"));
  }

  // Data from `src.cursor()` to where `ptr` stops will be appended to `field`.
  const char* ptr = src.cursor();
  for (;;) {
    if (ABSL_PREDICT_FALSE(ptr == src.limit())) {
      if (ABSL_PREDICT_FALSE(src.available() >
                             max_field_length_ - field.size())) {
        return MaxFieldLengthExceeded();
      }
      field.append(src.cursor(), src.available());
      src.move_cursor(src.available());
      if (ABSL_PREDICT_FALSE(!src.Pull())) {
        if (ABSL_PREDICT_FALSE(!src.healthy())) return Fail(src);
        recoverable_ = true;
        return Fail(absl::DataLossError("Missing closing quote"));
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
        ++line_number_;
        if (ABSL_PREDICT_FALSE(ptr == src.limit())) {
          if (ABSL_PREDICT_FALSE(src.available() >
                                 max_field_length_ - field.size())) {
            return MaxFieldLengthExceeded();
          }
          field.append(src.cursor(), src.available());
          src.move_cursor(src.available());
          if (ABSL_PREDICT_FALSE(!src.Pull())) {
            if (ABSL_PREDICT_FALSE(!src.healthy())) return Fail(src);
            recoverable_ = true;
            return Fail(absl::DataLossError("Missing closing quote"));
          }
          ptr = src.cursor();
        }
        if (*ptr == '\n') ++ptr;
        continue;
      case CharClass::kComment:
      case CharClass::kFieldSeparator:
        continue;
      default:
        break;
    }
    const size_t length = PtrDistance(src.cursor(), ptr - 1);
    if (ABSL_PREDICT_FALSE(length > max_field_length_ - field.size())) {
      return MaxFieldLengthExceeded();
    }
    field.append(src.cursor(), length);
    src.set_cursor(ptr);
    switch (char_class) {
      case CharClass::kOther:
      case CharClass::kLf:
      case CharClass::kCr:
      case CharClass::kComment:
      case CharClass::kFieldSeparator:
        RIEGELI_ASSERT_UNREACHABLE() << "Handled before switch";
      case CharClass::kQuote:
        if (ABSL_PREDICT_FALSE(!src.Pull())) {
          if (ABSL_PREDICT_FALSE(!src.healthy())) return Fail(src);
          return true;
        }
        if (*src.cursor() == '"') {
          // Quote written twice.
          ptr = src.cursor() + 1;
          continue;
        }
        return true;
      case CharClass::kEscape:
        if (ABSL_PREDICT_FALSE(!src.Pull())) {
          if (ABSL_PREDICT_FALSE(!src.healthy())) return Fail(src);
          recoverable_ = true;
          return Fail(absl::DataLossError("Missing character after escape"));
        }
        ptr = src.cursor() + 1;
        continue;
    }
    RIEGELI_ASSERT_UNREACHABLE()
        << "Unknown character class: " << static_cast<int>(char_class);
  }
}

inline bool CsvReaderBase::ReadFields(Reader& src,
                                      std::vector<std::string>& fields,
                                      size_t& index) {
  RIEGELI_ASSERT_EQ(index, 0)
      << "Failed precondition of CsvReaderBase::ReadFields(): "
         "initial index must be 0";
next_record:
  last_line_number_ = line_number_;
  if (standalone_record_) {
    if (record_index_ > 0) return false;
  } else {
    if (ABSL_PREDICT_FALSE(!src.Pull())) {
      // End of file at the beginning of a record.
      if (ABSL_PREDICT_FALSE(!src.healthy())) return Fail(src);
      return false;
    }
  }

next_field:
  if (ABSL_PREDICT_FALSE(index == max_num_fields_)) {
    recoverable_ = true;
    return Fail(absl::ResourceExhaustedError(
        absl::StrCat("Maximum number of fields exceeded: ", max_num_fields_)));
  }
  if (fields.size() == index) {
    fields.emplace_back();
  } else {
    fields[index].clear();
  }
  std::string& field = fields[index];

  // Data from `src.cursor()` to where `ptr` stops will be appended to `field`.
  const char* ptr = src.cursor();
  for (;;) {
    if (ABSL_PREDICT_FALSE(ptr == src.limit())) {
      if (ABSL_PREDICT_FALSE(src.available() >
                             max_field_length_ - field.size())) {
        return MaxFieldLengthExceeded();
      }
      field.append(src.cursor(), src.available());
      src.move_cursor(src.available());
      if (ABSL_PREDICT_FALSE(!src.Pull())) {
        if (ABSL_PREDICT_FALSE(!src.healthy())) return Fail(src);
        return true;
      }
      ptr = src.cursor();
    }
    const CharClass char_class =
        char_classes_[static_cast<unsigned char>(*ptr++)];
    if (ABSL_PREDICT_TRUE(char_class == CharClass::kOther)) continue;
    switch (char_class) {
      case CharClass::kComment:
        if (index == 0 && field.empty() && ptr - 1 == src.cursor()) {
          src.set_cursor(ptr);
          SkipLine(src);
          goto next_record;
        }
        continue;
      case CharClass::kEscape:
        continue;
      default:
        break;
    }
    const size_t length = PtrDistance(src.cursor(), ptr - 1);
    if (ABSL_PREDICT_FALSE(length > max_field_length_ - field.size())) {
      return MaxFieldLengthExceeded();
    }
    field.append(src.cursor(), length);
    src.set_cursor(ptr);
    switch (char_class) {
      case CharClass::kOther:
      case CharClass::kComment:
      case CharClass::kEscape:
        RIEGELI_ASSERT_UNREACHABLE() << "Handled before switch";
      case CharClass::kLf:
        ++line_number_;
        if (ABSL_PREDICT_FALSE(standalone_record_)) {
          return Fail(absl::DataLossError("Unexpected newline"));
        }
        return true;
      case CharClass::kCr:
        ++line_number_;
        if (ABSL_PREDICT_FALSE(standalone_record_)) {
          return Fail(absl::DataLossError("Unexpected newline"));
        }
        if (ABSL_PREDICT_FALSE(!src.Pull())) {
          if (ABSL_PREDICT_FALSE(!src.healthy())) return Fail(src);
          return true;
        }
        if (*src.cursor() == '\n') src.move_cursor(1);
        return true;
      case CharClass::kFieldSeparator:
        ++index;
        goto next_field;
      case CharClass::kQuote:
        if (ABSL_PREDICT_FALSE(!ReadQuoted(src, field))) return false;
        if (ABSL_PREDICT_FALSE(!src.Pull())) {
          if (ABSL_PREDICT_FALSE(!src.healthy())) return Fail(src);
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
            return Fail(
                absl::DataLossError("Unquoted data after closing quote"));
          case CharClass::kFieldSeparator:
            ++index;
            goto next_field;
          case CharClass::kLf:
            ++line_number_;
            if (ABSL_PREDICT_FALSE(standalone_record_)) {
              return Fail(absl::DataLossError("Unexpected newline"));
            }
            return true;
          case CharClass::kCr:
            ++line_number_;
            if (ABSL_PREDICT_FALSE(standalone_record_)) {
              return Fail(absl::DataLossError("Unexpected newline"));
            }
            if (ABSL_PREDICT_FALSE(!src.Pull())) {
              if (ABSL_PREDICT_FALSE(!src.healthy())) return Fail(src);
              return true;
            }
            if (*src.cursor() == '\n') src.move_cursor(1);
            return true;
          case CharClass::kQuote:
            RIEGELI_ASSERT_UNREACHABLE() << "Handled by ReadQuoted()";
        }
        RIEGELI_ASSERT_UNREACHABLE()
            << "Unknown character class: "
            << static_cast<int>(char_class_after_quoted);
    }
    RIEGELI_ASSERT_UNREACHABLE()
        << "Unknown character class: " << static_cast<int>(char_class);
  }
}

bool CsvReaderBase::ReadRecord(std::vector<std::string>& fields) {
  if (ABSL_PREDICT_FALSE(!healthy())) {
    fields.clear();
    return false;
  }
  Reader& src = *src_reader();
try_again:
  size_t index = 0;
  // Assign to existing elements of `fields` when possible and then `erase()`
  // excess elements, instead of calling `fields.clear()` upfront, to avoid
  // losing existing `std::string` allocations.
  if (ABSL_PREDICT_FALSE(!ReadFields(src, fields, index))) {
    if (recovery_ != nullptr && recoverable_) {
      recoverable_ = false;
      absl::Status status = this->status();
      MarkNotFailed();
      SkipLine(src);
      if (recovery_(std::move(status))) goto try_again;
    }
    fields.clear();
    return false;
  }
  fields.erase(fields.begin() + index + 1, fields.end());
  ++record_index_;
  return true;
}

}  // namespace riegeli
