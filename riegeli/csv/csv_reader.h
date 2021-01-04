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

#ifndef RIEGELI_CSV_CSV_READER_H_
#define RIEGELI_CSV_CSV_READER_H_

#include <stddef.h>
#include <stdint.h>

#include <array>
#include <functional>
#include <limits>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

class CsvReaderBase;

namespace internal {
bool ReadStandaloneRecord(CsvReaderBase& csv_reader,
                          std::vector<std::string>& fields);
}  // namespace internal

// Template parameter independent part of `CsvReader`.
class CsvReaderBase : public Object {
 public:
  class Options {
   public:
    Options() noexcept {}

    // Comment character.
    //
    // If not `absl::nullopt`, a line beginning with this character is skipped.
    //
    // Often used: '#'
    //
    // Default: `absl::nullopt`
    Options& set_comment(absl::optional<char> comment) & {
      RIEGELI_ASSERT(comment != '\n' && comment != '\r')
          << "Comment character conflicts with record separator";
      RIEGELI_ASSERT(comment != '"')
          << "Comment character conflicts with quote character";
      comment_ = comment;
      return *this;
    }
    Options&& set_comment(absl::optional<char> comment) && {
      return std::move(set_comment(comment));
    }
    absl::optional<char> comment() const { return comment_; }

    // Field separator.
    //
    // Default: ','
    Options& set_field_separator(char field_separator) & {
      RIEGELI_ASSERT(field_separator != '\n' && field_separator != '\r')
          << "Field separator conflicts with record separator";
      RIEGELI_ASSERT(field_separator != '"')
          << "Field separator conflicts with quote character";
      field_separator_ = field_separator;
      return *this;
    }
    Options&& set_field_separator(char field_separator) && {
      return std::move(set_field_separator(field_separator));
    }
    char field_separator() const { return field_separator_; }

    // Escape character.
    //
    // If not `absl::nullopt`, a character inside quotes preceded by escape is
    // treated literally instead of possibly having a special meaning.
    //
    // Default: `absl::nullopt`
    Options& set_escape(absl::optional<char> escape) & {
      RIEGELI_ASSERT(escape != '\n' && escape != '\r')
          << "Escape character conflicts with record separator";
      RIEGELI_ASSERT(escape != '"')
          << "Escape character conflicts with quote character";
      escape_ = escape;
      return *this;
    }
    Options&& set_escape(absl::optional<char> escape) && {
      return std::move(set_escape(escape));
    }
    absl::optional<char> escape() const { return escape_; }

    // Expected maximum number of fields.
    //
    // If this number is exceeded, reading fails with
    // `absl::ResourceExhaustedError()`.
    //
    // `max_num_fields` must be at least 1.
    // Default: `std::numeric_limits<size_t>::max()`.
    Options& set_max_num_fields(size_t max_num_fields) & {
      RIEGELI_ASSERT_GE(max_num_fields, 1u)
          << "Failed precondition of "
             "CsvReaderBase::Options::set_max_num_fields(): "
             "number of fields out of range";
      max_num_fields_ = max_num_fields;
      return *this;
    }
    Options&& set_max_num_fields(size_t max_num_fields) && {
      return std::move(set_max_num_fields(max_num_fields));
    }
    size_t max_num_fields() const { return max_num_fields_; }

    // Expected maximum field length.
    //
    // If this length is exceeded, reading fails with
    // `absl::ResourceExhaustedError()`.
    //
    // Default: `std::numeric_limits<size_t>::max()`.
    Options& set_max_field_length(size_t max_field_length) & {
      max_field_length_ = max_field_length;
      return *this;
    }
    Options&& set_max_field_length(size_t max_field_length) && {
      return std::move(set_max_field_length(max_field_length));
    }
    size_t max_field_length() const { return max_field_length_; }

    // Sets the recovery function to be called after skipping over an invalid
    // line.
    //
    // If the recovery function is set to `nullptr`, then an invalid line causes
    // `CsvReader` to fail.
    //
    // If the recovery function is set to a value other than `nullptr`, then
    // an invalid line causes `CsvReader` to skip over the invalid line and call
    // the recovery function. If the recovery function returns `true`, reading
    // continues. If the recovery function returns `false`, reading ends as if
    // the end of source was encountered.
    //
    // Calling `ReadRecord()` may cause the recovery function to be called (in
    // the same thread).
    //
    // Default: `nullptr`.
    Options& set_recovery(const std::function<bool(absl::Status)>& recovery) & {
      recovery_ = recovery;
      return *this;
    }
    Options& set_recovery(std::function<bool(absl::Status)>&& recovery) & {
      recovery_ = std::move(recovery);
      return *this;
    }
    Options&& set_recovery(
        const std::function<bool(absl::Status)>& recovery) && {
      return std::move(set_recovery(recovery));
    }
    Options&& set_recovery(std::function<bool(absl::Status)>&& recovery) && {
      return std::move(set_recovery(std::move(recovery)));
    }
    std::function<bool(absl::Status)>& recovery() & { return recovery_; }
    const std::function<bool(absl::Status)>& recovery() const& {
      return recovery_;
    }
    std::function<bool(absl::Status)>&& recovery() && {
      return std::move(recovery_);
    }
    const std::function<bool(absl::Status)>&& recovery() const&& {
      return std::move(recovery_);
    }

   private:
    absl::optional<char> comment_;
    char field_separator_ = ',';
    absl::optional<char> escape_;
    size_t max_num_fields_ = std::numeric_limits<size_t>::max();
    size_t max_field_length_ = std::numeric_limits<size_t>::max();
    std::function<bool(absl::Status)> recovery_;
  };

  // Returns the byte `Reader` being read from. Unchanged by `Close()`.
  virtual Reader* src_reader() = 0;
  virtual const Reader* src_reader() const = 0;

  // `CsvReader` overrides `Object::Fail()` to annotate the status with the
  // current line number. Derived classes which override it further should
  // include a call to `CsvReader::Fail()`.
  using Object::Fail;
  ABSL_ATTRIBUTE_COLD bool Fail(absl::Status status) override;

  // Reads the next record.
  //
  // Return values:
  //  * `true`                      - success (`fields` are set)
  //  * `false` (when `healthy()`)  - source ends (`fields` are empty)
  //  * `false` (when `!healthy()`) - failure (`fields` are empty)
  bool ReadRecord(std::vector<std::string>& fields);

  // The index of the most recently read record, starting from 0.
  //
  // `last_record_index()` is unchanged by `Close()`.
  //
  // Precondition: some `ReadRecord()` call succeeded.
  uint64_t last_record_index() const;

  // The index of the next record, starting from 0.
  //
  // `record_index()` is unchanged by `Close()`.
  uint64_t record_index() const { return record_index_; }

  // The number of the first line of the most recently read record (or attempted
  // to be read), starting from 1.
  //
  // A line is terminated by LF, CR, or CR LF  ("\n", "\r", or "\r\n").
  //
  // `last_line_number()` is unchanged by `Close()`.
  //
  // Precondition: `ReadRecord()` was called.
  int64_t last_line_number() const;

  // The number of the next line, starting from 1.
  //
  // A line is terminated by LF, CR, or CR LF  ("\n", "\r", or "\r\n").
  //
  // `line_number()` is unchanged by `Close()`.
  int64_t line_number() const { return line_number_; }

 protected:
  explicit CsvReaderBase(InitiallyClosed) noexcept;
  explicit CsvReaderBase(InitiallyOpen) noexcept;

  CsvReaderBase(CsvReaderBase&& that) noexcept;
  CsvReaderBase& operator=(CsvReaderBase&& that) noexcept;

  void Reset(InitiallyClosed);
  void Reset(InitiallyOpen);
  void Initialize(Reader* src, Options&& options);

  // Exposes a `Fail()` override which does not annotate the status with the
  // current position, unlike the public `CsvReader::Fail()`.
  ABSL_ATTRIBUTE_COLD bool FailWithoutAnnotation(absl::Status status);
  ABSL_ATTRIBUTE_COLD bool FailWithoutAnnotation(const Object& dependency);

 private:
  friend bool internal::ReadStandaloneRecord(CsvReaderBase& csv_reader,
                                             std::vector<std::string>& fields);

  enum class CharClass : uint8_t {
    kOther,
    kLf,
    kCr,
    kComment,
    kFieldSeparator,
    kQuote,
    kEscape,
  };

  ABSL_ATTRIBUTE_COLD bool MaxFieldLengthExceeded();
  void SkipLine(Reader& src);
  bool ReadQuoted(Reader& src, std::string& field);
  template <bool standalone_record>
  bool ReadFields(Reader& src, std::vector<std::string>& fields,
                  size_t& field_index);
  template <bool standalone_record>
  bool ReadRecordInternal(std::vector<std::string>& fields);

  // Lookup table for interpreting source characters.
  std::array<CharClass, std::numeric_limits<unsigned char>::max() + 1>
      char_classes_{};
  size_t max_num_fields_ = 0;
  size_t max_field_length_ = 0;
  std::function<bool(absl::Status)> recovery_;
  uint64_t record_index_ = 0;
  int64_t last_line_number_ = 0;
  int64_t line_number_ = 0;
  bool recoverable_ = false;
};

// `CsvReader` reads records of a CSV (comma-separated values) file.
//
// A basic variant of CSV is specified in https://tools.ietf.org/html/rfc4180.
// `CsvReader` reads RFC4180-compliant CSV files, and also supports some
// extensions.
//
// A record is terminated by a newline: LF, CR, or CR LF ("\n", "\r", or
// "\r\n"). Line terminator after the last record is optional.
//
// A record consists of a sequence of fields separated by a field separator
// (usually ',' or '\t'). Each record contains at least one field. In particular
// an empty line is interpreted as one empty field, except that an empty line
// after the last line terminator is not considered a record.
//
// By a common convention the first record consists of field names. This should
// be handled by the application; `CsvReader` does not treat the first record
// specially.
//
// Quotes ('"') around a field allow expressing special characters inside the
// field: field separator, LF, CR, or quote itself.
//
// To express a quote inside a quoted field, it must be written twice or
// preceded by an escape character.
//
// If an escape character is used (usually it is not), a character inside quotes
// preceded by escape is treated literally instead of possibly having a special
// meaning.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the byte `Reader`. `Src` must support
// `Dependency<Reader*, Src>`, e.g. `Reader*` (not owned, default),
// `std::unique_ptr<Reader>` (owned), `ChainReader<>` (owned).
//
// The current position is synchronized with the byte `Reader` between records.
template <typename Src = Reader*>
class CsvReader : public CsvReaderBase {
 public:
  // Creates a closed `CsvReader`.
  CsvReader() noexcept : CsvReaderBase(kInitiallyClosed) {}

  // Will read from the byte `Reader` provided by `src`.
  explicit CsvReader(const Src& src, Options options = Options());
  explicit CsvReader(Src&& src, Options options = Options());

  // Will read from the byte `Reader` provided by a `Src` constructed from
  // elements of `src_args`. This avoids constructing a temporary `Src` and
  // moving from it.
  template <typename... SrcArgs>
  explicit CsvReader(std::tuple<SrcArgs...> src_args,
                     Options options = Options());

  CsvReader(CsvReader&& that) noexcept;
  CsvReader& operator=(CsvReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `CsvReader`. This avoids
  // constructing a temporary `CsvReader` and moving from it.
  void Reset();
  void Reset(const Src& src, Options options = Options());
  void Reset(Src&& src, Options options = Options());
  template <typename... SrcArgs>
  void Reset(std::tuple<SrcArgs...> src_args, Options options = Options());

  // Returns the object providing and possibly owning the byte `Reader`.
  // Unchanged by `Close()`.
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  Reader* src_reader() override { return src_.get(); }
  const Reader* src_reader() const override { return src_.get(); }

 protected:
  void Done() override;

 private:
  // The object providing and possibly owning the byte `Reader`.
  Dependency<Reader*, Src> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
template <typename Src>
CsvReader(Src&& src, CsvReaderBase::Options options = CsvReaderBase::Options())
    -> CsvReader<std::decay_t<Src>>;
template <typename... SrcArgs>
CsvReader(std::tuple<SrcArgs...> src_args,
          CsvReaderBase::Options options = CsvReaderBase::Options())
    -> CsvReader<void>;  // Delete.
#endif

// Reads a single record from a CSV string.
//
// A record terminator must not be present.
absl::Status ReadCsvRecordFromString(
    absl::string_view src, std::vector<std::string>& fields,
    CsvReaderBase::Options options = CsvReaderBase::Options());

// Implementation details follow.

inline CsvReaderBase::CsvReaderBase(InitiallyClosed) noexcept
    : Object(kInitiallyClosed) {}

inline CsvReaderBase::CsvReaderBase(InitiallyOpen) noexcept
    : Object(kInitiallyOpen) {}

inline CsvReaderBase::CsvReaderBase(CsvReaderBase&& that) noexcept
    : Object(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      char_classes_(that.char_classes_),
      max_num_fields_(that.max_num_fields_),
      max_field_length_(that.max_field_length_),
      recovery_(std::move(that.recovery_)),
      record_index_(std::exchange(that.record_index_, 0)),
      last_line_number_(std::exchange(that.last_line_number_, 0)),
      line_number_(std::exchange(that.line_number_, 0)),
      recoverable_(std::exchange(that.recoverable_, false)) {}

inline CsvReaderBase& CsvReaderBase::operator=(CsvReaderBase&& that) noexcept {
  Object::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  char_classes_ = that.char_classes_;
  max_num_fields_ = that.max_num_fields_;
  max_field_length_ = that.max_field_length_;
  recovery_ = std::move(that.recovery_);
  record_index_ = std::exchange(that.record_index_, 0);
  last_line_number_ = std::exchange(that.last_line_number_, 0);
  line_number_ = std::exchange(that.line_number_, 0);
  recoverable_ = std::exchange(that.recoverable_, false);
  return *this;
}

inline void CsvReaderBase::Reset(InitiallyClosed) {
  Object::Reset(kInitiallyClosed);
}

inline void CsvReaderBase::Reset(InitiallyOpen) {
  Object::Reset(kInitiallyOpen);
  char_classes_ = {};
}

inline uint64_t CsvReaderBase::last_record_index() const {
  RIEGELI_ASSERT_NE(record_index_, 0u)
      << "Failed precondition of CsvReaderBase::last_record_index(): "
         "no record was read";
  return record_index_ - 1;
}

inline int64_t CsvReaderBase::last_line_number() const {
  RIEGELI_ASSERT_NE(last_line_number_, 0)
      << "Failed precondition of CsvReaderBase::last_line_number(): "
         "no record was read or attempted to be read";
  return last_line_number_;
}

template <typename Src>
inline CsvReader<Src>::CsvReader(const Src& src, Options options)
    : CsvReaderBase(kInitiallyOpen), src_(src) {
  Initialize(src_.get(), std::move(options));
}

template <typename Src>
inline CsvReader<Src>::CsvReader(Src&& src, Options options)
    : CsvReaderBase(kInitiallyOpen), src_(std::move(src)) {
  Initialize(src_.get(), std::move(options));
}

template <typename Src>
template <typename... SrcArgs>
inline CsvReader<Src>::CsvReader(std::tuple<SrcArgs...> src_args,
                                 Options options)
    : CsvReaderBase(kInitiallyOpen), src_(std::move(src_args)) {
  Initialize(src_.get(), std::move(options));
}

template <typename Src>
inline CsvReader<Src>::CsvReader(CsvReader&& that) noexcept
    : CsvReaderBase(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      src_(std::move(that.src_)) {}

template <typename Src>
inline CsvReader<Src>& CsvReader<Src>::operator=(CsvReader&& that) noexcept {
  CsvReaderBase::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  src_ = std::move(that.src_);
  return *this;
}

template <typename Src>
inline void CsvReader<Src>::Reset() {
  CsvReaderBase::Reset(kInitiallyClosed);
  src_.Reset();
}

template <typename Src>
inline void CsvReader<Src>::Reset(const Src& src, Options options) {
  CsvReaderBase::Reset(kInitiallyOpen);
  src_.Reset(src);
  Initialize(src_.get(), std::move(options));
}

template <typename Src>
inline void CsvReader<Src>::Reset(Src&& src, Options options) {
  CsvReaderBase::Reset(kInitiallyOpen);
  src_.Reset(std::move(src));
  Initialize(src_.get(), std::move(options));
}

template <typename Src>
template <typename... SrcArgs>
inline void CsvReader<Src>::Reset(std::tuple<SrcArgs...> src_args,
                                  Options options) {
  CsvReaderBase::Reset(kInitiallyOpen);
  src_.Reset(std::move(src_args));
  Initialize(src_.get(), std::move(options));
}

template <typename Src>
void CsvReader<Src>::Done() {
  CsvReaderBase::Done();
  if (src_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) Fail(*src_);
  }
}

}  // namespace riegeli

#endif  // RIEGELI_CSV_CSV_READER_H_
