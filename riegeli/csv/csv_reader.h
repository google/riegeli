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
#include <initializer_list>
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
#include "riegeli/base/assert.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/csv/csv_record.h"

namespace riegeli {

class CsvReaderBase;

namespace csv_internal {

bool ReadStandaloneRecord(CsvReaderBase& csv_reader,
                          std::vector<std::string>& record);

}  // namespace csv_internal

// Template parameter independent part of `CsvReader`.
class CsvReaderBase : public Object {
 public:
  class Options {
   public:
    Options() noexcept {}

    // If not `absl::nullopt`, automatically reads field names from the first
    // record, specifies how field names are normalized, and verifies that all
    // required fields are present (in any order).
    //
    // In this case `ReadRecord(CsvRecord&)` is supported. Otherwise no
    // particular header is assumed, and only `ReadRecord()` to a vector of
    // fields is supported.
    //
    // `set_required_header({})` specifies an empty set of required fields and
    // thus accepts any field names.
    //
    // If the file is empty, actual field names have duplicates, or some
    // required fields are not present, reading the header fails.
    //
    // `required_header()` and `assumed_header()` must not be both set.
    //
    // Default: `absl::nullopt`.
    Options& set_required_header(absl::optional<CsvHeader> header) & {
      required_header_ = std::move(header);
      return *this;
    }
    Options&& set_required_header(absl::optional<CsvHeader> header) && {
      return std::move(set_required_header(std::move(header)));
    }
    Options& set_required_header(
        std::initializer_list<absl::string_view> names) & {
      return set_required_header(CsvHeader(names));
    }
    Options&& set_required_header(
        std::initializer_list<absl::string_view> names) && {
      return std::move(set_required_header(names));
    }
    absl::optional<CsvHeader>& required_header() { return required_header_; }
    const absl::optional<CsvHeader>& required_header() const {
      return required_header_;
    }

    // If not `absl::nullopt`, a header is not read from the file, but
    // `ReadRecord(CsvRecord&)` is supported as if this header was present as
    // the first record.
    //
    // `required_header()` and `assumed_header()` must not be both set.
    //
    // Default: `absl::nullopt`.
    Options& set_assumed_header(absl::optional<CsvHeader> header) & {
      assumed_header_ = std::move(header);
      return *this;
    }
    Options&& set_assumed_header(absl::optional<CsvHeader> header) && {
      return std::move(set_assumed_header(std::move(header)));
    }
    Options& set_assumed_header(
        std::initializer_list<absl::string_view> names) & {
      return set_assumed_header(CsvHeader(names));
    }
    Options&& set_assumed_header(
        std::initializer_list<absl::string_view> names) && {
      return std::move(set_assumed_header(names));
    }
    absl::optional<CsvHeader>& assumed_header() { return assumed_header_; }
    const absl::optional<CsvHeader>& assumed_header() const {
      return assumed_header_;
    }

    // If `false`, an initial UTF-8 BOM is skipped if present.
    //
    // If `true`, an initial UTF-8 BOM if present is treated as a part of the
    // first field in the first record. This is unlikely to be the intent, but
    // this conforms to RFC4180.
    //
    // Default: `false`.
    Options& set_preserve_utf8_bom(bool preserve_utf8_bom) & {
      preserve_utf8_bom_ = preserve_utf8_bom;
      return *this;
    }
    Options&& set_preserve_utf8_bom(bool preserve_utf8_bom) && {
      return std::move(set_preserve_utf8_bom(preserve_utf8_bom));
    }
    bool preserve_utf8_bom() const { return preserve_utf8_bom_; }

    // If `false`, an empty line is interpreted as a record with one empty
    // field. This conforms to RFC4180.
    //
    // If `true`, empty lines are skipped.
    //
    // Default: `false`.
    Options& set_skip_empty_lines(bool skip_empty_lines) & {
      skip_empty_lines_ = skip_empty_lines;
      return *this;
    }
    Options&& set_skip_empty_lines(bool skip_empty_lines) && {
      return std::move(set_skip_empty_lines(skip_empty_lines));
    }
    bool skip_empty_lines() const { return skip_empty_lines_; }

    // Comment character.
    //
    // If not `absl::nullopt`, a line beginning with this character is skipped.
    // This is not covered by RFC4180.
    //
    // Often used: '#'.
    //
    // Default: `absl::nullopt`.
    Options& set_comment(absl::optional<char> comment) & {
      comment_ = comment;
      return *this;
    }
    Options&& set_comment(absl::optional<char> comment) && {
      return std::move(set_comment(comment));
    }
    absl::optional<char> comment() const { return comment_; }

    // Field separator.
    //
    // Default: ','.
    Options& set_field_separator(char field_separator) & {
      field_separator_ = field_separator;
      return *this;
    }
    Options&& set_field_separator(char field_separator) && {
      return std::move(set_field_separator(field_separator));
    }
    char field_separator() const { return field_separator_; }

    // Quote character.
    //
    // Quotes around a field allow expressing special characters inside the
    // field: LF, CR, comment character, field separator, or quote character
    // itself.
    //
    // To express a quote itself inside a field, it must be written twice when
    // the field is quoted, or preceded by an escape character.
    //
    // If `quote()` and `escape()` are both `absl::nullopt`, special characters
    // inside fields are not expressible.
    //
    // Default: '"'.
    Options& set_quote(absl::optional<char> quote) & {
      quote_ = quote;
      return *this;
    }
    Options&& set_quote(absl::optional<char> quote) && {
      return std::move(set_quote(quote));
    }
    absl::optional<char> quote() const { return quote_; }

    // Escape character.
    //
    // If not `absl::nullopt`, a character preceded by escape is treated
    // literally instead of possibly having a special meaning. This allows
    // expressing special characters inside a field: LF, CR, comment character,
    // field separator, or escape character itself. This is not covered by
    // RFC4180.
    //
    // If `quote()` and `escape()` are both `absl::nullopt`, special characters
    // inside fields are not expressible.
    //
    // Default: `absl::nullopt`.
    Options& set_escape(absl::optional<char> escape) & {
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
    //
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

    // Recovery function called after skipping over an invalid line.
    //
    // If `nullptr`, then an invalid line causes `CsvReader` to fail.
    //
    // If not `nullptr`, then an invalid line causes `CsvReader` to skip over
    // the invalid line and call the recovery function. If the recovery function
    // returns `true`, reading continues. If the recovery function returns
    // `false`, reading ends as if the end of source was encountered.
    //
    // Recovery is not applicable to reading the header with
    // `Options::required_header() != absl::nullopt`.
    //
    // Calling `ReadRecord()` may cause the recovery function to be called (in
    // the same thread).
    //
    // Default: `nullptr`.
    Options& set_recovery(
        const std::function<bool(absl::Status, CsvReaderBase&)>& recovery) & {
      recovery_ = recovery;
      return *this;
    }
    Options&& set_recovery(
        const std::function<bool(absl::Status, CsvReaderBase&)>& recovery) && {
      return std::move(set_recovery(recovery));
    }
    Options& set_recovery(
        std::function<bool(absl::Status, CsvReaderBase&)>&& recovery) & {
      recovery_ = std::move(recovery);
      return *this;
    }
    Options&& set_recovery(
        std::function<bool(absl::Status, CsvReaderBase&)>&& recovery) && {
      return std::move(set_recovery(std::move(recovery)));
    }
    std::function<bool(absl::Status, CsvReaderBase&)>& recovery() {
      return recovery_;
    }
    const std::function<bool(absl::Status, CsvReaderBase&)>& recovery() const {
      return recovery_;
    }

   private:
    absl::optional<CsvHeader> required_header_;
    absl::optional<CsvHeader> assumed_header_;
    bool preserve_utf8_bom_ = false;
    bool skip_empty_lines_ = false;
    absl::optional<char> comment_;
    char field_separator_ = ',';
    absl::optional<char> quote_ = '"';
    absl::optional<char> escape_;
    size_t max_num_fields_ = std::numeric_limits<size_t>::max();
    size_t max_field_length_ = std::numeric_limits<size_t>::max();
    std::function<bool(absl::Status, CsvReaderBase&)> recovery_;
  };

  // Returns the byte `Reader` being read from. Unchanged by `Close()`.
  virtual Reader* SrcReader() const = 0;

  // Changes the recovery function to be called after skipping over an invalid
  // line.
  //
  // See `Options::set_recovery()` for details.
  void set_recovery(
      const std::function<bool(absl::Status, CsvReaderBase&)>& recovery) {
    recovery_ = recovery;
  }
  void set_recovery(
      std::function<bool(absl::Status, CsvReaderBase&)>&& recovery) {
    recovery_ = std::move(recovery);
  }

  // Returns `true` if reading the header was requested or assumed, i.e.
  // `Options::required_header() != absl::nullopt ||
  //  Options::assumed_header() != absl::nullopt`.
  //
  // In this case `ReadRecord(CsvRecord&)` is supported. Otherwise no particular
  // header is assumed, and only `ReadRecord(std::vector<std::string>&)` is
  // supported.
  bool has_header() const { return has_header_; }

  // If `has_header()`, returns field names read from the first record. Returns
  // an empty header if reading the header failed.
  //
  // If `!has_header()`, returns an empty header.
  const CsvHeader& header() const { return header_; }

  // Reads the next record expressed as `CsvRecord`, with named fields.
  //
  // The old value of `record`, including `record.header()`, is overwritten.
  //
  // If the number of fields read is not the same as expected by the header,
  // `CsvReader` fails.
  //
  // If `ReadRecord()` returns `true`, `record` will contain all fields present
  // in the `header()`, and thus it is safe to access fields whose presence has
  // been verified in the `header()`.
  //
  // Precondition:
  //   `has_header()`, i.e. `Options::required_header() != absl::nullopt ||
  //                         Options::assumed_hedaer() != absl::nullopt`
  //
  // Return values:
  //  * `true`                 - success (`record` is set)
  //  * `false` (when `ok()`)  - source ends (`record` is empty)
  //  * `false` (when `!ok()`) - failure (`record` is empty)
  bool ReadRecord(CsvRecord& record);

  // Reads the next record expressed as a vector of fields.
  //
  // By a common convention each record should consist of the same number of
  // fields, but this is not enforced.
  //
  // Return values:
  //  * `true`                 - success (`record` is set)
  //  * `false` (when `ok()`)  - source ends (`record` is empty)
  //  * `false` (when `!ok()`) - failure (`record` is empty)
  bool ReadRecord(std::vector<std::string>& record);

  // Determines if a record follows without reading it, but skips intervening
  // comments.
  //
  // Return values:
  //  * `true`  - `ReadRecord()` would read the next record or fail
  //  * `false` - `ReadRecord()` would report that source ends or fail
  bool HasNextRecord();

  // The index of the most recently read record, starting from 0.
  //
  // The record count does not include any header read with
  // `Options::required_header() != absl::nullopt`.
  //
  // `last_record_index()` is unchanged by `Close()`.
  //
  // Precondition: some record was successfully read (`record_index() > 0`).
  uint64_t last_record_index() const;

  // The index of the next record, starting from 0.
  //
  // The record count does not include any header read with
  // `Options::required_header() != absl::nullopt`.
  //
  // `record_index()` is unchanged by `Close()`.
  uint64_t record_index() const { return record_index_; }

  // The number of the first line of the most recently read record (or attempted
  // to be read), starting from 1.
  //
  // This is 1 if no record was attempted to be read.
  //
  // A line is terminated by LF, CR, or CR-LF ("\n", "\r", or "\r\n").
  //
  // `last_line_number()` is unchanged by `Close()`.
  int64_t last_line_number() const { return last_line_number_; }

  // The number of the next line, starting from 1.
  //
  // A line is terminated by LF, CR, or CR-LF ("\n", "\r", or "\r\n").
  //
  // `line_number()` is unchanged by `Close()`.
  int64_t line_number() const { return line_number_; }

 protected:
  using Object::Object;

  CsvReaderBase(CsvReaderBase&& that) noexcept;
  CsvReaderBase& operator=(CsvReaderBase&& that) noexcept;

  void Reset(Closed);
  void Reset();
  void Initialize(Reader* src, Options&& options);
  // Fails, attributing this to `last_line_number()` instead of `line_number()`.
  ABSL_ATTRIBUTE_COLD void FailAtPreviousRecord(absl::Status status);
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateOverSrc(absl::Status status);

  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;

 private:
  friend bool csv_internal::ReadStandaloneRecord(
      CsvReaderBase& csv_reader, std::vector<std::string>& record);

  enum class CharClass : uint8_t {
    kOther,
    kLf,
    kCr,
    kComment,
    kFieldSeparator,
    kQuote,
    kEscape,
  };

  ABSL_ATTRIBUTE_COLD bool FailMaxFieldLengthExceeded();
  void SkipLine(Reader& src);
  bool ReadQuoted(Reader& src, std::string& field);
  bool ReadFields(Reader& src, std::vector<std::string>& fields,
                  size_t& field_index);
  bool ReadRecordInternal(std::vector<std::string>& record);

  bool standalone_record_ = false;
  bool has_header_ = false;
  CsvHeader header_;
  // Lookup table for interpreting source characters.
  std::array<CharClass, std::numeric_limits<unsigned char>::max() + 1>
      char_classes_{};
  bool skip_empty_lines_ = false;
  // Meaningful if `char_classes_` contains `CharClass::kQuote`.
  char quote_ = '\0';
  size_t max_num_fields_ = 0;
  size_t max_field_length_ = 0;
  std::function<bool(absl::Status, CsvReaderBase&)> recovery_;
  uint64_t record_index_ = 0;
  int64_t last_line_number_ = 1;
  int64_t line_number_ = 1;
  bool recoverable_ = false;
};

// `CsvReader` reads records of a CSV (comma-separated values) file.
//
// A basic variant of CSV is specified in https://tools.ietf.org/html/rfc4180,
// and some common extensions are described in
// https://specs.frictionlessdata.io/csv-dialect/.
//
// `CsvReader` reads RFC4180-compliant CSV files, and also supports some
// extensions.
//
// By a common convention the first record consists of field names. This is
// supported by `Options::required_header()` and `ReadRecord(CsvRecord&)`.
//
// A record is terminated by a newline: LF, CR, or CR-LF ("\n", "\r", or
// "\r\n"). Line terminator after the last record is optional.
//
// If skipping empty lines is requested (usually it is not), empty lines are
// skipped. If a comment character is set (usually it is not), a line beginning
// with the comment character is skipped.
//
// A record consists of a sequence of fields separated by a field separator
// (usually ',' or '\t'). Each record contains at least one field.
//
// Quotes (usually '"') around a field allow expressing special characters
// inside the field: LF, CR, comment character, field separator, or quote
// character itself.
//
// If an escape character is set (usually it is not), a character preceded by
// escape is treated literally instead of possibly having a special meaning.
// This is an alternative way of expressing special characters inside a field.
//
// To express a quote itself inside a field, it must be written twice when the
// field is quoted, or preceded by an escape character.
//
// Quotes are also useful for unambiguous interpretation of a record consisting
// of a single empty field or beginning with UTF-8 BOM.
//
// If neither a quote character nor an escape character is set, special
// characters inside fields are not expressible. In this case, reading a record
// consisting of a single empty field is incompatible with
// `Options::skip_empty_lines()`, and reading the first record beginning with
// UTF-8 BOM requires `Options::set_preserve_utf8_bom()`.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the byte `Reader`. `Src` must support
// `Dependency<Reader*, Src>`, e.g. `Reader*` (not owned, default),
// `ChainReader<>` (owned), `std::unique_ptr<Reader>` (owned),
// `AnyDependency<Reader*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as the value type of
// the first constructor argument. This requires C++17.
//
// The current position is synchronized with the byte `Reader` between records.
template <typename Src = Reader*>
class CsvReader : public CsvReaderBase {
 public:
  // Creates a closed `CsvReader`.
  explicit CsvReader(Closed) noexcept : CsvReaderBase(kClosed) {}

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
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(const Src& src,
                                          Options options = Options());
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Src&& src,
                                          Options options = Options());
  template <typename... SrcArgs>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(std::tuple<SrcArgs...> src_args,
                                          Options options = Options());

  // Returns the object providing and possibly owning the byte `Reader`.
  // Unchanged by `Close()`.
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  Reader* SrcReader() const override { return src_.get(); }

 protected:
  void Done() override;

 private:
  // The object providing and possibly owning the byte `Reader`.
  Dependency<Reader*, Src> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit CsvReader(Closed) -> CsvReader<DeleteCtad<Closed>>;
template <typename Src>
explicit CsvReader(const Src& src,
                   CsvReaderBase::Options options = CsvReaderBase::Options())
    -> CsvReader<std::decay_t<Src>>;
template <typename Src>
explicit CsvReader(Src&& src,
                   CsvReaderBase::Options options = CsvReaderBase::Options())
    -> CsvReader<std::decay_t<Src>>;
template <typename... SrcArgs>
explicit CsvReader(std::tuple<SrcArgs...> src_args,
                   CsvReaderBase::Options options = CsvReaderBase::Options())
    -> CsvReader<DeleteCtad<std::tuple<SrcArgs...>>>;
#endif

// Reads a single record from a CSV string.
//
// A record terminator must not be present in the string.
//
// Precondition: `options.required_header() == absl::nullopt`
absl::Status ReadCsvRecordFromString(
    absl::string_view src, std::vector<std::string>& record,
    CsvReaderBase::Options options = CsvReaderBase::Options());

// Implementation details follow.

inline CsvReaderBase::CsvReaderBase(CsvReaderBase&& that) noexcept
    : Object(static_cast<Object&&>(that)),
      standalone_record_(that.standalone_record_),
      has_header_(that.has_header_),
      header_(std::move(that.header_)),
      char_classes_(that.char_classes_),
      skip_empty_lines_(that.skip_empty_lines_),
      quote_(that.quote_),
      max_num_fields_(that.max_num_fields_),
      max_field_length_(that.max_field_length_),
      recovery_(std::move(that.recovery_)),
      record_index_(std::exchange(that.record_index_, 0)),
      last_line_number_(std::exchange(that.last_line_number_, 1)),
      line_number_(std::exchange(that.line_number_, 1)),
      recoverable_(std::exchange(that.recoverable_, false)) {}

inline CsvReaderBase& CsvReaderBase::operator=(CsvReaderBase&& that) noexcept {
  Object::operator=(static_cast<Object&&>(that));
  standalone_record_ = that.standalone_record_;
  has_header_ = that.has_header_;
  header_ = std::move(that.header_);
  char_classes_ = that.char_classes_;
  skip_empty_lines_ = that.skip_empty_lines_;
  quote_ = that.quote_;
  max_num_fields_ = that.max_num_fields_;
  max_field_length_ = that.max_field_length_;
  recovery_ = std::move(that.recovery_);
  record_index_ = std::exchange(that.record_index_, 0);
  last_line_number_ = std::exchange(that.last_line_number_, 1);
  line_number_ = std::exchange(that.line_number_, 1);
  recoverable_ = std::exchange(that.recoverable_, false);
  return *this;
}

inline void CsvReaderBase::Reset(Closed) {
  Object::Reset(kClosed);
  standalone_record_ = false;
  has_header_ = false;
  header_.Reset();
  recovery_ = nullptr;
  record_index_ = 0;
  last_line_number_ = 1;
  line_number_ = 1;
  recoverable_ = false;
}

inline void CsvReaderBase::Reset() {
  Object::Reset();
  standalone_record_ = false;
  has_header_ = false;
  header_.Reset();
  char_classes_ = {};
  recovery_ = nullptr;
  record_index_ = 0;
  last_line_number_ = 1;
  line_number_ = 1;
  recoverable_ = false;
}

inline uint64_t CsvReaderBase::last_record_index() const {
  RIEGELI_ASSERT_GT(record_index_, 0u)
      << "Failed precondition of CsvReaderBase::last_record_index(): "
         "no record was read";
  return record_index_ - 1;
}

template <typename Src>
inline CsvReader<Src>::CsvReader(const Src& src, Options options) : src_(src) {
  Initialize(src_.get(), std::move(options));
}

template <typename Src>
inline CsvReader<Src>::CsvReader(Src&& src, Options options)
    : src_(std::move(src)) {
  Initialize(src_.get(), std::move(options));
}

template <typename Src>
template <typename... SrcArgs>
inline CsvReader<Src>::CsvReader(std::tuple<SrcArgs...> src_args,
                                 Options options)
    : src_(std::move(src_args)) {
  Initialize(src_.get(), std::move(options));
}

template <typename Src>
inline CsvReader<Src>::CsvReader(CsvReader&& that) noexcept
    : CsvReaderBase(static_cast<CsvReaderBase&&>(that)),
      src_(std::move(that.src_)) {}

template <typename Src>
inline CsvReader<Src>& CsvReader<Src>::operator=(CsvReader&& that) noexcept {
  CsvReaderBase::operator=(CsvReaderBase(that));
  src_ = std::move(that.src_);
  return *this;
}

template <typename Src>
inline void CsvReader<Src>::Reset(Closed) {
  CsvReaderBase::Reset(kClosed);
  src_.Reset();
}

template <typename Src>
inline void CsvReader<Src>::Reset(const Src& src, Options options) {
  CsvReaderBase::Reset();
  src_.Reset(src);
  Initialize(src_.get(), std::move(options));
}

template <typename Src>
inline void CsvReader<Src>::Reset(Src&& src, Options options) {
  CsvReaderBase::Reset();
  src_.Reset(std::move(src));
  Initialize(src_.get(), std::move(options));
}

template <typename Src>
template <typename... SrcArgs>
inline void CsvReader<Src>::Reset(std::tuple<SrcArgs...> src_args,
                                  Options options) {
  CsvReaderBase::Reset();
  src_.Reset(std::move(src_args));
  Initialize(src_.get(), std::move(options));
}

template <typename Src>
void CsvReader<Src>::Done() {
  CsvReaderBase::Done();
  if (src_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) {
      FailWithoutAnnotation(AnnotateOverSrc(src_->status()));
    }
  }
}

}  // namespace riegeli

#endif  // RIEGELI_CSV_CSV_READER_H_
