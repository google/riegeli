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

#ifndef RIEGELI_CSV_CSV_WRITER_H_
#define RIEGELI_CSV_CSV_WRITER_H_

#include <stddef.h>
#include <stdint.h>

#include <array>
#include <initializer_list>
#include <limits>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/iterable.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/object.h"
#include "riegeli/base/reset.h"
#include "riegeli/bytes/string_writer.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/csv/csv_record.h"
#include "riegeli/lines/line_writing.h"
#include "riegeli/lines/newline.h"

namespace riegeli {

class CsvWriterBase;

namespace csv_internal {

template <typename Fields>
bool WriteStandaloneRecord(const Fields& record, CsvWriterBase& csv_writer);

}  // namespace csv_internal

// Template parameter independent part of `CsvWriter`.
class CsvWriterBase : public Object {
 public:
  class Options {
   public:
    Options() noexcept {}

    // If not `std::nullopt`, sets field names, and automatically writes them
    // as the first record.
    //
    // In this case `WriteRecord(CsvRecord)` is supported. Otherwise no
    // particular header is assumed, and only `WriteRecord()` from a sequence of
    // fields is supported.
    //
    // The CSV format does not support empty records. A header with no fields
    // will be written as an empty line, which will be read as a header
    // consisting of one empty field, or will be skipped if
    // `CsvReaderBase::Options::skip_empty_lines()`.
    //
    // Default: `std::nullopt`.
    Options& set_header(Initializer<std::optional<CsvHeader>> header) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      riegeli::Reset(header_, std::move(header));
      return *this;
    }
    Options&& set_header(Initializer<std::optional<CsvHeader>> header) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_header(std::move(header)));
    }
    Options& set_header(std::initializer_list<absl::string_view> names) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return set_header(Initializer<std::optional<CsvHeader>>(names));
    }
    Options&& set_header(std::initializer_list<absl::string_view> names) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_header(names));
    }
    std::optional<CsvHeader>& header() ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return header_;
    }
    const std::optional<CsvHeader>& header() const
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return header_;
    }

    // If not `std::nullopt`, a header is not written to the file, but
    // `WriteRecord(CsvRecord&)` is supported as if this header was written as
    // the first record.
    //
    // `header()` and `assumed_header()` must not be both set.
    //
    // Default: `std::nullopt`.
    Options& set_assumed_header(Initializer<std::optional<CsvHeader>> header) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      riegeli::Reset(assumed_header_, std::move(header));
      return *this;
    }
    Options&& set_assumed_header(
        Initializer<std::optional<CsvHeader>> header) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_assumed_header(std::move(header)));
    }
    Options& set_assumed_header(
        std::initializer_list<absl::string_view> names) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return set_assumed_header(Initializer<std::optional<CsvHeader>>(names));
    }
    Options&& set_assumed_header(
        std::initializer_list<absl::string_view> names) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_assumed_header(names));
    }
    std::optional<CsvHeader>& assumed_header() ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return assumed_header_;
    }
    const std::optional<CsvHeader>& assumed_header() const
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return assumed_header_;
    }

    // If `false`, does not write initial UTF-8 BOM. This conforms to RFC4180
    // and UTF-8 BOM is normally not used on Unix.
    //
    // If `true`, writes initial UTF-8 BOM. Microsoft Excel by default expects
    // UTF-8 BOM in order to recognize UTF-8 contents without prompting for the
    // encoding.
    //
    // By default `CsvReader` will understand files written with any option.
    //
    // Default: `false`.
    Options& set_write_utf8_bom(bool write_utf8_bom) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      write_utf8_bom_ = write_utf8_bom;
      return *this;
    }
    Options&& set_write_utf8_bom(bool write_utf8_bom) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_write_utf8_bom(write_utf8_bom));
    }
    bool write_utf8_bom() const { return write_utf8_bom_; }

    // Record terminator.
    //
    // RFC4180 requires `WriteNewline::kCrLf` but Unix normally uses `kLf`.
    // `CsvReader` will understand files written with any option.
    //
    // Default: `WriteNewline::kNative`.
    Options& set_newline(WriteNewline newline) & ABSL_ATTRIBUTE_LIFETIME_BOUND {
      newline_ = newline;
      return *this;
    }
    Options&& set_newline(WriteNewline newline) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_newline(newline));
    }
    WriteNewline newline() const { return newline_; }

    // Comment character.
    //
    // If not `std::nullopt`, fields containing this character will be quoted.
    // This is not covered by RFC4180.
    //
    // Often used: '#'.
    //
    // Default: `std::nullopt`.
    Options& set_comment(std::optional<char> comment) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      comment_ = comment;
      return *this;
    }
    Options&& set_comment(std::optional<char> comment) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_comment(comment));
    }
    std::optional<char> comment() const { return comment_; }

    // Field separator.
    //
    // Default: ','.
    Options& set_field_separator(char field_separator) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      field_separator_ = field_separator;
      return *this;
    }
    Options&& set_field_separator(char field_separator) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_field_separator(field_separator));
    }
    char field_separator() const { return field_separator_; }

    // Quote character.
    //
    // Quotes around a field allow expressing special characters inside the
    // field: LF, CR, comment character, field separator, or quote character
    // itself.
    //
    // To express a quote inside a quoted field, it must be written twice.
    //
    // Quotes are also used for unambiguous interpretation of a record
    // consisting of a single empty field or beginning with UTF-8 BOM.
    //
    // If `std::nullopt`, special characters inside fields are not expressible,
    // and `CsvWriter` fails if they are encountered, except that potential
    // ambiguities above skip quoting instead. In this case, reading a record
    // consisting of a single empty field is incompatible with
    // `CsvReaderBase::Options::skip_empty_lines()`, and reading the first
    // record beginning with UTF-8 BOM requires
    // `CsvReaderBase::Options::set_preserve_utf8_bom()`.
    //
    // Default: '"'.
    Options& set_quote(std::optional<char> quote) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      quote_ = quote;
      return *this;
    }
    Options&& set_quote(std::optional<char> quote) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_quote(quote));
    }
    std::optional<char> quote() const { return quote_; }

   private:
    std::optional<CsvHeader> header_;
    std::optional<CsvHeader> assumed_header_;
    bool write_utf8_bom_ = false;
    WriteNewline newline_ = WriteNewline::kNative;
    std::optional<char> comment_;
    char field_separator_ = ',';
    std::optional<char> quote_ = '"';
  };

  // Returns the byte `Writer` being written to. Unchanged by `Close()`.
  virtual Writer* DestWriter() const ABSL_ATTRIBUTE_LIFETIME_BOUND = 0;

  // Returns `true` if writing the header was requested, i.e. if
  // `Options::header() != std::nullopt`.
  //
  // In this case `WriteRecord(CsvRecord)` is supported. Otherwise no particular
  // header is assumed, and only `WriteRecord()` from a sequence of fields is
  // supported.
  bool has_header() const { return has_header_; }

  // If `has_header()`, returns field names set by `Options::header()` and
  // written to the first record.
  //
  // If `!has_header()`, returns an empty header.
  const CsvHeader& header() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return header_;
  }

  // Writes the next record expressed as `CsvRecord`, with named fields.
  //
  // The CSV format does not support empty records. A record with no fields will
  // be written as an empty line, which will be read as a record consisting of
  // one empty field, or will be skipped if
  // `CsvReaderBase::Options::skip_empty_lines()`.
  //
  // Preconditions:
  //  * `has_header()`, i.e. `Options::header() != std::nullopt`
  //  * `record.header() == header()`
  //
  // Return values:
  //  * `true`  - success (`ok()`)
  //  * `false` - failure (`!ok()`)
  bool WriteRecord(const CsvRecord& record);

  // Writes the next record expressed as a sequence of fields.
  //
  // The type of `record` must support iteration yielding `absl::string_view`:
  // `for (const absl::string_view field : record)`,
  // e.g. `std::vector<std::string>`.
  //
  // By a common convention each record should consist of the same number of
  // fields, but this is not enforced.
  //
  // The CSV format does not support empty records. A record with no fields will
  // be written as an empty line, which will be read as a record consisting of
  // one empty field, or will be skipped if
  // `CsvReaderBase::Options::skip_empty_lines()`.
  //
  // Return values:
  //  * `true`  - success (`ok()`)
  //  * `false` - failure (`!ok()`)
  template <
      typename Record,
      std::enable_if_t<IsIterableOf<Record, absl::string_view>::value, int> = 0>
  bool WriteRecord(const Record& record);
  bool WriteRecord(std::initializer_list<absl::string_view> record);

  // The index of the most recently written record, starting from 0.
  //
  // The record count does not include any header written with
  // `Options::header()`.
  //
  // `last_record_index()` is unchanged by `Close()`.
  //
  // Precondition: some record was successfully written (`record_index() > 0`).
  uint64_t last_record_index() const;

  // The index of the next record, starting from 0.
  //
  // The record count does not include any header written with
  // `Options::header()`.
  //
  // `record_index()` is unchanged by `Close()`.
  uint64_t record_index() const { return record_index_; }

 protected:
  using Object::Object;

  CsvWriterBase(CsvWriterBase&& that) noexcept;
  CsvWriterBase& operator=(CsvWriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset();
  void Initialize(Writer* dest, Options&& options);
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateOverDest(absl::Status status);

  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;

 private:
  template <typename Record>
  friend bool csv_internal::WriteStandaloneRecord(const Record& record,
                                                  CsvWriterBase& csv_writer);

  bool WriteQuoted(Writer& dest, absl::string_view field,
                   size_t already_scanned);
  bool WriteQuotes(Writer& dest);
  bool WriteFirstField(Writer& dest, absl::string_view field);
  bool WriteField(Writer& dest, absl::string_view field);
  template <typename Record>
  bool WriteRecordInternal(const Record& record);

  bool standalone_record_ = false;
  bool has_header_ = false;
  CsvHeader header_;
  // Lookup table for checking whether quotes are needed if the given character
  // is present in a field.
  //
  // Using `std::bitset` instead would make `CsvWriter` about 20% slower because
  // of a more complicated lookup code.
  std::array<bool, std::numeric_limits<unsigned char>::max() + 1>
      quotes_needed_{};
  WriteNewline newline_ = WriteNewline::kNative;
  char field_separator_ = '\0';
  std::optional<char> quote_;
  uint64_t record_index_ = 0;
};

// `CsvWriter` writes records to a CSV (comma-separated values) file.
//
// A basic variant of CSV is specified in https://tools.ietf.org/html/rfc4180,
// and some common extensions are described in
// https://specs.frictionlessdata.io/csv-dialect/.
//
// `CsvWriter` writes RFC4180-compliant CSV files if
// `Options::newline() == WriteNewline::kCrLf`, and also supports some
// extensions.
//
// By a common convention the first record consists of field names. This is
// supported by `Options::header()` and `WriteRecord(CsvRecord)`.
//
// A record is terminated by a newline: LF, CR, or CR-LF ("\n", "\r", or
// "\r\n").
//
// A record consists of a sequence of fields separated by a field separator
// (usually ',' or '\t'). Each record contains at least one field.
//
// Quotes (usually '"') around a field allow expressing special characters
// inside the field: LF, CR, comment character, field separator, or quote
// character itself.
//
// To express a quote inside a quoted field, it must be written twice.
//
// Quotes are also used for unambiguous interpretation of a record consisting of
// a single empty field or beginning with UTF-8 BOM.
//
// If quoting is turned off, special characters inside fields are not
// expressible, and `CsvWriter` fails if they are encountered, except that
// potential ambiguities above skip quoting instead. In this case, reading
// a record consisting of a single empty field is incompatible with
// `CsvReaderBase::Options::skip_empty_lines()`, and reading the first record
// beginning with UTF-8 BOM requires
// `CsvReaderBase::Options::set_preserve_utf8_bom()`.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the byte `Writer`. `Dest` must support
// `Dependency<Writer*, Dest>`, e.g. `Writer*` (not owned, default),
// `ChainWriter<>` (owned), `std::unique_ptr<Writer>` (owned),
// `Any<Writer*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as `TargetT` of the
// type of the first constructor argument.
//
// The current position is synchronized with the byte `Writer` between records.
template <typename Dest = Writer*>
class CsvWriter : public CsvWriterBase {
 public:
  // Creates a closed `CsvWriter`.
  explicit CsvWriter(Closed) noexcept : CsvWriterBase(kClosed) {}

  // Will write to the byte `Writer` provided by `dest`.
  explicit CsvWriter(Initializer<Dest> dest, Options options = Options());

  CsvWriter(CsvWriter&& that) = default;
  CsvWriter& operator=(CsvWriter&& that) = default;

  // Makes `*this` equivalent to a newly constructed `CsvWriter`. This avoids
  // constructing a temporary `CsvWriter` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Dest> dest,
                                          Options options = Options());

  // Returns the object providing and possibly owning the byte `Writer`.
  // Unchanged by `Close()`.
  Dest& dest() ABSL_ATTRIBUTE_LIFETIME_BOUND { return dest_.manager(); }
  const Dest& dest() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return dest_.manager();
  }
  Writer* DestWriter() const ABSL_ATTRIBUTE_LIFETIME_BOUND override {
    return dest_.get();
  }

 protected:
  void Done() override;

 private:
  // The object providing and possibly owning the byte `Writer`.
  Dependency<Writer*, Dest> dest_;
};

explicit CsvWriter(Closed) -> CsvWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit CsvWriter(Dest&& dest,
                   CsvWriterBase::Options options = CsvWriterBase::Options())
    -> CsvWriter<TargetT<Dest>>;

// Writes a single record to a CSV string.
//
// A record terminator will not be included.
//
// The type of `record` must support iteration yielding `absl::string_view`:
// `for (const absl::string_view field : record)`,
// e.g. `std::vector<std::string>`.
//
// Preconditions:
//  * `options.header() == std::nullopt`
//  * if `options.quote() == std::nullopt`, fields do not include inexpressible
//    characters: LF, CR, comment character, field separator.
template <
    typename Record,
    std::enable_if_t<IsIterableOf<Record, absl::string_view>::value, int> = 0>
std::string WriteCsvRecordToString(
    const Record& record,
    CsvWriterBase::Options options = CsvWriterBase::Options());
std::string WriteCsvRecordToString(
    std::initializer_list<absl::string_view> record,
    CsvWriterBase::Options options = CsvWriterBase::Options());

// Implementation details follow.

inline CsvWriterBase::CsvWriterBase(CsvWriterBase&& that) noexcept
    : Object(static_cast<Object&&>(that)),
      standalone_record_(that.standalone_record_),
      has_header_(that.has_header_),
      header_(std::move(that.header_)),
      quotes_needed_(that.quotes_needed_),
      newline_(that.newline_),
      field_separator_(that.field_separator_),
      quote_(that.quote_),
      record_index_(std::exchange(that.record_index_, 0)) {}

inline CsvWriterBase& CsvWriterBase::operator=(CsvWriterBase&& that) noexcept {
  Object::operator=(static_cast<Object&&>(that));
  standalone_record_ = that.standalone_record_;
  has_header_ = that.has_header_;
  header_ = std::move(that.header_);
  quotes_needed_ = that.quotes_needed_;
  newline_ = that.newline_;
  field_separator_ = that.field_separator_;
  quote_ = that.quote_;
  record_index_ = std::exchange(that.record_index_, 0);
  return *this;
}

inline void CsvWriterBase::Reset(Closed) {
  Object::Reset(kClosed);
  standalone_record_ = false;
  has_header_ = false;
  header_.Reset();
  record_index_ = 0;
}

inline void CsvWriterBase::Reset() {
  Object::Reset();
  standalone_record_ = false;
  has_header_ = false;
  header_.Reset();
  quotes_needed_ = {};
  record_index_ = 0;
}

namespace csv_internal {

template <typename Record>
inline bool WriteStandaloneRecord(const Record& record,
                                  CsvWriterBase& csv_writer) {
  csv_writer.standalone_record_ = true;
  return csv_writer.WriteRecordInternal(record);
}

}  // namespace csv_internal

template <typename Record,
          std::enable_if_t<IsIterableOf<Record, absl::string_view>::value, int>>
inline bool CsvWriterBase::WriteRecord(const Record& record) {
  return WriteRecordInternal(record);
}

inline bool CsvWriterBase::WriteRecord(
    std::initializer_list<absl::string_view> record) {
  return WriteRecord<>(record);
}

template <typename Record>
inline bool CsvWriterBase::WriteRecordInternal(const Record& record) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (standalone_record_) {
    RIEGELI_ASSERT_EQ(record_index_, 0u)
        << "Failed precondition of CsvWriterBase::WriteRecordInternal(): "
           "called more than once by WriteCsvRecordToString()";
  }
  Writer& dest = *DestWriter();
  using std::begin;
  auto iter = begin(record);
  using std::end;
  auto end_iter = end(record);
  if (iter != end_iter) {
    const absl::string_view field = *iter;
    if (ABSL_PREDICT_FALSE(!WriteFirstField(dest, field))) return false;
    if (++iter != end_iter) {
      do {
        if (ABSL_PREDICT_FALSE(!dest.Write(field_separator_))) {
          return FailWithoutAnnotation(AnnotateOverDest(dest.status()));
        }
        const absl::string_view field = *iter;
        if (ABSL_PREDICT_FALSE(!WriteField(dest, field))) return false;
      } while (++iter != end_iter);
    } else if (field.empty() &&
               ABSL_PREDICT_TRUE(field_separator_ != kUtf8Bom[0])) {
      // Quote a single empty field if not already quoted, to avoid writing an
      // empty line which might be skipped by some readers.
      if (ABSL_PREDICT_FALSE(!WriteQuotes(dest))) return false;
    }
  }
  if (!standalone_record_) {
    if (ABSL_PREDICT_FALSE(!WriteLine(dest, newline_))) {
      return FailWithoutAnnotation(AnnotateOverDest(dest.status()));
    }
  }
  ++record_index_;
  return true;
}

inline uint64_t CsvWriterBase::last_record_index() const {
  RIEGELI_ASSERT_GT(record_index_, 0u)
      << "Failed precondition of CsvWriterBase::last_record_index(): "
         "no record was written";
  return record_index_ - 1;
}

template <typename Dest>
inline CsvWriter<Dest>::CsvWriter(Initializer<Dest> dest, Options options)
    : dest_(std::move(dest)) {
  Initialize(dest_.get(), std::move(options));
}

template <typename Dest>
inline void CsvWriter<Dest>::Reset(Closed) {
  CsvWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void CsvWriter<Dest>::Reset(Initializer<Dest> dest, Options options) {
  CsvWriterBase::Reset();
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), std::move(options));
}

template <typename Dest>
void CsvWriter<Dest>::Done() {
  CsvWriterBase::Done();
  if (dest_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) {
      FailWithoutAnnotation(AnnotateOverDest(dest_->status()));
    }
  }
}

template <typename Record,
          std::enable_if_t<IsIterableOf<Record, absl::string_view>::value, int>>
std::string WriteCsvRecordToString(const Record& record,
                                   CsvWriterBase::Options options) {
  RIEGELI_ASSERT(options.header() == std::nullopt)
      << "Failed precondition of WriteCsvRecordToString(): "
         "options.header() != std::nullopt not applicable";
  std::string dest;
  CsvWriter<StringWriter<>> csv_writer(riegeli::Maker(&dest),
                                       std::move(options));
  csv_internal::WriteStandaloneRecord(record, csv_writer);
  // This can fail if `std::string` overflows, or if quoting is turned off and
  // fields include inexpressible characters.
  RIEGELI_CHECK(csv_writer.Close()) << csv_writer.status();
  return dest;
}

inline std::string WriteCsvRecordToString(
    std::initializer_list<absl::string_view> record,
    CsvWriterBase::Options options) {
  return WriteCsvRecordToString<>(record, std::move(options));
}

}  // namespace riegeli

#endif  // RIEGELI_CSV_CSV_WRITER_H_
