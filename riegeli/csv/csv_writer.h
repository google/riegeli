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
#include <iterator>
#include <limits>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/string_writer.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/csv/containers.h"
#include "riegeli/csv/csv_record.h"
#include "riegeli/lines/line_writing.h"

namespace riegeli {

class CsvWriterBase;

namespace internal {
template <typename Fields>
bool WriteStandaloneRecord(const Fields& record, CsvWriterBase& csv_writer);
}  // namespace internal

// Template parameter independent part of `CsvWriter`.
class CsvWriterBase : public Object {
 public:
  // Line terminator representation to write.
  using Newline = WriteLineOptions::Newline;

  class Options {
   public:
    Options() noexcept {}

    // If `!header.empty()`, sets field names, and automatically writes them as
    // the first record.
    //
    // Default: `CsvHeader()`
    Options& set_header(CsvHeader header) & {
      header_ = std::move(header);
      return *this;
    }
    Options&& set_header(CsvHeader header) && {
      return std::move(set_header(std::move(header)));
    }
    CsvHeader& header() & { return header_; }
    const CsvHeader& header() const& { return header_; }
    CsvHeader&& header() && { return std::move(header_); }
    const CsvHeader&& header() const&& { return std::move(header_); }

    // Record terminator.
    //
    // Default: `Newline::kLf`.
    Options& set_newline(Newline newline) & {
      newline_ = newline;
      return *this;
    }
    Options&& set_newline(Newline newline) && {
      return std::move(set_newline(newline));
    }
    Newline newline() const { return newline_; }

    // Comment character.
    //
    // If not `absl::nullopt`, fields containing this character will be quoted.
    //
    // Often used: '#'
    //
    // Default: `absl::nullopt`
    Options& set_comment(absl::optional<char> comment) & {
      RIEGELI_ASSERT(comment != '\n' && comment != '\r')
          << "Comment character conflicts with record separator";
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
    // To express a quote inside a quoted field, it must be written twice or
    // preceded by an escape character.
    //
    // If `absl::nullopt`, special characters inside fields are not expressible,
    // and `CsvWriter` fails if they are encountered.
    //
    // Default: `"`
    Options& set_quote(absl::optional<char> quote) & {
      RIEGELI_ASSERT(quote != '\n' && quote != '\r')
          << "Quote character conflicts with record separator";
      quote_ = quote;
      return *this;
    }
    Options&& set_quote(absl::optional<char> quote) && {
      return std::move(set_quote(quote));
    }
    absl::optional<char> quote() const { return quote_; }

   private:
    CsvHeader header_;
    Newline newline_ = Newline::kLf;
    absl::optional<char> comment_;
    char field_separator_ = ',';
    absl::optional<char> quote_ = '"';
  };

  // Returns the byte `Writer` being written to. Unchanged by `Close()`.
  virtual Writer* dest_writer() = 0;
  virtual const Writer* dest_writer() const = 0;

  // `CsvWriter` overrides `Object::Fail()` to annotate the status with the
  // current record index. Derived classes which override it further should
  // include a call to `CsvWriter::Fail()`.
  using Object::Fail;
  ABSL_ATTRIBUTE_COLD bool Fail(absl::Status status) override;

  // Returns field names, as set by `Options::set_header()`.
  //
  // If `header().empty()`, no particular field names are assumed, and
  // `WriteRecord(CsvRecord)` is not supported; only `WriteRecord()` from a
  // sequence of fields is supported.
  const CsvHeader& header() const { return header_; }

  // Writes the next record expressed as `CsvRecord`, with named fields.
  //
  // Preconditions:
  //  * `!header().empty()`, i.e. `Options::set_header(header)` was used with a
  //    non-empty header
  //  * `record.header() == header()`
  //
  // Return values:
  //  * `true`  - success (`healthy()`)
  //  * `false` - failure (`!healthy()`)
  bool WriteRecord(const CsvRecord& record);

  // Writes the next record expressed as a sequence of fields.
  //
  // By a common convention each record should consist of the same number of
  // fields, but this is not enforced.
  //
  // The type of `record` must support iteration yielding `absl::string_view`:
  // `for (absl::string_view field : record)`, e.g. `std::vector<std::string>`.
  //
  // The CSV format does not support empty records: writing a record with no
  // fields has the same effect as writing a record containing one empty field.
  //
  // Return values:
  //  * `true`  - success (`healthy()`)
  //  * `false` - failure (`!healthy()`)
  template <
      typename Record,
      std::enable_if_t<internal::IsIterableOf<Record, absl::string_view>::value,
                       int> = 0>
  bool WriteRecord(const Record& record);
  bool WriteRecord(std::initializer_list<absl::string_view> record);

  // The index of the most recently written record, starting from 0.
  //
  // The record count does not include any header written with
  // `Options::set_header()`.
  //
  // `last_record_index()` is unchanged by `Close()`.
  //
  // Precondition: some record was successfully written (`record_index() > 0`).
  uint64_t last_record_index() const;

  // The index of the next record, starting from 0.
  //
  // The record count does not include any header written with
  // `Options::set_header()`.
  //
  // `record_index()` is unchanged by `Close()`.
  uint64_t record_index() const { return record_index_; }

 protected:
  explicit CsvWriterBase(InitiallyClosed) noexcept;
  explicit CsvWriterBase(InitiallyOpen) noexcept;

  CsvWriterBase(CsvWriterBase&& that) noexcept;
  CsvWriterBase& operator=(CsvWriterBase&& that) noexcept;

  void Reset(InitiallyClosed);
  void Reset(InitiallyOpen);
  void Initialize(Writer* dest, Options&& options);

  // Exposes a `Fail()` override which does not annotate the status with the
  // current position, unlike the public `CsvWriter::Fail()`.
  ABSL_ATTRIBUTE_COLD bool FailWithoutAnnotation(absl::Status status);
  ABSL_ATTRIBUTE_COLD bool FailWithoutAnnotation(const Object& dependency);

 private:
  template <typename Record>
  friend bool internal::WriteStandaloneRecord(const Record& record,
                                              CsvWriterBase& csv_writer);

  bool WriteQuoted(Writer& dest, absl::string_view field,
                   size_t already_scanned);
  bool WriteField(Writer& dest, absl::string_view field);
  template <bool standalone_record, typename Record>
  bool WriteRecordInternal(const Record& record);

  CsvHeader header_;
  // Lookup table for checking whether quotes are needed if the given character
  // is present in a field.
  //
  // Using `std::bitset` instead would make `CsvWriter` about 20% slower because
  // of a more complicated lookup code.
  std::array<bool, std::numeric_limits<unsigned char>::max() + 1>
      quotes_needed_{};
  Newline newline_ = Newline::kLf;
  char field_separator_ = '\0';
  absl::optional<char> quote_;
  uint64_t record_index_ = 0;
};

// `CsvWriter` writes records to a CSV (comma-separated values) file.
//
// A basic variant of CSV is specified in https://tools.ietf.org/html/rfc4180,
// and some common extensions are described in
// https://specs.frictionlessdata.io/csv-dialect/.
//
// `CsvWriter` writes RFC4180-compliant CSV files with
// `Options::set_newline(CsvWriterBase::Newline::kCrLf)`, and also supports some
// extensions.
//
// By a common convention the first record consists of field names. This is
// supported by `Options::set_header()` and `WriteRecord(CsvRecord)`.
//
// A record is terminated by a newline: LF, CR, or CR LF ("\n", "\r", or
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
// If quoting is turned off, special characters inside fields are not
// expressible, and `CsvWriter` fails if they are encountered.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the byte `Writer`. `Dest` must support
// `Dependency<Writer*, Dest>`, e.g. `Writer*` (not owned, default),
// `std::unique_ptr<Writer>` (owned), `ChainWriter<>` (owned).
//
// The current position is synchronized with the byte `Writer` between records.
template <typename Dest = Writer*>
class CsvWriter : public CsvWriterBase {
 public:
  // Creates a closed `CsvWriter`.
  CsvWriter() noexcept : CsvWriterBase(kInitiallyClosed) {}

  // Will write to the byte `Writer` or `ChunkWriter` provided by `dest`.
  explicit CsvWriter(const Dest& dest, Options options = Options());
  explicit CsvWriter(Dest&& dest, Options options = Options());

  // Will write to the byte `Writer` or `ChunkWriter` provided by a `Dest`
  // constructed from elements of `dest_args`. This avoids constructing a
  // temporary `Dest` and moving from it.
  template <typename... DestArgs>
  explicit CsvWriter(std::tuple<DestArgs...> dest_args,
                     Options options = Options());

  CsvWriter(CsvWriter&& that) noexcept;
  CsvWriter& operator=(CsvWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `CsvWriter`. This avoids
  // constructing a temporary `CsvWriter` and moving from it.
  void Reset();
  void Reset(const Dest& dest, Options options = Options());
  void Reset(Dest&& dest, Options options = Options());
  template <typename... DestArgs>
  void Reset(std::tuple<DestArgs...> dest_args, Options options = Options());

  // Returns the object providing and possibly owning the byte `Writer`.
  // Unchanged by `Close()`.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  Writer* dest_writer() override { return dest_.get(); }
  const Writer* dest_writer() const override { return dest_.get(); }

 protected:
  void Done() override;

 private:
  // The object providing and possibly owning the byte `Writer`.
  Dependency<Writer*, Dest> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
template <typename Dest>
CsvWriter(Dest&& dest,
          CsvWriterBase::Options options = CsvWriterBase::Options())
    -> CsvWriter<std::decay_t<Dest>>;
template <typename... DestArgs>
CsvWriter(std::tuple<DestArgs...> dest_args,
          CsvWriterBase::Options options = CsvWriterBase::Options())
    -> CsvWriter<void>;  // Delete.
#endif

// Writes a single record to a CSV string.
//
// A record terminator will not be included.
//
// The type of `record` must support iteration yielding `absl::string_view`:
// `for (absl::string_view field : record)`, e.g. `std::vector<std::string>`.
//
// Precondition: if `options.quote() == absl::nullopt`, fields do not include
// inexpressible characters: LF, CR, comment character, field separator.
template <
    typename Record,
    std::enable_if_t<internal::IsIterableOf<Record, absl::string_view>::value,
                     int> = 0>
std::string WriteCsvRecordToString(
    const Record& record,
    CsvWriterBase::Options options = CsvWriterBase::Options());
std::string WriteCsvRecordToString(
    std::initializer_list<absl::string_view> record,
    CsvWriterBase::Options options = CsvWriterBase::Options());

// Implementation details follow.

inline CsvWriterBase::CsvWriterBase(InitiallyClosed) noexcept
    : Object(kInitiallyClosed) {}

inline CsvWriterBase::CsvWriterBase(InitiallyOpen) noexcept
    : Object(kInitiallyOpen) {}

inline CsvWriterBase::CsvWriterBase(CsvWriterBase&& that) noexcept
    : Object(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      header_(std::move(that.header_)),
      quotes_needed_(that.quotes_needed_),
      newline_(that.newline_),
      field_separator_(that.field_separator_),
      quote_(that.quote_),
      record_index_(std::exchange(that.record_index_, 0)) {}

inline CsvWriterBase& CsvWriterBase::operator=(CsvWriterBase&& that) noexcept {
  Object::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  header_ = std::move(that.header_);
  quotes_needed_ = that.quotes_needed_;
  newline_ = that.newline_;
  field_separator_ = that.field_separator_;
  quote_ = that.quote_;
  record_index_ = std::exchange(that.record_index_, 0);
  return *this;
}

inline void CsvWriterBase::Reset(InitiallyClosed) {
  Object::Reset(kInitiallyClosed);
  header_.Reset();
  record_index_ = 0;
}

inline void CsvWriterBase::Reset(InitiallyOpen) {
  Object::Reset(kInitiallyOpen);
  header_.Reset();
  quotes_needed_ = {};
  record_index_ = 0;
}

template <typename Record,
          std::enable_if_t<
              internal::IsIterableOf<Record, absl::string_view>::value, int>>
bool CsvWriterBase::WriteRecord(const Record& record) {
  return WriteRecordInternal<false>(record);
}

namespace internal {

template <typename Record>
inline bool WriteStandaloneRecord(const Record& record,
                                  CsvWriterBase& csv_writer) {
  return csv_writer.WriteRecordInternal<true>(record);
}

}  // namespace internal

template <bool standalone_record, typename Record>
inline bool CsvWriterBase::WriteRecordInternal(const Record& record) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (standalone_record) {
    RIEGELI_ASSERT_EQ(record_index_, 0u)
        << "Failed precondition of CsvWriterBase::WriteRecordInternal(): "
           "called more than once when standalone_record is true";
  }
  Writer& dest = *dest_writer();
  using std::begin;
  auto iter = begin(record);
  using std::end;
  auto end_iter = end(record);
  if (iter != end_iter) {
    for (;;) {
      const absl::string_view field = *iter;
      if (ABSL_PREDICT_FALSE(!WriteField(dest, field))) return false;
      ++iter;
      if (iter == end_iter) break;
      if (ABSL_PREDICT_FALSE(!dest.WriteChar(field_separator_))) {
        return Fail(dest);
      }
    }
  }
  if (!standalone_record) {
    if (ABSL_PREDICT_FALSE(
            !WriteLine(dest, WriteLineOptions().set_newline(newline_)))) {
      return Fail(dest);
    }
  }
  ++record_index_;
  return true;
}

inline bool CsvWriterBase::WriteRecord(
    std::initializer_list<absl::string_view> record) {
  return WriteRecord<std::initializer_list<absl::string_view>>(record);
}

inline uint64_t CsvWriterBase::last_record_index() const {
  RIEGELI_ASSERT_GT(record_index_, 0u)
      << "Failed precondition of CsvWriterBase::last_record_index(): "
         "no record was written";
  return record_index_ - 1;
}

template <typename Dest>
inline CsvWriter<Dest>::CsvWriter(const Dest& dest, Options options)
    : CsvWriterBase(kInitiallyOpen), dest_(dest) {
  Initialize(dest_.get(), std::move(options));
}

template <typename Dest>
inline CsvWriter<Dest>::CsvWriter(Dest&& dest, Options options)
    : CsvWriterBase(kInitiallyOpen), dest_(std::move(dest)) {
  Initialize(dest_.get(), std::move(options));
}

template <typename Dest>
template <typename... DestArgs>
inline CsvWriter<Dest>::CsvWriter(std::tuple<DestArgs...> dest_args,
                                  Options options)
    : CsvWriterBase(kInitiallyOpen), dest_(std::move(dest_args)) {
  Initialize(dest_.get(), std::move(options));
}

template <typename Dest>
inline CsvWriter<Dest>::CsvWriter(CsvWriter&& that) noexcept
    : CsvWriterBase(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      dest_(std::move(that.dest_)) {}

template <typename Dest>
inline CsvWriter<Dest>& CsvWriter<Dest>::operator=(CsvWriter&& that) noexcept {
  CsvWriterBase::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  dest_ = std::move(that.dest_);
  return *this;
}

template <typename Dest>
inline void CsvWriter<Dest>::Reset() {
  CsvWriterBase::Reset(kInitiallyClosed);
  dest_.Reset();
}

template <typename Dest>
inline void CsvWriter<Dest>::Reset(const Dest& dest, Options options) {
  CsvWriterBase::Reset(kInitiallyOpen);
  dest_.Reset(dest);
  Initialize(dest_.get(), std::move(options));
}

template <typename Dest>
inline void CsvWriter<Dest>::Reset(Dest&& dest, Options options) {
  CsvWriterBase::Reset(kInitiallyOpen);
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), std::move(options));
}

template <typename Dest>
template <typename... DestArgs>
inline void CsvWriter<Dest>::Reset(std::tuple<DestArgs...> dest_args,
                                   Options options) {
  CsvWriterBase::Reset(kInitiallyOpen);
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get(), std::move(options));
}

template <typename Dest>
void CsvWriter<Dest>::Done() {
  CsvWriterBase::Done();
  if (dest_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) Fail(*dest_);
  }
}

template <typename Record,
          std::enable_if_t<
              internal::IsIterableOf<Record, absl::string_view>::value, int>>
std::string WriteCsvRecordToString(const Record& record,
                                   CsvWriterBase::Options options) {
  std::string dest;
  CsvWriter<StringWriter<>> csv_writer(std::forward_as_tuple(&dest),
                                       std::move(options));
  internal::WriteStandaloneRecord(record, csv_writer);
  // This can fail if `std::string` overflows, or if quoting is turned off and
  // fields include inexpressible characters.
  RIEGELI_CHECK(csv_writer.Close()) << csv_writer.status();
  return dest;
}

inline std::string WriteCsvRecordToString(
    std::initializer_list<absl::string_view> record,
    CsvWriterBase::Options options) {
  return WriteCsvRecordToString<std::initializer_list<absl::string_view>>(
      record, std::move(options));
}

}  // namespace riegeli

#endif  // RIEGELI_CSV_CSV_WRITER_H_
