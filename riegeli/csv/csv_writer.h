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

#include <array>
#include <initializer_list>
#include <limits>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/resetter.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/lines/line_writing.h"

namespace riegeli {

// Template parameter independent part of `CsvWriter`.
class CsvWriterBase : public Object {
 public:
  // Line terminator representation to write.
  using Newline = WriteLineOptions::Newline;

  class Options {
   public:
    Options() noexcept {}

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

   private:
    Newline newline_ = Newline::kLf;
    char field_separator_ = ',';
  };

  // Returns the byte `Writer` being written to. Unchanged by `Close()`.
  virtual Writer* dest_writer() = 0;
  virtual const Writer* dest_writer() const = 0;

  // Writes the next record.
  //
  // The type of the record must support iteration yielding `absl::string_view`:
  // `for (absl::string_view field : fields)`, e.g. `std::vector<std::string>`.
  //
  // The CSV format does not support empty records: writing a record with no
  // fields has the same effect as writing a record containing one empty field.
  //
  // Return values:
  //  * `true`  - success (`healthy()`)
  //  * `false` - failure (`!healthy()`)
  template <typename Fields>
  bool WriteRecord(const Fields& fields);
  bool WriteRecord(std::initializer_list<absl::string_view> fields);

 protected:
  explicit CsvWriterBase(InitiallyClosed) noexcept;
  explicit CsvWriterBase(InitiallyOpen) noexcept;

  CsvWriterBase(CsvWriterBase&& that) noexcept;
  CsvWriterBase& operator=(CsvWriterBase&& that) noexcept;

  void Reset(InitiallyClosed);
  void Reset(InitiallyOpen);
  void Initialize(Writer* dest, Options&& options);

 private:
  bool WriteQuoted(Writer& dest, absl::string_view field,
                   size_t already_scanned);
  bool WriteField(Writer& dest, absl::string_view field);

  // Lookup table for checking whether quotes are needed if the given character
  // is present in a field.
  //
  // Using `std::bitset` instead would make `CsvWriter` about 20% slower because
  // of a more complicated lookup code.
  std::array<bool, std::numeric_limits<unsigned char>::max() + 1>
      quotes_needed_{};
  Newline newline_ = Newline::kLf;
  char field_separator_ = '\0';
};

// `CsvWriter` writes records to a CSV (comma-separated values) file.
//
// A basic variant of CSV is specified in https://tools.ietf.org/html/rfc4180.
// `CsvWriter` writes RFC4180-compliant CSV files with
// `CsvWriterBase::Options().set_newline(CsvWriterBase::Newline::kCrLf)`,
// and also supports some extensions.
//
// A record is terminated by a newline: LF, CR, or CR LF ("\n", "\r", or
// "\r\n").
//
// A record consists of a sequence of fields separated by a field separator
// (usually ',' or '\t'). Each record contains at least one field.
//
// By a common convention the first record consists of field names. This should
// be handled by the application; `CsvWriter` does not treat the first record
// specially.
//
// Quotes ('"') around a field allow expressing special characters inside the
// field: field separator, LF, CR, or quote itself.
//
// To express a quote inside a quoted field, it must be written twice.
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

// Implementation details follow.

inline CsvWriterBase::CsvWriterBase(InitiallyClosed) noexcept
    : Object(kInitiallyClosed) {}

inline CsvWriterBase::CsvWriterBase(InitiallyOpen) noexcept
    : Object(kInitiallyOpen) {}

inline CsvWriterBase::CsvWriterBase(CsvWriterBase&& that) noexcept
    : Object(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      quotes_needed_(that.quotes_needed_),
      newline_(that.newline_),
      field_separator_(that.field_separator_) {}

inline CsvWriterBase& CsvWriterBase::operator=(CsvWriterBase&& that) noexcept {
  Object::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  quotes_needed_ = that.quotes_needed_;
  newline_ = that.newline_;
  field_separator_ = that.field_separator_;
  return *this;
}

inline void CsvWriterBase::Reset(InitiallyClosed) {
  Object::Reset(kInitiallyClosed);
}

inline void CsvWriterBase::Reset(InitiallyOpen) {
  Object::Reset(kInitiallyOpen);
  quotes_needed_ = {};
}

template <typename Fields>
bool CsvWriterBase::WriteRecord(const Fields& fields) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Writer& dest = *dest_writer();
  bool first = true;
  for (const absl::string_view field : fields) {
    if (first) {
      first = false;
    } else {
      if (ABSL_PREDICT_FALSE(!dest.WriteChar(field_separator_))) {
        return Fail(dest);
      }
    }
    if (ABSL_PREDICT_FALSE(!WriteField(dest, field))) return false;
  }
  if (ABSL_PREDICT_FALSE(
          !WriteLine(dest, WriteLineOptions().set_newline(newline_)))) {
    return Fail(dest);
  }
  return true;
}

inline bool CsvWriterBase::WriteRecord(
    std::initializer_list<absl::string_view> fields) {
  return WriteRecord<std::initializer_list<absl::string_view>>(fields);
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

template <typename Dest>
struct Resetter<CsvWriter<Dest>> : ResetterByReset<CsvWriter<Dest>> {};

}  // namespace riegeli

#endif  // RIEGELI_CSV_CSV_WRITER_H_
