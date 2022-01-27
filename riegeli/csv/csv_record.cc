// Copyright 2021 Google LLC
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

#include "riegeli/csv/csv_record.h"

#include <stddef.h>

#include <cstring>
#include <functional>
#include <initializer_list>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/intrusive_ref_count.h"
#include "riegeli/bytes/string_writer.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/csv/containers.h"

namespace riegeli {

namespace {

inline void WriteDebugQuoted(absl::string_view src, Writer& writer,
                             size_t already_scanned) {
  writer.WriteChar('"');
  const char* start = src.data();
  const char* next_to_check = src.data() + already_scanned;
  const char* const limit = src.data() + src.size();
  // Write characters in the range [`start`..`limit`), except that if quotes are
  // found in the range [`next_to_check`..`limit`), write them twice.
  while (const char* const next_quote = static_cast<const char*>(std::memchr(
             next_to_check, '"', PtrDistance(next_to_check, limit)))) {
    writer.Write(start, PtrDistance(start, next_quote + 1));
    start = next_quote;
    next_to_check = next_quote + 1;
  }
  writer.Write(start, PtrDistance(start, limit));
  writer.WriteChar('"');
}

inline std::string DebugQuotedIfNeeded(absl::string_view src) {
  std::string dest;
  StringWriter<> writer(&dest);
  csv_internal::WriteDebugQuotedIfNeeded(src, writer);
  writer.Close();
  return dest;
}

}  // namespace

namespace csv_internal {

void WriteDebugQuotedIfNeeded(absl::string_view src, Writer& writer) {
  for (size_t i = 0; i < src.size(); ++i) {
    switch (src[i]) {
      // For correct CSV syntax.
      case '\n':
      case '\r':
      case ',':
      case '"':
      // For unambiguous `CsvRecord::DebugString()`.
      case ':':
      // For unambiguous appending of the rest of an error message.
      case ';':
        WriteDebugQuoted(src, writer, i);
        return;
    }
  }
  writer.Write(src);
}

}  // namespace csv_internal

std::string AsciiCaseInsensitive(absl::string_view name) {
  return absl::AsciiStrToLower(name);
}

inline CsvHeader::Payload::Payload(const Payload& that)
    : normalizer(that.normalizer),
      index_to_name(that.index_to_name),
      name_to_index(that.name_to_index) {}

CsvHeader::CsvHeader(std::vector<std::string>&& names)
    : CsvHeader(nullptr, std::move(names)) {}

CsvHeader::CsvHeader(std::initializer_list<absl::string_view> names)
    : CsvHeader(nullptr, csv_internal::ToVectorOfStrings(names)) {}

CsvHeader::CsvHeader(std::function<std::string(absl::string_view)> normalizer) {
  if (normalizer != nullptr) payload_.reset(new Payload(std::move(normalizer)));
}

CsvHeader::CsvHeader(std::function<std::string(absl::string_view)> normalizer,
                     std::vector<std::string>&& names) {
  const absl::Status status = TryReset(std::move(normalizer), std::move(names));
  RIEGELI_CHECK(status.ok())
      << "Failed precondition of CsvHeader::CsvHeader(): " << status.message();
}

CsvHeader::CsvHeader(std::function<std::string(absl::string_view)> normalizer,
                     std::initializer_list<absl::string_view> names)
    : CsvHeader(std::move(normalizer), csv_internal::ToVectorOfStrings(names)) {
}

void CsvHeader::Reset() { payload_.reset(); }

void CsvHeader::Reset(std::vector<std::string>&& names) {
  Reset(nullptr, std::move(names));
}

void CsvHeader::Reset(std::initializer_list<absl::string_view> names) {
  Reset(nullptr, csv_internal::ToVectorOfStrings(names));
}

void CsvHeader::Reset(
    std::function<std::string(absl::string_view)> normalizer) {
  if (normalizer == nullptr) {
    payload_.reset();
  } else {
    payload_.reset(new Payload(std::move(normalizer)));
  }
}

void CsvHeader::Reset(std::function<std::string(absl::string_view)> normalizer,
                      std::vector<std::string>&& names) {
  const absl::Status status = TryReset(std::move(normalizer), std::move(names));
  RIEGELI_CHECK(status.ok())
      << "Failed precondition of CsvHeader::Reset(): " << status.message();
}

void CsvHeader::Reset(std::function<std::string(absl::string_view)> normalizer,
                      std::initializer_list<absl::string_view> names) {
  Reset(std::move(normalizer), csv_internal::ToVectorOfStrings(names));
}

absl::Status CsvHeader::TryReset(std::vector<std::string>&& names) {
  return TryReset(nullptr, std::move(names));
}

absl::Status CsvHeader::TryReset(
    std::initializer_list<absl::string_view> names) {
  return TryReset(nullptr, csv_internal::ToVectorOfStrings(names));
}

absl::Status CsvHeader::TryReset(
    std::function<std::string(absl::string_view)> normalizer,
    std::vector<std::string>&& names) {
  if (normalizer == nullptr && names.empty()) {
    payload_.reset();
    return absl::OkStatus();
  }
  EnsureUniqueOwner();
  payload_->normalizer = std::move(normalizer);
  payload_->name_to_index.clear();
  std::vector<absl::string_view> duplicate_names;
  for (size_t index = 0; index < names.size(); ++index) {
    const std::pair<absl::flat_hash_map<std::string, size_t>::iterator, bool>
        insert_result =
            payload_->normalizer == nullptr
                ? payload_->name_to_index.emplace(names[index], index)
                : payload_->name_to_index.emplace(
                      payload_->normalizer(names[index]), index);
    if (ABSL_PREDICT_FALSE(!insert_result.second)) {
      duplicate_names.push_back(names[index]);
    }
  }
  if (ABSL_PREDICT_FALSE(!duplicate_names.empty())) {
    payload_.reset();
    StringWriter<std::string> message;
    message.Write("Duplicate field names: ");
    for (std::vector<absl::string_view>::const_iterator iter =
             duplicate_names.cbegin();
         iter != duplicate_names.cend(); ++iter) {
      if (iter != duplicate_names.cbegin()) message.WriteChar(',');
      csv_internal::WriteDebugQuotedIfNeeded(*iter, message);
    }
    message.Close();
    return absl::FailedPreconditionError(message.dest());
  }
  payload_->index_to_name = std::move(names);
  return absl::OkStatus();
}

absl::Status CsvHeader::TryReset(
    std::function<std::string(absl::string_view)> normalizer,
    std::initializer_list<absl::string_view> names) {
  return TryReset(std::move(normalizer),
                  csv_internal::ToVectorOfStrings(names));
}

void CsvHeader::Add(absl::string_view name) { Add(std::string(name)); }

template <typename Name,
          std::enable_if_t<std::is_same<Name, std::string>::value, int>>
void CsvHeader::Add(Name&& name) {
  // `std::move(name)` is correct and `std::forward<Name>(name)` is not
  // necessary: `Name` is always `std::string`, never an lvalue reference.
  const absl::Status status = TryAdd(std::move(name));
  RIEGELI_CHECK(status.ok())
      << "Failed precondition of CsvHeader::Add(): " << status.message();
}

template void CsvHeader::Add(std::string&& name);

absl::Status CsvHeader::TryAdd(absl::string_view name) {
  return TryAdd(std::string(name));
}

template <typename Name,
          std::enable_if_t<std::is_same<Name, std::string>::value, int>>
absl::Status CsvHeader::TryAdd(Name&& name) {
  EnsureUniqueOwner();
  const size_t index = payload_->index_to_name.size();
  // `std::move(name)` is correct and `std::forward<Name>(name)` is not
  // necessary: `Name` is always `std::string`, never an lvalue reference.
  const std::pair<absl::flat_hash_map<std::string, size_t>::iterator, bool>
      insert_result = payload_->normalizer == nullptr
                          ? payload_->name_to_index.emplace(name, index)
                          : payload_->name_to_index.emplace(
                                payload_->normalizer(name), index);
  if (ABSL_PREDICT_FALSE(!insert_result.second)) {
    RIEGELI_ASSERT(!empty())
        << "It should not have been needed to ensure that an empty CsvHeader "
           "has payload_ == nullptr because a duplicate field name is possible "
           "only if some fields were already present";
    StringWriter<std::string> message;
    message.Write("Duplicate field name: ");
    csv_internal::WriteDebugQuotedIfNeeded(name, message);
    message.Close();
    return absl::FailedPreconditionError(message.dest());
  }
  payload_->index_to_name.push_back(std::move(name));
  return absl::OkStatus();
}

template absl::Status CsvHeader::TryAdd(std::string&& name);

CsvHeader::iterator CsvHeader::find(absl::string_view name) const {
  if (ABSL_PREDICT_FALSE(payload_ == nullptr)) return iterator();
  const absl::flat_hash_map<std::string, size_t>::const_iterator iter =
      payload_->normalizer == nullptr
          ? payload_->name_to_index.find(name)
          : payload_->name_to_index.find(payload_->normalizer(name));
  if (ABSL_PREDICT_FALSE(iter == payload_->name_to_index.cend())) {
    return iterator(payload_->index_to_name.data() +
                    payload_->index_to_name.size());
  }
  return iterator(payload_->index_to_name.data() + iter->second);
}

bool CsvHeader::contains(absl::string_view name) const {
  if (ABSL_PREDICT_FALSE(payload_ == nullptr)) return false;
  const absl::flat_hash_map<std::string, size_t>::const_iterator iter =
      payload_->normalizer == nullptr
          ? payload_->name_to_index.find(name)
          : payload_->name_to_index.find(payload_->normalizer(name));
  return iter != payload_->name_to_index.cend();
}

absl::optional<size_t> CsvHeader::IndexOf(absl::string_view name) const {
  if (ABSL_PREDICT_FALSE(payload_ == nullptr)) return absl::nullopt;
  const absl::flat_hash_map<std::string, size_t>::const_iterator iter =
      payload_->normalizer == nullptr
          ? payload_->name_to_index.find(name)
          : payload_->name_to_index.find(payload_->normalizer(name));
  if (ABSL_PREDICT_FALSE(iter == payload_->name_to_index.cend())) {
    return absl::nullopt;
  }
  return iter->second;
}

inline void CsvHeader::EnsureUniqueOwner() {
  if (payload_ == nullptr) {
    payload_.reset(new Payload());
  } else if (ABSL_PREDICT_FALSE(!payload_->has_unique_owner())) {
    payload_.reset(new Payload(*payload_));
  }
}

std::string CsvHeader::DebugString() const {
  std::string result;
  StringWriter<> writer(&result);
  for (iterator iter = cbegin(); iter != cend(); ++iter) {
    if (iter != cbegin()) writer.WriteChar(',');
    csv_internal::WriteDebugQuotedIfNeeded(*iter, writer);
  }
  writer.Close();
  return result;
}

std::ostream& operator<<(std::ostream& out, const CsvHeader& header) {
  return out << header.DebugString();
}

CsvRecord::CsvRecord(CsvHeader header, std::vector<std::string>&& fields) {
  const absl::Status status = TryReset(std::move(header), std::move(fields));
  RIEGELI_CHECK(status.ok())
      << "Failed precondition of CsvRecord::CsvRecord(): " << status.message();
}

CsvRecord::CsvRecord(CsvHeader header,
                     std::initializer_list<absl::string_view> fields)
    : CsvRecord(std::move(header), csv_internal::ToVectorOfStrings(fields)) {}

void CsvRecord::Reset() {
  header_.Reset();
  fields_.clear();
}

void CsvRecord::Reset(CsvHeader header) {
  header_ = std::move(header);
  fields_.resize(header_.size());
  Clear();
}

void CsvRecord::Reset(CsvHeader header, std::vector<std::string>&& fields) {
  const absl::Status status = TryReset(std::move(header), std::move(fields));
  RIEGELI_CHECK(status.ok())
      << "Failed precondition of CsvRecord::Reset(): " << status.message();
}

void CsvRecord::Reset(CsvHeader header,
                      std::initializer_list<absl::string_view> fields) {
  Reset(std::move(header), csv_internal::ToVectorOfStrings(fields));
}

absl::Status CsvRecord::TryReset(CsvHeader header,
                                 std::vector<std::string>&& fields) {
  if (ABSL_PREDICT_FALSE(header.size() != fields.size())) {
    header_.Reset();
    fields_.clear();
    return absl::FailedPreconditionError(
        absl::StrCat("Mismatched number of CSV fields: header has ",
                     header.size(), ", record has ", fields.size()));
  }
  header_ = std::move(header);
  fields_ = std::move(fields);
  return absl::OkStatus();
}

absl::Status CsvRecord::TryReset(
    CsvHeader header, std::initializer_list<absl::string_view> fields) {
  return TryReset(std::move(header), csv_internal::ToVectorOfStrings(fields));
}

void CsvRecord::Clear() {
  for (std::string& value : fields_) value.clear();
}

std::string& CsvRecord::operator[](absl::string_view name) {
  const CsvHeader::iterator name_iter = header_.find(name);
  RIEGELI_CHECK(name_iter != header_.end())
      << "Failed precondition of CsvRecord::operator[](): missing field name: "
      << DebugQuotedIfNeeded(name) << "; existing field names: " << header_;
  return fields_[name_iter - header_.begin()];
}

const std::string& CsvRecord::operator[](absl::string_view name) const {
  const CsvHeader::iterator name_iter = header_.find(name);
  RIEGELI_CHECK(name_iter != header_.end())
      << "Failed precondition of CsvRecord::operator[](): missing field name: "
      << DebugQuotedIfNeeded(name) << "; existing field names: " << header_;
  return fields_[name_iter - header_.begin()];
}

CsvRecord::iterator CsvRecord::find(absl::string_view name) {
  const CsvHeader::iterator name_iter = header_.find(name);
  RIEGELI_ASSERT(name_iter >= header_.begin() && name_iter <= header_.end())
      << "Failed precondition of CsvRecord::find(): "
         "field name iterator does not belong to the same header";
  return iterator(name_iter, fields_.begin() + (name_iter - header_.begin()));
}

CsvRecord::const_iterator CsvRecord::find(absl::string_view name) const {
  const CsvHeader::iterator name_iter = header_.find(name);
  RIEGELI_ASSERT(name_iter >= header_.begin() && name_iter <= header_.end())
      << "Failed precondition of CsvRecord::find(): "
         "field name iterator does not belong to the same header";
  return const_iterator(name_iter,
                        fields_.cbegin() + (name_iter - header_.begin()));
}

bool CsvRecord::contains(absl::string_view name) const {
  return header_.contains(name);
}

void CsvRecord::Merge(
    std::initializer_list<std::pair<absl::string_view, absl::string_view>>
        src) {
  Merge<
      std::initializer_list<std::pair<absl::string_view, absl::string_view>>&>(
      src);
}

absl::Status CsvRecord::TryMerge(
    std::initializer_list<std::pair<absl::string_view, absl::string_view>>
        src) {
  return TryMerge<
      std::initializer_list<std::pair<absl::string_view, absl::string_view>>&>(
      src);
}

absl::Status CsvRecord::FailMerge(
    const std::vector<std::string>& missing_names) const {
  StringWriter<std::string> message;
  message.Write("Missing field names: ");
  for (std::vector<std::string>::const_iterator iter = missing_names.cbegin();
       iter != missing_names.cend(); ++iter) {
    if (iter != missing_names.cbegin()) message.WriteChar(',');
    csv_internal::WriteDebugQuotedIfNeeded(*iter, message);
  }
  message.Write("; existing field names: ");
  for (CsvHeader::const_iterator iter = header_.cbegin();
       iter != header_.cend(); ++iter) {
    if (iter != header_.cbegin()) message.WriteChar(',');
    csv_internal::WriteDebugQuotedIfNeeded(*iter, message);
  }
  message.Close();
  return absl::FailedPreconditionError(message.dest());
}

std::string CsvRecord::DebugString() const {
  RIEGELI_ASSERT_EQ(header_.size(), fields_.size())
      << "Failed invariant of CsvRecord: "
         "mismatched length of CSV header and fields";
  std::string result;
  StringWriter<> writer(&result);
  for (const_iterator iter = cbegin(); iter != cend(); ++iter) {
    if (iter != cbegin()) writer.WriteChar(',');
    csv_internal::WriteDebugQuotedIfNeeded(iter->first, writer);
    writer.WriteChar(':');
    csv_internal::WriteDebugQuotedIfNeeded(iter->second, writer);
  }
  writer.Close();
  return result;
}

std::ostream& operator<<(std::ostream& out, const CsvRecord& record) {
  return out << record.DebugString();
}

}  // namespace riegeli
