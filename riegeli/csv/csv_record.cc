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

#include <initializer_list>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/intrusive_ref_count.h"
#include "riegeli/bytes/string_writer.h"

namespace riegeli {

inline CsvHeader::Payload::Payload(const Payload& that)
    : index_to_name(that.index_to_name), name_to_index(that.name_to_index) {}

CsvHeader::CsvHeader(std::vector<std::string> names) {
  Reset(std::move(names));
}

CsvHeader::CsvHeader(std::initializer_list<absl::string_view> names) {
  Reset(names);
}

void CsvHeader::Reset() { payload_.reset(); }

void CsvHeader::Reset(std::vector<std::string> names) {
  const absl::Status status = TryReset(std::move(names));
  RIEGELI_CHECK(status.ok())
      << "Failed precondition of CsvHeader::Reset(): " << status.message();
}

void CsvHeader::Reset(std::initializer_list<absl::string_view> names) {
  Reset<std::initializer_list<absl::string_view>>(names);
}

absl::Status CsvHeader::TryReset(std::vector<std::string> names) {
  EnsureUniqueOwner();
  payload_->name_to_index.clear();
  std::vector<absl::string_view> duplicate_names;
  for (size_t index = 0; index < names.size(); ++index) {
    const std::pair<absl::flat_hash_map<std::string, size_t>::iterator, bool>
        insert_result = payload_->name_to_index.emplace(names[index], index);
    if (ABSL_PREDICT_FALSE(!insert_result.second)) {
      duplicate_names.push_back(names[index]);
    }
  }
  if (ABSL_PREDICT_FALSE(!duplicate_names.empty())) {
    payload_.reset();
    return absl::FailedPreconditionError(absl::StrCat(
        "Duplicate field name(s): ", absl::StrJoin(duplicate_names, ", ")));
  }
  payload_->index_to_name = std::move(names);
  return absl::OkStatus();
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
      insert_result = payload_->name_to_index.emplace(name, index);
  if (ABSL_PREDICT_FALSE(!insert_result.second)) {
    RIEGELI_ASSERT(!empty())
        << "It should not have been needed to ensure that an empty CsvHeader "
           "has payload_ == nullptr because a duplicate field name is possible "
           "only if some fields were already present";
    return absl::FailedPreconditionError(
        absl::StrCat("Duplicate field name: ", name));
  }
  payload_->index_to_name.push_back(std::move(name));
  return absl::OkStatus();
}

template absl::Status CsvHeader::TryAdd(std::string&& name);

absl::Status CsvHeader::TryReset(
    std::initializer_list<absl::string_view> names) {
  return TryReset<std::initializer_list<absl::string_view>>(names);
}

CsvHeader::iterator CsvHeader::find(absl::string_view name) const {
  if (ABSL_PREDICT_FALSE(payload_ == nullptr)) return iterator();
  const absl::flat_hash_map<std::string, size_t>::const_iterator iter =
      payload_->name_to_index.find(name);
  if (ABSL_PREDICT_FALSE(iter == payload_->name_to_index.cend())) {
    return iterator(payload_->index_to_name.data() +
                    payload_->index_to_name.size());
  }
  return iterator(payload_->index_to_name.data() + iter->second);
}

bool CsvHeader::contains(absl::string_view name) const {
  if (ABSL_PREDICT_FALSE(payload_ == nullptr)) return false;
  return payload_->name_to_index.find(name) != payload_->name_to_index.cend();
}

absl::optional<size_t> CsvHeader::IndexOf(absl::string_view name) const {
  if (ABSL_PREDICT_FALSE(payload_ == nullptr)) return absl::nullopt;
  const absl::flat_hash_map<std::string, size_t>::const_iterator iter =
      payload_->name_to_index.find(name);
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
  riegeli::StringWriter<> writer(&result);
  writer.WriteChar('{');
  for (iterator iter = cbegin(); iter != cend(); ++iter) {
    if (iter != cbegin()) writer.Write(", ");
    writer.WriteChar('"');
    writer.Write(absl::Utf8SafeCEscape(*iter));
    writer.WriteChar('"');
  }
  writer.WriteChar('}');
  writer.Close();
  return result;
}

std::ostream& operator<<(std::ostream& out, const CsvHeader& header) {
  return out << header.DebugString();
}

void CsvRecord::Reset() {
  header_.Reset();
  fields_.clear();
}

void CsvRecord::Reset(CsvHeader header) {
  header_ = std::move(header);
  fields_.resize(header_.size());
  Clear();
}

void CsvRecord::Reset(CsvHeader header, std::vector<std::string> fields) {
  const absl::Status status = TryReset(std::move(header), std::move(fields));
  RIEGELI_CHECK(status.ok())
      << "Failed precondition of CsvRecord::Reset(): " << status.message();
}

absl::Status CsvRecord::TryReset(CsvHeader header,
                                 std::vector<std::string> fields) {
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

void CsvRecord::Clear() {
  for (std::string& value : fields_) value.clear();
}

std::string& CsvRecord::operator[](absl::string_view name) {
  const CsvHeader::iterator name_iter = header_.find(name);
  RIEGELI_CHECK(name_iter != header_.end())
      << "Failed precondition of CsvRecord::operator[](): "
         "unknown field name: "
      << name;
  return fields_[name_iter - header_.begin()];
}

const std::string& CsvRecord::operator[](absl::string_view name) const {
  const CsvHeader::iterator name_iter = header_.find(name);
  RIEGELI_CHECK(name_iter != header_.end())
      << "Failed precondition of CsvRecord::operator[](): "
         "unknown field name: "
      << name;
  return fields_[name_iter - header_.begin()];
}

std::string& CsvRecord::operator[](CsvHeader::iterator name_iter) {
  RIEGELI_ASSERT(name_iter >= header_.begin() && name_iter <= header_.end())
      << "Failed precondition of CsvRecord::operator[](): "
         "field name iterator does not belong to the same header";
  RIEGELI_CHECK(name_iter != header_.end())
      << "Failed precondition of CsvRecord::operator[](): "
         "unknown field name";
  return fields_[name_iter - header_.begin()];
}

const std::string& CsvRecord::operator[](CsvHeader::iterator name_iter) const {
  RIEGELI_ASSERT(name_iter >= header_.begin() && name_iter <= header_.end())
      << "Failed precondition of CsvRecord::operator[](): "
         "field name iterator does not belong to the same header";
  RIEGELI_CHECK(name_iter != header_.end())
      << "Failed precondition of CsvRecord::operator[](): "
         "unknown field name";
  return fields_[name_iter - header_.begin()];
}

CsvRecord::iterator CsvRecord::find(absl::string_view name) {
  return find(header_.find(name));
}

CsvRecord::const_iterator CsvRecord::find(absl::string_view name) const {
  return find(header_.find(name));
}

CsvRecord::iterator CsvRecord::find(CsvHeader::iterator name_iter) {
  RIEGELI_ASSERT(name_iter >= header_.begin() && name_iter <= header_.end())
      << "Failed precondition of CsvRecord::find(): "
         "field name iterator does not belong to the same header";
  return iterator(name_iter, fields_.begin() + (name_iter - header_.begin()));
}

CsvRecord::const_iterator CsvRecord::find(CsvHeader::iterator name_iter) const {
  RIEGELI_ASSERT(name_iter >= header_.begin() && name_iter <= header_.end())
      << "Failed precondition of CsvRecord::find(): "
         "field name iterator does not belong to the same header";
  return const_iterator(name_iter,
                        fields_.cbegin() + (name_iter - header_.begin()));
}

bool CsvRecord::contains(absl::string_view name) const {
  return header_.contains(name);
}

std::string CsvRecord::DebugString() const {
  RIEGELI_ASSERT_EQ(header_.size(), fields_.size())
      << "Failed invariant of CsvRecord: "
         "mismatched length of CSV header and fields";
  std::string result;
  riegeli::StringWriter<> writer(&result);
  writer.WriteChar('{');
  for (const_iterator iter = cbegin(); iter != cend(); ++iter) {
    if (iter != cbegin()) writer.Write(", ");
    writer.WriteChar('"');
    writer.Write(absl::Utf8SafeCEscape(iter->first));
    writer.Write("\": \"");
    writer.Write(absl::Utf8SafeCEscape(iter->second));
    writer.WriteChar('"');
  }
  writer.WriteChar('}');
  writer.Close();
  return result;
}

std::ostream& operator<<(std::ostream& out, const CsvRecord& record) {
  return out << record.DebugString();
}

}  // namespace riegeli
