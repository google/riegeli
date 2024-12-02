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
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/base/const_init.h"
#include "absl/base/optimization.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/shared_ptr.h"
#include "riegeli/bytes/ostream_writer.h"
#include "riegeli/bytes/string_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

namespace {

inline void WriteDebugQuoted(absl::string_view src, Writer& dest,
                             size_t already_scanned) {
  dest.Write('"');
  const char* start = src.data();
  const char* next_to_check = src.data() + already_scanned;
  const char* const limit = src.data() + src.size();
  // Write characters in the range [`start`..`limit`), except that if quotes are
  // found in the range [`next_to_check`..`limit`), write them twice.
  while (const char* const next_quote = static_cast<const char*>(std::memchr(
             next_to_check, '"', PtrDistance(next_to_check, limit)))) {
    dest.Write(absl::string_view(start, PtrDistance(start, next_quote + 1)));
    start = next_quote;
    next_to_check = next_quote + 1;
  }
  dest.Write(absl::string_view(start, PtrDistance(start, limit)), '"');
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

void WriteDebugQuotedIfNeeded(absl::string_view src, Writer& dest) {
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
        WriteDebugQuoted(src, dest, i);
        return;
    }
  }
  dest.Write(src);
}

}  // namespace csv_internal

std::string AsciiCaseInsensitive(absl::string_view name) {
  return absl::AsciiStrToLower(name);
}

inline CsvHeader::Payload::Payload(const Payload& that)
    : normalizer(that.normalizer),
      index_to_name(that.index_to_name),
      name_to_index(that.name_to_index) {}

ABSL_CONST_INIT absl::Mutex CsvHeader::payload_cache_mutex_(absl::kConstInit);
ABSL_CONST_INIT SharedPtr<CsvHeader::Payload> CsvHeader::payload_cache_;

CsvHeader::CsvHeader(std::initializer_list<absl::string_view> names) {
  const absl::Status status = TryResetInternal(nullptr, names);
  RIEGELI_CHECK_OK(status) << "Failed precondition of CsvHeader::CsvHeader()";
}

CsvHeader& CsvHeader::operator=(
    std::initializer_list<absl::string_view> names) {
  const absl::Status status = TryResetInternal(nullptr, names);
  RIEGELI_CHECK_OK(status) << "Failed precondition of CsvHeader::operator=()";
  return *this;
}

CsvHeader::CsvHeader(std::function<std::string(absl::string_view)> normalizer)
    : payload_(normalizer == nullptr ? nullptr
                                     : SharedPtr<Payload>(riegeli::Maker(
                                           std::move(normalizer)))) {}

CsvHeader::CsvHeader(std::function<std::string(absl::string_view)> normalizer,
                     std::initializer_list<absl::string_view> names) {
  const absl::Status status = TryResetInternal(std::move(normalizer), names);
  RIEGELI_CHECK_OK(status) << "Failed precondition of CsvHeader::CsvHeader()";
}

void CsvHeader::Reset() { payload_.Reset(); }

void CsvHeader::Reset(std::initializer_list<absl::string_view> names) {
  const absl::Status status = TryResetInternal(nullptr, names);
  RIEGELI_CHECK_OK(status) << "Failed precondition of CsvHeader::Reset()";
}

void CsvHeader::Reset(
    std::function<std::string(absl::string_view)> normalizer) {
  if (normalizer == nullptr) {
    payload_.Reset();
  } else {
    payload_.Reset(riegeli::Maker(std::move(normalizer)));
  }
}

void CsvHeader::Reset(std::function<std::string(absl::string_view)> normalizer,
                      std::initializer_list<absl::string_view> names) {
  const absl::Status status = TryResetInternal(std::move(normalizer), names);
  RIEGELI_CHECK_OK(status) << "Failed precondition of CsvHeader::Reset()";
}

absl::Status CsvHeader::TryReset(
    std::initializer_list<absl::string_view> names) {
  return TryResetInternal(nullptr, names);
}

absl::Status CsvHeader::TryReset(
    std::function<std::string(absl::string_view)> normalizer,
    std::initializer_list<absl::string_view> names) {
  return TryResetInternal(std::move(normalizer), names);
}

absl::Status CsvHeader::TryResetUncached(
    std::function<std::string(absl::string_view)>&& normalizer,
    std::vector<std::string>&& names) {
  EnsureUnique();
  payload_->normalizer = std::move(normalizer);
  payload_->name_to_index.clear();
  payload_->name_to_index.reserve(names.size());
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
    payload_.Reset();
    StringWriter<std::string> message;
    message.Write("Duplicate field names: ");
    for (std::vector<absl::string_view>::const_iterator iter =
             duplicate_names.cbegin();
         iter != duplicate_names.cend(); ++iter) {
      if (iter != duplicate_names.cbegin()) message.Write(',');
      csv_internal::WriteDebugQuotedIfNeeded(*iter, message);
    }
    message.Close();
    return absl::FailedPreconditionError(message.dest());
  }
  payload_->index_to_name = std::move(names);
  if (payload_->normalizer == nullptr) {
    SharedPtr<Payload> old_payload_cache;
    {
      absl::MutexLock lock(&payload_cache_mutex_);
      old_payload_cache = std::exchange(payload_cache_, payload_);
    }
    // Destroy `old_payload_cache` after releasing `payload_cache_mutex_`.
  }
  return absl::OkStatus();
}

void CsvHeader::Reserve(size_t size) {
  EnsureUnique();
  payload_->index_to_name.reserve(size);
  payload_->name_to_index.reserve(size);
}

void CsvHeader::Add(Initializer<std::string>::AllowingExplicit name) {
  const absl::Status status = TryAdd(std::move(name));
  RIEGELI_CHECK_OK(status) << "Failed precondition of CsvHeader::Add()";
}

absl::Status CsvHeader::TryAdd(
    Initializer<std::string>::AllowingExplicit name) {
  EnsureUnique();
  std::string name_string = std::move(name);
  const size_t index = payload_->index_to_name.size();
  const std::pair<absl::flat_hash_map<std::string, size_t>::iterator, bool>
      insert_result = payload_->normalizer == nullptr
                          ? payload_->name_to_index.emplace(name_string, index)
                          : payload_->name_to_index.emplace(
                                payload_->normalizer(name_string), index);
  if (ABSL_PREDICT_FALSE(!insert_result.second)) {
    RIEGELI_ASSERT(!empty())
        << "It should not have been needed to ensure that an empty CsvHeader "
           "has payload_ == nullptr because a duplicate field name is possible "
           "only if some fields were already present";
    StringWriter<std::string> message;
    message.Write("Duplicate field name: ");
    csv_internal::WriteDebugQuotedIfNeeded(name_string, message);
    message.Close();
    return absl::FailedPreconditionError(message.dest());
  }
  payload_->index_to_name.push_back(std::move(name_string));
  return absl::OkStatus();
}

CsvHeader::iterator CsvHeader::find(absl::string_view name) const
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  if (ABSL_PREDICT_FALSE(payload_ == nullptr)) return iterator();
  const absl::flat_hash_map<std::string, size_t>::const_iterator iter =
      payload_->normalizer == nullptr
          ? payload_->name_to_index.find(name)
          : payload_->name_to_index.find(payload_->normalizer(name));
  if (ABSL_PREDICT_FALSE(iter == payload_->name_to_index.cend())) {
    return iterator(payload_->index_to_name.cend());
  }
  return iterator(payload_->index_to_name.cbegin() + iter->second);
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

bool CsvHeader::Equal(const CsvHeader& a, const CsvHeader& b) {
  if (ABSL_PREDICT_TRUE(a.payload_ == b.payload_)) return true;
  if (a.payload_ == nullptr || b.payload_ == nullptr) return false;
  return a.payload_->index_to_name == b.payload_->index_to_name;
}

inline void CsvHeader::EnsureUnique() {
  if (payload_ == nullptr) {
    payload_.Reset(riegeli::Maker());
  } else if (ABSL_PREDICT_FALSE(!payload_.IsUnique())) {
    payload_ = SharedPtr<Payload>(*payload_);
  }
}

void CsvHeader::WriteDebugStringTo(Writer& dest) const {
  for (iterator iter = cbegin(); iter != cend(); ++iter) {
    if (iter != cbegin()) dest.Write(',');
    csv_internal::WriteDebugQuotedIfNeeded(*iter, dest);
  }
}

std::string CsvHeader::DebugString() const {
  std::string result;
  StringWriter<> writer(&result);
  WriteDebugStringTo(writer);
  writer.Close();
  return result;
}

void CsvHeader::Output(std::ostream& dest) const {
  OStreamWriter<> writer(&dest);
  WriteDebugStringTo(writer);
  writer.Close();
}

CsvRecord::CsvRecord(CsvHeader header,
                     std::initializer_list<absl::string_view> fields) {
  const absl::Status status = TryResetInternal(std::move(header), fields);
  RIEGELI_CHECK_OK(status) << "Failed precondition of CsvRecord::CsvRecord()";
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

void CsvRecord::Reset(CsvHeader header,
                      std::initializer_list<absl::string_view> fields) {
  const absl::Status status = TryResetInternal(std::move(header), fields);
  RIEGELI_CHECK_OK(status) << "Failed precondition of CsvRecord::Reset()";
}

absl::Status CsvRecord::TryReset(
    CsvHeader header, std::initializer_list<absl::string_view> fields) {
  return TryResetInternal(std::move(header), fields);
}

absl::Status CsvRecord::TryResetInternal(CsvHeader&& header,
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

void CsvRecord::Clear() {
  for (std::string& value : fields_) value.clear();
}

std::string& CsvRecord::operator[](absl::string_view name)
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  const CsvHeader::iterator name_iter = header_.find(name);
  RIEGELI_CHECK(name_iter != header_.end())
      << "Failed precondition of CsvRecord::operator[]: missing field name: "
      << DebugQuotedIfNeeded(name) << "; existing field names: " << header_;
  return fields_[name_iter - header_.begin()];
}

const std::string& CsvRecord::operator[](absl::string_view name) const
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  const CsvHeader::iterator name_iter = header_.find(name);
  RIEGELI_CHECK(name_iter != header_.end())
      << "Failed precondition of CsvRecord::operator[]: missing field name: "
      << DebugQuotedIfNeeded(name) << "; existing field names: " << header_;
  return fields_[name_iter - header_.begin()];
}

CsvRecord::iterator CsvRecord::find(absl::string_view name)
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  const CsvHeader::iterator name_iter = header_.find(name);
  RIEGELI_ASSERT(name_iter >= header_.begin() && name_iter <= header_.end())
      << "Failed precondition of CsvRecord::find(): "
         "field name iterator does not belong to the same header";
  return iterator(name_iter, fields_.begin() + (name_iter - header_.begin()));
}

CsvRecord::const_iterator CsvRecord::find(absl::string_view name) const
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
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
  Merge<std::initializer_list<std::pair<absl::string_view, absl::string_view>>>(
      std::move(src));
}

absl::Status CsvRecord::TryMerge(
    std::initializer_list<std::pair<absl::string_view, absl::string_view>>
        src) {
  return TryMerge<
      std::initializer_list<std::pair<absl::string_view, absl::string_view>>>(
      std::move(src));
}

absl::Status CsvRecord::FailMissingNames(
    absl::Span<const std::string> missing_names) const {
  StringWriter<std::string> message;
  message.Write("Missing field names: ");
  for (absl::Span<const std::string>::const_iterator iter =
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
  return absl::FailedPreconditionError(message.dest());
}

bool CsvRecord::Equal(const CsvRecord& a, const CsvRecord& b) {
  return a.header() == b.header() && a.fields() == b.fields();
}

void CsvRecord::WriteDebugStringTo(Writer& dest) const {
  RIEGELI_ASSERT_EQ(header_.size(), fields_.size())
      << "Failed invariant of CsvRecord: "
         "mismatched length of CSV header and fields";
  for (const_iterator iter = cbegin(); iter != cend(); ++iter) {
    if (iter != cbegin()) dest.Write(',');
    csv_internal::WriteDebugQuotedIfNeeded(iter->first, dest);
    dest.Write(':');
    csv_internal::WriteDebugQuotedIfNeeded(iter->second, dest);
  }
}

std::string CsvRecord::DebugString() const {
  std::string result;
  StringWriter<> writer(&result);
  WriteDebugStringTo(writer);
  writer.Close();
  return result;
}

void CsvRecord::Output(std::ostream& dest) const {
  OStreamWriter<> writer(&dest);
  WriteDebugStringTo(writer);
  writer.Close();
}

}  // namespace riegeli
