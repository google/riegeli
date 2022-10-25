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
#include "riegeli/base/intrusive_ref_count.h"
#include "riegeli/bytes/ostream_writer.h"
#include "riegeli/bytes/string_writer.h"
#include "riegeli/bytes/writer.h"

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
    writer.Write(absl::string_view(start, PtrDistance(start, next_quote + 1)));
    start = next_quote;
    next_to_check = next_quote + 1;
  }
  writer.Write(absl::string_view(start, PtrDistance(start, limit)));
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

ABSL_CONST_INIT absl::Mutex CsvHeader::payload_cache_mutex_(absl::kConstInit);
ABSL_CONST_INIT RefCountedPtr<CsvHeader::Payload> CsvHeader::payload_cache_;

CsvHeader::CsvHeader(std::initializer_list<absl::string_view> names) {
  const absl::Status status = TryResetInternal(nullptr, names);
  RIEGELI_CHECK(status.ok())
      << "Failed precondition of CsvHeader::CsvHeader(): " << status.message();
}

CsvHeader::CsvHeader(std::function<std::string(absl::string_view)> normalizer)
    : payload_(normalizer == nullptr
                   ? nullptr
                   : MakeRefCounted<Payload>(std::move(normalizer))) {}

CsvHeader::CsvHeader(std::function<std::string(absl::string_view)> normalizer,
                     std::initializer_list<absl::string_view> names) {
  const absl::Status status = TryResetInternal(std::move(normalizer), names);
  RIEGELI_CHECK(status.ok())
      << "Failed precondition of CsvHeader::CsvHeader(): " << status.message();
}

void CsvHeader::Reset() { payload_.reset(); }

void CsvHeader::Reset(std::initializer_list<absl::string_view> names) {
  const absl::Status status = TryResetInternal(nullptr, names);
  RIEGELI_CHECK(status.ok())
      << "Failed precondition of CsvHeader::Reset(): " << status.message();
}

void CsvHeader::Reset(
    std::function<std::string(absl::string_view)> normalizer) {
  payload_ = normalizer == nullptr
                 ? nullptr
                 : MakeRefCounted<Payload>(std::move(normalizer));
}

void CsvHeader::Reset(std::function<std::string(absl::string_view)> normalizer,
                      std::initializer_list<absl::string_view> names) {
  const absl::Status status = TryResetInternal(std::move(normalizer), names);
  RIEGELI_CHECK(status.ok())
      << "Failed precondition of CsvHeader::Reset(): " << status.message();
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
  if (payload_->normalizer == nullptr) {
    RefCountedPtr<Payload> old_payload_cache;
    {
      absl::MutexLock lock(&payload_cache_mutex_);
      old_payload_cache = payload_cache_.exchange(payload_);
    }
    // Destroy `old_payload_cache` after releasing `payload_cache_mutex_`.
  }
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

bool operator==(const CsvHeader& a, const CsvHeader& b) {
  if (ABSL_PREDICT_TRUE(a.payload_ == b.payload_)) return true;
  if (a.payload_ == nullptr || b.payload_ == nullptr) return false;
  return a.payload_->index_to_name == b.payload_->index_to_name;
}

inline void CsvHeader::EnsureUniqueOwner() {
  if (payload_ == nullptr) {
    payload_ = MakeRefCounted<Payload>();
  } else if (ABSL_PREDICT_FALSE(!payload_->has_unique_owner())) {
    payload_ = MakeRefCounted<Payload>(*payload_);
  }
}

void CsvHeader::WriteDebugStringTo(Writer& writer) const {
  for (iterator iter = cbegin(); iter != cend(); ++iter) {
    if (iter != cbegin()) writer.WriteChar(',');
    csv_internal::WriteDebugQuotedIfNeeded(*iter, writer);
  }
}

std::string CsvHeader::DebugString() const {
  std::string result;
  StringWriter<> writer(&result);
  WriteDebugStringTo(writer);
  writer.Close();
  return result;
}

std::ostream& operator<<(std::ostream& out, const CsvHeader& header) {
  OStreamWriter writer(&out);
  header.WriteDebugStringTo(writer);
  writer.Close();
  return out;
}

CsvRecord::CsvRecord(CsvHeader header,
                     std::initializer_list<absl::string_view> fields) {
  const absl::Status status = TryResetInternal(std::move(header), fields);
  RIEGELI_CHECK(status.ok())
      << "Failed precondition of CsvRecord::CsvRecord(): " << status.message();
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
  RIEGELI_CHECK(status.ok())
      << "Failed precondition of CsvRecord::Reset(): " << status.message();
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

absl::Status CsvRecord::FailMissingNames(
    absl::Span<const std::string> missing_names) const {
  StringWriter<std::string> message;
  message.Write("Missing field names: ");
  for (absl::Span<const std::string>::const_iterator iter =
           missing_names.cbegin();
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

bool operator==(const CsvRecord& a, const CsvRecord& b) {
  return a.header() == b.header() && a.fields() == b.fields();
}

void CsvRecord::WriteDebugStringTo(Writer& writer) const {
  RIEGELI_ASSERT_EQ(header_.size(), fields_.size())
      << "Failed invariant of CsvRecord: "
         "mismatched length of CSV header and fields";
  for (const_iterator iter = cbegin(); iter != cend(); ++iter) {
    if (iter != cbegin()) writer.WriteChar(',');
    csv_internal::WriteDebugQuotedIfNeeded(iter->first, writer);
    writer.WriteChar(':');
    csv_internal::WriteDebugQuotedIfNeeded(iter->second, writer);
  }
}

std::string CsvRecord::DebugString() const {
  std::string result;
  StringWriter<> writer(&result);
  WriteDebugStringTo(writer);
  writer.Close();
  return result;
}

std::ostream& operator<<(std::ostream& out, const CsvRecord& record) {
  OStreamWriter writer(&out);
  record.WriteDebugStringTo(writer);
  writer.Close();
  return out;
}

}  // namespace riegeli
