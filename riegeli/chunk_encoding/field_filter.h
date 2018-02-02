// Copyright 2017 Google LLC
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

#ifndef RIEGELI_CHUNK_ENCODING_INCLUDE_FIELDS_H_
#define RIEGELI_CHUNK_ENCODING_INCLUDE_FIELDS_H_

#include <stdint.h>
#include <initializer_list>
#include <utility>
#include <vector>

#include "riegeli/base/base.h"

namespace riegeli {

// Specifies a set of fields to include.
class FieldFilter {
 public:
  using Field = std::vector<uint32_t>;

  // Includes all fields, do not filter anything out.
  static FieldFilter All() noexcept;

  // Includes only the specified fields.
  template <typename Iterator>
  FieldFilter(Iterator begin, Iterator end) : fields_(begin, end) {}

  // Includes only the specified fields.
  FieldFilter(std::initializer_list<Field> fields) : fields_(fields) {}

  // Starts with an empty set. Fields can be added with AddField().
  FieldFilter() = default;

  // Not defaulted because vector::vector(vector&&) and
  // vector::operator=(vector&&) are not noexcept before C++17.
  FieldFilter(FieldFilter&&) noexcept;
  FieldFilter& operator=(FieldFilter&&) noexcept;

  FieldFilter(const FieldFilter&);
  FieldFilter& operator=(const FieldFilter&);

  // Adds a field to the set.
  FieldFilter& AddField(Field field) & {
    fields_.push_back(std::move(field));
    return *this;
  }
  FieldFilter&& AddField(Field field) && {
    return std::move(AddField(std::move(field)));
  }

  bool include_all() const { return include_all_; }

  const std::vector<Field>& fields() const { return fields_; };

 private:
  bool include_all_ = false;
  std::vector<Field> fields_;
};

// Implementation details follow.

inline FieldFilter FieldFilter::All() noexcept {
  FieldFilter filter;
  filter.include_all_ = true;
  return filter;
}

inline FieldFilter::FieldFilter(FieldFilter&& src) noexcept
    : include_all_(riegeli::exchange(src.include_all_, false)),
      fields_(std::move(src.fields_)) {}

inline FieldFilter& FieldFilter::operator=(FieldFilter&& src) noexcept {
  include_all_ = riegeli::exchange(src.include_all_, false);
  fields_ = std::move(src.fields_);
  return *this;
}

inline FieldFilter::FieldFilter(const FieldFilter& src)
    : include_all_(src.include_all_), fields_(src.fields_) {}

inline FieldFilter& FieldFilter::operator=(const FieldFilter& src) {
  include_all_ = src.include_all_;
  fields_ = src.fields_;
  return *this;
}

}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_INCLUDE_FIELDS_H_
