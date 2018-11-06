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

#ifndef RIEGELI_CHUNK_ENCODING_FIELD_PROJECTION_H_
#define RIEGELI_CHUNK_ENCODING_FIELD_PROJECTION_H_

#include <stdint.h>
#include <initializer_list>
#include <utility>

#include "absl/container/inlined_vector.h"
#include "riegeli/base/base.h"

namespace riegeli {

// Specifies a proto field path.
class Field {
 public:
  using Path = absl::InlinedVector<uint32_t, 1>;

  // Specifies the path using a sequence of proto field tags descending from the
  // root message.
  //
  // Tags can be obtained from Type::k*FieldNumber constants exported by
  // compiled proto messages, or from FieldDescriptor::number().
  /*implicit*/ Field(std::initializer_list<uint32_t> path);

  // Starts with the root message. Field tags can be added by AddTag().
  Field() noexcept {}

  Field(Field&& that) noexcept;
  Field& operator=(Field&& that) noexcept;

  Field(const Field& that);
  Field& operator=(const Field& that);

  // Adds a field tag to the end of the path.
  Field& AddTag(uint32_t tag) &;
  Field&& AddTag(uint32_t tag) &&;

  // Returns the sequence of proto field tags descending from the root message.
  const Path& path() const { return path_; }

 private:
  static void AssertValid(uint32_t tag);

  Path path_;
};

// Specifies a set of fields to include.
class FieldProjection {
 public:
  using Fields = absl::InlinedVector<Field, 1>;

  // Includes all fields.
  static FieldProjection All();

  // Includes only the specified fields.
  /*implicit*/ FieldProjection(std::initializer_list<Field> fields);

  // Starts with an empty set to include. Fields can be added by AddField().
  FieldProjection() noexcept {}

  FieldProjection(FieldProjection&&) noexcept;
  FieldProjection& operator=(FieldProjection&&) noexcept;

  FieldProjection(const FieldProjection&);
  FieldProjection& operator=(const FieldProjection&);

  // Adds a field to the set to include.
  FieldProjection& AddField(Field field) &;
  FieldProjection&& AddField(Field field) &&;

  // Returns true if all fields are included, i.e. if the root message is
  // included.
  bool includes_all() const;

  // Returns the set of fields to include.
  const Fields& fields() const { return fields_; }

 private:
  Fields fields_;
};

// Implementation details follow.

inline Field::Field(std::initializer_list<uint32_t> path) : path_(path) {
  for (const uint32_t tag : path_) AssertValid(tag);
}

inline void Field::AssertValid(uint32_t tag) {
  RIEGELI_ASSERT_GE(tag, 1u) << "Field tag out of range";
  RIEGELI_ASSERT_LE(tag, (uint32_t{1} << 29) - 1) << "Field tag out of range";
}

inline Field::Field(Field&& that) noexcept : path_(std::move(that.path_)) {}

inline Field& Field::operator=(Field&& that) noexcept {
  path_ = std::move(that.path_);
  return *this;
}

inline Field::Field(const Field& that) : path_(that.path_) {}

inline Field& Field::operator=(const Field& that) {
  path_ = that.path_;
  return *this;
}

inline Field& Field::AddTag(uint32_t tag) & {
  AssertValid(tag);
  path_.push_back(tag);
  return *this;
}

inline Field&& Field::AddTag(uint32_t tag) && { return std::move(AddTag(tag)); }

inline FieldProjection FieldProjection::All() {
  FieldProjection field_projection;
  field_projection.AddField(Field());
  return field_projection;
}

inline FieldProjection::FieldProjection(std::initializer_list<Field> fields)
    : fields_(fields) {}

inline FieldProjection::FieldProjection(FieldProjection&& that) noexcept
    : fields_(std::move(that.fields_)) {}

inline FieldProjection& FieldProjection::operator=(
    FieldProjection&& that) noexcept {
  fields_ = std::move(that.fields_);
  return *this;
}

inline FieldProjection::FieldProjection(const FieldProjection& that)
    : fields_(that.fields_) {}

inline FieldProjection& FieldProjection::operator=(
    const FieldProjection& that) {
  fields_ = that.fields_;
  return *this;
}

inline FieldProjection& FieldProjection::AddField(Field field) & {
  fields_.push_back(std::move(field));
  return *this;
}

inline FieldProjection&& FieldProjection::AddField(Field field) && {
  return std::move(AddField(std::move(field)));
}

inline bool FieldProjection::includes_all() const {
  for (const Field& field : fields_) {
    if (field.path().empty()) return true;
  }
  return false;
}

}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_FIELD_PROJECTION_H_
