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

#include <initializer_list>
#include <utility>

#include "absl/container/inlined_vector.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/initializer.h"

namespace riegeli {

// Specifies a proto field path.
class Field {
 public:
  using Path = absl::InlinedVector<int, 1>;

  // A special field number value which can be added to the end of the path.
  //
  // It preserves field existence but ignores its value, which is replaced with
  // a default value for the type (zero, empty string, empty message).
  //
  // This is useful to include a required field which is not otherwise needed.
  // This works similarly to specifying a non-existent child field, but applies
  // not only to submessages.
  //
  // Warning: for a repeated field this preserves the field count only if the
  // field is not packed.
  static constexpr int kExistenceOnly = 0;

  // Specifies the path using a sequence of proto field numbers descending from
  // the root message.
  //
  // Field numbers can be obtained from `Type::k*FieldNumber` constants exported
  // by compiled proto messages, or from `FieldDescriptor::number()`.
  /*implicit*/ Field(std::initializer_list<int> path);
  Field& operator=(std::initializer_list<int> path);

  // Starts with the root message. Field path can be built with
  // `AddFieldNumber()`.
  Field() = default;

  Field(const Field& that) = default;
  Field& operator=(const Field& that) = default;

  Field(Field&& that) = default;
  Field& operator=(Field&& that) = default;

  // Adds a field to the end of the path.
  Field& AddFieldNumber(int field_number) &;
  Field&& AddFieldNumber(int field_number) &&;

  // Returns the sequence of proto field numbers descending from the root
  // message.
  const Path& path() const { return path_; }

 private:
  static void AssertValid(int field_number);

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
  FieldProjection& operator=(std::initializer_list<Field> fields);

  // Starts with an empty set to include. Fields can be added by `AddField()`.
  FieldProjection() = default;

  FieldProjection(const FieldProjection&) = default;
  FieldProjection& operator=(const FieldProjection&) = default;

  FieldProjection(FieldProjection&&) = default;
  FieldProjection& operator=(FieldProjection&&) = default;

  // Adds a field to the set to include.
  FieldProjection& AddField(Initializer<Field> field) &;
  FieldProjection&& AddField(Initializer<Field> field) &&;
  FieldProjection& AddField(std::initializer_list<int> path) &;
  FieldProjection&& AddField(std::initializer_list<int> path) &&;

  // Returns true if all fields are included, i.e. if the root message is
  // included.
  bool includes_all() const;

  // Returns the set of fields to include.
  const Fields& fields() const { return fields_; }

 private:
  Fields fields_;
};

// Implementation details follow.

inline Field::Field(std::initializer_list<int> path) : path_(path) {
  for (const int field_number : path_) AssertValid(field_number);
}

inline Field& Field::operator=(std::initializer_list<int> path) {
  for (const int field_number : path) AssertValid(field_number);
  path_ = path;
  return *this;
}

inline void Field::AssertValid(int field_number) {
  static_assert(kExistenceOnly == 0,
                "Field::AssertValid() assumes that kExistenceOnly == 0");
  RIEGELI_ASSERT_GE(field_number, 0) << "Field number out of range";
  RIEGELI_ASSERT_LE(field_number, (1 << 29) - 1) << "Field number out of range";
}

inline Field& Field::AddFieldNumber(int field_number) & {
  AssertValid(field_number);
  path_.push_back(field_number);
  return *this;
}

inline Field&& Field::AddFieldNumber(int field_number) && {
  return std::move(AddFieldNumber(field_number));
}

inline FieldProjection FieldProjection::All() {
  FieldProjection field_projection;
  field_projection.AddField(Field());
  return field_projection;
}

inline FieldProjection::FieldProjection(std::initializer_list<Field> fields)
    : fields_(fields) {}

inline FieldProjection& FieldProjection::operator=(
    std::initializer_list<Field> fields) {
  fields_ = fields;
  return *this;
}

inline FieldProjection& FieldProjection::AddField(Initializer<Field> field) & {
  fields_.emplace_back(std::move(field));
  return *this;
}

inline FieldProjection&& FieldProjection::AddField(
    Initializer<Field> field) && {
  return std::move(AddField(std::move(field)));
}

inline FieldProjection& FieldProjection::AddField(
    std::initializer_list<int> path) & {
  return AddField(Initializer<Field>(path));
}

inline FieldProjection&& FieldProjection::AddField(
    std::initializer_list<int> path) && {
  return std::move(AddField(std::move(path)));
}

inline bool FieldProjection::includes_all() const {
  for (const Field& field : fields_) {
    if (field.path().empty()) return true;
  }
  return false;
}

}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_FIELD_PROJECTION_H_
