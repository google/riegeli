// Copyright 2026 Google LLC
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

#include "riegeli/messages/serialized_message_assembler.h"

#include <stddef.h>
#include <stdint.h>

#include <string>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/container/inlined_vector.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/cord_iterator_span.h"
#include "riegeli/base/reset.h"
#include "riegeli/bytes/limiting_reader.h"
#include "riegeli/messages/field_handler_map.h"
#include "riegeli/messages/serialized_message_reader2.h"
#include "riegeli/messages/serialized_message_writer.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

SerializedMessageAssembler::Builder::Builder(Builder&& that) noexcept = default;

SerializedMessageAssembler::Builder&
SerializedMessageAssembler::Builder::operator=(Builder&& that) noexcept =
    default;

SerializedMessageAssembler::Builder::~Builder() = default;

void SerializedMessageAssembler::Builder::Reset() {
  next_parent_for_add_ = {/*raw=*/0};
  next_field_for_remove_ = {/*raw=*/0};
  RIEGELI_ASSERT_EQ(root_builder_.field_for_remove.raw,
                    FieldForRemove::kUnregistered);
  root_builder_.parent_for_add = ParentForAdd();
  root_builder_.children.clear();
}

SerializedMessageAssembler::ParentForAdd
SerializedMessageAssembler::Builder::RegisterParentForAdd(
    ParentPath parent_path) {
  RegisteredFieldBuilder& parent_builder = RegisterParentInternal(parent_path);
  if (parent_builder.parent_for_add.raw == ParentForAdd::kUnregistered) {
    parent_builder.parent_for_add.raw = next_parent_for_add_.raw++;
  }
  return parent_builder.parent_for_add;
}

SerializedMessageAssembler::FieldForRemove
SerializedMessageAssembler::Builder::RegisterFieldForRemove(
    ParentPath parent_path, int field_number) {
  RegisteredFieldBuilder& parent_builder = RegisterParentInternal(parent_path);
  RegisteredFieldBuilder& field_builder =
      parent_builder.children.try_emplace(field_number).first->second;
  if (field_builder.field_for_remove.raw == FieldForRemove::kUnregistered) {
    field_builder.field_for_remove.raw = next_field_for_remove_.raw++;
  }
  return field_builder.field_for_remove;
}

SerializedMessageAssembler::RegisteredFieldBuilder&
SerializedMessageAssembler::Builder::RegisterParentInternal(
    ParentPath parent_path) {
  RegisteredFieldBuilder* parent_builder = &root_builder_;
  for (const int field_number : parent_path) {
    parent_builder =
        &parent_builder->children.try_emplace(field_number).first->second;
  }
  return *parent_builder;
}

SerializedMessageAssembler::Session::Session(Session&& that) noexcept
    : fields_to_add_(std::move(that.fields_to_add_)),
      fields_to_remove_(std::exchange(that.fields_to_remove_, {})) {}

SerializedMessageAssembler::Session&
SerializedMessageAssembler::Session::operator=(Session&& that) noexcept {
  fields_to_add_ = std::move(that.fields_to_add_);
  fields_to_remove_ = std::exchange(that.fields_to_remove_, {});
  return *this;
}

SerializedMessageAssembler::Session::~Session() = default;

void SerializedMessageAssembler::Session::Reset() {
  for (FieldValues& field_values : fields_to_add_) {
    field_values.clear();
  }
  fields_to_remove_.clear();
}
void SerializedMessageAssembler::Session::Reset(
    const SerializedMessageAssembler& assembler) {
  Reset();
  fields_to_add_.resize(assembler.num_fields_for_add());
  fields_to_remove_.resize(assembler.num_fields_for_remove());
}

SerializedMessageAssembler::SerializedMessageAssembler(
    SerializedMessageAssembler&& that) noexcept = default;

SerializedMessageAssembler& SerializedMessageAssembler::operator=(
    SerializedMessageAssembler&& that) noexcept = default;

SerializedMessageAssembler::~SerializedMessageAssembler() = default;

SerializedMessageAssembler::SerializedMessageAssembler(Builder&& builder)
    : num_fields_for_add_(builder.next_parent_for_add_),
      num_fields_for_remove_(builder.next_field_for_remove_),
      root_(Build(
          /*field_number=*/0, std::move(builder.root_builder_))) {}

void SerializedMessageAssembler::Reset() {
  num_fields_for_add_ = {/*raw=*/0};
  num_fields_for_remove_ = {/*raw=*/0};
  RIEGELI_ASSERT_EQ(root_.field_number, 0);
  root_.parent_for_add = ParentForAdd();
  root_.submessages.clear();
  root_.handlers.Reset();
}

void SerializedMessageAssembler::Reset(Builder&& builder) {
  num_fields_for_add_ = builder.next_parent_for_add_;
  num_fields_for_remove_ = builder.next_field_for_remove_;
  RIEGELI_ASSERT_EQ(root_.field_number, 0);
  root_.submessages.clear();
  Build(
      /*field_number=*/0, std::move(builder.root_builder_), root_);
}

struct SerializedMessageAssembler::LeafHandler {
  static constexpr int kFieldNumber = kUnboundFieldNumber;

  absl::Status HandleVarint(
      uint64_t repr,
      ABSL_ATTRIBUTE_UNUSED absl::Span<FieldValues> fields_to_add,
      absl::Span<const bool> fields_to_remove,
      ABSL_ATTRIBUTE_UNUSED absl::Span<bool> submessages_rewritten,
      SerializedMessageWriter& message_writer) const {
    if (field_for_remove.raw < fields_to_remove.size() &&
        fields_to_remove[field_for_remove.raw]) {
      return absl::OkStatus();
    }
    return message_writer.WriteUInt64(field_number, repr);
  }

  absl::Status HandleFixed32(
      uint32_t repr,
      ABSL_ATTRIBUTE_UNUSED absl::Span<FieldValues> fields_to_add,
      absl::Span<const bool> fields_to_remove,
      ABSL_ATTRIBUTE_UNUSED absl::Span<bool> submessages_rewritten,
      SerializedMessageWriter& message_writer) const {
    if (field_for_remove.raw < fields_to_remove.size() &&
        fields_to_remove[field_for_remove.raw]) {
      return absl::OkStatus();
    }
    return message_writer.WriteFixed32(field_number, repr);
  }

  absl::Status HandleFixed64(
      uint64_t repr,
      ABSL_ATTRIBUTE_UNUSED absl::Span<FieldValues> fields_to_add,
      absl::Span<const bool> fields_to_remove,
      ABSL_ATTRIBUTE_UNUSED absl::Span<bool> submessages_rewritten,
      SerializedMessageWriter& message_writer) const {
    if (field_for_remove.raw < fields_to_remove.size() &&
        fields_to_remove[field_for_remove.raw]) {
      return absl::OkStatus();
    }
    return message_writer.WriteFixed64(field_number, repr);
  }

  absl::Status HandleLengthDelimitedFromReader(
      ReaderSpan<> repr,
      ABSL_ATTRIBUTE_UNUSED absl::Span<FieldValues> fields_to_add,
      absl::Span<const bool> fields_to_remove,
      ABSL_ATTRIBUTE_UNUSED absl::Span<bool> submessages_rewritten,
      SerializedMessageWriter& message_writer) const {
    if (field_for_remove.raw < fields_to_remove.size() &&
        fields_to_remove[field_for_remove.raw]) {
      return SkipLengthDelimitedFromReader(std::move(repr));
    }
    return message_writer.WriteString(field_number, std::move(repr));
  }

  absl::Status HandleLengthDelimitedFromCord(
      CordIteratorSpan repr, ABSL_ATTRIBUTE_UNUSED std::string& scratch,
      ABSL_ATTRIBUTE_UNUSED absl::Span<FieldValues> fields_to_add,
      absl::Span<const bool> fields_to_remove,
      ABSL_ATTRIBUTE_UNUSED absl::Span<bool> submessages_rewritten,
      SerializedMessageWriter& message_writer) const {
    if (field_for_remove.raw < fields_to_remove.size() &&
        fields_to_remove[field_for_remove.raw]) {
      return SkipLengthDelimitedFromCord(std::move(repr));
    }
    return message_writer.WriteString(field_number, std::move(repr));
  }

  absl::Status HandleLengthDelimitedFromString(
      absl::string_view repr,
      ABSL_ATTRIBUTE_UNUSED absl::Span<FieldValues> fields_to_add,
      absl::Span<const bool> fields_to_remove,
      ABSL_ATTRIBUTE_UNUSED absl::Span<bool> submessages_rewritten,
      SerializedMessageWriter& message_writer) const {
    if (field_for_remove.raw < fields_to_remove.size() &&
        fields_to_remove[field_for_remove.raw]) {
      return absl::OkStatus();
    }
    return message_writer.WriteString(field_number, repr);
  }

  absl::Status HandleBeginGroup(
      ABSL_ATTRIBUTE_UNUSED absl::Span<FieldValues> fields_to_add,
      absl::Span<const bool> fields_to_remove,
      ABSL_ATTRIBUTE_UNUSED absl::Span<bool> submessages_rewritten,
      SerializedMessageWriter& message_writer) const {
    if (field_for_remove.raw < fields_to_remove.size() &&
        fields_to_remove[field_for_remove.raw]) {
      return absl::OkStatus();
    }
    return message_writer.OpenGroup(field_number);
  }

  absl::Status HandleEndGroup(
      ABSL_ATTRIBUTE_UNUSED absl::Span<FieldValues> fields_to_add,
      absl::Span<const bool> fields_to_remove,
      ABSL_ATTRIBUTE_UNUSED absl::Span<bool> submessages_rewritten,
      SerializedMessageWriter& message_writer) const {
    if (field_for_remove.raw < fields_to_remove.size() &&
        fields_to_remove[field_for_remove.raw]) {
      return absl::OkStatus();
    }
    return message_writer.CloseGroup(field_number);
  }

  int field_number;
  FieldForRemove field_for_remove;
};

struct SerializedMessageAssembler::SubmessageHandler {
  static constexpr int kFieldNumber = kUnboundFieldNumber;

  absl::Status HandleLengthDelimitedFromReader(
      ReaderSpan<> repr, absl::Span<FieldValues> fields_to_add,
      absl::Span<const bool> fields_to_remove,
      absl::Span<bool> submessages_rewritten,
      SerializedMessageWriter& message_writer) const {
    if (field_for_remove.raw < fields_to_remove.size() &&
        fields_to_remove[field_for_remove.raw]) {
      return SkipLengthDelimitedFromReader(std::move(repr));
    }
    submessages_rewritten[submessage_index_in_parent] = true;
    message_writer.OpenLengthDelimited();
    if (absl::Status status =
            RewriteFields(fields_to_add, fields_to_remove, *message,
                          std::move(repr), message_writer);
        ABSL_PREDICT_FALSE(!status.ok())) {
      return status;
    }
    return message_writer.CloseLengthDelimited(message->field_number);
  }

  absl::Status HandleLengthDelimitedFromCord(
      CordIteratorSpan repr, ABSL_ATTRIBUTE_UNUSED std::string& scratch,
      absl::Span<FieldValues> fields_to_add,
      absl::Span<const bool> fields_to_remove,
      absl::Span<bool> submessages_rewritten,
      SerializedMessageWriter& message_writer) const {
    if (field_for_remove.raw < fields_to_remove.size() &&
        fields_to_remove[field_for_remove.raw]) {
      return SkipLengthDelimitedFromCord(std::move(repr));
    }
    submessages_rewritten[submessage_index_in_parent] = true;
    message_writer.OpenLengthDelimited();
    if (absl::Status status =
            RewriteFields(fields_to_add, fields_to_remove, *message,
                          std::move(repr), message_writer);
        ABSL_PREDICT_FALSE(!status.ok())) {
      return status;
    }
    return message_writer.CloseLengthDelimited(message->field_number);
  }

  absl::Status HandleLengthDelimitedFromString(
      absl::string_view repr, absl::Span<FieldValues> fields_to_add,
      absl::Span<const bool> fields_to_remove,
      absl::Span<bool> submessages_rewritten,
      SerializedMessageWriter& message_writer) const {
    if (field_for_remove.raw < fields_to_remove.size() &&
        fields_to_remove[field_for_remove.raw]) {
      return absl::OkStatus();
    }
    submessages_rewritten[submessage_index_in_parent] = true;
    message_writer.OpenLengthDelimited();
    if (absl::Status status = RewriteFields(fields_to_add, fields_to_remove,
                                            *message, repr, message_writer);
        ABSL_PREDICT_FALSE(!status.ok())) {
      return status;
    }
    return message_writer.CloseLengthDelimited(message->field_number);
  }

  SubmessageIndex submessage_index_in_parent;
  FieldForRemove field_for_remove;
  const RegisteredMessage* message;
};

void SerializedMessageAssembler::Build(int field_number,
                                       RegisteredFieldBuilder&& message_builder,
                                       RegisteredMessage& message) {
  RIEGELI_ASSERT(message.submessages.empty())
      << "Failed precondition of SerializedMessageAssembler::Build(): "
         "message not empty";
  message.field_number = field_number;
  message.parent_for_add = message_builder.parent_for_add;
  size_t num_submessages = 0;
  RewriteHandlers::Builder fields_builder;
  for (auto& entry : message_builder.children) {
    if (entry.second.parent_for_add.raw != ParentForAdd::kUnregistered ||
        !entry.second.children.empty()) {
      ++num_submessages;
    }
  }
  message.submessages.resize(num_submessages);
  size_t submessage_index = 0;
  for (auto& entry : message_builder.children) {
    if (entry.second.parent_for_add.raw != ParentForAdd::kUnregistered ||
        !entry.second.children.empty()) {
      RegisteredMessage* const submessage =
          &message.submessages[submessage_index];
      fields_builder.RegisterField(
          entry.first,
          SubmessageHandler{IntCast<SubmessageIndex>(submessage_index),
                            entry.second.field_for_remove, submessage});
      Build(entry.first, std::move(entry.second), *submessage);
      ++submessage_index;
    } else {
      fields_builder.RegisterField(
          entry.first, LeafHandler{entry.first, entry.second.field_for_remove});
    }
  }
  RIEGELI_ASSERT_EQ(submessage_index, num_submessages)
      << "The whole submessages array should have been filled";
  riegeli::Reset(message.handlers, std::move(fields_builder));
}

template absl::Status SerializedMessageAssembler::RewriteFields(
    absl::Span<FieldValues> fields_to_add,
    absl::Span<const bool> fields_to_remove, const RegisteredMessage& message,
    ReaderSpan<>&& src, SerializedMessageWriter& message_writer);
template absl::Status SerializedMessageAssembler::RewriteFields(
    absl::Span<FieldValues> fields_to_add,
    absl::Span<const bool> fields_to_remove, const RegisteredMessage& message,
    CordIteratorSpan&& src, SerializedMessageWriter& message_writer);
template absl::Status SerializedMessageAssembler::RewriteFields(
    absl::Span<FieldValues> fields_to_add,
    absl::Span<const bool> fields_to_remove, const RegisteredMessage& message,
    absl::string_view&& src, SerializedMessageWriter& message_writer);

absl::Status SerializedMessageAssembler::WriteNewFields(
    absl::Span<FieldValues> fields_to_add, const RegisteredMessage& message,
    absl::Span<const bool> submessages_rewritten,
    SerializedMessageWriter& message_writer) {
  const absl::Span<const RegisteredMessage> submessages = message.submessages;
  RIEGELI_ASSERT_EQ(submessages.size(), submessages_rewritten.size());

  // Create submessages for new fields and write these fields.
  for (size_t submessage_index = 0; submessage_index < submessages.size();
       ++submessage_index) {
    if (submessages_rewritten[submessage_index]) continue;
    const RegisteredMessage& submessage = submessages[submessage_index];
    message_writer.OpenLengthDelimited();
    if (absl::Status status = WriteNewFields(
            fields_to_add, submessage,
            SubmessageSet(submessage.submessages.size()), message_writer);
        ABSL_PREDICT_FALSE(!status.ok())) {
      return status;
    }
    if (absl::Status status = message_writer.CloseOptionalLengthDelimited(
            submessage.field_number);
        ABSL_PREDICT_FALSE(!status.ok())) {
      return status;
    }
  }

  // Write new fields directly to this message.
  if (message.parent_for_add.raw < fields_to_add.size()) {
    FieldValues& field_values = fields_to_add[message.parent_for_add.raw];
    for (FieldValue& field_value : field_values) {
      if (absl::Status status = std::move(field_value)(message_writer);
          ABSL_PREDICT_FALSE(!status.ok())) {
        return status;
      }
    }
    field_values.clear();
  }

  return absl::OkStatus();
}

}  // namespace riegeli
