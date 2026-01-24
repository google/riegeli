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

#ifndef RIEGELI_MESSAGES_SERIALIZED_MESSAGE_ASSEMBLER_H_
#define RIEGELI_MESSAGES_SERIALIZED_MESSAGE_ASSEMBLER_H_

#include <stddef.h>
#include <stdint.h>

#include <functional>
#include <limits>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/container/inlined_vector.h"
#include "absl/container/linked_hash_map.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/types/span.h"
#include "riegeli/base/bytes_ref.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/cord_iterator_span.h"
#include "riegeli/base/dependency.h"
#include "riegeli/bytes/chain_writer.h"
#include "riegeli/bytes/cord_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/string_writer.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/messages/field_handler_map.h"
#include "riegeli/messages/serialized_message_reader2.h"
#include "riegeli/messages/serialized_message_writer.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

// `SerializedMessageAssembler` writes a serialized proto message based on a
// serialized base message and added and/or removed fields.
//
// Parents of added fields are created implicitly if they are not present in the
// base message.
//
// A `SerializedMessageAssembler` object maintains a structure of fields to be
// added and/or removed, which can be efficiently reused for multiple messages.
//
// Usage:
// ```
//   // Register message structure.
//   riegeli::SerializedMessageAssembler::Builder message_assembler_builder;
//   for (const auto& field_name : fields_to_add) {
//     field_name.parent_for_add =
//         message_assembler_builder.RegisterParentForAdd(
//             field_name.parent_path);
//   }
//   for (const auto& field_name : fields_to_remove) {
//     field_name.parent_for_remove =
//         message_assembler_builder.RegisterFieldForRemove(
//             field_name.parent_path, field_name.field_number);
//   }
//   SerializedMessageAssembler message_assembler(
//       std::move(message_assembler_builder));
//
//   // Assemble messages.
//   for (auto& message : messages) {
//     riegeli::SerializedMessageAssembler::Session session(message_assembler);
//     for (const auto& field : message.fields_to_add) {
//       session.AddField(
//           field.parent_for_add,
//           [field_number = field.field_number, value = field.value](
//               riegeli::SerializedMessageWriter& message_writer) mutable {
//             // Example for a string field:
//             return message_writer.WriteString(field_number,
//                                               std::move(value));
//           });
//     }
//     for (const auto& field : message.fields_to_remove) {
//       session.RemoveField(field.parent_for_remove);
//     }
//     std::move(session).WriteMessage(message_assembler, base_message,
//                                     message.serialized);
//   }
// ```
//
// Alternatively, the stages of registration and assembly can be interleaved.
// This is reasonable for assembling a single message. This would be inefficient
// for multiple messages.
// ```
//   riegeli::SerializedMessageAssembler::Builder message_assembler_builder;
//   riegeli::SerializedMessageAssembler::Session session;
//   for (const auto& field : fields_to_add) {
//     session.AddField(
//         message_assembler_builder.RegisterParentForAdd(field.parent_path),
//         [field_number = field.field_number, value = field.value](
//             riegeli::SerializedMessageWriter& message_writer) mutable {
//           // Example for a string field:
//           return message_writer.WriteString(field_number, std::move(value));
//         });
//   }
//   for (const auto& field : fields_to_remove) {
//     session.RemoveField(message_assembler_builder.RegisterFieldForRemove(
//         field.parent_path, field.field_number));
//   }
//   SerializedMessageAssembler message_assembler(
//       std::move(message_assembler_builder));
//   std::move(session).WriteMessage(message_assembler, base_message,
//                                   serialized_message);
// ```
class SerializedMessageAssembler {
 public:
  // A sequence of field numbers identifying a length-delimited field.
  //
  // Each element of a `ParentPath` should correspond to a singular submessage
  // field contained in the previous message, except that the last element may
  // correspond to a singular `string` or `bytes` field, or to a packed repeated
  // field.
  //
  // An empty `ParentPath` indicates the root message.
  using ParentPath = absl::Span<const int>;

  // Identifies the parent of fields to be added, binding
  // `Builder::RegisterParentForAdd()` with `Session::AddField()`.
  // Usually treated as an opaque type.
  struct ParentForAdd {
    static constexpr uint32_t kUnregistered =
        std::numeric_limits<uint32_t>::max();
    uint32_t raw = kUnregistered;
  };

  // Identifies a field to be removed, binding
  // `Builder::RegisterFieldForRemove()` with `Session::RemoveField()`.
  // Usually treated as an opaque type.
  struct FieldForRemove {
    static constexpr uint32_t kUnregistered =
        std::numeric_limits<uint32_t>::max();
    uint32_t raw = kUnregistered;
  };

  // Prepares a `SerializedMessageAssembler`.
  class Builder;

  // Maintains field values during writing a single message.
  class Session;

  // Creates an empty `SerializedMessageAssembler`. Designed for `Reset()`.
  SerializedMessageAssembler() = default;

  // Builds a `SerializedMessageAssembler`.
  explicit SerializedMessageAssembler(Builder&& builder);

  SerializedMessageAssembler(SerializedMessageAssembler&& that) noexcept;
  SerializedMessageAssembler& operator=(
      SerializedMessageAssembler&& that) noexcept;

  ~SerializedMessageAssembler();

  // Makes `*this` equivalent to a newly constructed
  // `SerializedMessageAssembler`.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset();
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Builder&& builder);

 private:
  // A value to be written to a `SerializedMessageWriter`. Its type varies,
  // so it is represented as an invocable.
  using FieldValue = absl::AnyInvocable<absl::Status(
      SerializedMessageWriter& message_writer) &&>;
  // Values to be written to a `SerializedMessageWriter` under one parent.
  using FieldValues = std::vector<FieldValue>;

  // Which submessages of a given message are present in the base message,
  // in the order corresponding to `RegisteredMessage::submessages`.
  using SubmessageSet = absl::InlinedVector<bool, 8>;
  // An index into `SubmessageSet`.
  using SubmessageIndex = uint32_t;

  // Field handlers for reading the base message.
  using RewriteHandlers = FieldHandlerMap<
      // Remaining field values to write, indexed by `ParentForAdd`.
      const absl::Span<FieldValues>,
      // Fields to remove, indexed by `FieldForRemove`.
      const absl::Span<const bool>,
      // Which submessages of this message have been rewritten, in the order
      // corresponding to `RegisteredMessage::submessages`. Submessages absent
      // in the base message will be created at the end of the message.
      const absl::Span<bool>,
      // The destination message writer.
      SerializedMessageWriter>;

  // Copies unhandled fields.
  using CopyingHandler =
      CopyingFieldHandler<const absl::Span<FieldValues>,
                          const absl::Span<const bool>, const absl::Span<bool>,
                          SerializedMessageWriter>;

  // During registration, maintains information about a field or root.
  struct RegisteredFieldBuilder {
    ParentForAdd parent_for_add;
    FieldForRemove field_for_remove;  // Not used in root.
    absl::linked_hash_map<int, RegisteredFieldBuilder> children;
  };

  // During assembly, maintains information about a message, including root.
  struct RegisteredMessage {
    RegisteredMessage() noexcept : field_number(0) {}
    explicit RegisteredMessage(int field_number) : field_number(field_number) {}

    int field_number;  // Not used in root.
    ParentForAdd parent_for_add;
    // In registration order. Must not be reallocated after initialization.
    std::vector<RegisteredMessage> submessages;
    // `handlers` may refer to elements of `submessages`.
    RewriteHandlers handlers;
  };

  struct LeafHandler;
  struct SubmessageHandler;

  static RegisteredMessage Build(int field_number,
                                 RegisteredFieldBuilder&& message_builder) {
    RegisteredMessage message;
    Build(field_number, std::move(message_builder), message);
    return message;
  }

  static void Build(int field_number, RegisteredFieldBuilder&& message_builder,
                    RegisteredMessage& message);

  // Copies fields, rewriting submessages present in the base message.
  template <typename Src>
  static absl::Status RewriteFields(absl::Span<FieldValues> fields_to_add,
                                    absl::Span<const bool> fields_to_remove,
                                    const RegisteredMessage& message, Src&& src,
                                    SerializedMessageWriter& message_writer);

  // Writes fields from submessages which were not present in the base message,
  // and fields added directly to the message. Creates required parent
  // submessages if they turn out to be non-empty.
  static absl::Status WriteNewFields(
      absl::Span<FieldValues> fields_to_add, const RegisteredMessage& message,
      absl::Span<const bool> submessages_rewritten,
      SerializedMessageWriter& message_writer);

  size_t num_fields_for_add() const { return size_t{num_fields_for_add_.raw}; }
  size_t num_fields_for_remove() const {
    return size_t{num_fields_for_remove_.raw};
  }

  ParentForAdd num_fields_for_add_ = {/*raw=*/0};
  FieldForRemove num_fields_for_remove_ = {/*raw=*/0};
  RegisteredMessage root_;
};

class SerializedMessageAssembler::Builder {
 public:
  // Creates an empty `SerializedMessageAssembler::Builder`.
  Builder() = default;

  Builder(Builder&& that) noexcept;
  Builder& operator=(Builder&& that) noexcept;

  ~Builder();

  // Makes `*this` equivalent to a newly constructed
  // `SerializedMessageAssembler::Builder`.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset();

  // Registers interest in writing fields under this parent.
  //
  // If the parent message is present in the base message (and not removed),
  // and/or is used for multiple fields, then its contents are merged. If it is
  // present multiple times in the base message, then contents are merged to its
  // first occurrence.
  //
  // Returns a `ParentForAdd` to be passed to `Session::AddField()`.
  //
  // Can be called multiple times with the same `parent_path`, returning the
  // same `ParentForAdd` each time.
  ParentForAdd RegisterParentForAdd(ParentPath parent_path);

  // Registers interest in removing this field from the base message.
  //
  // Returns a `FieldForRemove` to be passed to `Session::RemoveField()`.
  //
  // Can be called multiple times with the same `parent_path` and
  // `field_number`, returning the same `FieldForRemove` each time.
  FieldForRemove RegisterFieldForRemove(ParentPath parent_path,
                                        int field_number);

 private:
  friend class SerializedMessageAssembler;  // For member variables.

  RegisteredFieldBuilder& RegisterParentInternal(ParentPath parent_path);

  ParentForAdd next_parent_for_add_ = {/*raw=*/0};
  FieldForRemove next_field_for_remove_ = {/*raw=*/0};
  RegisteredFieldBuilder root_builder_;
};

class SerializedMessageAssembler::Session {
 private:
  template <typename Src>
  struct IsSource
      : std::disjunction<TargetRefSupportsDependency<Reader*, Src>,
                         std::is_convertible<Src&&, BytesRef>,
                         std::is_convertible<Src&&, Chain>,
                         std::is_convertible<Src&&, const absl::Cord&>,
                         std::is_convertible<Src&&, CordIteratorSpan>> {};

  template <typename Dest>
  struct IsDestination
      : std::disjunction<TargetRefSupportsDependency<Writer*, Dest>,
                         std::is_convertible<Dest&&, std::string&>,
                         std::is_convertible<Dest&&, Chain&>,
                         std::is_convertible<Dest&&, absl::Cord&>> {};

 public:
  // The constructor to use when the `SerializedMessageAssembler` does not have
  // registered fields yet. In this case there is no benefit in passing the
  // `SerializedMessageAssembler` to the constructor.
  Session() = default;

  // The constructor to use when the `SerializedMessageAssembler` has fields
  // already registered. In this case this constructor is more efficient than
  // the default constructor.
  explicit Session(const SerializedMessageAssembler& assembler)
      : fields_to_add_(assembler.num_fields_for_add()),
        fields_to_remove_(assembler.num_fields_for_remove()) {}

  // The source `Session` is left empty.
  Session(Session&& that) noexcept;
  Session& operator=(Session&& that) noexcept;

  ~Session();

  // Makes `*this` equivalent to a newly constructed `Session`.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset();
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      const SerializedMessageAssembler& assembler);

  // Adds the value of a field identified by `parent_for_add`, which was
  // returned by `Builder::RegisterParentForAdd()`.
  //
  // `action` will be called exactly once by a successful `WriteMessage()`,
  // with a `SerializedMessageWriter&` parameter. It should write the field
  // there and return `absl::Status`.
  //
  // If the parent message is present in the base message (and not removed),
  // and/or `AddField()` is called multiple times with the same
  // `parent_for_add`, then the contents of the parent message are merged. If it
  // is present multiple times in the base message, then contents are merged to
  // its first occurrence.
  //
  // If multiple fields are written to a parent message, then they are written
  // separately rather than merged.
  //
  // The consequences of having multiple occurrences of a field follow the proto
  // semantics of merging:
  //
  //  * A singular non-message field overrides the previous value.
  //  * A singular message field is merged with the previous value.
  //  * A repeated field has a new element added.
  //
  // In the case of a singular field or a packed repeated field, this makes the
  // serialized representation non-canonical or with redundant overridden data.
  // In these cases it is better to avoid having multiple occurrences. Even
  // though regular proto parsing merges these occurrences, this can be
  // confusing for non-standard parsers, and the serialized representation is
  // larger than necessary.
  //
  // It follows that there are two ways to write a length-delimited field:
  //
  //  1. Specify its parent in `RegisterParentForAdd()`.
  //     Write the whole field in `AddField()` action.
  //
  //  2. Specify the field itself in `RegisterParentForAdd()`.
  //     Write its contents only in `AddField()` action.
  //
  // If the field is not present in the base message (or removed), and
  // `AddField()` is called once, then both ways have the same effect.
  // The first way is more efficient, because the length of field contents
  // is usually known in advance, in which case the contents can be written
  // directly instead of gathering them separately to compute their length
  // when they are complete.
  //
  // Otherwise, the first way writes a separate field, which may undergo
  // implicit merging during parsing, while the second way merges it in the
  // serialized message.
  template <
      typename Action,
      std::enable_if_t<std::is_invocable_v<Action&&, SerializedMessageWriter&>,
                       int> = 0>
  void AddField(ParentForAdd parent_for_add, Action&& action);

  // Removes from the base message the field identified by `parent_for_remove`,
  // which was returned by `Builder::RegisterFieldForRemove()`.
  //
  // If the field occurs multiple times, all its occurrences are removed.
  //
  // If `RemoveField()` is called multiple times with the same
  // `parent_for_remove`, the effect is the same as a single call.
  //
  // This does not apply to fields added with `AddField()`, only to fields from
  // the base message.
  void RemoveField(FieldForRemove parent_for_remove);

  // Writes a serialized message based on the serialized message in `base_src`
  // (empty by default), adding fields specified by `Session::AddField()`,
  // and removing fields specified by `Session::RemoveField()`.
  //
  // The `Session` is left empty.
  template <
      typename BaseSrc, typename Dest,
      std::enable_if_t<
          std::conjunction_v<IsSource<BaseSrc>, IsDestination<Dest>>, int> = 0>
  absl::Status WriteMessage(const SerializedMessageAssembler& message_assembler,
                            BaseSrc&& base_src, Dest&& dest) &&;
  template <typename Dest,
            std::enable_if_t<IsDestination<Dest>::value, int> = 0>
  absl::Status WriteMessage(const SerializedMessageAssembler& message_assembler,
                            Dest&& dest) &&;

 private:
  template <
      typename Dest,
      std::enable_if_t<TargetRefSupportsDependency<Writer*, Dest>::value, int>>
  static Dest&& MakeWriter(Dest&& dest) {
    return std::forward<Dest>(dest);
  }
  static StringWriter<> MakeWriter(std::string& dest) {
    return StringWriter(&dest);
  }
  static ChainWriter<> MakeWriter(Chain& dest) { return ChainWriter(&dest); }
  static CordWriter<> MakeWriter(absl::Cord& dest) { return CordWriter(&dest); }

  template <typename BaseSrc, typename Dest>
  absl::Status WriteMessageInternal(
      const SerializedMessageAssembler& message_assembler, BaseSrc&& base_src,
      Dest&& dest) &&;
  template <typename Dest>
  absl::Status WriteMessageInternal(
      const SerializedMessageAssembler& message_assembler, Dest&& dest) &&;

  // Indexed by `ParentForAdd`.
  std::vector<FieldValues> fields_to_add_;
  // Indexed by `FieldForRemove`.
  absl::InlinedVector<bool, 8> fields_to_remove_;
};

// Implementation details follow.

template <typename Action,
          std::enable_if_t<
              std::is_invocable_v<Action&&, SerializedMessageWriter&>, int>>
inline void SerializedMessageAssembler::Session::AddField(
    ParentForAdd parent_for_add, Action&& action) {
  if (ABSL_PREDICT_FALSE(parent_for_add.raw >= fields_to_add_.size())) {
    fields_to_add_.resize(parent_for_add.raw + 1);
  }
  fields_to_add_[parent_for_add.raw].emplace_back(std::forward<Action>(action));
}

inline void SerializedMessageAssembler::Session::RemoveField(
    FieldForRemove parent_for_remove) {
  if (ABSL_PREDICT_FALSE(parent_for_remove.raw >= fields_to_remove_.size())) {
    fields_to_remove_.resize(parent_for_remove.raw + 1);
  }
  fields_to_remove_[parent_for_remove.raw] = true;
}

template <typename BaseSrc, typename Dest,
          std::enable_if_t<
              std::conjunction_v<
                  SerializedMessageAssembler::Session::IsSource<BaseSrc>,
                  SerializedMessageAssembler::Session::IsDestination<Dest>>,
              int>>
absl::Status SerializedMessageAssembler::Session::WriteMessage(
    const SerializedMessageAssembler& message_assembler, BaseSrc&& base_src,
    Dest&& dest) && {
  return std::move(*this).WriteMessageInternal(
      message_assembler, std::forward<BaseSrc>(base_src),
      MakeWriter(std::forward<Dest>(dest)));
}

template <
    typename Dest,
    std::enable_if_t<
        SerializedMessageAssembler::Session::IsDestination<Dest>::value, int>>
absl::Status SerializedMessageAssembler::Session::WriteMessage(
    const SerializedMessageAssembler& message_assembler, Dest&& dest) && {
  return std::move(*this).WriteMessageInternal(
      message_assembler, MakeWriter(std::forward<Dest>(dest)));
}

template <typename BaseSrc, typename Dest>
inline absl::Status SerializedMessageAssembler::Session::WriteMessageInternal(
    const SerializedMessageAssembler& message_assembler, BaseSrc&& base_src,
    Dest&& dest) && {
  DependencyRef<Writer*, Dest> dest_dep(std::forward<Dest>(dest));
  SerializedMessageWriter message_writer(dest_dep.get());

  absl::Status status = RewriteFields(
      absl::MakeSpan(fields_to_add_), absl::MakeSpan(fields_to_remove_),
      message_assembler.root_, std::forward<BaseSrc>(base_src), message_writer);

  // Each element of `fields_to_add_` is already cleared.
  fields_to_remove_.clear();
  if (dest_dep.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_dep->Close())) {
      status.Update(dest_dep->status());
    }
  }
  return status;
}

template <typename Dest>
inline absl::Status SerializedMessageAssembler::Session::WriteMessageInternal(
    const SerializedMessageAssembler& message_assembler, Dest&& dest) && {
  DependencyRef<Writer*, Dest> dest_dep(std::forward<Dest>(dest));
  SerializedMessageWriter message_writer(dest_dep.get());

  absl::Status status =
      WriteNewFields(absl::MakeSpan(fields_to_add_), message_assembler.root_,
                     SubmessageSet(message_assembler.root_.submessages.size()),
                     message_writer);

  // Each element of `fields_to_add_` is already cleared.
  fields_to_remove_.clear();
  if (dest_dep.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_dep->Close())) {
      status.Update(dest_dep->status());
    }
  }
  return status;
}

template <typename Src>
absl::Status SerializedMessageAssembler::RewriteFields(
    absl::Span<FieldValues> fields_to_add,
    absl::Span<const bool> fields_to_remove, const RegisteredMessage& message,
    Src&& src, SerializedMessageWriter& message_writer) {
  SubmessageSet submessages_rewritten(message.submessages.size());

  // Rewrite fields which are present in the base message.
  if (absl::Status status =
          SerializedMessageReader2<
              const absl::Span<FieldValues>, const absl::Span<const bool>,
              const absl::Span<bool>, SerializedMessageWriter>(
              std::cref(message.handlers), CopyingHandler())
              .ReadMessage(
                  std::forward<Src>(src), fields_to_add, fields_to_remove,
                  absl::MakeSpan(submessages_rewritten), message_writer);
      ABSL_PREDICT_FALSE(!status.ok())) {
    return status;
  }

  // Write new fields which were not present in the base message.
  return WriteNewFields(fields_to_add, message, submessages_rewritten,
                        message_writer);
}

extern template absl::Status SerializedMessageAssembler::RewriteFields(
    absl::Span<FieldValues> fields_to_add,
    absl::Span<const bool> fields_to_remove, const RegisteredMessage& message,
    ReaderSpan<>&& src, SerializedMessageWriter& message_writer);
extern template absl::Status SerializedMessageAssembler::RewriteFields(
    absl::Span<FieldValues> fields_to_add,
    absl::Span<const bool> fields_to_remove, const RegisteredMessage& message,
    CordIteratorSpan&& src, SerializedMessageWriter& message_writer);
extern template absl::Status SerializedMessageAssembler::RewriteFields(
    absl::Span<FieldValues> fields_to_add,
    absl::Span<const bool> fields_to_remove, const RegisteredMessage& message,
    absl::string_view&& src, SerializedMessageWriter& message_writer);

}  // namespace riegeli

#endif  // RIEGELI_MESSAGES_SERIALIZED_MESSAGE_ASSEMBLER_H_
