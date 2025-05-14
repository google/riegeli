// Copyright 2025 Google LLC
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

#ifndef RIEGELI_MESSAGES_SERIALIZED_MESSAGE_READER_
#define RIEGELI_MESSAGES_SERIALIZED_MESSAGE_READER_

#include <stddef.h>
#include <stdint.h>

#include <functional>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/casts.h"
#include "absl/base/optimization.h"
#include "absl/container/flat_hash_map.h"
#include "absl/meta/type_traits.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/any.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/global.h"
#include "riegeli/base/type_erased_ref.h"
#include "riegeli/base/type_traits.h"
#include "riegeli/bytes/limiting_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/messages/message_wire_format.h"
#include "riegeli/messages/parse_message.h"
#include "riegeli/messages/serialized_message_internal.h"
#include "riegeli/varint/varint_reading.h"

namespace riegeli {

// Template parameter independent part of `SerializedMessageReader`.
class SerializedMessageReaderBase {
 protected:
  SerializedMessageReaderBase() = default;

  SerializedMessageReaderBase(const SerializedMessageReaderBase& that) =
      default;
  SerializedMessageReaderBase& operator=(
      const SerializedMessageReaderBase& that) = default;

  SerializedMessageReaderBase(SerializedMessageReaderBase&& that) = default;
  SerializedMessageReaderBase& operator=(SerializedMessageReaderBase&& that) =
      default;

  void OnInt32(
      absl::Span<const int> field_path,
      std::function<absl::Status(int32_t value, TypeErasedRef context)> action);
  void OnUInt32(
      absl::Span<const int> field_path,
      std::function<absl::Status(uint32_t value, TypeErasedRef context)>
          action);
  void OnUInt64(
      absl::Span<const int> field_path,
      std::function<absl::Status(uint64_t value, TypeErasedRef context)>
          action);
  void OnBool(
      absl::Span<const int> field_path,
      std::function<absl::Status(bool value, TypeErasedRef context)> action);
  void OnFixed32(
      absl::Span<const int> field_path,
      std::function<absl::Status(uint32_t value, TypeErasedRef context)>
          action);
  void OnFixed64(
      absl::Span<const int> field_path,
      std::function<absl::Status(uint64_t value, TypeErasedRef context)>
          action);
  void OnStringView(absl::Span<const int> field_path,
                    std::function<absl::Status(absl::string_view value,
                                               TypeErasedRef context)>
                        action);
  void OnString(
      absl::Span<const int> field_path,
      std::function<absl::Status(std::string&& value, TypeErasedRef context)>
          action);
  void OnChain(
      absl::Span<const int> field_path,
      std::function<absl::Status(Chain&& value, TypeErasedRef context)> action);
  void OnCord(
      absl::Span<const int> field_path,
      std::function<absl::Status(absl::Cord&& value, TypeErasedRef context)>
          action);
  void OnLengthDelimited(absl::Span<const int> field_path,
                         std::function<absl::Status(LimitingReaderBase& src,
                                                    TypeErasedRef context)>
                             action);
  void OnLengthUnchecked(
      absl::Span<const int> field_path,
      std::function<absl::Status(size_t length, LimitingReaderBase& src,
                                 TypeErasedRef context)>
          action);
  void BeforeMessage(absl::Span<const int> field_path,
                     std::function<absl::Status(TypeErasedRef context)> action);
  void AfterMessage(absl::Span<const int> field_path,
                    std::function<absl::Status(TypeErasedRef context)> action);

  void OnOther(std::function<absl::Status(uint32_t tag, LimitingReaderBase& src,
                                          TypeErasedRef context)>
                   default_action) {
    on_other_ = std::move(default_action);
  }
  void BeforeOtherMessage(
      std::function<absl::Status(int field_number, TypeErasedRef context)>
          action) {
    before_other_message_ = std::move(action);
  }
  void AfterOtherMessage(
      std::function<absl::Status(int field_number, TypeErasedRef context)>
          action) {
    after_other_message_ = std::move(action);
  }

  absl::Status Read(AnyRef<Reader*> src, TypeErasedRef context) const;

 private:
  static constexpr uint32_t kNumDefinedWireTypes = 6;

  struct Field {
    std::function<absl::Status(LimitingReaderBase& src, TypeErasedRef context)>
        actions[kNumDefinedWireTypes];
    std::function<absl::Status(TypeErasedRef context)> before_message;
    std::function<absl::Status(TypeErasedRef context)> after_message;
    absl::flat_hash_map<int, Field> children;
  };

  static absl::Status SkipField(uint32_t tag, LimitingReaderBase& src,
                                TypeErasedRef context);
  static absl::Status NoActionForSubmessage(int field_number,
                                            TypeErasedRef context);
  static absl::Status NoActionForRoot(TypeErasedRef context);

  void SetAction(absl::Span<const int> field_path, WireType wire_type,
                 std::function<absl::Status(LimitingReaderBase& src,
                                            TypeErasedRef context)>
                     action);

  Field* GetField(absl::Span<const int> field_path);

  absl::Status ReadRootMessage(LimitingReaderBase& src,
                               TypeErasedRef context) const;

  absl::Status ReadMessage(LimitingReaderBase& src,
                           const absl::flat_hash_map<int, Field>& fields,
                           TypeErasedRef context) const;

  std::function<absl::Status(uint32_t tag, LimitingReaderBase& src,
                             TypeErasedRef context)>
      on_other_ = SkipField;
  std::function<absl::Status(int field_number, TypeErasedRef context)>
      before_other_message_ = NoActionForSubmessage;
  std::function<absl::Status(int field_number, TypeErasedRef context)>
      after_other_message_ = NoActionForSubmessage;
  std::function<absl::Status(TypeErasedRef context)> before_root_ =
      NoActionForRoot;
  std::function<absl::Status(TypeErasedRef context)> after_root_ =
      NoActionForRoot;
  absl::flat_hash_map<int, Field> root_;
};

// `SerializedMessageReader` reads a serialized message using configured actions
// to be performed when encountering specific fields.
//
// The object holds registered actions, independent from the message object.
// Each reading is a separate `Read()` call.
//
// `Context&` is passed to the actions as the last argument if `Context` is not
// `void` and the action is invocable with that argument, otherwise it is not
// passed. `Context` should hold any state specific to the particular message
// object, so that the `SerializedMessageReader` object can be reused.
//
// An action returns `absl::Status`, non-OK causing an early exit.
//
// Functions working on strings are applicable to any length-delimited field:
// `string`, `bytes`, submessage, or a packed repeated field.
//
// See `SerializedMessageRewriter` for producing an edited version of the
// message.
template <typename Context = void>
class SerializedMessageReader : public SerializedMessageReaderBase {
 public:
  // ```
  // const auto& message_reader = SerializedMessageReader<Context>::Global(
  //   [](SerializedMessageReader<Context>& message_reader) {
  //     ...
  //   });
  // ```
  //
  // Returns a const reference to a `SerializedMessageReader` object, with the
  // initializer once called on its non-const reference.
  //
  // The object is created when `Global()` is first called with the given
  // initializer type, and is never destroyed.
  //
  // The initializer should set the actions. This is the recommended way to
  // create a `SerializedMessageReader` object with a fixed set of fields
  // to be read, while `Context` should hold any state specific to the
  // particular message object, so that the `SerializedMessageReader` object
  // can be reused.
  //
  // The initializer type should be a lambda with no captures. This restriction
  // is a safeguard against making the object dependent on local state, which
  // would be misleadingly ignored for subsequent calls. Since distinct lambdas
  // have distinct types, distinct call sites with lambdas return references to
  // distinct objects.
  template <typename Initialize,
            std::enable_if_t<
                absl::conjunction<
                    std::is_empty<Initialize>,
                    is_invocable<Initialize, SerializedMessageReader&>>::value,
                int> = 0>
  static const SerializedMessageReader& Global(Initialize initialize);

  SerializedMessageReader() = default;

  SerializedMessageReader(const SerializedMessageReader& that) = default;
  SerializedMessageReader& operator=(const SerializedMessageReader& that) =
      default;

  SerializedMessageReader(SerializedMessageReader&& that) = default;
  SerializedMessageReader& operator=(SerializedMessageReader&& that) = default;

  // Sets the action to be performed when encountering a field identified by
  // `field_path` of field numbers from the root through submessages.
  //
  // `action` is invoked with `value` being the value read.
  //
  // For numeric types, `action` is invoked also for each element of a packed
  // repeated field.
  //
  // A field is accepted only if the value can be losslessly represented in the
  // specified type, otherwise reading fails. This is in contrast to native
  // proto parsing, which e.g. silently truncates `int64` on the wire to `int32`
  // in the message object.
  //
  // Precondition: `!field_path.empty()`
  template <typename Action,
            std::enable_if_t<serialized_message_internal::IsAction<
                                 Context, Action, int32_t>::value,
                             int> = 0>
  void OnInt32(absl::Span<const int> field_path, Action action);
  template <typename Action,
            std::enable_if_t<serialized_message_internal::IsAction<
                                 Context, Action, int64_t>::value,
                             int> = 0>
  void OnInt64(absl::Span<const int> field_path, Action action);
  template <typename Action,
            std::enable_if_t<serialized_message_internal::IsAction<
                                 Context, Action, uint32_t>::value,
                             int> = 0>
  void OnUInt32(absl::Span<const int> field_path, Action action);
  template <typename Action,
            std::enable_if_t<serialized_message_internal::IsAction<
                                 Context, Action, uint64_t>::value,
                             int> = 0>
  void OnUInt64(absl::Span<const int> field_path, Action action);
  template <typename Action,
            std::enable_if_t<serialized_message_internal::IsAction<
                                 Context, Action, int32_t>::value,
                             int> = 0>
  void OnSInt32(absl::Span<const int> field_path, Action action);
  template <typename Action,
            std::enable_if_t<serialized_message_internal::IsAction<
                                 Context, Action, int64_t>::value,
                             int> = 0>
  void OnSInt64(absl::Span<const int> field_path, Action action);
  template <typename Action,
            std::enable_if_t<serialized_message_internal::IsAction<
                                 Context, Action, bool>::value,
                             int> = 0>
  void OnBool(absl::Span<const int> field_path, Action action);
  template <typename Action,
            std::enable_if_t<serialized_message_internal::IsAction<
                                 Context, Action, uint32_t>::value,
                             int> = 0>
  void OnFixed32(absl::Span<const int> field_path, Action action);
  template <typename Action,
            std::enable_if_t<serialized_message_internal::IsAction<
                                 Context, Action, uint64_t>::value,
                             int> = 0>
  void OnFixed64(absl::Span<const int> field_path, Action action);
  template <typename Action,
            std::enable_if_t<serialized_message_internal::IsAction<
                                 Context, Action, int32_t>::value,
                             int> = 0>
  void OnSFixed32(absl::Span<const int> field_path, Action action);
  template <typename Action,
            std::enable_if_t<serialized_message_internal::IsAction<
                                 Context, Action, int64_t>::value,
                             int> = 0>
  void OnSFixed64(absl::Span<const int> field_path, Action action);
  template <typename Action,
            std::enable_if_t<serialized_message_internal::IsAction<
                                 Context, Action, float>::value,
                             int> = 0>
  void OnFloat(absl::Span<const int> field_path, Action action);
  template <typename Action,
            std::enable_if_t<serialized_message_internal::IsAction<
                                 Context, Action, double>::value,
                             int> = 0>
  void OnDouble(absl::Span<const int> field_path, Action action);
  template <typename Action,
            std::enable_if_t<serialized_message_internal::IsAction<
                                 Context, Action, absl::string_view>::value,
                             int> = 0>
  void OnStringView(absl::Span<const int> field_path, Action action);
  template <typename Action,
            std::enable_if_t<serialized_message_internal::IsAction<
                                 Context, Action, std::string&&>::value,
                             int> = 0>
  void OnString(absl::Span<const int> field_path, Action action);
  template <typename Action,
            std::enable_if_t<serialized_message_internal::IsAction<
                                 Context, Action, Chain&&>::value,
                             int> = 0>
  void OnChain(absl::Span<const int> field_path, Action action);
  template <typename Action,
            std::enable_if_t<serialized_message_internal::IsAction<
                                 Context, Action, absl::Cord&&>::value,
                             int> = 0>
  void OnCord(absl::Span<const int> field_path, Action action);
  template <typename MessageType, typename Action,
            std::enable_if_t<serialized_message_internal::IsAction<
                                 Context, Action, MessageType&&>::value,
                             int> = 0>
  void OnParsedMessage(absl::Span<const int> field_path, Action action,
                       ParseOptions options = {});

  // Sets the action to be performed when encountering a length-delimited field
  // identified by `field_path` of field numbers from the root through
  // submessages.
  //
  // `action` is invoked with `src` from which the value can be read. `src` will
  // contain the field contents (between `src.pos()` and `src.max_pos()`, with
  // `src.max_length()`). `action` can read any part of `src`.
  //
  // Precondition: `!field_path.empty()`
  template <typename Action,
            std::enable_if_t<serialized_message_internal::IsAction<
                                 Context, Action, LimitingReaderBase&>::value,
                             int> = 0>
  void OnLengthDelimited(absl::Span<const int> field_path, Action action);

  // Sets the action to be performed when encountering a length-delimited field
  // identified by `field_path` of field numbers from the root through
  // submessages.
  //
  // `action` is invoked with `length`, and `src` from which the value will be
  // read. The first `length` bytes of `src` will contain the field contents.
  // `action` must read exactly `length` bytes from `src`, unless it fails.
  // This is unchecked.
  //
  // `OnLengthUnchecked()` is more efficient than `OnLengthDelimited()`.
  //
  // Precondition: `!field_path.empty()`
  template <
      typename Action,
      std::enable_if_t<serialized_message_internal::IsAction<
                           Context, Action, size_t, LimitingReaderBase&>::value,
                       int> = 0>
  void OnLengthUnchecked(absl::Span<const int> field_path, Action action);

  // Sets the action to be performed when encountering a submessage field
  // identified by `field_path` of field numbers from the root through
  // submessages. An empty `field_path` specified the root message.
  //
  // The field will be processed in any case.
  template <typename Action,
            std::enable_if_t<
                serialized_message_internal::IsAction<Context, Action>::value,
                int> = 0>
  void BeforeMessage(absl::Span<const int> field_path, Action action);
  template <typename Action,
            std::enable_if_t<
                serialized_message_internal::IsAction<Context, Action>::value,
                int> = 0>
  void AfterMessage(absl::Span<const int> field_path, Action action);

  // Sets the action to be performed when there is no specific action registered
  // for this field.
  //
  // `action` is invoked with the field `tag`, and `src` positioned between
  // the field tag and field contents. It must leave `src` positioned after
  // field contents.
  template <typename Action,
            std::enable_if_t<serialized_message_internal::IsAction<
                                 Context, Action, uint32_t, Reader&>::value,
                             int> = 0>
  void OnOther(Action action);

  // Sets the actions to be performed when encountering some submessage field,
  // when there is no specific action registered for this field using
  // `BeforeMessage()` or `AfterMessage()`, but some actions for its nested
  // fields are registered. It can prepare the context for processing
  // the submessage.
  //
  // `action` is invoked with the `field_number`.
  template <typename Action,
            std::enable_if_t<serialized_message_internal::IsAction<
                                 Context, Action, int>::value,
                             int> = 0>
  void BeforeOtherMessage(Action action);
  template <typename Action,
            std::enable_if_t<serialized_message_internal::IsAction<
                                 Context, Action, int>::value,
                             int> = 0>
  void AfterOtherMessage(Action action);

  // Reads a serialized message from `src` using configured actions.
  //
  // A reference to `context` is passed to the actions.
  template <typename DependentContext = Context,
            std::enable_if_t<!std::is_void<DependentContext>::value, int> = 0>
  absl::Status Read(AnyRef<Reader*> src,
                    type_identity_t<DependentContext&> context) const;
  template <typename DependentContext = Context,
            std::enable_if_t<!std::is_void<DependentContext>::value, int> = 0>
  absl::Status Read(AnyRef<Reader*> src,
                    type_identity_t<DependentContext&&> context) const;
  template <typename DependentContext = Context,
            std::enable_if_t<std::is_void<DependentContext>::value, int> = 0>
  absl::Status Read(AnyRef<Reader*> src) const;
};

// Implementation details follow.

template <typename Context>
template <
    typename Initialize,
    std::enable_if_t<
        absl::conjunction<
            std::is_empty<Initialize>,
            is_invocable<Initialize, SerializedMessageReader<Context>&>>::value,
        int>>
inline const SerializedMessageReader<Context>&
SerializedMessageReader<Context>::Global(Initialize initialize) {
  return riegeli::Global([] { return SerializedMessageReader(); }, initialize);
}

template <typename Context>
template <typename Action,
          std::enable_if_t<serialized_message_internal::IsAction<
                               Context, Action, int32_t>::value,
                           int>>
inline void SerializedMessageReader<Context>::OnInt32(
    absl::Span<const int> field_path, Action action) {
  SerializedMessageReaderBase::OnInt32(
      field_path,
      [action = std::move(action)](int32_t value, TypeErasedRef context) {
        return serialized_message_internal::InvokeAction<Context>(
            context, action, value);
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<serialized_message_internal::IsAction<
                               Context, Action, int64_t>::value,
                           int>>
inline void SerializedMessageReader<Context>::OnInt64(
    absl::Span<const int> field_path, Action action) {
  SerializedMessageReaderBase::OnUInt64(
      field_path,
      [action = std::move(action)](uint64_t value, TypeErasedRef context) {
        return serialized_message_internal::InvokeAction<Context>(
            context, action, static_cast<int64_t>(value));
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<serialized_message_internal::IsAction<
                               Context, Action, uint32_t>::value,
                           int>>
inline void SerializedMessageReader<Context>::OnUInt32(
    absl::Span<const int> field_path, Action action) {
  SerializedMessageReaderBase::OnUInt32(
      field_path,
      [action = std::move(action)](uint32_t value, TypeErasedRef context) {
        return serialized_message_internal::InvokeAction<Context>(
            context, action, value);
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<serialized_message_internal::IsAction<
                               Context, Action, uint64_t>::value,
                           int>>
inline void SerializedMessageReader<Context>::OnUInt64(
    absl::Span<const int> field_path, Action action) {
  SerializedMessageReaderBase::OnUInt64(
      field_path,
      [action = std::move(action)](uint64_t value, TypeErasedRef context) {
        return serialized_message_internal::InvokeAction<Context>(
            context, action, value);
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<serialized_message_internal::IsAction<
                               Context, Action, int32_t>::value,
                           int>>
inline void SerializedMessageReader<Context>::OnSInt32(
    absl::Span<const int> field_path, Action action) {
  SerializedMessageReaderBase::OnUInt32(
      field_path,
      [action = std::move(action)](uint32_t value, TypeErasedRef context) {
        return serialized_message_internal::InvokeAction<Context>(
            context, action, DecodeVarintSigned32(value));
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<serialized_message_internal::IsAction<
                               Context, Action, int64_t>::value,
                           int>>
inline void SerializedMessageReader<Context>::OnSInt64(
    absl::Span<const int> field_path, Action action) {
  SerializedMessageReaderBase::OnUInt64(
      field_path,
      [action = std::move(action)](uint64_t value, TypeErasedRef context) {
        return serialized_message_internal::InvokeAction<Context>(
            context, action, DecodeVarintSigned64(value));
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<serialized_message_internal::IsAction<
                               Context, Action, bool>::value,
                           int>>
inline void SerializedMessageReader<Context>::OnBool(
    absl::Span<const int> field_path, Action action) {
  SerializedMessageReaderBase::OnBool(
      field_path,
      [action = std::move(action)](bool value, TypeErasedRef context) {
        return serialized_message_internal::InvokeAction<Context>(
            context, action, value);
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<serialized_message_internal::IsAction<
                               Context, Action, uint32_t>::value,
                           int>>
inline void SerializedMessageReader<Context>::OnFixed32(
    absl::Span<const int> field_path, Action action) {
  SerializedMessageReaderBase::OnFixed32(
      field_path,
      [action = std::move(action)](uint32_t value, TypeErasedRef context) {
        return serialized_message_internal::InvokeAction<Context>(
            context, action, value);
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<serialized_message_internal::IsAction<
                               Context, Action, uint64_t>::value,
                           int>>
inline void SerializedMessageReader<Context>::OnFixed64(
    absl::Span<const int> field_path, Action action) {
  SerializedMessageReaderBase::OnFixed64(
      field_path,
      [action = std::move(action)](uint64_t value, TypeErasedRef context) {
        return serialized_message_internal::InvokeAction<Context>(
            context, action, value);
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<serialized_message_internal::IsAction<
                               Context, Action, int32_t>::value,
                           int>>
inline void SerializedMessageReader<Context>::OnSFixed32(
    absl::Span<const int> field_path, Action action) {
  SerializedMessageReaderBase::OnFixed32(
      field_path,
      [action = std::move(action)](uint32_t value, TypeErasedRef context) {
        return serialized_message_internal::InvokeAction<Context>(
            context, action, static_cast<int32_t>(value));
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<serialized_message_internal::IsAction<
                               Context, Action, int64_t>::value,
                           int>>
inline void SerializedMessageReader<Context>::OnSFixed64(
    absl::Span<const int> field_path, Action action) {
  SerializedMessageReaderBase::OnFixed64(
      field_path,
      [action = std::move(action)](uint64_t value, TypeErasedRef context) {
        return serialized_message_internal::InvokeAction<Context>(
            context, action, static_cast<int64_t>(value));
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<serialized_message_internal::IsAction<
                               Context, Action, float>::value,
                           int>>
inline void SerializedMessageReader<Context>::OnFloat(
    absl::Span<const int> field_path, Action action) {
  SerializedMessageReaderBase::OnFixed32(
      field_path,
      [action = std::move(action)](uint32_t value, TypeErasedRef context) {
        return serialized_message_internal::InvokeAction<Context>(
            context, action, absl::bit_cast<float>(value));
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<serialized_message_internal::IsAction<
                               Context, Action, double>::value,
                           int>>
inline void SerializedMessageReader<Context>::OnDouble(
    absl::Span<const int> field_path, Action action) {
  SerializedMessageReaderBase::OnFixed64(
      field_path,
      [action = std::move(action)](uint64_t value, TypeErasedRef context) {
        return serialized_message_internal::InvokeAction<Context>(
            context, action, absl::bit_cast<double>(value));
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<serialized_message_internal::IsAction<
                               Context, Action, absl::string_view>::value,
                           int>>
inline void SerializedMessageReader<Context>::OnStringView(
    absl::Span<const int> field_path, Action action) {
  SerializedMessageReaderBase::OnStringView(
      field_path, [action = std::move(action)](absl::string_view value,
                                               TypeErasedRef context) {
        return serialized_message_internal::InvokeAction<Context>(
            context, action, value);
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<serialized_message_internal::IsAction<
                               Context, Action, std::string&&>::value,
                           int>>
inline void SerializedMessageReader<Context>::OnString(
    absl::Span<const int> field_path, Action action) {
  SerializedMessageReaderBase::OnString(
      field_path,
      [action = std::move(action)](std::string&& value, TypeErasedRef context) {
        return serialized_message_internal::InvokeAction<Context>(
            context, action, std::move(value));
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<serialized_message_internal::IsAction<
                               Context, Action, Chain&&>::value,
                           int>>
inline void SerializedMessageReader<Context>::OnChain(
    absl::Span<const int> field_path, Action action) {
  SerializedMessageReaderBase::OnChain(
      field_path,
      [action = std::move(action)](Chain&& value, TypeErasedRef context) {
        return serialized_message_internal::InvokeAction<Context>(
            context, action, std::move(value));
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<serialized_message_internal::IsAction<
                               Context, Action, absl::Cord&&>::value,
                           int>>
inline void SerializedMessageReader<Context>::OnCord(
    absl::Span<const int> field_path, Action action) {
  SerializedMessageReaderBase::OnCord(
      field_path,
      [action = std::move(action)](absl::Cord&& value, TypeErasedRef context) {
        return serialized_message_internal::InvokeAction<Context>(
            context, action, std::move(value));
      });
}

template <typename Context>
template <typename MessageType, typename Action,
          std::enable_if_t<serialized_message_internal::IsAction<
                               Context, Action, MessageType&&>::value,
                           int>>
inline void SerializedMessageReader<Context>::OnParsedMessage(
    absl::Span<const int> field_path, Action action,
    ParseOptions parse_options) {
  SerializedMessageReaderBase::OnLengthUnchecked(
      field_path,
      [action = std::move(action), parse_options](
          size_t length, LimitingReaderBase& src, TypeErasedRef context) {
        MessageType message;
        if (absl::Status status = riegeli::ParseMessageWithLength(
                src, length, message, parse_options);
            ABSL_PREDICT_FALSE(!status.ok())) {
          return status;
        }
        return serialized_message_internal::InvokeAction<Context>(
            context, action, std::move(message));
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<serialized_message_internal::IsAction<
                               Context, Action, LimitingReaderBase&>::value,
                           int>>
inline void SerializedMessageReader<Context>::OnLengthDelimited(
    absl::Span<const int> field_path, Action action) {
  SerializedMessageReaderBase::OnLengthDelimited(
      field_path, [action = std::move(action)](LimitingReaderBase& src,
                                               TypeErasedRef context) {
        return serialized_message_internal::InvokeAction<Context>(context,
                                                                  action, src);
      });
}

template <typename Context>
template <
    typename Action,
    std::enable_if_t<serialized_message_internal::IsAction<
                         Context, Action, size_t, LimitingReaderBase&>::value,
                     int>>
inline void SerializedMessageReader<Context>::OnLengthUnchecked(
    absl::Span<const int> field_path, Action action) {
  SerializedMessageReaderBase::OnLengthUnchecked(
      field_path,
      [action = std::move(action)](size_t length, LimitingReaderBase& src,
                                   TypeErasedRef context) {
        return serialized_message_internal::InvokeAction<Context>(
            context, action, length, src);
      });
}

template <typename Context>
template <
    typename Action,
    std::enable_if_t<
        serialized_message_internal::IsAction<Context, Action>::value, int>>
inline void SerializedMessageReader<Context>::BeforeMessage(
    absl::Span<const int> field_path, Action action) {
  SerializedMessageReaderBase::BeforeMessage(
      field_path, [action = std::move(action)](TypeErasedRef context) {
        return serialized_message_internal::InvokeAction<Context>(context,
                                                                  action);
      });
}

template <typename Context>
template <
    typename Action,
    std::enable_if_t<
        serialized_message_internal::IsAction<Context, Action>::value, int>>
inline void SerializedMessageReader<Context>::AfterMessage(
    absl::Span<const int> field_path, Action action) {
  SerializedMessageReaderBase::AfterMessage(
      field_path, [action = std::move(action)](TypeErasedRef context) {
        return serialized_message_internal::InvokeAction<Context>(context,
                                                                  action);
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<serialized_message_internal::IsAction<
                               Context, Action, uint32_t, Reader&>::value,
                           int>>
inline void SerializedMessageReader<Context>::OnOther(Action action) {
  SerializedMessageReaderBase::OnOther(
      [action = std::move(action)](uint32_t tag, LimitingReaderBase& src,
                                   TypeErasedRef context) {
        return serialized_message_internal::InvokeAction<Context>(
            context, action, tag, src);
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<serialized_message_internal::IsAction<
                               Context, Action, int>::value,
                           int>>
inline void SerializedMessageReader<Context>::BeforeOtherMessage(
    Action action) {
  SerializedMessageReaderBase::BeforeOtherMessage(
      [action = std::move(action)](int field_number, TypeErasedRef context) {
        return serialized_message_internal::InvokeAction<Context>(
            context, action, field_number);
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<serialized_message_internal::IsAction<
                               Context, Action, int>::value,
                           int>>
inline void SerializedMessageReader<Context>::AfterOtherMessage(Action action) {
  SerializedMessageReaderBase::AfterOtherMessage(
      [action = std::move(action)](int field_number, TypeErasedRef context) {
        return serialized_message_internal::InvokeAction<Context>(
            context, action, field_number);
      });
}

template <typename Context>
template <typename DependentContext,
          std::enable_if_t<!std::is_void<DependentContext>::value, int>>
inline absl::Status SerializedMessageReader<Context>::Read(
    AnyRef<Reader*> src, type_identity_t<DependentContext&> context) const {
  return SerializedMessageReaderBase::Read(std::move(src),
                                           TypeErasedRef(context));
}

template <typename Context>
template <typename DependentContext,
          std::enable_if_t<!std::is_void<DependentContext>::value, int>>
inline absl::Status SerializedMessageReader<Context>::Read(
    AnyRef<Reader*> src, type_identity_t<DependentContext&&> context) const {
  return SerializedMessageReaderBase::Read(std::move(src),
                                           TypeErasedRef(context));
}

template <typename Context>
template <typename DependentContext,
          std::enable_if_t<std::is_void<DependentContext>::value, int>>
inline absl::Status SerializedMessageReader<Context>::Read(
    AnyRef<Reader*> src) const {
  return SerializedMessageReaderBase::Read(std::move(src), TypeErasedRef());
}

}  // namespace riegeli

#endif  // RIEGELI_MESSAGES_SERIALIZED_MESSAGE_READER_
