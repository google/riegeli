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

#ifndef RIEGELI_MESSAGES_SERIALIZED_MESSAGE_REWRITER_
#define RIEGELI_MESSAGES_SERIALIZED_MESSAGE_REWRITER_

#include <stddef.h>
#include <stdint.h>

#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
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
#include "riegeli/bytes/chain_writer.h"
#include "riegeli/bytes/limiting_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/messages/parse_message.h"
#include "riegeli/messages/serialized_message_internal.h"
#include "riegeli/messages/serialized_message_reader.h"
#include "riegeli/messages/serialized_message_writer.h"  // IWYU pragma: export

namespace riegeli {

namespace serialized_message_rewriter_internal {

template <bool has_message_rewriter_context>
class MessageReaderContext;

template <>
class MessageReaderContext<true> {
 public:
  explicit MessageReaderContext(Writer* dest ABSL_ATTRIBUTE_LIFETIME_BOUND,
                                TypeErasedRef message_rewriter_context
                                    ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : message_writer_(dest),
        message_rewriter_context_(message_rewriter_context) {}

  SerializedMessageWriter& message_writer() { return message_writer_; }
  TypeErasedRef message_rewriter_context() const {
    return message_rewriter_context_;
  }

 private:
  SerializedMessageWriter message_writer_;
  TypeErasedRef message_rewriter_context_;
};

template <>
class MessageReaderContext<false> {
 public:
  explicit MessageReaderContext(Writer* dest ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : message_writer_(dest) {}

  SerializedMessageWriter& message_writer() { return message_writer_; }
  static TypeErasedRef message_rewriter_context() { return TypeErasedRef(); }

 private:
  SerializedMessageWriter message_writer_;
};

absl::Status CopyField(uint32_t tag, Reader& src, Writer& dest);

}  // namespace serialized_message_rewriter_internal

// `SerializedMessageRewriter` rewrites a serialized message using configured
// actions to be performed when encountering specific fields.
//
// The object holds registered actions, independent from the message object.
// Each rewrite is a separate `Rewrite()` call.
//
// `Context&` is passed to the actions as the last argument if `Context` is not
// `void` and the action is invocable with that argument, otherwise it is not
// passed. `Context` should hold any state specific to the particular message
// object, so that the `SerializedMessageRewriter` object can be reused.
//
// An action returns `absl::Status`, non-OK causing an early exit.
//
// Functions working on strings are applicable to any length-delimited field:
// `string`, `bytes`, submessage, or a packed repeated field.
//
// See `SerializedMessageReader` for reading a message without producing its
// edited version.
template <typename Context = void>
class SerializedMessageRewriter {
 public:
  // ```
  // const auto& message_rewriter = SerializedMessageRewriter<Context>::Global(
  //   [](SerializedMessageRewriter<Context>& message_rewriter) {
  //     ...
  //   });
  // ```
  //
  // Returns a const reference to a `SerializedMessageRewriter` object, with the
  // initializer once called on its non-const reference.
  //
  // The object is created when `Global()` is first called with the given
  // initializer type, and is never destroyed.
  //
  // The initializer should set the actions. This is the recommended way to
  // create a `SerializedMessageRewriter` object with a fixed set of fields
  // to be rewritten, while `Context` should hold any state specific to the
  // particular message object, so that the `SerializedMessageRewriter` object
  // can be reused.
  //
  // The initializer type should be a lambda with no captures. This restriction
  // is a safeguard against making the object dependent on local state, which
  // would be misleadingly ignored for subsequent calls. Since distinct lambdas
  // have distinct types, distinct call sites with lambdas return references to
  // distinct objects.
  template <
      typename Initialize,
      std::enable_if_t<
          absl::conjunction<
              std::is_empty<Initialize>,
              is_invocable<Initialize, SerializedMessageRewriter&>>::value,
          int> = 0>
  static const SerializedMessageRewriter& Global(Initialize initialize);

  SerializedMessageRewriter() noexcept;

  SerializedMessageRewriter(const SerializedMessageRewriter& that) = default;
  SerializedMessageRewriter& operator=(const SerializedMessageRewriter& that) =
      default;

  SerializedMessageRewriter(SerializedMessageRewriter&& that) = default;
  SerializedMessageRewriter& operator=(SerializedMessageRewriter&& that) =
      default;

  // Sets the action to be performed when encountering a field identified by
  // `field_path` of field numbers from the root through submessages.
  //
  // `action` is invoked with `value` being the value read, and `dest`
  // positioned between fields.
  //
  // The field will not be implicitly copied. `action` can write replacement
  // fields to `dest`, or do nothing to remove the field.
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
            std::enable_if_t<
                serialized_message_internal::IsAction<
                    Context, Action, int32_t, SerializedMessageWriter&>::value,
                int> = 0>
  void OnInt32(absl::Span<const int> field_path, Action action);
  template <typename Action,
            std::enable_if_t<
                serialized_message_internal::IsAction<
                    Context, Action, int64_t, SerializedMessageWriter&>::value,
                int> = 0>
  void OnInt64(absl::Span<const int> field_path, Action action);
  template <typename Action,
            std::enable_if_t<
                serialized_message_internal::IsAction<
                    Context, Action, uint32_t, SerializedMessageWriter&>::value,
                int> = 0>
  void OnUInt32(absl::Span<const int> field_path, Action action);
  template <typename Action,
            std::enable_if_t<
                serialized_message_internal::IsAction<
                    Context, Action, uint64_t, SerializedMessageWriter&>::value,
                int> = 0>
  void OnUInt64(absl::Span<const int> field_path, Action action);
  template <typename Action,
            std::enable_if_t<
                serialized_message_internal::IsAction<
                    Context, Action, int32_t, SerializedMessageWriter&>::value,
                int> = 0>
  void OnSInt32(absl::Span<const int> field_path, Action action);
  template <typename Action,
            std::enable_if_t<
                serialized_message_internal::IsAction<
                    Context, Action, int64_t, SerializedMessageWriter&>::value,
                int> = 0>
  void OnSInt64(absl::Span<const int> field_path, Action action);
  template <typename Action,
            std::enable_if_t<
                serialized_message_internal::IsAction<
                    Context, Action, bool, SerializedMessageWriter&>::value,
                int> = 0>
  void OnBool(absl::Span<const int> field_path, Action action);
  template <typename Action,
            std::enable_if_t<
                serialized_message_internal::IsAction<
                    Context, Action, uint32_t, SerializedMessageWriter&>::value,
                int> = 0>
  void OnFixed32(absl::Span<const int> field_path, Action action);
  template <typename Action,
            std::enable_if_t<
                serialized_message_internal::IsAction<
                    Context, Action, uint64_t, SerializedMessageWriter&>::value,
                int> = 0>
  void OnFixed64(absl::Span<const int> field_path, Action action);
  template <typename Action,
            std::enable_if_t<
                serialized_message_internal::IsAction<
                    Context, Action, int32_t, SerializedMessageWriter&>::value,
                int> = 0>
  void OnSFixed32(absl::Span<const int> field_path, Action action);
  template <typename Action,
            std::enable_if_t<
                serialized_message_internal::IsAction<
                    Context, Action, int64_t, SerializedMessageWriter&>::value,
                int> = 0>
  void OnSFixed64(absl::Span<const int> field_path, Action action);
  template <typename Action,
            std::enable_if_t<
                serialized_message_internal::IsAction<
                    Context, Action, float, SerializedMessageWriter&>::value,
                int> = 0>
  void OnFloat(absl::Span<const int> field_path, Action action);
  template <typename Action,
            std::enable_if_t<
                serialized_message_internal::IsAction<
                    Context, Action, double, SerializedMessageWriter&>::value,
                int> = 0>
  void OnDouble(absl::Span<const int> field_path, Action action);
  template <typename Action,
            std::enable_if_t<serialized_message_internal::IsAction<
                                 Context, Action, absl::string_view,
                                 SerializedMessageWriter&>::value,
                             int> = 0>
  void OnStringView(absl::Span<const int> field_path, Action action);
  template <typename Action,
            std::enable_if_t<serialized_message_internal::IsAction<
                                 Context, Action, std::string&&,
                                 SerializedMessageWriter&>::value,
                             int> = 0>
  void OnString(absl::Span<const int> field_path, Action action);
  template <typename Action,
            std::enable_if_t<
                serialized_message_internal::IsAction<
                    Context, Action, Chain&&, SerializedMessageWriter&>::value,
                int> = 0>
  void OnChain(absl::Span<const int> field_path, Action action);
  template <typename Action,
            std::enable_if_t<serialized_message_internal::IsAction<
                                 Context, Action, absl::Cord&&,
                                 SerializedMessageWriter&>::value,
                             int> = 0>
  void OnCord(absl::Span<const int> field_path, Action action);
  template <typename MessageType, typename Action,
            std::enable_if_t<serialized_message_internal::IsAction<
                                 Context, Action, MessageType&&,
                                 SerializedMessageWriter&>::value,
                             int> = 0>
  void OnParsedMessage(absl::Span<const int> field_path, Action action,
                       ParseOptions options = {});

  // Sets the action to be performed when encountering a length-delimited field
  // identified by `field_path` of field numbers from the root through
  // submessages.
  //
  // `action` is invoked with `src` from which the value will be read, and
  // `dest` positioned between fields. `src` will contain the field contents
  // (between `src.pos()` and `src.max_pos()`, with `src.max_length()`).
  // `action` can read any part of `src`.
  //
  // The field will not be implicitly copied. `action` can write replacement
  // fields to `dest`, or do nothing to remove the field.
  //
  // Precondition: `!field_path.empty()`
  template <typename Action,
            std::enable_if_t<serialized_message_internal::IsAction<
                                 Context, Action, LimitingReaderBase&,
                                 SerializedMessageWriter&>::value,
                             int> = 0>
  void OnLengthDelimited(absl::Span<const int> field_path, Action action);

  // Sets the action to be performed when encountering a length-delimited field
  // identified by `field_path` of field numbers from the root through
  // submessages.
  //
  // `action` is invoked with `length`, `src` from which the value will be read,
  // and `dest` positioned between fields. The first `length` bytes of `src`
  // will contain the field contents. `src` can be shorter only if the original
  // source is truncated, in which case parsing will fail anyway. `action` must
  // read exactly `length` bytes from `src`, unless it fails. This is unchecked.
  //
  // The field will not be implicitly copied. `action` can write replacement
  // fields to `dest`, or do nothing to remove the field.
  //
  // `OnLengthUnchecked()` is more efficient than `OnLengthDelimited()`.
  //
  // Precondition: `!field_path.empty()`
  template <typename Action,
            std::enable_if_t<serialized_message_internal::IsAction<
                                 Context, Action, size_t, LimitingReaderBase&,
                                 SerializedMessageWriter&>::value,
                             int> = 0>
  void OnLengthUnchecked(absl::Span<const int> field_path, Action action);

  // Sets the action to be performed when encountering a submessage field
  // identified by `field_path` of field numbers from the root through
  // submessages. An empty `field_path` specified the root message.
  //
  // The action is performed in addition to the base action which writes the
  // same field to `dest`:
  //
  //  * For `BeforeMessage()` with a non-empty `field_path`:
  //    `dest.OpenLengthDelimited()`.
  //  * For `AfterMessage()` with a non-empty `field_path`:
  //    `dest.CloseLengthDelimited(field_number)` with `field_number`
  //    being the last element of `field_path`.
  //  * With an empty `field_path`: none.
  //
  // `action` is invoked with `dest` positioned before/after the contents of
  // the submessage.
  template <
      typename Action,
      std::enable_if_t<serialized_message_internal::IsAction<
                           Context, Action, SerializedMessageWriter&>::value,
                       int> = 0>
  void BeforeMessage(absl::Span<const int> field_path, Action action);
  template <
      typename Action,
      std::enable_if_t<serialized_message_internal::IsAction<
                           Context, Action, SerializedMessageWriter&>::value,
                       int> = 0>
  void AfterMessage(absl::Span<const int> field_path, Action action);

  // Like `BeforeMessage()` and `AfterMessage()`, but the base actions are not
  // performed. This means that the same field will not be implicitly written to
  // `dest`.
  //
  // Nested fields are processed normally. Hence `dest` must be set up to write
  // nested fields to the right place, possibly by temporarily exchanging `dest`
  // with another `SerializedMessageWriter`.
  template <
      typename Action,
      std::enable_if_t<serialized_message_internal::IsAction<
                           Context, Action, SerializedMessageWriter&>::value,
                       int> = 0>
  void ReplaceBeforeMessage(absl::Span<const int> field_path, Action action);
  template <
      typename Action,
      std::enable_if_t<serialized_message_internal::IsAction<
                           Context, Action, SerializedMessageWriter&>::value,
                       int> = 0>
  void ReplaceAfterMessage(absl::Span<const int> field_path, Action action);

  // Rewrites a serialized message from `src` to `dest` using configured
  // actions.
  //
  // A reference to `context` is passed to the actions.
  template <typename DependentContext = Context,
            std::enable_if_t<!std::is_void<DependentContext>::value, int> = 0>
  absl::Status Rewrite(AnyRef<Reader*> src, AnyRef<Writer*> dest,
                       type_identity_t<DependentContext&> context) const;
  template <typename DependentContext = Context,
            std::enable_if_t<!std::is_void<DependentContext>::value, int> = 0>
  absl::Status Rewrite(AnyRef<Reader*> src, AnyRef<Writer*> dest,
                       type_identity_t<DependentContext&&> context) const;
  template <typename DependentContext = Context,
            std::enable_if_t<std::is_void<DependentContext>::value, int> = 0>
  absl::Status Rewrite(AnyRef<Reader*> src, AnyRef<Writer*> dest) const;

 private:
  using MessageReaderContext =
      serialized_message_rewriter_internal::MessageReaderContext<
          !std::is_void<Context>::value>;

  SerializedMessageReader<MessageReaderContext> message_reader_;
};

// Implementation details follow.

template <typename Context>
template <
    typename Initialize,
    std::enable_if_t<
        absl::conjunction<std::is_empty<Initialize>,
                          is_invocable<Initialize, SerializedMessageRewriter<
                                                       Context>&>>::value,
        int>>
inline const SerializedMessageRewriter<Context>&
SerializedMessageRewriter<Context>::Global(Initialize initialize) {
  return riegeli::Global([] { return SerializedMessageRewriter(); },
                         initialize);
}

template <typename Context>
SerializedMessageRewriter<Context>::SerializedMessageRewriter() noexcept {
  message_reader_.OnOther(
      [](uint32_t tag, Reader& src, MessageReaderContext& context) {
        return serialized_message_rewriter_internal::CopyField(
            tag, src, context.message_writer().writer());
      });
  message_reader_.BeforeOtherMessage([](ABSL_ATTRIBUTE_UNUSED int field_number,
                                        MessageReaderContext& context) {
    context.message_writer().OpenLengthDelimited();
    return absl::OkStatus();
  });
  message_reader_.AfterOtherMessage(
      [](int field_number, MessageReaderContext& context) {
        return context.message_writer().CloseLengthDelimited(field_number);
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<
              serialized_message_internal::IsAction<
                  Context, Action, int32_t, SerializedMessageWriter&>::value,
              int>>
inline void SerializedMessageRewriter<Context>::OnInt32(
    absl::Span<const int> field_path, Action action) {
  message_reader_.OnInt32(
      field_path, [action = std::move(action)](int32_t value,
                                               MessageReaderContext& context) {
        return serialized_message_internal::InvokeAction<Context>(
            context.message_rewriter_context(), action, value,
            context.message_writer());
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<
              serialized_message_internal::IsAction<
                  Context, Action, int64_t, SerializedMessageWriter&>::value,
              int>>
inline void SerializedMessageRewriter<Context>::OnInt64(
    absl::Span<const int> field_path, Action action) {
  message_reader_.OnInt64(
      field_path, [action = std::move(action)](int64_t value,
                                               MessageReaderContext& context) {
        return serialized_message_internal::InvokeAction<Context>(
            context.message_rewriter_context(), action, value,
            context.message_writer());
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<
              serialized_message_internal::IsAction<
                  Context, Action, uint32_t, SerializedMessageWriter&>::value,
              int>>
inline void SerializedMessageRewriter<Context>::OnUInt32(
    absl::Span<const int> field_path, Action action) {
  message_reader_.OnUInt32(
      field_path, [action = std::move(action)](uint32_t value,
                                               MessageReaderContext& context) {
        return serialized_message_internal::InvokeAction<Context>(
            context.message_rewriter_context(), action, value,
            context.message_writer());
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<
              serialized_message_internal::IsAction<
                  Context, Action, uint64_t, SerializedMessageWriter&>::value,
              int>>
inline void SerializedMessageRewriter<Context>::OnUInt64(
    absl::Span<const int> field_path, Action action) {
  message_reader_.OnUInt64(
      field_path, [action = std::move(action)](uint64_t value,
                                               MessageReaderContext& context) {
        return serialized_message_internal::InvokeAction<Context>(
            context.message_rewriter_context(), action, value,
            context.message_writer());
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<
              serialized_message_internal::IsAction<
                  Context, Action, int32_t, SerializedMessageWriter&>::value,
              int>>
inline void SerializedMessageRewriter<Context>::OnSInt32(
    absl::Span<const int> field_path, Action action) {
  message_reader_.OnSInt32(
      field_path, [action = std::move(action)](int32_t value,
                                               MessageReaderContext& context) {
        return serialized_message_internal::InvokeAction<Context>(
            context.message_rewriter_context(), action, value,
            context.message_writer());
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<
              serialized_message_internal::IsAction<
                  Context, Action, int64_t, SerializedMessageWriter&>::value,
              int>>
inline void SerializedMessageRewriter<Context>::OnSInt64(
    absl::Span<const int> field_path, Action action) {
  message_reader_.OnSInt64(
      field_path, [action = std::move(action)](int64_t value,
                                               MessageReaderContext& context) {
        return serialized_message_internal::InvokeAction<Context>(
            context.message_rewriter_context(), action, value,
            context.message_writer());
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<
              serialized_message_internal::IsAction<
                  Context, Action, bool, SerializedMessageWriter&>::value,
              int>>
inline void SerializedMessageRewriter<Context>::OnBool(
    absl::Span<const int> field_path, Action action) {
  message_reader_.OnBool(
      field_path,
      [action = std::move(action)](bool value, MessageReaderContext& context) {
        return serialized_message_internal::InvokeAction<Context>(
            context.message_rewriter_context(), action, value,
            context.message_writer());
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<
              serialized_message_internal::IsAction<
                  Context, Action, uint32_t, SerializedMessageWriter&>::value,
              int>>
inline void SerializedMessageRewriter<Context>::OnFixed32(
    absl::Span<const int> field_path, Action action) {
  message_reader_.OnFixed32(
      field_path, [action = std::move(action)](uint32_t value,
                                               MessageReaderContext& context) {
        return serialized_message_internal::InvokeAction<Context>(
            context.message_rewriter_context(), action, value,
            context.message_writer());
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<
              serialized_message_internal::IsAction<
                  Context, Action, uint64_t, SerializedMessageWriter&>::value,
              int>>
inline void SerializedMessageRewriter<Context>::OnFixed64(
    absl::Span<const int> field_path, Action action) {
  message_reader_.OnFixed64(
      field_path, [action = std::move(action)](uint64_t value,
                                               MessageReaderContext& context) {
        return serialized_message_internal::InvokeAction<Context>(
            context.message_rewriter_context(), action, value,
            context.message_writer());
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<
              serialized_message_internal::IsAction<
                  Context, Action, int32_t, SerializedMessageWriter&>::value,
              int>>
inline void SerializedMessageRewriter<Context>::OnSFixed32(
    absl::Span<const int> field_path, Action action) {
  message_reader_.OnSFixed32(
      field_path, [action = std::move(action)](int32_t value,
                                               MessageReaderContext& context) {
        return serialized_message_internal::InvokeAction<Context>(
            context.message_rewriter_context(), action, value,
            context.message_writer());
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<
              serialized_message_internal::IsAction<
                  Context, Action, int64_t, SerializedMessageWriter&>::value,
              int>>
inline void SerializedMessageRewriter<Context>::OnSFixed64(
    absl::Span<const int> field_path, Action action) {
  message_reader_.OnSFixed64(
      field_path, [action = std::move(action)](int64_t value,
                                               MessageReaderContext& context) {
        return serialized_message_internal::InvokeAction<Context>(
            context.message_rewriter_context(), action, value,
            context.message_writer());
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<
              serialized_message_internal::IsAction<
                  Context, Action, float, SerializedMessageWriter&>::value,
              int>>
inline void SerializedMessageRewriter<Context>::OnFloat(
    absl::Span<const int> field_path, Action action) {
  message_reader_.OnFloat(
      field_path,
      [action = std::move(action)](float value, MessageReaderContext& context) {
        return serialized_message_internal::InvokeAction<Context>(
            context.message_rewriter_context(), action, value,
            context.message_writer());
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<
              serialized_message_internal::IsAction<
                  Context, Action, double, SerializedMessageWriter&>::value,
              int>>
inline void SerializedMessageRewriter<Context>::OnDouble(
    absl::Span<const int> field_path, Action action) {
  message_reader_.OnDouble(
      field_path, [action = std::move(action)](double value,
                                               MessageReaderContext& context) {
        return serialized_message_internal::InvokeAction<Context>(
            context.message_rewriter_context(), action, value,
            context.message_writer());
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<serialized_message_internal::IsAction<
                               Context, Action, absl::string_view,
                               SerializedMessageWriter&>::value,
                           int>>
inline void SerializedMessageRewriter<Context>::OnStringView(
    absl::Span<const int> field_path, Action action) {
  message_reader_.OnStringView(
      field_path, [action = std::move(action)](absl::string_view value,
                                               MessageReaderContext& context) {
        return serialized_message_internal::InvokeAction<Context>(
            context.message_rewriter_context(), action, value,
            context.message_writer());
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<serialized_message_internal::IsAction<
                               Context, Action, std::string&&,
                               SerializedMessageWriter&>::value,
                           int>>
inline void SerializedMessageRewriter<Context>::OnString(
    absl::Span<const int> field_path, Action action) {
  message_reader_.OnString(
      field_path, [action = std::move(action)](std::string&& value,
                                               MessageReaderContext& context) {
        return serialized_message_internal::InvokeAction<Context>(
            context.message_rewriter_context(), action, std::move(value),
            context.message_writer());
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<
              serialized_message_internal::IsAction<
                  Context, Action, Chain&&, SerializedMessageWriter&>::value,
              int>>
inline void SerializedMessageRewriter<Context>::OnChain(
    absl::Span<const int> field_path, Action action) {
  message_reader_.OnChain(
      field_path, [action = std::move(action)](Chain&& value,
                                               MessageReaderContext& context) {
        return serialized_message_internal::InvokeAction<Context>(
            context.message_rewriter_context(), action, std::move(value),
            context.message_writer());
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<serialized_message_internal::IsAction<
                               Context, Action, absl::Cord&&,
                               SerializedMessageWriter&>::value,
                           int>>
inline void SerializedMessageRewriter<Context>::OnCord(
    absl::Span<const int> field_path, Action action) {
  message_reader_.OnCord(
      field_path, [action = std::move(action)](absl::Cord&& value,
                                               MessageReaderContext& context) {
        return serialized_message_internal::InvokeAction<Context>(
            context.message_rewriter_context(), action, std::move(value),
            context.message_writer());
      });
}

template <typename Context>
template <typename MessageType, typename Action,
          std::enable_if_t<serialized_message_internal::IsAction<
                               Context, Action, MessageType&&,
                               SerializedMessageWriter&>::value,
                           int>>
inline void SerializedMessageRewriter<Context>::OnParsedMessage(
    absl::Span<const int> field_path, Action action,
    ParseOptions parse_options) {
  message_reader_.OnLengthUnchecked(
      field_path, [action = std::move(action), parse_options](
                      size_t length, LimitingReaderBase& src,
                      MessageReaderContext& context) {
        MessageType message;
        if (absl::Status status = riegeli::ParseMessageWithLength(
                src, length, message, parse_options);
            ABSL_PREDICT_FALSE(!status.ok())) {
          return status;
        }
        return serialized_message_internal::InvokeAction<Context>(
            context.message_rewriter_context(), action, std::move(message),
            context.message_writer());
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<serialized_message_internal::IsAction<
                               Context, Action, LimitingReaderBase&,
                               SerializedMessageWriter&>::value,
                           int>>
inline void SerializedMessageRewriter<Context>::OnLengthDelimited(
    absl::Span<const int> field_path, Action action) {
  message_reader_.OnLengthDelimited(
      field_path, [action = std::move(action)](LimitingReaderBase& src,
                                               MessageReaderContext& context) {
        return serialized_message_internal::InvokeAction<Context>(
            context.message_rewriter_context(), action, src,
            context.message_writer());
      });
}

template <typename Context>
template <typename Action,
          std::enable_if_t<serialized_message_internal::IsAction<
                               Context, Action, size_t, LimitingReaderBase&,
                               SerializedMessageWriter&>::value,
                           int>>
inline void SerializedMessageRewriter<Context>::OnLengthUnchecked(
    absl::Span<const int> field_path, Action action) {
  message_reader_.OnLengthUnchecked(
      field_path,
      [action = std::move(action)](size_t length, LimitingReaderBase& src,
                                   MessageReaderContext& context) {
        return serialized_message_internal::InvokeAction<Context>(
            context.message_rewriter_context(), action, length, src,
            context.message_writer());
      });
}

template <typename Context>
template <
    typename Action,
    std::enable_if_t<serialized_message_internal::IsAction<
                         Context, Action, SerializedMessageWriter&>::value,
                     int>>
inline void SerializedMessageRewriter<Context>::BeforeMessage(
    absl::Span<const int> field_path, Action action) {
  if (field_path.empty()) {
    ReplaceBeforeMessage(field_path, std::move(action));
  } else {
    message_reader_.BeforeMessage(
        field_path,
        [action = std::move(action)](MessageReaderContext& context) {
          context.message_writer().OpenLengthDelimited();
          return serialized_message_internal::InvokeAction<Context>(
              context.message_rewriter_context(), action,
              context.message_writer());
        });
  }
}

template <typename Context>
template <
    typename Action,
    std::enable_if_t<serialized_message_internal::IsAction<
                         Context, Action, SerializedMessageWriter&>::value,
                     int>>
inline void SerializedMessageRewriter<Context>::AfterMessage(
    absl::Span<const int> field_path, Action action) {
  if (field_path.empty()) {
    ReplaceAfterMessage(field_path, std::move(action));
  } else {
    const int field_number = field_path.back();
    message_reader_.AfterMessage(
        field_path, [action = std::move(action),
                     field_number](MessageReaderContext& context) {
          if (absl::Status status =
                  serialized_message_internal::InvokeAction<Context>(
                      context.message_rewriter_context(), action,
                      context.message_writer());
              ABSL_PREDICT_FALSE(!status.ok())) {
            return status;
          }
          return context.message_writer().CloseLengthDelimited(field_number);
        });
  }
}

template <typename Context>
template <
    typename Action,
    std::enable_if_t<serialized_message_internal::IsAction<
                         Context, Action, SerializedMessageWriter&>::value,
                     int>>
inline void SerializedMessageRewriter<Context>::ReplaceBeforeMessage(
    absl::Span<const int> field_path, Action action) {
  message_reader_.BeforeMessage(field_path, [action = std::move(action)](
                                                MessageReaderContext& context) {
    return serialized_message_internal::InvokeAction<Context>(
        context.message_rewriter_context(), action, context.message_writer());
  });
}

template <typename Context>
template <
    typename Action,
    std::enable_if_t<serialized_message_internal::IsAction<
                         Context, Action, SerializedMessageWriter&>::value,
                     int>>
inline void SerializedMessageRewriter<Context>::ReplaceAfterMessage(
    absl::Span<const int> field_path, Action action) {
  message_reader_.AfterMessage(field_path, [action = std::move(action)](
                                               MessageReaderContext& context) {
    return serialized_message_internal::InvokeAction<Context>(
        context.message_rewriter_context(), action, context.message_writer());
  });
}

template <typename Context>
template <typename DependentContext,
          std::enable_if_t<!std::is_void<DependentContext>::value, int>>
inline absl::Status SerializedMessageRewriter<Context>::Rewrite(
    AnyRef<Reader*> src, AnyRef<Writer*> dest,
    type_identity_t<DependentContext&> context) const {
  absl::Status status = message_reader_.Read(
      std::move(src), MessageReaderContext(dest.get(), TypeErasedRef(context)));
  if (dest.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest->Close())) status.Update(dest->status());
  }
  return status;
}

template <typename Context>
template <typename DependentContext,
          std::enable_if_t<!std::is_void<DependentContext>::value, int>>
inline absl::Status SerializedMessageRewriter<Context>::Rewrite(
    AnyRef<Reader*> src, AnyRef<Writer*> dest,
    type_identity_t<DependentContext&&> context) const {
  return Rewrite(std::move(src), std::move(dest), context);
}

template <typename Context>
template <typename DependentContext,
          std::enable_if_t<std::is_void<DependentContext>::value, int>>
inline absl::Status SerializedMessageRewriter<Context>::Rewrite(
    AnyRef<Reader*> src, AnyRef<Writer*> dest) const {
  absl::Status status =
      message_reader_.Read(std::move(src), MessageReaderContext(dest.get()));
  if (dest.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest->Close())) status.Update(dest->status());
  }
  return status;
}

}  // namespace riegeli

#endif  // RIEGELI_MESSAGES_SERIALIZED_MESSAGE_REWRITER_
