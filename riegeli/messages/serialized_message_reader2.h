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

#ifndef RIEGELI_MESSAGES_SERIALIZED_MESSAGE_READER2_H_
#define RIEGELI_MESSAGES_SERIALIZED_MESSAGE_READER2_H_

#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/bytes_ref.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/cord_iterator_span.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/reset.h"
#include "riegeli/base/type_traits.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/cord_reader.h"
#include "riegeli/bytes/limiting_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/string_reader.h"
#include "riegeli/endian/endian_reading.h"
#include "riegeli/messages/message_wire_format.h"
#include "riegeli/messages/serialized_message_reader_internal.h"
#include "riegeli/varint/varint_reading.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

// Overview of `SerializedMessageReader2`
// --------------------------------------
//
// `SerializedMessageReader2` reads a serialized proto message, performing
// specified actions on encountering particular fields, instead of filling an
// in-memory message object like in `ParseMessage()`.
//
// Use cases:
//
//  * Processing a subset of fields without the overhead of materializing the
//    message object, i.e. without processing the remaining fields, without
//    processing fields contained in submessages which can be processed as a
//    whole, and without keeping the whole parsed message in memory.
//
//  * Processing a message in a way known at runtime, possibly with the schema
//    known at runtime, possibly partially known.
//
//  * Processing messages with so many elements of toplevel repeated fields that
//    the total message size exceeds 2GiB. This is not a great idea in itself,
//    because such messages cannot be processed using native proto parsing and
//    serialization.
//
// Fields to be handled are specified by field handlers. For each field present
// in the serialized message, the first handler which accepts the field is
// invoked. If no handler accepts the field, the field is skipped.
//
// The serialized message is read from a `Reader`, `Cord`, or string. Other
// sources can be expressed as a `Reader` or string.

// Field handlers
// --------------
//
// Field handlers handle fields with particular field numbers and wire types
// (varint, fixed32, fixed64, length-delimited, start-group, or end-group)
// by invoking specific actions.
//
// There are three categories of field handlers:
//
//  * Static: the field number and wire type are statically known.
//
//  * Dynamic: they can handle a variety of field numbers and wire types,
//    determined at runtime.
//
//  * Unbound: the field number is unspecified, while the wire type is
//    statically known.
//
// Static and dynamic field handlers can be used directly with
// `SerializedMessageReader2`. Unbound field handlers are meant to be registered
// with a `FieldHandlerMap`, with field numbers known at runtime.
//
// A family of functions returning static and unbound field handlers is defined
// in namespace `riegeli::field_handlers`.
//
// The primary dynamic field handlers are `FieldHandlerMap` and
// `CopyingFieldHandler`.
//
// All field handlers stored in a single `SerializedMessageReader2` are usually
// conceptually associated with a single message type.
//
// If a field handler returns a failed `absl::Status`, `ReadMessage()` is
// cancelled and propagates the status, annotated by the `Reader` and/or with
// the field number. Annotations are skipped for `absl::CancelledError()` to
// make it more efficient to cancel a handler when cancellation is likely.
//
// A field handler can also be expressed as a reference to a const-qualified
// proper field handler, to avoid `SerializedMessageReader2` taking the
// ownership. Use `std::cref()` in a `SerializedMessageReader2()` call.

// Context types
// -------------
//
// It is recommended to make field handler actions stateless, i.e. independent
// from any state specific to the message object being read. This makes field
// handlers and `SerializedMessageReader2` themselves stateless. Such a
// `SerializedMessageReader2` can usually be stored in a `static constexpr`
// variable and reused for multiple messages.
//
// To facilitate this, the `SerializedMessageReader2`, its field handlers, and
// their actions are parameterized by a sequence of `Context` types. Actions of
// field handlers receive additional `Context&...` parameters. Reading a
// serialized message provides `Context&...` arguments.

// Field handler protocol
// ----------------------
//
// Users of defined field handlers do not need to be concerned with this.
// This is relevant for writing custom field handlers.
//
// A field handler of a length-delimited field is applicable to a `Reader`
// source, and possibly also directly applicable to a `Cord` and/or string
// source. Applicability to a string source implies applicability to a `Reader`
// and `Cord` source, by reading a `string_view` from the `Reader` or `Cord`
// first.
//
// If `SerializedMessageReader2::ReadMessage()` is called with a `Cord` or
// string, and some field handlers are not directly applicable to that source,
// then the source is wrapped in an appropriate `Reader`. This is done on the
// level of the whole `ReadMessage()`, instead of creating a `Reader` for each
// affected field, so that the cost of creating a `Reader` is paid once.
//
// Field handlers have a `static constexpr int kFieldNumber` member variable:
//  * For static field handlers: a positive field number.
//  * For dynamic field handlers: `kDynamicFieldNumber`.
//  * For unbound field handlers: `kUnboundFieldNumber`.
//
// Static and unbound field handlers provide at least one of the following
// member functions, with parameters followed by `Context&...`:
// ```
//   absl::Status HandleVarint(uint64_t repr) const;
//
//   absl::Status HandleFixed32(uint32_t repr) const;
//
//   absl::Status HandleFixed64(uint64_t repr) const;
//
//   // Directly applicable to a `Reader` source.
//   //
//   // `HandleLengthDelimitedFromReader()` must read to the end of the
//   // `ReaderSpan<>` or fail. `SkipLengthDelimitedFromReader()` can be used
//   // to ensure this property.
//   absl::Status HandleLengthDelimitedFromReader(ReaderSpan<> repr) const;
//
//   // Directly applicable to a `Cord` source.
//   //
//   // May use `scratch` for storage for temporary data.
//   //
//   // `HandleLengthDelimitedFromCord()` must read to the end of the
//   // `CordIteratorSpan` or fail. `SkipLengthDelimitedFromCord()` can be used
//   // to ensure this property.
//   absl::Status HandleLengthDelimitedFromCord(CordIteratorSpan repr,
//                                              std::string& scratch) const;
//
//   // Directly applicable to a string source.
//   absl::Status HandleLengthDelimitedFromString(absl::string_view repr)
//       const;
//
//   absl::Status HandleStartGroup() const;
//
//   absl::Status HandleEndGroup() const;
// ```
//
// For a `Reader` source, `HandleLengthDelimitedFromReader()` or
// `HandleLengthDelimitedFromString()` is used, depending on what is defined.
//
// For a `Cord` source, `HandleLengthDelimitedFromCord()` or
// `HandleLengthDelimitedFromString()` is used, depending on what is defined.
// If neither is defined but `HandleLengthDelimitedFromReader()` is defined,
// then the source is wrapped in a `CordReader`.
//
// For a string source, `HandleLengthDelimitedFromString()` is used. If it is
// not defined but `HandleLengthDelimitedFromReader()` is defined, then the
// source is wrapped in a `StringReader`.
//
// For a string source, if `HandleLengthDelimitedFromString()` is used, then the
// `absl::string_view` is guaranteed to be a substring of the original string.
// This guarantee is absent for a `Reader` or `Cord` source.
//
// Dynamic field handlers provide at least one of the following pairs of
// member functions corresponding to some wire type `X` as above, with `T...`
// as above, and with `DynamicHandleX()` parameters followed by `Context&...`
// ```
//   MaybeAccepted AcceptX(int field_number) const;`
//
//   absl::Status DynamicHandleX(Accepted accepted, T... repr) const;
// ```
//
// For length-delimited fields, a single `AcceptLengthDelimited()` function
// corresponds to `DynamicHandleLengthDelimitedFromReader()`,
// `DynamicHandleLengthDelimitedFromCord()`, and/or
// `DynamicHandleLengthDelimitedFromString()`.
//
// `MaybeAccepted` is some type explicitly convertible to `bool`, with
// `operator*` returning some `Accepted` type, such as `std::optional<Accepted>`
// or `Accepted*`. If `AcceptX()` returns a value explicitly convertible to
// `true`, then the field is accepted, and the corresponding `HandleX()`
// function is called with the result of `operator*` as the first argument.

// In `FieldHandler::kFieldNumber`, marks a dynamic field handler.
inline constexpr int kDynamicFieldNumber =
    serialized_message_reader_internal::kDynamicFieldNumber;

// In `FieldHandler::kFieldNumber`, marks an unbound field handler.
inline constexpr int kUnboundFieldNumber =
    serialized_message_reader_internal::kUnboundFieldNumber;

// `IsFieldHandler<T, Context...>::value` is `true` if `T` is a valid argument
// type for `SerializedMessageReader2<Context...>`.
template <typename T, typename... Context>
struct IsFieldHandler;

// `IsUnboundFieldHandler<T, Context...>::value` is `true` if `T` is a valid
// argument type for `FieldHandlerMap::RegisterField()`.
template <typename T, typename... Context>
struct IsUnboundFieldHandler;

// For technical reasons related to template argument deduction,
// `SerializedMessageReader2` is not a class template but a function template.
// Its return type is called `SerializedMessageReaderType`.
//
// The type is usually spelled `const auto`, preferably `static constexpr auto`.
//
// `FieldHandlers` is a `std::tuple` of field handlers.
template <typename FieldHandlers, typename... Context>
class SerializedMessageReaderType;

template <typename... FieldHandlers, typename... Context>
class SerializedMessageReaderType<std::tuple<FieldHandlers...>, Context...> {
 public:
  // Creates a `SerializedMessageReader2` with default-initialized field
  // handlers. Designed for `Reset()`.
  SerializedMessageReaderType() = default;

  // Constructs a `SerializedMessageReader2` from field handlers.
  template <
      typename... FieldHandlerInitializers,
      std::enable_if_t<
          std::conjunction_v<
              std::bool_constant<(sizeof...(FieldHandlerInitializers) > 0)>,
              NotSameRef<SerializedMessageReaderType,
                         FieldHandlerInitializers&&...>,
              std::is_convertible<FieldHandlerInitializers&&,
                                  FieldHandlers>...>,
          int> = 0>
  explicit constexpr SerializedMessageReaderType(
      FieldHandlerInitializers&&... field_handlers)
      : field_handlers_(
            std::forward<FieldHandlerInitializers>(field_handlers)...) {}

  SerializedMessageReaderType(const SerializedMessageReaderType&) = default;
  SerializedMessageReaderType& operator=(const SerializedMessageReaderType&) =
      default;

  SerializedMessageReaderType(SerializedMessageReaderType&& that) = default;
  SerializedMessageReaderType& operator=(SerializedMessageReaderType&& that) =
      default;

  // Makes `*this` equivalent to a newly constructed `SerializedMessageReader2`.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset() {
    ResetImpl(std::index_sequence_for<FieldHandlers...>());
  }
  template <
      typename... FieldHandlerInitializers,
      std::enable_if_t<
          std::conjunction_v<
              std::bool_constant<(sizeof...(FieldHandlerInitializers) > 0)>,
              SupportsReset<FieldHandlers, FieldHandlerInitializers&&>...>,
          int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      FieldHandlerInitializers&&... field_handlers) {
    ResetImpl(std::index_sequence_for<FieldHandlers...>(),
              std::forward<FieldHandlerInitializers>(field_handlers)...);
  }

  // Reads a serialized message, letting field handlers process the fields.
  //
  // Reading from a string is more efficient than from other sources if all
  // field handlers are applicable to a string source. Otherwise, the source is
  // wrapped in an appropriate `Reader` if needed.
  //
  // For a `Reader` source, if any field handler handles length-delimited
  // fields, then the root `Reader` will be wrapped in a `LimitingReader`,
  // unless it is already statically known to be a `LimitingReaderBase`.
  // This allows field handlers to use `ScopedLimiter` for reading the value
  // of a length-delimited field.
  //
  // If `Reader::SupportsSize()` and it was wrapped in a `LimitingReader`,
  // then the `LimitingReader` is initially limited to the whole message.
  // This helps parsing untrusted data: if the size of the message is bounded,
  // then claimed lengths of length-delimited fields are bounded as well,
  // and thus it is safe to e.g. pass such a length to `Reader::Read()`.
  template <typename Src,
            std::enable_if_t<TargetRefSupportsDependency<Reader*, Src>::value,
                             int> = 0>
  absl::Status ReadMessage(Src&& src, Context&... context) const;
  absl::Status ReadMessage(BytesRef src, Context&... context) const;
  absl::Status ReadMessage(const Chain& src, Context&... context) const;
  absl::Status ReadMessage(const absl::Cord& src, Context&... context) const;
  absl::Status ReadMessage(CordIteratorSpan src, Context&... context) const;

 private:
  template <size_t... indices>
  void ResetImpl(std::index_sequence<indices...>) {
    (riegeli::Reset(std::get<indices>(field_handlers_)), ...);
  }
  template <
      size_t... indices, typename... FieldHandlerInitializers,
      std::enable_if_t<(sizeof...(FieldHandlerInitializers) > 0), int> = 0>
  void ResetImpl(std::index_sequence<indices...>,
                 FieldHandlerInitializers&&... field_handlers) {
    (riegeli::Reset(std::get<indices>(field_handlers_),
                    std::forward<FieldHandlerInitializers>(field_handlers)...),
     ...);
  }

  template <typename ReaderType>
  absl::Status ReadMessageFromReader(ReaderType& src,
                                     Context&... context) const;
  absl::Status ReadMessageFromCord(absl::Cord::CharIterator& src,
                                   size_t available, Context&... context) const;
  absl::Status ReadMessageFromString(absl::string_view src,
                                     Context&... context) const;

  template <size_t index = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE bool HandleVarintField(
      int field_number, uint64_t value, absl::Status& status,
      Context&... context) const {
    if constexpr (index < sizeof...(FieldHandlers)) {
      return (serialized_message_reader_internal::ReadVarintField(
                  field_number, value, status, std::get<index>(field_handlers_),
                  context...) ||
              HandleVarintField<index + 1>(field_number, value, status,
                                           context...));
    } else {
      return false;
    }
  }

  template <size_t index = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE bool HandleFixed32Field(
      int field_number, uint32_t value, absl::Status& status,
      Context&... context) const {
    if constexpr (index < sizeof...(FieldHandlers)) {
      return (serialized_message_reader_internal::ReadFixed32Field(
                  field_number, value, status, std::get<index>(field_handlers_),
                  context...) ||
              HandleFixed32Field<index + 1>(field_number, value, status,
                                            context...));
    } else {
      return false;
    }
  }

  template <size_t index = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE bool HandleFixed64Field(
      int field_number, uint64_t value, absl::Status& status,
      Context&... context) const {
    if constexpr (index < sizeof...(FieldHandlers)) {
      return (serialized_message_reader_internal::ReadFixed64Field(
                  field_number, value, status, std::get<index>(field_handlers_),
                  context...) ||
              HandleFixed64Field<index + 1>(field_number, value, status,
                                            context...));
    } else {
      return false;
    }
  }

  template <size_t index = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE bool HandleLengthDelimitedFieldFromReader(
      int field_number, LimitingReaderBase& src, size_t length,
      absl::Status& status, Context&... context) const {
    if constexpr (index < sizeof...(FieldHandlers)) {
      return (serialized_message_reader_internal::
                  ReadLengthDelimitedFieldFromReader(
                      field_number, src, length, status,
                      std::get<index>(field_handlers_), context...) ||
              HandleLengthDelimitedFieldFromReader<index + 1>(
                  field_number, src, length, status, context...));
    } else {
      return false;
    }
  }

  template <size_t index = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE bool HandleLengthDelimitedFieldFromCord(
      int field_number, absl::Cord::CharIterator& src, size_t length,
      std::string& scratch, absl::Status& status, Context&... context) const {
    if constexpr (index < sizeof...(FieldHandlers)) {
      return (
          serialized_message_reader_internal::ReadLengthDelimitedFieldFromCord(
              field_number, src, length, scratch, status,
              std::get<index>(field_handlers_), context...) ||
          HandleLengthDelimitedFieldFromCord<index + 1>(
              field_number, src, length, scratch, status, context...));
    } else {
      return false;
    }
  }

  template <size_t index = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE bool HandleLengthDelimitedFieldFromString(
      int field_number, const char* cursor, size_t length, absl::Status& status,
      Context&... context) const {
    if constexpr (index < sizeof...(FieldHandlers)) {
      return (serialized_message_reader_internal::
                  ReadLengthDelimitedFieldFromString(
                      field_number, cursor, length, status,
                      std::get<index>(field_handlers_), context...) ||
              HandleLengthDelimitedFieldFromString<index + 1>(
                  field_number, cursor, length, status, context...));
    } else {
      return false;
    }
  }

  template <size_t index = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE bool HandleStartGroupField(
      int field_number, absl::Status& status, Context&... context) const {
    if constexpr (index < sizeof...(FieldHandlers)) {
      return (
          serialized_message_reader_internal::ReadStartGroupField(
              field_number, status, std::get<index>(field_handlers_),
              context...) ||
          HandleStartGroupField<index + 1>(field_number, status, context...));
    } else {
      return false;
    }
  }

  template <size_t index = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE bool HandleEndGroupField(
      int field_number, absl::Status& status, Context&... context) const {
    if constexpr (index < sizeof...(FieldHandlers)) {
      return (serialized_message_reader_internal::ReadEndGroupField(
                  field_number, status, std::get<index>(field_handlers_),
                  context...) ||
              HandleEndGroupField<index + 1>(field_number, status, context...));
    } else {
      return false;
    }
  }

  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS std::tuple<FieldHandlers...> field_handlers_;
};

// Returns a `SerializedMessageReaderType` which reads serialized messages using
// the given field handlers.
//
// Typical usage:
// ```
//   static constexpr auto message_reader =
//       riegeli::SerializedMessageReader2<Context...>(
//           field_handlers...);
//   absl::Status status = message_reader.ReadMessage(src, context...);
// ```
//
// In the `message_reader` name, it can be helpful to replace `message` with the
// actual message type being read.
//
// `Context` types must be specified explicitly for `SerializedMessageReader2`.
// Field handlers and their actions must accept compatible `Context&...`
// parameters.
template <
    typename... Context, typename... FieldHandlerInitializers
#if !__cpp_concepts
    ,
    std::enable_if_t<std::conjunction_v<IsFieldHandler<
                         TargetT<FieldHandlerInitializers>, Context...>...>,
                     int> = 0
#endif
    >
#if __cpp_concepts
// For conjunctions, `requires` gives better error messages than
// `std::enable_if_t`, indicating the relevant argument.
  requires(
      IsFieldHandler<TargetT<FieldHandlerInitializers>, Context...>::value &&
      ...)
#endif
constexpr SerializedMessageReaderType<
    std::tuple<TargetT<FieldHandlerInitializers>...>, Context...>
SerializedMessageReader2(FieldHandlerInitializers&&... field_handlers) {
  return SerializedMessageReaderType<
      std::tuple<TargetT<FieldHandlerInitializers>...>, Context...>(
      std::forward<FieldHandlerInitializers>(field_handlers)...);
}

// In the field handler protocol, `HandleLengthDelimitedFromReader()` must read
// to the end of the `ReaderSpan<>` or fail.
//
// `SkipLengthDelimitedFromReader()` can be used to ensure this property.
// With an action, the part of the field not read by the action is skipped.
// Without an action, the whole field is skipped.

template <
    typename Action,
    std::enable_if_t<std::is_invocable_r_v<absl::Status, Action>, int> = 0>
absl::Status SkipLengthDelimitedFromReader(const ReaderSpan<>& value,
                                           Action&& action);

absl::Status SkipLengthDelimitedFromReader(ReaderSpan<> value);

// In the field handler protocol, `HandleLengthDelimitedFromCord()` must read
// to the end of the `CordIteratorSpan` or fail.
//
// `SkipLengthDelimitedFromCord()` can be used to ensure this property.
// With an action, the part of the field not read by the action is skipped.
// Without an action, the whole field is skipped.

template <
    typename Action,
    std::enable_if_t<std::is_invocable_r_v<absl::Status, Action>, int> = 0>
absl::Status SkipLengthDelimitedFromCord(const CordIteratorSpan& value,
                                         Action&& action);

absl::Status SkipLengthDelimitedFromCord(CordIteratorSpan value);

// Implementation details follow.

template <typename T, typename... Context>
struct IsFieldHandler
    : std::disjunction<serialized_message_reader_internal::IsStaticFieldHandler<
                           T, Context...>,
                       serialized_message_reader_internal::
                           IsDynamicFieldHandler<T, Context...>> {};

template <typename T, typename... Context>
struct IsUnboundFieldHandler
    : std::conjunction<
          serialized_message_reader_internal::
              IsFieldHandlerWithUnboundFieldNumber<T>,
          std::disjunction<
              serialized_message_reader_internal::IsStaticFieldHandlerForVarint<
                  T, Context...>,
              serialized_message_reader_internal::
                  IsStaticFieldHandlerForFixed32<T, Context...>,
              serialized_message_reader_internal::
                  IsStaticFieldHandlerForFixed64<T, Context...>,
              serialized_message_reader_internal::
                  IsStaticFieldHandlerForLengthDelimited<T, Context...>,
              serialized_message_reader_internal::
                  IsStaticFieldHandlerForStartGroup<T, Context...>,
              serialized_message_reader_internal::
                  IsStaticFieldHandlerForEndGroup<T, Context...>>> {};

template <typename... FieldHandlers, typename... Context>
template <typename ReaderType>
absl::Status SerializedMessageReaderType<
    std::tuple<FieldHandlers...>,
    Context...>::ReadMessageFromReader(ReaderType& src,
                                       Context&... context) const {
  uint32_t tag;
  while (ReadVarint32(src, tag)) {
    const int field_number = GetTagFieldNumber(tag);
    switch (GetTagWireType(tag)) {
      case WireType::kVarint: {
        if constexpr (std::disjunction_v<
                          serialized_message_reader_internal::
                              IsFieldHandlerForVarint<FieldHandlers,
                                                      Context...>...>) {
          uint64_t value;
          if (ABSL_PREDICT_FALSE(!ReadVarint64(src, value))) {
            return serialized_message_reader_internal::ReadVarintError(
                src, field_number);
          }
          absl::Status status;
          if (HandleVarintField(field_number, value, status, context...)) {
            if (ABSL_PREDICT_FALSE(!status.ok())) {
              return serialized_message_reader_internal::
                  AnnotateWithSourceAndFieldNumber(std::move(status), src,
                                                   field_number);
            }
          }
        } else {
          // The value is not needed. Use more efficient `SkipVarint64()`
          // instead of `ReadVarint64()`.
          if (ABSL_PREDICT_FALSE(!SkipVarint64(src))) {
            return serialized_message_reader_internal::ReadVarintError(
                src, field_number);
          }
        }
        continue;
      }
      case WireType::kFixed32: {
        uint32_t value;
        if (ABSL_PREDICT_FALSE(!ReadLittleEndian<uint32_t>(src, value))) {
          return serialized_message_reader_internal::ReadFixed32Error(
              src, field_number);
        }
        absl::Status status;
        if (HandleFixed32Field(field_number, value, status, context...)) {
          if (ABSL_PREDICT_FALSE(!status.ok())) {
            return serialized_message_reader_internal::
                AnnotateWithSourceAndFieldNumber(std::move(status), src,
                                                 field_number);
          }
        }
        continue;
      }
      case WireType::kFixed64: {
        uint64_t value;
        if (ABSL_PREDICT_FALSE(!ReadLittleEndian<uint64_t>(src, value))) {
          return serialized_message_reader_internal::ReadFixed64Error(
              src, field_number);
        }
        absl::Status status;
        if (HandleFixed64Field(field_number, value, status, context...)) {
          if (ABSL_PREDICT_FALSE(!status.ok())) {
            return serialized_message_reader_internal::
                AnnotateWithSourceAndFieldNumber(std::move(status), src,
                                                 field_number);
          }
        }
        continue;
      }
      case WireType::kLengthDelimited: {
        uint32_t length;
        if (ABSL_PREDICT_FALSE(
                !ReadVarint32(src, length) ||
                length > uint32_t{std::numeric_limits<int32_t>::max()})) {
          return serialized_message_reader_internal::
              ReadLengthDelimitedLengthError(src, field_number);
        }
        if constexpr (std::disjunction_v<
                          serialized_message_reader_internal::
                              IsFieldHandlerForLengthDelimited<
                                  FieldHandlers, Context...>...>) {
          static_assert(
              std::is_same_v<ReaderType, LimitingReaderBase>,
              "If there are any field handlers for length-delimited fields, "
              "ReadInternal() must be called with LimitingReaderBase");
          if (ABSL_PREDICT_FALSE(length > src.max_length())) {
            return serialized_message_reader_internal::NotEnoughError(
                src, field_number, length);
          }
          const Position end_pos = src.pos() + size_t{length};
          absl::Status status;
          if (HandleLengthDelimitedFieldFromReader(
                  field_number, src, size_t{length}, status, context...)) {
            if (ABSL_PREDICT_FALSE(!status.ok())) {
              return serialized_message_reader_internal::
                  AnnotateWithFieldNumber(std::move(status), field_number);
            }
            RIEGELI_ASSERT_EQ(src.pos(), end_pos)
                << "A field handler of a length-delimited field "
                   "must read to the end of the ReaderSpan<> or fail; "
                   "the value of field "
                << field_number << " at position " << (end_pos - size_t{length})
                << " has length " << size_t{length} << " but length "
                << static_cast<int64_t>(src.pos() - (end_pos - size_t{length}))
                << " has been read; "
                   "consider using SkipLengthDelimitedFromReader()";
            continue;
          }
        }
        if (ABSL_PREDICT_FALSE(!src.Skip(size_t{length}))) {
          return serialized_message_reader_internal::
              ReadLengthDelimitedValueError(src, field_number);
        }
        continue;
      }
      case WireType::kStartGroup: {
        absl::Status status;
        if (HandleStartGroupField(field_number, status, context...)) {
          if (ABSL_PREDICT_FALSE(!status.ok())) {
            return serialized_message_reader_internal::
                AnnotateWithSourceAndFieldNumber(std::move(status), src,
                                                 field_number);
          }
        }
        continue;
      }
      case WireType::kEndGroup: {
        absl::Status status;
        if (HandleEndGroupField(field_number, status, context...)) {
          if (ABSL_PREDICT_FALSE(!status.ok())) {
            return serialized_message_reader_internal::
                AnnotateWithSourceAndFieldNumber(std::move(status), src,
                                                 field_number);
          }
        }
        continue;
      }
      case WireType::kInvalid6:
      case WireType::kInvalid7:
        return serialized_message_reader_internal::InvalidWireTypeError(tag);
    }
    RIEGELI_ASSUME_UNREACHABLE()
        << "Impossible wire type: " << static_cast<int>(GetTagWireType(tag));
  }
  if (ABSL_PREDICT_FALSE(src.available() > 0)) {
    return serialized_message_reader_internal::ReadTagError(src);
  }
  return absl::OkStatus();
}

template <typename... FieldHandlers, typename... Context>
inline absl::Status SerializedMessageReaderType<
    std::tuple<FieldHandlers...>,
    Context...>::ReadMessageFromCord(absl::Cord::CharIterator& src,
                                     size_t available,
                                     Context&... context) const {
  const size_t limit = CordIteratorSpan::Remaining(src) - available;
  std::string scratch;
  uint32_t tag;
  while (ReadVarint32(src, CordIteratorSpan::Remaining(src) - limit, tag)) {
    const int field_number = GetTagFieldNumber(tag);
    switch (GetTagWireType(tag)) {
      case WireType::kVarint: {
        if constexpr (std::disjunction_v<
                          serialized_message_reader_internal::
                              IsFieldHandlerForVarint<FieldHandlers,
                                                      Context...>...>) {
          uint64_t value;
          if (ABSL_PREDICT_FALSE(!ReadVarint64(
                  src, CordIteratorSpan::Remaining(src) - limit, value))) {
            return serialized_message_reader_internal::ReadVarintError(
                field_number);
          }
          absl::Status status;
          if (HandleVarintField(field_number, value, status, context...)) {
            if (ABSL_PREDICT_FALSE(!status.ok())) {
              return serialized_message_reader_internal::
                  AnnotateWithFieldNumber(std::move(status), field_number);
            }
          }
        } else {
          // The value is not needed. Use more efficient `SkipVarint64()`
          // instead of `ReadVarint64()`.
          if (ABSL_PREDICT_FALSE(!SkipVarint64(
                  src, CordIteratorSpan::Remaining(src) - limit))) {
            return serialized_message_reader_internal::ReadVarintError(
                field_number);
          }
        }
        continue;
      }
      case WireType::kFixed32: {
        if (ABSL_PREDICT_FALSE(CordIteratorSpan::Remaining(src) - limit <
                               sizeof(uint32_t))) {
          return serialized_message_reader_internal::ReadFixed32Error(
              field_number);
        }
        if constexpr (std::disjunction_v<
                          serialized_message_reader_internal::
                              IsFieldHandlerForFixed32<FieldHandlers,
                                                       Context...>...>) {
          char buffer[sizeof(uint32_t)];
          CordIteratorSpan::Read(src, sizeof(uint32_t), buffer);
          const uint32_t value = ReadLittleEndian<uint32_t>(buffer);
          absl::Status status;
          if (HandleFixed32Field(field_number, value, status, context...)) {
            if (ABSL_PREDICT_FALSE(!status.ok())) {
              return serialized_message_reader_internal::
                  AnnotateWithFieldNumber(std::move(status), field_number);
            }
          }
        } else {
          // The value is not needed. Use more efficient `Cord::Advance()`
          // instead of `CordIteratorSpan::Read()`.
          absl::Cord::Advance(&src, sizeof(uint32_t));
        }
        continue;
      }
      case WireType::kFixed64: {
        if (ABSL_PREDICT_FALSE(CordIteratorSpan::Remaining(src) - limit <
                               sizeof(uint64_t))) {
          return serialized_message_reader_internal::ReadFixed64Error(
              field_number);
        }
        if constexpr (std::disjunction_v<
                          serialized_message_reader_internal::
                              IsFieldHandlerForFixed64<FieldHandlers,
                                                       Context...>...>) {
          char buffer[sizeof(uint64_t)];
          CordIteratorSpan::Read(src, sizeof(uint64_t), buffer);
          const uint64_t value = ReadLittleEndian<uint64_t>(buffer);
          absl::Status status;
          if (HandleFixed64Field(field_number, value, status, context...)) {
            if (ABSL_PREDICT_FALSE(!status.ok())) {
              return serialized_message_reader_internal::
                  AnnotateWithFieldNumber(std::move(status), field_number);
            }
          }
        } else {
          // The value is not needed. Use more efficient `Cord::Advance()`
          // instead of `CordIteratorSpan::Read()`.
          absl::Cord::Advance(&src, sizeof(uint64_t));
        }
        continue;
      }
      case WireType::kLengthDelimited: {
        uint32_t length;
        if (ABSL_PREDICT_FALSE(
                !ReadVarint32(src, CordIteratorSpan::Remaining(src) - limit,
                              length) ||
                length > uint32_t{std::numeric_limits<int32_t>::max()})) {
          return serialized_message_reader_internal::
              ReadLengthDelimitedLengthError(field_number);
        }
        const size_t available_for_value =
            CordIteratorSpan::Remaining(src) - limit;
        if (ABSL_PREDICT_FALSE(length > available_for_value)) {
          return serialized_message_reader_internal::NotEnoughError(
              field_number, length, available_for_value);
        }
        if constexpr (std::disjunction_v<
                          serialized_message_reader_internal::
                              IsFieldHandlerForLengthDelimited<
                                  FieldHandlers, Context...>...>) {
          absl::Status status;
          if (HandleLengthDelimitedFieldFromCord(field_number, src,
                                                 size_t{length}, scratch,
                                                 status, context...)) {
            if (ABSL_PREDICT_FALSE(!status.ok())) {
              return serialized_message_reader_internal::
                  AnnotateWithFieldNumber(std::move(status), field_number);
            }
            RIEGELI_ASSERT_EQ(CordIteratorSpan::Remaining(src),
                              available_for_value + limit - size_t{length})
                << "A field handler of a length-delimited field "
                   "must read to the end of the CordIteratorSpan or fail; "
                   "the value of field "
                << field_number << " has length " << size_t{length}
                << " but length "
                << (available_for_value + limit -
                    CordIteratorSpan::Remaining(src))
                << " has been read";
            continue;
          }
        }
        absl::Cord::Advance(&src, size_t{length});
        continue;
      }
      case WireType::kStartGroup: {
        absl::Status status;
        if (HandleStartGroupField(field_number, status, context...)) {
          if (ABSL_PREDICT_FALSE(!status.ok())) {
            return serialized_message_reader_internal::AnnotateWithFieldNumber(
                std::move(status), field_number);
          }
        }
        continue;
      }
      case WireType::kEndGroup: {
        absl::Status status;
        if (HandleEndGroupField(field_number, status, context...)) {
          if (ABSL_PREDICT_FALSE(!status.ok())) {
            return serialized_message_reader_internal::AnnotateWithFieldNumber(
                std::move(status), field_number);
          }
        }
        continue;
      }
      case WireType::kInvalid6:
      case WireType::kInvalid7:
        return serialized_message_reader_internal::InvalidWireTypeError(tag);
    }
    RIEGELI_ASSUME_UNREACHABLE()
        << "Impossible wire type: " << static_cast<int>(GetTagWireType(tag));
  }
  if (ABSL_PREDICT_FALSE(CordIteratorSpan::Remaining(src) > limit)) {
    return serialized_message_reader_internal::ReadTagError();
  }
  return absl::OkStatus();
}

template <typename... FieldHandlers, typename... Context>
inline absl::Status SerializedMessageReaderType<
    std::tuple<FieldHandlers...>,
    Context...>::ReadMessageFromString(absl::string_view src,
                                       Context&... context) const {
  const char* absl_nullable cursor = src.data();
  const char* const absl_nullable limit = src.data() + src.size();
  uint32_t tag;
  while (const size_t tag_length =
             ReadVarint32(cursor, PtrDistance(cursor, limit), tag)) {
    RIEGELI_ASSERT(cursor != nullptr);
    RIEGELI_ASSERT(limit != nullptr);
    cursor += tag_length;
    const int field_number = GetTagFieldNumber(tag);
    switch (GetTagWireType(tag)) {
      case WireType::kVarint: {
        if constexpr (std::disjunction_v<
                          serialized_message_reader_internal::
                              IsFieldHandlerForVarint<FieldHandlers,
                                                      Context...>...>) {
          uint64_t value;
          const size_t length_of_value =
              ReadVarint64(cursor, PtrDistance(cursor, limit), value);
          if (ABSL_PREDICT_FALSE(length_of_value == 0)) {
            return serialized_message_reader_internal::ReadVarintError(
                field_number);
          }
          cursor += length_of_value;
          absl::Status status;
          if (HandleVarintField(field_number, value, status, context...)) {
            if (ABSL_PREDICT_FALSE(!status.ok())) {
              return serialized_message_reader_internal::
                  AnnotateWithFieldNumber(std::move(status), field_number);
            }
          }
        } else {
          // The value is not needed. Use more efficient `SkipVarint64()`
          // instead of `ReadVarint64()`.
          const size_t length_of_value =
              SkipVarint64(cursor, PtrDistance(cursor, limit));
          if (ABSL_PREDICT_FALSE(length_of_value == 0)) {
            return serialized_message_reader_internal::ReadVarintError(
                field_number);
          }
          cursor += length_of_value;
        }
        continue;
      }
      case WireType::kFixed32: {
        if (ABSL_PREDICT_FALSE(PtrDistance(cursor, limit) < sizeof(uint32_t))) {
          return serialized_message_reader_internal::ReadFixed32Error(
              field_number);
        }
        const uint32_t value = ReadLittleEndian<uint32_t>(cursor);
        cursor += sizeof(uint32_t);
        absl::Status status;
        if (HandleFixed32Field(field_number, value, status, context...)) {
          if (ABSL_PREDICT_FALSE(!status.ok())) {
            return serialized_message_reader_internal::AnnotateWithFieldNumber(
                std::move(status), field_number);
          }
        }
        continue;
      }
      case WireType::kFixed64: {
        if (ABSL_PREDICT_FALSE(PtrDistance(cursor, limit) < sizeof(uint64_t))) {
          return serialized_message_reader_internal::ReadFixed64Error(
              field_number);
        }
        const uint64_t value = ReadLittleEndian<uint64_t>(cursor);
        cursor += sizeof(uint64_t);
        absl::Status status;
        if (HandleFixed64Field(field_number, value, status, context...)) {
          if (ABSL_PREDICT_FALSE(!status.ok())) {
            return serialized_message_reader_internal::AnnotateWithFieldNumber(
                std::move(status), field_number);
          }
        }
        continue;
      }
      case WireType::kLengthDelimited: {
        uint32_t length;
        const size_t length_of_length =
            ReadVarint32(cursor, PtrDistance(cursor, limit), length);
        if (ABSL_PREDICT_FALSE(
                length_of_length == 0 ||
                length > uint32_t{std::numeric_limits<int32_t>::max()})) {
          return serialized_message_reader_internal::
              ReadLengthDelimitedLengthError(field_number);
        }
        cursor += length_of_length;
        const size_t available_for_value = PtrDistance(cursor, limit);
        if (ABSL_PREDICT_FALSE(length > available_for_value)) {
          return serialized_message_reader_internal::NotEnoughError(
              field_number, length, available_for_value);
        }
        absl::Status status;
        if (HandleLengthDelimitedFieldFromString(
                field_number, cursor, size_t{length}, status, context...)) {
          if (ABSL_PREDICT_FALSE(!status.ok())) {
            return serialized_message_reader_internal::AnnotateWithFieldNumber(
                std::move(status), field_number);
          }
        }
        cursor += size_t{length};
        continue;
      }
      case WireType::kStartGroup: {
        absl::Status status;
        if (HandleStartGroupField(field_number, status, context...)) {
          if (ABSL_PREDICT_FALSE(!status.ok())) {
            return serialized_message_reader_internal::AnnotateWithFieldNumber(
                std::move(status), field_number);
          }
        }
        continue;
      }
      case WireType::kEndGroup: {
        absl::Status status;
        if (HandleEndGroupField(field_number, status, context...)) {
          if (ABSL_PREDICT_FALSE(!status.ok())) {
            return serialized_message_reader_internal::AnnotateWithFieldNumber(
                std::move(status), field_number);
          }
        }
        continue;
      }
      case WireType::kInvalid6:
      case WireType::kInvalid7:
        return serialized_message_reader_internal::InvalidWireTypeError(tag);
    }
    RIEGELI_ASSUME_UNREACHABLE()
        << "Impossible wire type: " << static_cast<int>(GetTagWireType(tag));
  }
  if (ABSL_PREDICT_FALSE(cursor < limit)) {
    return serialized_message_reader_internal::ReadTagError();
  }
  return absl::OkStatus();
}

template <typename... FieldHandlers, typename... Context>
template <
    typename Src,
    std::enable_if_t<TargetRefSupportsDependency<Reader*, Src>::value, int>>
absl::Status SerializedMessageReaderType<
    std::tuple<FieldHandlers...>, Context...>::ReadMessage(Src&& src,
                                                           Context&... context)
    const {
  DependencyRef<Reader*, Src> src_dep(std::forward<Src>(src));
  if (src_dep.IsOwning()) src_dep->SetReadAllHint(true);

  absl::Status status;
  if constexpr (std::disjunction_v<serialized_message_reader_internal::
                                       IsFieldHandlerForLengthDelimited<
                                           FieldHandlers, Context...>...>) {
    if constexpr (std::is_convertible_v<
                      typename DependencyRef<Reader*, Src>::Subhandle,
                      LimitingReaderBase*>) {
      status = ReadMessageFromReader<LimitingReaderBase>(*src_dep, context...);
    } else {
      LimitingReaderBase::Options options;
      if (src_dep->SupportsSize()) {
        const std::optional<Position> size = src_dep->Size();
        if (ABSL_PREDICT_TRUE(size != std::nullopt)) options.set_max_pos(*size);
      }
      LimitingReader<> limiting_reader(src_dep.get(), options);
      status = ReadMessageFromReader<LimitingReaderBase>(limiting_reader,
                                                         context...);
      if (ABSL_PREDICT_FALSE(!limiting_reader.Close())) {
        status.Update(limiting_reader.status());
      }
    }
  } else {
    status = ReadMessageFromReader<Reader>(*src_dep, context...);
  }

  if (src_dep.IsOwning()) {
    if (ABSL_PREDICT_TRUE(status.ok())) src_dep->VerifyEnd();
    if (ABSL_PREDICT_FALSE(!src_dep->Close())) status.Update(src_dep->status());
  }
  return status;
}

template <typename... FieldHandlers, typename... Context>
absl::Status SerializedMessageReaderType<
    std::tuple<FieldHandlers...>, Context...>::ReadMessage(BytesRef src,
                                                           Context&... context)
    const {
  if constexpr (std::conjunction_v<serialized_message_reader_internal::
                                       IsFieldHandlerFromString<
                                           FieldHandlers, Context...>...>) {
    return ReadMessageFromString(src, context...);
  } else {
    return ReadMessage(StringReader(src), context...);
  }
}

template <typename... FieldHandlers, typename... Context>
absl::Status SerializedMessageReaderType<
    std::tuple<FieldHandlers...>, Context...>::ReadMessage(const Chain& src,
                                                           Context&... context)
    const {
  return ReadMessage(ChainReader(&src), context...);
}

template <typename... FieldHandlers, typename... Context>
absl::Status SerializedMessageReaderType<
    std::tuple<FieldHandlers...>,
    Context...>::ReadMessage(const absl::Cord& src, Context&... context) const {
  if constexpr (std::conjunction_v<std::disjunction<
                    serialized_message_reader_internal::IsFieldHandlerFromCord<
                        FieldHandlers, Context...>,
                    serialized_message_reader_internal::
                        IsFieldHandlerFromString<FieldHandlers,
                                                 Context...>>...>) {
    absl::Cord::CharIterator iter = src.char_begin();
    return ReadMessageFromCord(iter, CordIteratorSpan::Remaining(iter),
                               context...);
  } else {
    return ReadMessage(CordReader(&src), context...);
  }
}

template <typename... FieldHandlers, typename... Context>
absl::Status SerializedMessageReaderType<
    std::tuple<FieldHandlers...>, Context...>::ReadMessage(CordIteratorSpan src,
                                                           Context&... context)
    const {
  if constexpr (std::conjunction_v<std::disjunction<
                    serialized_message_reader_internal::IsFieldHandlerFromCord<
                        FieldHandlers, Context...>,
                    serialized_message_reader_internal::
                        IsFieldHandlerFromString<FieldHandlers,
                                                 Context...>>...>) {
    return ReadMessageFromCord(src.iterator(), src.length(), context...);
  } else {
    return ReadMessage(CordReader(std::move(src)), context...);
  }
}

template <typename Action,
          std::enable_if_t<std::is_invocable_r_v<absl::Status, Action>, int>>
inline absl::Status SkipLengthDelimitedFromReader(const ReaderSpan<>& value,
                                                  Action&& action) {
  LimitingReaderBase& reader = value.reader();
  const Position end_pos = reader.pos() + value.length();
  if (absl::Status status = std::forward<Action>(action)();
      ABSL_PREDICT_FALSE(!status.ok())) {
    return status;
  }
  if (ABSL_PREDICT_FALSE(!reader.Seek(end_pos))) {
    return serialized_message_reader_internal::ReadLengthDelimitedValueError(
        reader);
  }
  return absl::OkStatus();
}

inline absl::Status SkipLengthDelimitedFromReader(ReaderSpan<> value) {
  if (ABSL_PREDICT_FALSE(!value.reader().Skip(value.length()))) {
    return serialized_message_reader_internal::ReadLengthDelimitedValueError(
        value.reader());
  }
  return absl::OkStatus();
}

template <typename Action,
          std::enable_if_t<std::is_invocable_r_v<absl::Status, Action>, int>>
inline absl::Status SkipLengthDelimitedFromCord(const CordIteratorSpan& value,
                                                Action&& action) {
  absl::Cord::CharIterator& iterator = value.iterator();
  const size_t limit = CordIteratorSpan::Remaining(iterator) - value.length();
  if (absl::Status status = std::forward<Action>(action)();
      ABSL_PREDICT_FALSE(!status.ok())) {
    return status;
  }
  RIEGELI_ASSERT_GE(CordIteratorSpan::Remaining(iterator), limit)
      << "Length-delimited field handler action read past field contents";
  if (ABSL_PREDICT_FALSE(CordIteratorSpan::Remaining(iterator) != limit)) {
    absl::Cord::Advance(&iterator,
                        CordIteratorSpan::Remaining(iterator) - limit);
  }
  return absl::OkStatus();
}

inline absl::Status SkipLengthDelimitedFromCord(CordIteratorSpan value) {
  absl::Cord::Advance(&value.iterator(), value.length());
  return absl::OkStatus();
}

}  // namespace riegeli

#endif  // RIEGELI_MESSAGES_SERIALIZED_MESSAGE_READER2_H_
