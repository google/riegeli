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
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/bytes_ref.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/type_traits.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/limiting_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/endian/endian_reading.h"
#include "riegeli/messages/message_wire_format.h"
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
//  * Processing messages with so many elements of toplevel repeated fields that
//    the total message size exceeds 2GiB. This is not a great idea in itself,
//    because such messages cannot be processed using native proto parsing and
//    serialization.
//
// Fields to be handled are specified by field handlers. For each field present
// in the serialized message, the first handler which accepts the field is
// invoked. If no handler accepts the field, the field is skipped.
//
// The serialized message is read from a `Reader` or a string.

// Field handlers
// --------------
//
// Field handlers handle fields with particular field numbers and wire types
// (varint, fixed32, fixed64, length-delimited, start-group, or end-group)
// by invoking specific actions.
//
// A family of functions returning field handlers is defined in namespace
// `riegeli::field_handlers`.
//
// Field handlers stored in a single `SerializedMessageReader2` are usually
// conceptually associated with a single message type.
//
// If a field handler returns a failed `absl::Status`, `Read()` is cancelled
// and propagates the status, annotated by the `Reader` and/or with the field
// number. Annotations are skipped for `absl::CancelledError()` to make it more
// efficient to cancel a handler when cancellation is likely.

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
// Field handlers have a `static constexpr int kFieldNumber` member variable
// with a positive field number.
//
// Field handlers provide at least one of the following member functions, with
// parameters followed by `Context&...`:
// ```
//   absl::Status HandleVarint(uint64_t value) const;
//
//   absl::Status HandleFixed32(uint32_t value) const;
//
//   absl::Status HandleFixed64(uint64_t value) const;
//
//   // Overload for a `Reader` source.
//   //
//   // `HandleLengthDelimited()` must read to the end of the `ReaderSpan<>`
//   // or fail. `SkipLengthDelimited()` can be used to skip the whole field.
//   absl::Status HandleLengthDelimited(ReaderSpan<> value) const;
//
//   // Overload for a string source.
//   //
//   // The `absl::string_view` is guaranteed to be a substring of the original
//   // string.
//   absl::Status HandleLengthDelimited(absl::string_view value) const;
//
//   absl::Status HandleStartGroup() const;
//
//   absl::Status HandleEndGroup() const;
// ```

// `IsFieldHandler<T, Context...>::value` is `true` if `T` is a valid argument
// type for `SerializedMessageReader2<Context...>`.
template <typename T, typename... Context>
struct IsFieldHandler;

// `IsFieldHandlerFromReader<T, Context...>::value` is `true` if `T` is a valid
// argument type for `SerializedMessageReader2<Context...>` for use with a
// `Reader` source.
template <typename T, typename... Context>
struct IsFieldHandlerFromReader;

// `IsFieldHandlerFromString<T, Context...>::value` is `true` if `T` is a valid
// argument type for `SerializedMessageReader2<Context...>` for use with a
// string source.
template <typename T, typename... Context>
struct IsFieldHandlerFromString;

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
  // Constructs a `SerializedMessageReaderType` from field handlers.
  template <
      typename... FieldHandlerInitializers,
      std::enable_if_t<
          std::conjunction_v<NotSameRef<SerializedMessageReaderType,
                                        FieldHandlerInitializers&&...>,
                             std::is_constructible<
                                 FieldHandlers, FieldHandlerInitializers&&>...>,
          int> = 0>
  explicit constexpr SerializedMessageReaderType(
      FieldHandlerInitializers&&... field_handlers)
      : field_handlers_(
            std::forward<FieldHandlerInitializers>(field_handlers)...) {}

  // Reads a serialized message from a `Reader`, letting field handlers process
  // the fields.
  //
  // If any field handler handles length-delimited fields, then the root
  // `Reader` will be wrapped in a `LimitingReader` unless it is already
  // statically known to be a `LimitingReaderBase`. This allows field handlers
  // to use `ScopedLimiter` for reading the value of a length-delimited field.
  //
  // If `Reader::SupportsSize()` and it was wrapped in a `LimitingReader`, then
  // the `LimitingReader` is initially limited to the whole message. This helps
  // parsing untrusted data: if the size of the message is bounded, then claimed
  // lengths of length-delimited fields are bounded as well, and thus it is safe
  // to e.g. pass such a length to `Reader::Read()`.
  template <typename Src
#if !__cpp_concepts
            ,
            std::enable_if_t<
                std::conjunction_v<
                    TargetRefSupportsDependency<Reader*, Src>,
                    IsFieldHandlerFromReader<FieldHandlers, Context...>...>,
                int> = 0
#endif
            >
#if __cpp_concepts
  // For conjunctions, `requires` gives better error messages than
  // `std::enable_if_t`, indicating the relevant argument.
    requires TargetRefSupportsDependency<Reader*, Src>::value &&
             (IsFieldHandlerFromReader<FieldHandlers, Context...>::value && ...)
#endif
  absl::Status Read(Src&& src, Context&... context) const;

// Reads a serialized message from a string, letting field handlers process
// the fields.
//
// This is more efficient than reading from a `StringReader`. Some field
// handlers for length-delimited fields need a different implementation though.
#if !__cpp_concepts
  template <
      typename DependentVoid = void,
      std::enable_if_t<std::conjunction_v<std::is_void<DependentVoid>,
                                          IsFieldHandlerFromString<
                                              FieldHandlers, Context...>...>,
                       int> = 0>
#endif
  absl::Status Read(BytesRef src, Context&... context) const
#if __cpp_concepts
      // For conjunctions, `requires` gives better error messages than
      // `std::enable_if_t`, indicating the relevant argument.
    requires(IsFieldHandlerFromString<FieldHandlers, Context...>::value && ...)
#endif
  ;

 private:
  template <typename ReaderType>
  absl::Status ReadInternal(ReaderType& src, Context&... context) const;

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
//   absl::Status status = message_reader.Read(src, context...);
// ```
//
// `Context` types must be specified explicitly for `SerializedMessageReader2`.
// Field handlers and their actions must accept compatible `Context&...`
// parameters.
template <typename... Context, typename... FieldHandlerInitializers
#if !__cpp_concepts
          ,
          std::enable_if_t<
              std::conjunction_v<IsFieldHandler<
                  std::decay_t<FieldHandlerInitializers>, Context...>...>,
              int> = 0
#endif
          >
#if __cpp_concepts
// For conjunctions, `requires` gives better error messages than
// `std::enable_if_t`, indicating the relevant argument.
  requires(IsFieldHandler<std::decay_t<FieldHandlerInitializers>,
                          Context...>::value &&
           ...)
#endif
constexpr SerializedMessageReaderType<
    std::tuple<std::decay_t<FieldHandlerInitializers>...>, Context...>
SerializedMessageReader2(FieldHandlerInitializers&&... field_handlers) {
  return SerializedMessageReaderType<
      std::tuple<std::decay_t<FieldHandlerInitializers>...>, Context...>(
      std::forward<FieldHandlerInitializers>(field_handlers)...);
}

// When handling a length-delimited field available as `ReaderSpan<>`,
// a field handler must read to the end of the `ReaderSpan<>` or fail.
//
// When handling a length-delimited field available as `absl::string_view`,
// a field handler does not have such a requirement.
//
// If a field handler wants to skip a length-delimited field, possibly
// conditionally, then it can call `SkipLengthDelimited()` once to skip to
// the end of the `ReaderSpan<>` instead of reading it. This does nothing for
// `absl::string_view`.
//
// This is applicable only if the field is skipped as a whole. If a field
// handler reads the `ReaderSpan<>` partially and does not fail, then it can
// fulfill the requirement as follows:
// ```
//   const riegeli::Position end_pos = value.reader().pos() + value.length();
//   ...
//   value.reader().Seek(end_pos);
// ```
absl::Status SkipLengthDelimited(ReaderSpan<> value);
absl::Status SkipLengthDelimited(absl::string_view value);

// Implementation details follow.

namespace serialized_message_reader_internal {

template <typename T, typename Enable = void>
struct IsFieldHandlerWithFieldNumber : std::false_type {};

template <typename T>
struct IsFieldHandlerWithFieldNumber<
    T, std::enable_if_t<std::is_convertible_v<decltype(T::kFieldNumber), int>>>
    : std::true_type {};

template <typename T, typename Enable, typename... Context>
struct IsFieldHandlerForVarintImpl : std::false_type {};

template <typename T, typename... Context>
struct IsFieldHandlerForVarintImpl<
    T,
    std::enable_if_t<std::is_convertible_v<
        decltype(std::declval<const T&>().HandleVarint(
            std::declval<uint64_t>(), std::declval<Context&>()...)),
        absl::Status>>,
    Context...> : std::true_type {};

template <typename T, typename Enable, typename... Context>
struct IsFieldHandlerForFixed32Impl : std::false_type {};

template <typename T, typename... Context>
struct IsFieldHandlerForFixed32Impl<
    T,
    std::enable_if_t<std::is_convertible_v<
        decltype(std::declval<const T&>().HandleFixed32(
            std::declval<uint32_t>(), std::declval<Context&>()...)),
        absl::Status>>,
    Context...> : std::true_type {};

template <typename T, typename Enable, typename... Context>
struct IsFieldHandlerForFixed64Impl : std::false_type {};

template <typename T, typename... Context>
struct IsFieldHandlerForFixed64Impl<
    T,
    std::enable_if_t<std::is_convertible_v<
        decltype(std::declval<const T&>().HandleFixed64(
            std::declval<uint64_t>(), std::declval<Context&>()...)),
        absl::Status>>,
    Context...> : std::true_type {};

template <typename T, typename Enable, typename... Context>
struct IsFieldHandlerForLengthDelimitedFromReaderImpl : std::false_type {};

template <typename T, typename... Context>
struct IsFieldHandlerForLengthDelimitedFromReaderImpl<
    T,
    std::enable_if_t<std::is_convertible_v<
        decltype(std::declval<const T&>().HandleLengthDelimited(
            std::declval<ReaderSpan<>>(), std::declval<Context&>()...)),
        absl::Status>>,
    Context...> : std::true_type {};

template <typename T, typename Enable, typename... Context>
struct IsFieldHandlerForLengthDelimitedFromStringImpl : std::false_type {};

template <typename T, typename... Context>
struct IsFieldHandlerForLengthDelimitedFromStringImpl<
    T,
    std::enable_if_t<std::is_convertible_v<
        decltype(std::declval<const T&>().HandleLengthDelimited(
            std::declval<absl::string_view>(), std::declval<Context&>()...)),
        absl::Status>>,
    Context...> : std::true_type {};

template <typename T, typename Enable, typename... Context>
struct IsFieldHandlerForStartGroupImpl : std::false_type {};

template <typename T, typename... Context>
struct IsFieldHandlerForStartGroupImpl<
    T,
    std::enable_if_t<std::is_convertible_v<
        decltype(std::declval<const T&>().HandleStartGroup(
            std::declval<Context&>()...)),
        absl::Status>>,
    Context...> : std::true_type {};

template <typename T, typename Enable, typename... Context>
struct IsFieldHandlerForEndGroupImpl : std::false_type {};

template <typename T, typename... Context>
struct IsFieldHandlerForEndGroupImpl<
    T,
    std::enable_if_t<
        std::is_convertible_v<decltype(std::declval<const T&>().HandleEndGroup(
                                  std::declval<Context&>()...)),
                              absl::Status>>,
    Context...> : std::true_type {};

template <typename T, typename... Context>
using IsFieldHandlerForVarint =
    IsFieldHandlerForVarintImpl<T, void, Context...>;

template <typename T, typename... Context>
using IsFieldHandlerForFixed32 =
    IsFieldHandlerForFixed32Impl<T, void, Context...>;

template <typename T, typename... Context>
using IsFieldHandlerForFixed64 =
    IsFieldHandlerForFixed64Impl<T, void, Context...>;

template <typename T, typename... Context>
using IsFieldHandlerForLengthDelimitedFromReader =
    IsFieldHandlerForLengthDelimitedFromReaderImpl<T, void, Context...>;

template <typename T, typename... Context>
using IsFieldHandlerForLengthDelimitedFromString =
    IsFieldHandlerForLengthDelimitedFromStringImpl<T, void, Context...>;

template <typename T, typename... Context>
using IsFieldHandlerForStartGroup =
    IsFieldHandlerForStartGroupImpl<T, void, Context...>;

template <typename T, typename... Context>
using IsFieldHandlerForEndGroup =
    IsFieldHandlerForEndGroupImpl<T, void, Context...>;

ABSL_ATTRIBUTE_COLD absl::Status AnnotateWithFieldNumberSlow(
    absl::Status status, int field_number);
ABSL_ATTRIBUTE_COLD absl::Status AnnotateWithSourceAndFieldNumberSlow(
    absl::Status status, Reader& src, int field_number);

inline absl::Status AnnotateWithFieldNumber(absl::Status status,
                                            int field_number) {
  // Comparison against `absl::CancelledError()` is a fast path of
  // `absl::IsCancelled()`.
  if (ABSL_PREDICT_FALSE(status != absl::CancelledError())) {
    status = AnnotateWithFieldNumberSlow(std::move(status), field_number);
  }
  return status;
}

inline absl::Status AnnotateWithSourceAndFieldNumber(absl::Status status,
                                                     Reader& src,
                                                     int field_number) {
  // Comparison against `absl::CancelledError()` is a fast path of
  // `absl::IsCancelled()`.
  if (ABSL_PREDICT_FALSE(status != absl::CancelledError())) {
    status = AnnotateWithSourceAndFieldNumberSlow(std::move(status), src,
                                                  field_number);
  }
  return status;
}

ABSL_ATTRIBUTE_COLD absl::Status ReadTagError(Reader& src);
ABSL_ATTRIBUTE_COLD absl::Status ReadTagError();
ABSL_ATTRIBUTE_COLD absl::Status ReadVarintError(Reader& src, int field_number);
ABSL_ATTRIBUTE_COLD absl::Status ReadVarintError(int field_number);
ABSL_ATTRIBUTE_COLD absl::Status ReadFixed32Error(Reader& src,
                                                  int field_number);
ABSL_ATTRIBUTE_COLD absl::Status ReadFixed32Error(int field_number);
ABSL_ATTRIBUTE_COLD absl::Status ReadFixed64Error(Reader& src,
                                                  int field_number);
ABSL_ATTRIBUTE_COLD absl::Status ReadFixed64Error(int field_number);
ABSL_ATTRIBUTE_COLD absl::Status NotEnoughError(LimitingReaderBase& src,
                                                int field_number,
                                                uint32_t expected_length);
ABSL_ATTRIBUTE_COLD absl::Status NotEnoughError(int field_number,
                                                uint32_t expected_length,
                                                size_t available);
ABSL_ATTRIBUTE_COLD absl::Status ReadLengthDelimitedLengthError(
    Reader& src, int field_number);
ABSL_ATTRIBUTE_COLD absl::Status ReadLengthDelimitedLengthError(
    int field_number);
ABSL_ATTRIBUTE_COLD absl::Status ReadLengthDelimitedValueError(
    Reader& src, int field_number);
ABSL_ATTRIBUTE_COLD absl::Status ReadLengthDelimitedValueError(Reader& src);
ABSL_ATTRIBUTE_COLD absl::Status InvalidWireTypeError(Reader& src,
                                                      uint32_t tag);
ABSL_ATTRIBUTE_COLD absl::Status InvalidWireTypeError(uint32_t tag);

template <typename FieldHandler, typename... Context>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool ReadVarintField(
    int field_number, uint64_t value, absl::Status& status,
    const FieldHandler& field_handler, Context&... context) {
  if constexpr (IsFieldHandlerForVarint<FieldHandler, Context...>::value) {
    if (field_number == FieldHandler::kFieldNumber) {
      status = field_handler.HandleVarint(value, context...);
      return true;
    }
  }
  return false;
}

template <typename FieldHandler, typename... Context>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool ReadFixed32Field(
    int field_number, uint32_t value, absl::Status& status,
    const FieldHandler& field_handler, Context&... context) {
  if constexpr (IsFieldHandlerForFixed32<FieldHandler, Context...>::value) {
    if (field_number == FieldHandler::kFieldNumber) {
      status = field_handler.HandleFixed32(value, context...);
      return true;
    }
  }
  return false;
}

template <typename FieldHandler, typename... Context>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool ReadFixed64Field(
    int field_number, uint64_t value, absl::Status& status,
    const FieldHandler& field_handler, Context&... context) {
  if constexpr (IsFieldHandlerForFixed64<FieldHandler, Context...>::value) {
    if (field_number == FieldHandler::kFieldNumber) {
      status = field_handler.HandleFixed64(value, context...);
      return true;
    }
  }
  return false;
}

template <typename FieldHandler, typename... Context>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool ReadLengthDelimitedFieldFromReader(
    int field_number, LimitingReaderBase& src, size_t length,
    absl::Status& status, const FieldHandler& field_handler,
    Context&... context) {
  if constexpr (IsFieldHandlerForLengthDelimitedFromReader<FieldHandler,
                                                           Context...>::value) {
    if (field_number == FieldHandler::kFieldNumber) {
      status = field_handler.HandleLengthDelimited(ReaderSpan<>(&src, length),
                                                   context...);
      return true;
    }
  }
  return false;
}

template <typename FieldHandler, typename... Context>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool ReadLengthDelimitedFieldFromString(
    int field_number, const char* src, size_t length, absl::Status& status,
    const FieldHandler& field_handler, Context&... context) {
  if constexpr (IsFieldHandlerForLengthDelimitedFromString<FieldHandler,
                                                           Context...>::value) {
    if (field_number == FieldHandler::kFieldNumber) {
      status = field_handler.HandleLengthDelimited(
          absl::string_view(src, length), context...);
      return true;
    }
  }
  return false;
}

template <typename FieldHandler, typename... Context>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool ReadStartGroupField(
    int field_number, absl::Status& status, const FieldHandler& field_handler,
    Context&... context) {
  if constexpr (IsFieldHandlerForStartGroup<FieldHandler, Context...>::value) {
    if (field_number == FieldHandler::kFieldNumber) {
      status = field_handler.HandleStartGroup(context...);
      return true;
    }
  }
  return false;
}

template <typename FieldHandler, typename... Context>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool ReadEndGroupField(
    int field_number, absl::Status& status, const FieldHandler& field_handler,
    Context&... context) {
  if constexpr (IsFieldHandlerForEndGroup<FieldHandler, Context...>::value) {
    if (field_number == FieldHandler::kFieldNumber) {
      status = field_handler.HandleEndGroup(context...);
      return true;
    }
  }
  return false;
}

}  // namespace serialized_message_reader_internal

template <typename T, typename... Context>
struct IsFieldHandler
    : std::conjunction<
          serialized_message_reader_internal::IsFieldHandlerWithFieldNumber<T>,
          std::disjunction<
              serialized_message_reader_internal::IsFieldHandlerForVarint<
                  T, Context...>,
              serialized_message_reader_internal::IsFieldHandlerForFixed32<
                  T, Context...>,
              serialized_message_reader_internal::IsFieldHandlerForFixed64<
                  T, Context...>,
              std::disjunction<
                  serialized_message_reader_internal::
                      IsFieldHandlerForLengthDelimitedFromReader<T, Context...>,
                  serialized_message_reader_internal::
                      IsFieldHandlerForLengthDelimitedFromString<T,
                                                                 Context...>>,
              serialized_message_reader_internal::IsFieldHandlerForStartGroup<
                  T, Context...>,
              serialized_message_reader_internal::IsFieldHandlerForEndGroup<
                  T, Context...>>> {};

template <typename T, typename... Context>
struct IsFieldHandlerFromReader
    : std::conjunction<
          serialized_message_reader_internal::IsFieldHandlerWithFieldNumber<T>,
          std::disjunction<
              serialized_message_reader_internal::IsFieldHandlerForVarint<
                  T, Context...>,
              serialized_message_reader_internal::IsFieldHandlerForFixed32<
                  T, Context...>,
              serialized_message_reader_internal::IsFieldHandlerForFixed64<
                  T, Context...>,
              serialized_message_reader_internal::
                  IsFieldHandlerForLengthDelimitedFromReader<T, Context...>,
              serialized_message_reader_internal::IsFieldHandlerForStartGroup<
                  T, Context...>,
              serialized_message_reader_internal::IsFieldHandlerForEndGroup<
                  T, Context...>>,
          std::disjunction<
              serialized_message_reader_internal::
                  IsFieldHandlerForLengthDelimitedFromReader<T, Context...>,
              std::negation<serialized_message_reader_internal::
                                IsFieldHandlerForLengthDelimitedFromString<
                                    T, Context...>>>> {};

template <typename T, typename... Context>
struct IsFieldHandlerFromString
    : std::conjunction<
          serialized_message_reader_internal::IsFieldHandlerWithFieldNumber<T>,
          std::disjunction<
              serialized_message_reader_internal::IsFieldHandlerForVarint<
                  T, Context...>,
              serialized_message_reader_internal::IsFieldHandlerForFixed32<
                  T, Context...>,
              serialized_message_reader_internal::IsFieldHandlerForFixed64<
                  T, Context...>,
              serialized_message_reader_internal::
                  IsFieldHandlerForLengthDelimitedFromString<T, Context...>,
              serialized_message_reader_internal::IsFieldHandlerForStartGroup<
                  T, Context...>,
              serialized_message_reader_internal::IsFieldHandlerForEndGroup<
                  T, Context...>>,
          std::disjunction<
              serialized_message_reader_internal::
                  IsFieldHandlerForLengthDelimitedFromString<T, Context...>,
              std::negation<serialized_message_reader_internal::
                                IsFieldHandlerForLengthDelimitedFromReader<
                                    T, Context...>>>> {};

template <typename... FieldHandlers, typename... Context>
template <typename Src
#if !__cpp_concepts
          ,
          std::enable_if_t<
              std::conjunction_v<
                  TargetRefSupportsDependency<Reader*, Src>,
                  IsFieldHandlerFromReader<FieldHandlers, Context...>...>,
              int>
#endif
          >
#if __cpp_concepts
  requires TargetRefSupportsDependency<Reader*, Src>::value &&
           (IsFieldHandlerFromReader<FieldHandlers, Context...>::value && ...)
#endif
absl::Status
SerializedMessageReaderType<std::tuple<FieldHandlers...>, Context...>::Read(
    Src&& src, Context&... context) const {
  DependencyRef<Reader*, Src> src_dep(std::forward<Src>(src));
  if (src_dep.IsOwning()) src_dep->SetReadAllHint(true);

  absl::Status status;
  if constexpr (std::disjunction_v<
                    serialized_message_reader_internal::
                        IsFieldHandlerForLengthDelimitedFromReader<
                            FieldHandlers, Context...>...>) {
    if constexpr (std::is_convertible_v<
                      typename DependencyRef<Reader*, Src>::Subhandle,
                      LimitingReaderBase*>) {
      status = ReadInternal<LimitingReaderBase>(*src_dep, context...);
    } else {
      LimitingReaderBase::Options options;
      if (src_dep->SupportsSize()) {
        const std::optional<Position> size = src_dep->Size();
        if (ABSL_PREDICT_TRUE(size != std::nullopt)) options.set_max_pos(*size);
      }
      LimitingReader<> limiting_reader(src_dep.get(), options);
      status = ReadInternal<LimitingReaderBase>(limiting_reader, context...);
      if (ABSL_PREDICT_FALSE(!limiting_reader.Close())) {
        status.Update(limiting_reader.status());
      }
    }
  } else {
    status = ReadInternal<Reader>(*src_dep, context...);
  }

  if (src_dep.IsOwning()) {
    if (ABSL_PREDICT_TRUE(status.ok())) src_dep->VerifyEnd();
    if (ABSL_PREDICT_FALSE(!src_dep->Close())) status.Update(src_dep->status());
  }
  return status;
}

template <typename... FieldHandlers, typename... Context>
template <typename ReaderType>
absl::Status SerializedMessageReaderType<
    std::tuple<FieldHandlers...>, Context...>::ReadInternal(ReaderType& src,
                                                            Context&... context)
    const {
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
          if (std::apply(
                  [&](const auto&... field_handlers) {
                    return (serialized_message_reader_internal::ReadVarintField(
                                field_number, value, status, field_handlers,
                                context...) ||
                            ...);
                  },
                  field_handlers_)) {
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
        if (ABSL_PREDICT_FALSE(!ReadLittleEndian32(src, value))) {
          return serialized_message_reader_internal::ReadFixed32Error(
              src, field_number);
        }
        absl::Status status;
        if (std::apply(
                [&](const auto&... field_handlers) {
                  return (serialized_message_reader_internal::ReadFixed32Field(
                              field_number, value, status, field_handlers,
                              context...) ||
                          ...);
                },
                field_handlers_)) {
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
        if (ABSL_PREDICT_FALSE(!ReadLittleEndian64(src, value))) {
          return serialized_message_reader_internal::ReadFixed64Error(
              src, field_number);
        }
        absl::Status status;
        if (std::apply(
                [&](const auto&... field_handlers) {
                  return (serialized_message_reader_internal::ReadFixed64Field(
                              field_number, value, status, field_handlers,
                              context...) ||
                          ...);
                },
                field_handlers_)) {
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
                              IsFieldHandlerForLengthDelimitedFromReader<
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
          if (std::apply(
                  [&](const auto&... field_handlers) {
                    return (serialized_message_reader_internal::
                                ReadLengthDelimitedFieldFromReader(
                                    field_number, src, size_t{length}, status,
                                    field_handlers, context...) ||
                            ...);
                  },
                  field_handlers_)) {
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
                << " has been read; consider using SkipLengthDelimited()";
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
        if (std::apply(
                [&](const auto&... field_handlers) {
                  return (
                      serialized_message_reader_internal::ReadStartGroupField(
                          field_number, status, field_handlers, context...) ||
                      ...);
                },
                field_handlers_)) {
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
        if (std::apply(
                [&](const auto&... field_handlers) {
                  return (
                      serialized_message_reader_internal::ReadEndGroupField(
                          field_number, status, field_handlers, context...) ||
                      ...);
                },
                field_handlers_)) {
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
    RIEGELI_ASSERT_UNREACHABLE()
        << "Impossible wire type: " << static_cast<int>(GetTagWireType(tag));
  }
  if (ABSL_PREDICT_FALSE(src.available() > 0)) {
    return serialized_message_reader_internal::ReadTagError(src);
  }
  return absl::OkStatus();
}

template <typename... FieldHandlers, typename... Context>
#if !__cpp_concepts
template <
    typename DependentVoid,
    std::enable_if_t<std::conjunction_v<std::is_void<DependentVoid>,
                                        IsFieldHandlerFromString<
                                            FieldHandlers, Context...>...>,
                     int>>
#endif
absl::Status
SerializedMessageReaderType<std::tuple<FieldHandlers...>, Context...>::Read(
    BytesRef src, Context&... context) const
#if __cpp_concepts
    // For conjunctions, `requires` gives better error messages than
    // `std::enable_if_t`, indicating the relevant argument.
  requires(IsFieldHandlerFromString<FieldHandlers, Context...>::value && ...)
#endif
{
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
          if (std::apply(
                  [&](const auto&... field_handlers) {
                    return (serialized_message_reader_internal::ReadVarintField(
                                field_number, value, status, field_handlers,
                                context...) ||
                            ...);
                  },
                  field_handlers_)) {
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
        const uint32_t value = ReadLittleEndian32(cursor);
        cursor += sizeof(uint32_t);
        absl::Status status;
        if (std::apply(
                [&](const auto&... field_handlers) {
                  return (serialized_message_reader_internal::ReadFixed32Field(
                              field_number, value, status, field_handlers,
                              context...) ||
                          ...);
                },
                field_handlers_)) {
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
        const uint64_t value = ReadLittleEndian64(cursor);
        cursor += sizeof(uint64_t);
        absl::Status status;
        if (std::apply(
                [&](const auto&... field_handlers) {
                  return (serialized_message_reader_internal::ReadFixed64Field(
                              field_number, value, status, field_handlers,
                              context...) ||
                          ...);
                },
                field_handlers_)) {
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
        if (std::apply(
                [&](const auto&... field_handlers) {
                  return (serialized_message_reader_internal::
                              ReadLengthDelimitedFieldFromString(
                                  field_number, cursor, size_t{length}, status,
                                  field_handlers, context...) ||
                          ...);
                },
                field_handlers_)) {
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
        if (std::apply(
                [&](const auto&... field_handlers) {
                  return (
                      serialized_message_reader_internal::ReadStartGroupField(
                          field_number, status, field_handlers, context...) ||
                      ...);
                },
                field_handlers_)) {
          if (ABSL_PREDICT_FALSE(!status.ok())) {
            return serialized_message_reader_internal::AnnotateWithFieldNumber(
                std::move(status), field_number);
          }
        }
        continue;
      }
      case WireType::kEndGroup: {
        absl::Status status;
        if (std::apply(
                [&](const auto&... field_handlers) {
                  return (
                      serialized_message_reader_internal::ReadEndGroupField(
                          field_number, status, field_handlers, context...) ||
                      ...);
                },
                field_handlers_)) {
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
    RIEGELI_ASSERT_UNREACHABLE()
        << "Impossible wire type: " << static_cast<int>(GetTagWireType(tag));
  }
  if (ABSL_PREDICT_FALSE(cursor < limit)) {
    return serialized_message_reader_internal::ReadTagError();
  }
  return absl::OkStatus();
}

inline absl::Status SkipLengthDelimited(ReaderSpan<> value) {
  if (ABSL_PREDICT_FALSE(!value.reader().Skip(value.length()))) {
    return serialized_message_reader_internal::ReadLengthDelimitedValueError(
        value.reader());
  }
  return absl::OkStatus();
}

inline absl::Status SkipLengthDelimited(
    ABSL_ATTRIBUTE_UNUSED absl::string_view value) {
  return absl::OkStatus();
}

}  // namespace riegeli

#endif  // RIEGELI_MESSAGES_SERIALIZED_MESSAGE_READER2_H_
