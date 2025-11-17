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
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/bytes_ref.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/type_traits.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/cord_reader.h"
#include "riegeli/bytes/limiting_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/string_reader.h"
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
// The serialized message is read from a `Reader`, string, `Chain`, or `Cord`.

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
// Field handlers stored in a single `SerializedMessageReader2` are usually
// conceptually associated with a single message type.
//
// If a field handler returns a failed `absl::Status`, `ReadMessage()` is
// cancelled and propagates the status, annotated by the `Reader` and/or with
// the field number. Annotations are skipped for `absl::CancelledError()` to
// make it more efficient to cancel a handler when cancellation is likely.
//
// A field handler can also be expressed as a raw pointer to a const-qualified
// proper field handler. A proper field handler is owned by the
// `SerializedMessageReader2`. By passing a pointer, the field handler can be
// managed outside of the `SerializedMessageReader2`.

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
// A field handler is applicable to a `Reader` source, and possibly also to a
// string source. If `SerializedMessageReader2::ReadMessage()` is called with a
// string and all field handlers are applicable to a string source, then field
// handlers are called with the string source. Otherwise, the source is wrapped
// in an appropriate `Reader` if needed.
//
// Field handlers have a `static constexpr int kFieldNumber` member variable:
//  * For static field handlers: a positive field number.
//  * For dynamic field handlers: `kDynamicFieldNumber`.
//  * For unbound field handlers: `kUnboundFieldNumber`.
//
// Static and unbound field handlers provide at least one of the following
// member functions, with parameters followed by `Context&...`:
// ```
//   absl::Status HandleVarint(uint64_t value) const;
//
//   absl::Status HandleFixed32(uint32_t value) const;
//
//   absl::Status HandleFixed64(uint64_t value) const;
//
//   // Applicable to a `Reader` source. If `HandleLengthDelimitedFromReader()`
//   // is defined but `HandleLengthDelimitedFromString()` is not, then the
//   // field handler is not applicable to a string source.
//   //
//   // `HandleLengthDelimitedFromReader()` must read to the end of the
//   // `ReaderSpan<>` or fail. `SkipLengthDelimited()` can be used to skip the
//   // whole field.
//   absl::Status HandleLengthDelimitedFromReader(ReaderSpan<> value) const;
//
//   // Applicable to a string source. If `HandleLengthDelimitedFromString()`
//   // is defined but `HandleLengthDelimitedFromReader()` is not, then
//   // `HandleLengthDelimitedFromString()` is used also for a `Reader` source,
//   // after reading the value to `absl::string_view`.
//   //
//   // For a string source, the `absl::string_view` is guaranteed to be
//   // a substring of the original string.
//   absl::Status HandleLengthDelimitedFromString(absl::string_view value)
//       const;
//
//   absl::Status HandleStartGroup() const;
//
//   absl::Status HandleEndGroup() const;
// ```
//
// Dynamic field handlers provide at least one of the following pairs of
// member functions corresponding to some wire type `X` as above, with `T...`
// as above, and with `HandleX()` parameters followed by `Context&...`
// ```
//   MaybeAccepted AcceptX(int field_number) const;`
//
//   absl::Status HandleX(Accepted accepted, T... value) const;
// ```
//
// For length-delimited fields, a single `AcceptLengthDelimited()` function
// corresponds to both `HandleLengthDelimitedFromReader()` and
// `HandleLengthDelimitedFromString()`.
//
// `MaybeAccepted` is some type explicitly convertible to `bool`, with
// `operator*` returning some `Accepted` type, such as `std::optional<Accepted>`
// or `Accepted*`. If `AcceptX()` returns a value explicitly convertible to
// `true`, then the field is accepted, and the corresponding `HandleX()`
// function is called with the result of `operator*` as the first argument.

// In `FieldHandler::kFieldNumber`, marks a dynamic field handler.
inline constexpr int kDynamicFieldNumber = -1;

// In `FieldHandler::kFieldNumber`, marks an unbound field handler.
inline constexpr int kUnboundFieldNumber = -2;

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
  // and thus it is safe to e.g. pass such a length to `Reader::ReadMessage()`.
  template <typename Src,
            std::enable_if_t<TargetRefSupportsDependency<Reader*, Src>::value,
                             int> = 0>
  absl::Status ReadMessage(Src&& src, Context&... context) const;
  absl::Status ReadMessage(BytesRef src, Context&... context) const;
  absl::Status ReadMessage(const Chain& src, Context&... context) const;
  absl::Status ReadMessage(const absl::Cord& src, Context&... context) const;

 private:
  template <typename ReaderType>
  absl::Status ReadFromReader(ReaderType& src, Context&... context) const;
  absl::Status ReadFromString(absl::string_view src, Context&... context) const;

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
// `Context` types must be specified explicitly for `SerializedMessageReader2`.
// Field handlers and their actions must accept compatible `Context&...`
// parameters.
template <typename... Context, typename... FieldHandlerInitializers
#if !__cpp_concepts
          ,
          std::enable_if_t<
              std::conjunction_v<IsFieldHandler<
                  std::remove_pointer_t<std::decay_t<FieldHandlerInitializers>>,
                  Context...>...>,
              int> = 0
#endif
          >
#if __cpp_concepts
// For conjunctions, `requires` gives better error messages than
// `std::enable_if_t`, indicating the relevant argument.
  requires(IsFieldHandler<
               std::remove_pointer_t<std::decay_t<FieldHandlerInitializers>>,
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
struct IsFieldHandlerWithStaticFieldNumber : std::false_type {};

template <typename T>
struct IsFieldHandlerWithStaticFieldNumber<
    T, std::enable_if_t<(T::kFieldNumber > 0)>> : std::true_type {};

template <typename T, typename Enable, typename... Context>
struct IsStaticFieldHandlerForVarintImpl : std::false_type {};

template <typename T, typename... Context>
struct IsStaticFieldHandlerForVarintImpl<
    T,
    std::enable_if_t<std::is_convertible_v<
        decltype(std::declval<const T&>().HandleVarint(
            std::declval<uint64_t>(), std::declval<Context&>()...)),
        absl::Status>>,
    Context...> : std::true_type {};

template <typename T, typename Enable, typename... Context>
struct IsStaticFieldHandlerForFixed32Impl : std::false_type {};

template <typename T, typename... Context>
struct IsStaticFieldHandlerForFixed32Impl<
    T,
    std::enable_if_t<std::is_convertible_v<
        decltype(std::declval<const T&>().HandleFixed32(
            std::declval<uint32_t>(), std::declval<Context&>()...)),
        absl::Status>>,
    Context...> : std::true_type {};

template <typename T, typename Enable, typename... Context>
struct IsStaticFieldHandlerForFixed64Impl : std::false_type {};

template <typename T, typename... Context>
struct IsStaticFieldHandlerForFixed64Impl<
    T,
    std::enable_if_t<std::is_convertible_v<
        decltype(std::declval<const T&>().HandleFixed64(
            std::declval<uint64_t>(), std::declval<Context&>()...)),
        absl::Status>>,
    Context...> : std::true_type {};

template <typename T, typename Enable, typename... Context>
struct IsStaticFieldHandlerForLengthDelimitedFromReaderImpl : std::false_type {
};

template <typename T, typename... Context>
struct IsStaticFieldHandlerForLengthDelimitedFromReaderImpl<
    T,
    std::enable_if_t<std::is_convertible_v<
        decltype(std::declval<const T&>().HandleLengthDelimitedFromReader(
            std::declval<ReaderSpan<>>(), std::declval<Context&>()...)),
        absl::Status>>,
    Context...> : std::true_type {};

template <typename T, typename Enable, typename... Context>
struct IsStaticFieldHandlerForLengthDelimitedFromStringImpl : std::false_type {
};

template <typename T, typename... Context>
struct IsStaticFieldHandlerForLengthDelimitedFromStringImpl<
    T,
    std::enable_if_t<std::is_convertible_v<
        decltype(std::declval<const T&>().HandleLengthDelimitedFromString(
            std::declval<absl::string_view>(), std::declval<Context&>()...)),
        absl::Status>>,
    Context...> : std::true_type {};

template <typename T, typename Enable, typename... Context>
struct IsStaticFieldHandlerForStartGroupImpl : std::false_type {};

template <typename T, typename... Context>
struct IsStaticFieldHandlerForStartGroupImpl<
    T,
    std::enable_if_t<std::is_convertible_v<
        decltype(std::declval<const T&>().HandleStartGroup(
            std::declval<Context&>()...)),
        absl::Status>>,
    Context...> : std::true_type {};

template <typename T, typename Enable, typename... Context>
struct IsStaticFieldHandlerForEndGroupImpl : std::false_type {};

template <typename T, typename... Context>
struct IsStaticFieldHandlerForEndGroupImpl<
    T,
    std::enable_if_t<
        std::is_convertible_v<decltype(std::declval<const T&>().HandleEndGroup(
                                  std::declval<Context&>()...)),
                              absl::Status>>,
    Context...> : std::true_type {};

template <typename T, typename... Context>
using IsStaticFieldHandlerForVarint =
    IsStaticFieldHandlerForVarintImpl<T, void, Context...>;

template <typename T, typename... Context>
using IsStaticFieldHandlerForFixed32 =
    IsStaticFieldHandlerForFixed32Impl<T, void, Context...>;

template <typename T, typename... Context>
using IsStaticFieldHandlerForFixed64 =
    IsStaticFieldHandlerForFixed64Impl<T, void, Context...>;

template <typename T, typename... Context>
using IsStaticFieldHandlerForLengthDelimitedFromReader =
    IsStaticFieldHandlerForLengthDelimitedFromReaderImpl<T, void, Context...>;

template <typename T, typename... Context>
using IsStaticFieldHandlerForLengthDelimitedFromString =
    IsStaticFieldHandlerForLengthDelimitedFromStringImpl<T, void, Context...>;

template <typename T, typename... Context>
struct IsStaticFieldHandlerForLengthDelimited
    : std::disjunction<
          IsStaticFieldHandlerForLengthDelimitedFromReader<T, Context...>,
          IsStaticFieldHandlerForLengthDelimitedFromString<T, Context...>> {};

template <typename T, typename... Context>
using IsStaticFieldHandlerForStartGroup =
    IsStaticFieldHandlerForStartGroupImpl<T, void, Context...>;

template <typename T, typename... Context>
using IsStaticFieldHandlerForEndGroup =
    IsStaticFieldHandlerForEndGroupImpl<T, void, Context...>;

template <typename T, typename... Context>
struct IsStaticFieldHandlerFromString
    : std::conjunction<
          IsFieldHandlerWithStaticFieldNumber<T>,
          std::disjunction<
              IsStaticFieldHandlerForVarint<T, Context...>,
              IsStaticFieldHandlerForFixed32<T, Context...>,
              IsStaticFieldHandlerForFixed64<T, Context...>,
              IsStaticFieldHandlerForLengthDelimitedFromString<T, Context...>,
              IsStaticFieldHandlerForStartGroup<T, Context...>,
              IsStaticFieldHandlerForEndGroup<T, Context...>>,
          std::disjunction<
              IsStaticFieldHandlerForLengthDelimitedFromString<T, Context...>,
              std::negation<IsStaticFieldHandlerForLengthDelimitedFromReader<
                  T, Context...>>>> {};

template <typename T, typename... Context>
struct IsStaticFieldHandler
    : std::conjunction<
          IsFieldHandlerWithStaticFieldNumber<T>,
          std::disjunction<
              IsStaticFieldHandlerForVarint<T, Context...>,
              IsStaticFieldHandlerForFixed32<T, Context...>,
              IsStaticFieldHandlerForFixed64<T, Context...>,
              IsStaticFieldHandlerForLengthDelimited<T, Context...>,
              IsStaticFieldHandlerForStartGroup<T, Context...>,
              IsStaticFieldHandlerForEndGroup<T, Context...>>> {};

template <typename T, typename Enable = void>
struct IsFieldHandlerWithDynamicFieldNumber : std::false_type {};

template <typename T>
struct IsFieldHandlerWithDynamicFieldNumber<
    T, std::enable_if_t<T::kFieldNumber == kDynamicFieldNumber>>
    : std::true_type {};

template <typename T, typename Enable, typename... Context>
struct IsDynamicFieldHandlerForVarintImpl : std::false_type {};

template <typename T, typename... Context>
struct IsDynamicFieldHandlerForVarintImpl<
    T,
    std::enable_if_t<std::conjunction_v<
        std::is_constructible<bool,
                              decltype(std::declval<const T&>().AcceptVarint(
                                  std::declval<int>()))>,
        std::is_convertible<
            decltype(std::declval<const T&>().HandleVarint(
                *std::declval<const T&>().AcceptVarint(std::declval<int>()),
                std::declval<uint64_t>(), std::declval<Context&>()...)),
            absl::Status>>>,
    Context...> : std::true_type {};

template <typename T, typename Enable, typename... Context>
struct IsDynamicFieldHandlerForFixed32Impl : std::false_type {};

template <typename T, typename... Context>
struct IsDynamicFieldHandlerForFixed32Impl<
    T,
    std::enable_if_t<std::conjunction_v<
        std::is_constructible<bool,
                              decltype(std::declval<const T&>().AcceptFixed32(
                                  std::declval<int>()))>,
        std::is_convertible<
            decltype(std::declval<const T&>().HandleFixed32(
                *std::declval<const T&>().AcceptFixed32(std::declval<int>()),
                std::declval<uint32_t>(), std::declval<Context&>()...)),
            absl::Status>>>,
    Context...> : std::true_type {};

template <typename T, typename Enable, typename... Context>
struct IsDynamicFieldHandlerForFixed64Impl : std::false_type {};

template <typename T, typename... Context>
struct IsDynamicFieldHandlerForFixed64Impl<
    T,
    std::enable_if_t<std::conjunction_v<
        std::is_constructible<bool,
                              decltype(std::declval<const T&>().AcceptFixed64(
                                  std::declval<int>()))>,
        std::is_convertible<
            decltype(std::declval<const T&>().HandleFixed64(
                *std::declval<const T&>().AcceptFixed64(std::declval<int>()),
                std::declval<uint64_t>(), std::declval<Context&>()...)),
            absl::Status>>>,
    Context...> : std::true_type {};

template <typename T, typename Enable, typename... Context>
struct IsDynamicFieldHandlerForLengthDelimitedFromReaderImpl : std::false_type {
};

template <typename T, typename... Context>
struct IsDynamicFieldHandlerForLengthDelimitedFromReaderImpl<
    T,
    std::enable_if_t<std::conjunction_v<
        std::is_constructible<
            bool, decltype(std::declval<const T&>().AcceptLengthDelimited(
                      std::declval<int>()))>,
        std::is_convertible<
            decltype(std::declval<const T&>().HandleLengthDelimitedFromReader(
                *std::declval<const T&>().AcceptLengthDelimited(
                    std::declval<int>()),
                std::declval<ReaderSpan<>>(), std::declval<Context&>()...)),
            absl::Status>>>,
    Context...> : std::true_type {};

template <typename T, typename Enable, typename... Context>
struct IsDynamicFieldHandlerForLengthDelimitedFromStringImpl : std::false_type {
};

template <typename T, typename... Context>
struct IsDynamicFieldHandlerForLengthDelimitedFromStringImpl<
    T,
    std::enable_if_t<std::conjunction_v<
        std::is_constructible<
            bool, decltype(std::declval<const T&>().AcceptLengthDelimited(
                      std::declval<int>()))>,
        std::is_convertible<
            decltype(std::declval<const T&>().HandleLengthDelimitedFromString(
                *std::declval<const T&>().AcceptLengthDelimited(
                    std::declval<int>()),
                std::declval<absl::string_view>(),
                std::declval<Context&>()...)),
            absl::Status>>>,
    Context...> : std::true_type {};

template <typename T, typename Enable, typename... Context>
struct IsDynamicFieldHandlerForStartGroupImpl : std::false_type {};

template <typename T, typename... Context>
struct IsDynamicFieldHandlerForStartGroupImpl<
    T,
    std::enable_if_t<std::conjunction_v<
        std::is_constructible<
            bool, decltype(std::declval<const T&>().AcceptStartGroup(
                      std::declval<int>()))>,
        std::is_convertible<decltype(std::declval<const T&>().HandleStartGroup(
                                *std::declval<const T&>().AcceptStartGroup(
                                    std::declval<int>()),
                                std::declval<Context&>()...)),
                            absl::Status>>>,
    Context...> : std::true_type {};

template <typename T, typename Enable, typename... Context>
struct IsDynamicFieldHandlerForEndGroupImpl : std::false_type {};

template <typename T, typename... Context>
struct IsDynamicFieldHandlerForEndGroupImpl<
    T,
    std::enable_if_t<std::conjunction_v<
        std::is_constructible<bool,
                              decltype(std::declval<const T&>().AcceptEndGroup(
                                  std::declval<int>()))>,
        std::is_convertible<decltype(std::declval<const T&>().HandleEndGroup(
                                *std::declval<const T&>().AcceptEndGroup(
                                    std::declval<int>()),
                                std::declval<Context&>()...)),
                            absl::Status>>>,
    Context...> : std::true_type {};

template <typename T, typename... Context>
using IsDynamicFieldHandlerForVarint =
    IsDynamicFieldHandlerForVarintImpl<T, void, Context...>;

template <typename T, typename... Context>
using IsDynamicFieldHandlerForFixed32 =
    IsDynamicFieldHandlerForFixed32Impl<T, void, Context...>;

template <typename T, typename... Context>
using IsDynamicFieldHandlerForFixed64 =
    IsDynamicFieldHandlerForFixed64Impl<T, void, Context...>;

template <typename T, typename... Context>
using IsDynamicFieldHandlerForLengthDelimitedFromReader =
    IsDynamicFieldHandlerForLengthDelimitedFromReaderImpl<T, void, Context...>;

template <typename T, typename... Context>
using IsDynamicFieldHandlerForLengthDelimitedFromString =
    IsDynamicFieldHandlerForLengthDelimitedFromStringImpl<T, void, Context...>;

template <typename T, typename... Context>
struct IsDynamicFieldHandlerForLengthDelimited
    : std::disjunction<
          IsDynamicFieldHandlerForLengthDelimitedFromReader<T, Context...>,
          IsDynamicFieldHandlerForLengthDelimitedFromString<T, Context...>> {};

template <typename T, typename... Context>
using IsDynamicFieldHandlerForStartGroup =
    IsDynamicFieldHandlerForStartGroupImpl<T, void, Context...>;

template <typename T, typename... Context>
using IsDynamicFieldHandlerForEndGroup =
    IsDynamicFieldHandlerForEndGroupImpl<T, void, Context...>;

template <typename T, typename... Context>
struct IsDynamicFieldHandlerFromString
    : std::conjunction<
          IsFieldHandlerWithDynamicFieldNumber<T>,
          std::disjunction<
              IsDynamicFieldHandlerForVarint<T, Context...>,
              IsDynamicFieldHandlerForFixed32<T, Context...>,
              IsDynamicFieldHandlerForFixed64<T, Context...>,
              IsDynamicFieldHandlerForLengthDelimitedFromString<T, Context...>,
              IsDynamicFieldHandlerForStartGroup<T, Context...>,
              IsDynamicFieldHandlerForEndGroup<T, Context...>>,
          std::disjunction<
              IsDynamicFieldHandlerForLengthDelimitedFromString<T, Context...>,
              std::negation<IsDynamicFieldHandlerForLengthDelimitedFromReader<
                  T, Context...>>>> {};

template <typename T, typename... Context>
struct IsDynamicFieldHandler
    : std::conjunction<
          IsFieldHandlerWithDynamicFieldNumber<T>,
          std::disjunction<
              IsDynamicFieldHandlerForVarint<T, Context...>,
              IsDynamicFieldHandlerForFixed32<T, Context...>,
              IsDynamicFieldHandlerForFixed64<T, Context...>,
              IsDynamicFieldHandlerForLengthDelimited<T, Context...>,
              IsDynamicFieldHandlerForStartGroup<T, Context...>,
              IsDynamicFieldHandlerForEndGroup<T, Context...>>> {};

template <typename T, typename... Context>
struct IsFieldHandlerFromString
    : std::disjunction<serialized_message_reader_internal::
                           IsStaticFieldHandlerFromString<T, Context...>,
                       serialized_message_reader_internal::
                           IsDynamicFieldHandlerFromString<T, Context...>> {};

template <typename T, typename Enable = void>
struct IsFieldHandlerWithUnboundFieldNumber : std::false_type {};

template <typename T>
struct IsFieldHandlerWithUnboundFieldNumber<
    T, std::enable_if_t<(T::kFieldNumber == kUnboundFieldNumber)>>
    : std::true_type {};

template <typename FieldHandler>
inline const std::remove_pointer_t<FieldHandler>& DerefPointer(
    const FieldHandler& field_handler) {
  if constexpr (std::is_pointer_v<FieldHandler>) {
    return *field_handler;
  } else {
    return field_handler;
  }
}

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
  if constexpr (IsStaticFieldHandlerForVarint<FieldHandler,
                                              Context...>::value) {
    if (field_number == FieldHandler::kFieldNumber) {
      status = field_handler.HandleVarint(value, context...);
      return true;
    }
  }
  if constexpr (IsDynamicFieldHandlerForVarint<FieldHandler,
                                               Context...>::value) {
    auto maybe_accepted = field_handler.AcceptVarint(field_number);
    if (maybe_accepted) {
      status = field_handler.HandleVarint(*std::move(maybe_accepted), value,
                                          context...);
      return true;
    }
  }
  return false;
}

template <typename FieldHandler, typename... Context>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool ReadFixed32Field(
    int field_number, uint32_t value, absl::Status& status,
    const FieldHandler& field_handler, Context&... context) {
  if constexpr (IsStaticFieldHandlerForFixed32<FieldHandler,
                                               Context...>::value) {
    if (field_number == FieldHandler::kFieldNumber) {
      status = field_handler.HandleFixed32(value, context...);
      return true;
    }
  }
  if constexpr (IsDynamicFieldHandlerForFixed32<FieldHandler,
                                                Context...>::value) {
    auto maybe_accepted = field_handler.AcceptFixed32(field_number);
    if (maybe_accepted) {
      status = field_handler.HandleFixed32(*std::move(maybe_accepted), value,
                                           context...);
      return true;
    }
  }
  return false;
}

template <typename FieldHandler, typename... Context>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool ReadFixed64Field(
    int field_number, uint64_t value, absl::Status& status,
    const FieldHandler& field_handler, Context&... context) {
  if constexpr (IsStaticFieldHandlerForFixed64<FieldHandler,
                                               Context...>::value) {
    if (field_number == FieldHandler::kFieldNumber) {
      status = field_handler.HandleFixed64(value, context...);
      return true;
    }
  }
  if constexpr (IsDynamicFieldHandlerForFixed64<FieldHandler,
                                                Context...>::value) {
    auto maybe_accepted = field_handler.AcceptFixed64(field_number);
    if (maybe_accepted) {
      status = field_handler.HandleFixed64(*std::move(maybe_accepted), value,
                                           context...);
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
  if constexpr (IsStaticFieldHandlerForLengthDelimitedFromReader<
                    FieldHandler, Context...>::value) {
    if (field_number == FieldHandler::kFieldNumber) {
      status = field_handler.HandleLengthDelimitedFromReader(
          ReaderSpan<>(&src, length), context...);
      return true;
    }
  } else if constexpr (IsStaticFieldHandlerForLengthDelimitedFromString<
                           FieldHandler, Context...>::value) {
    if (field_number == FieldHandler::kFieldNumber) {
      absl::string_view value_string;
      if (ABSL_PREDICT_FALSE(!src.Read(length, value_string))) {
        status = ReadLengthDelimitedValueError(src);
        return true;
      }
      status = field_handler.HandleLengthDelimitedFromString(value_string,
                                                             context...);
      return true;
    }
  }
  if constexpr (IsDynamicFieldHandlerForLengthDelimitedFromReader<
                    FieldHandler, Context...>::value) {
    auto maybe_accepted = field_handler.AcceptLengthDelimited(field_number);
    if (maybe_accepted) {
      status = field_handler.HandleLengthDelimitedFromReader(
          *std::move(maybe_accepted), ReaderSpan<>(&src, length), context...);
      return true;
    }
  } else if constexpr (IsDynamicFieldHandlerForLengthDelimitedFromString<
                           FieldHandler, Context...>::value) {
    auto maybe_accepted = field_handler.AcceptLengthDelimited(field_number);
    if (maybe_accepted) {
      absl::string_view value_string;
      if (ABSL_PREDICT_FALSE(!src.Read(length, value_string))) {
        status = ReadLengthDelimitedValueError(src);
        return true;
      }
      status = field_handler.HandleLengthDelimitedFromString(
          *std::move(maybe_accepted), value_string, context...);
      return true;
    }
  }
  return false;
}

template <typename FieldHandler, typename... Context>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool ReadLengthDelimitedFieldFromString(
    int field_number, const char* src, size_t length, absl::Status& status,
    const FieldHandler& field_handler, Context&... context) {
  if constexpr (IsStaticFieldHandlerForLengthDelimitedFromString<
                    FieldHandler, Context...>::value) {
    if (field_number == FieldHandler::kFieldNumber) {
      status = field_handler.HandleLengthDelimitedFromString(
          absl::string_view(src, length), context...);
      return true;
    }
  }
  if constexpr (IsDynamicFieldHandlerForLengthDelimitedFromString<
                    FieldHandler, Context...>::value) {
    auto maybe_accepted = field_handler.AcceptLengthDelimited(field_number);
    if (maybe_accepted) {
      status = field_handler.HandleLengthDelimitedFromString(
          *std::move(maybe_accepted), absl::string_view(src, length),
          context...);
      return true;
    }
  }
  return false;
}

template <typename FieldHandler, typename... Context>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool ReadStartGroupField(
    int field_number, absl::Status& status, const FieldHandler& field_handler,
    Context&... context) {
  if constexpr (IsStaticFieldHandlerForStartGroup<FieldHandler,
                                                  Context...>::value) {
    if (field_number == FieldHandler::kFieldNumber) {
      status = field_handler.HandleStartGroup(context...);
      return true;
    }
  }
  if constexpr (IsDynamicFieldHandlerForStartGroup<FieldHandler,
                                                   Context...>::value) {
    auto maybe_accepted = field_handler.AcceptStartGroup(field_number);
    if (maybe_accepted) {
      status = field_handler.HandleStartGroup(*std::move(maybe_accepted),
                                              context...);
      return true;
    }
  }
  return false;
}

template <typename FieldHandler, typename... Context>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool ReadEndGroupField(
    int field_number, absl::Status& status, const FieldHandler& field_handler,
    Context&... context) {
  if constexpr (IsStaticFieldHandlerForEndGroup<FieldHandler,
                                                Context...>::value) {
    if (field_number == FieldHandler::kFieldNumber) {
      status = field_handler.HandleEndGroup(context...);
      return true;
    }
  }
  if constexpr (IsDynamicFieldHandlerForEndGroup<FieldHandler,
                                                 Context...>::value) {
    auto maybe_accepted = field_handler.AcceptEndGroup(field_number);
    if (maybe_accepted) {
      status =
          field_handler.HandleEndGroup(*std::move(maybe_accepted), context...);
      return true;
    }
  }
  return false;
}

}  // namespace serialized_message_reader_internal

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
    Context...>::ReadFromReader(ReaderType& src, Context&... context) const {
  uint32_t tag;
  while (ReadVarint32(src, tag)) {
    const int field_number = GetTagFieldNumber(tag);
    switch (GetTagWireType(tag)) {
      case WireType::kVarint: {
        if constexpr (
            std::disjunction_v<
                serialized_message_reader_internal::
                    IsStaticFieldHandlerForVarint<
                        std::remove_pointer_t<FieldHandlers>, Context...>...,
                serialized_message_reader_internal::
                    IsDynamicFieldHandlerForVarint<
                        std::remove_pointer_t<FieldHandlers>, Context...>...>) {
          uint64_t value;
          if (ABSL_PREDICT_FALSE(!ReadVarint64(src, value))) {
            return serialized_message_reader_internal::ReadVarintError(
                src, field_number);
          }
          absl::Status status;
          if (std::apply(
                  [&](const auto&... field_handlers) {
                    return (
                        serialized_message_reader_internal::ReadVarintField(
                            field_number, value, status,
                            serialized_message_reader_internal::DerefPointer(
                                field_handlers),
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
                              field_number, value, status,
                              serialized_message_reader_internal::DerefPointer(
                                  field_handlers),
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
                              field_number, value, status,
                              serialized_message_reader_internal::DerefPointer(
                                  field_handlers),
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
        if constexpr (
            std::disjunction_v<
                serialized_message_reader_internal::
                    IsStaticFieldHandlerForLengthDelimited<
                        std::remove_pointer_t<FieldHandlers>, Context...>...,
                serialized_message_reader_internal::
                    IsDynamicFieldHandlerForLengthDelimited<
                        std::remove_pointer_t<FieldHandlers>, Context...>...>) {
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
                                    serialized_message_reader_internal::
                                        DerefPointer(field_handlers),
                                    context...) ||
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
                          field_number, status,
                          serialized_message_reader_internal::DerefPointer(
                              field_handlers),
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
      case WireType::kEndGroup: {
        absl::Status status;
        if (std::apply(
                [&](const auto&... field_handlers) {
                  return (serialized_message_reader_internal::ReadEndGroupField(
                              field_number, status,
                              serialized_message_reader_internal::DerefPointer(
                                  field_handlers),
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
inline absl::Status SerializedMessageReaderType<
    std::tuple<FieldHandlers...>,
    Context...>::ReadFromString(absl::string_view src,
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
        if constexpr (
            std::disjunction_v<
                serialized_message_reader_internal::
                    IsStaticFieldHandlerForVarint<
                        std::remove_pointer_t<FieldHandlers>, Context...>...,
                serialized_message_reader_internal::
                    IsDynamicFieldHandlerForVarint<
                        std::remove_pointer_t<FieldHandlers>, Context...>...>) {
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
                    return (
                        serialized_message_reader_internal::ReadVarintField(
                            field_number, value, status,
                            serialized_message_reader_internal::DerefPointer(
                                field_handlers),
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
                              field_number, value, status,
                              serialized_message_reader_internal::DerefPointer(
                                  field_handlers),
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
                              field_number, value, status,
                              serialized_message_reader_internal::DerefPointer(
                                  field_handlers),
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
                  return (
                      serialized_message_reader_internal::
                          ReadLengthDelimitedFieldFromString(
                              field_number, cursor, size_t{length}, status,
                              serialized_message_reader_internal::DerefPointer(
                                  field_handlers),
                              context...) ||
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
                          field_number, status,
                          serialized_message_reader_internal::DerefPointer(
                              field_handlers),
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
      case WireType::kEndGroup: {
        absl::Status status;
        if (std::apply(
                [&](const auto&... field_handlers) {
                  return (serialized_message_reader_internal::ReadEndGroupField(
                              field_number, status,
                              serialized_message_reader_internal::DerefPointer(
                                  field_handlers),
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
                                       IsStaticFieldHandlerForLengthDelimited<
                                           std::remove_pointer_t<FieldHandlers>,
                                           Context...>...,
                                   serialized_message_reader_internal::
                                       IsDynamicFieldHandlerForLengthDelimited<
                                           std::remove_pointer_t<FieldHandlers>,
                                           Context...>...>) {
    if constexpr (std::is_convertible_v<
                      typename DependencyRef<Reader*, Src>::Subhandle,
                      LimitingReaderBase*>) {
      status = ReadFromReader<LimitingReaderBase>(*src_dep, context...);
    } else {
      LimitingReaderBase::Options options;
      if (src_dep->SupportsSize()) {
        const std::optional<Position> size = src_dep->Size();
        if (ABSL_PREDICT_TRUE(size != std::nullopt)) options.set_max_pos(*size);
      }
      LimitingReader<> limiting_reader(src_dep.get(), options);
      status = ReadFromReader<LimitingReaderBase>(limiting_reader, context...);
      if (ABSL_PREDICT_FALSE(!limiting_reader.Close())) {
        status.Update(limiting_reader.status());
      }
    }
  } else {
    status = ReadFromReader<Reader>(*src_dep, context...);
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
                                           std::remove_pointer_t<FieldHandlers>,
                                           Context...>...>) {
    return ReadFromString(src, context...);
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
  return ReadMessage(CordReader(&src), context...);
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
