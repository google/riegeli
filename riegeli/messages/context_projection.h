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

#ifndef RIEGELI_MESSAGES_CONTEXT_PROJECTION_H_
#define RIEGELI_MESSAGES_CONTEXT_PROJECTION_H_

#include <stddef.h>
#include <stdint.h>

#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/cord_iterator_span.h"
#include "riegeli/bytes/limiting_reader.h"
#include "riegeli/messages/serialized_message_reader_internal.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

// The type of `ContextAt<indices...>`.
template <size_t... indices>
struct ContextAtImpl;

// A projection for `ContextProjection()` which selects a subset of context
// parameters. The other context parameters are ignored.
template <size_t... indices>
constexpr ContextAtImpl<indices...> ContextAt = {};

// The type returned by `ContextProjection()` with a single projection.
template <auto projection, typename FieldHandler>
class ContextProjectionImpl;

// Adapts a field handler to depend on a part of the context. Makes a field
// handler taking some outer context parameters from a field handler taking
// inner context parameters.
//
// `projection...` parameters are applied sequentially to transform the outer
// context to the inner context. Each `projection` is an invocable which
// returns either a reference or a tuple of references. Common projections
// include `ContextAt<indices...>` to select a subset of context parameters,
// and `&Context::member` to select a member of the only context parameter.

template <typename FieldHandler>
constexpr FieldHandler&& ContextProjection(FieldHandler&& field_handler) {
  return std::forward<FieldHandler>(field_handler);
}

template <auto projection, typename FieldHandler>
constexpr ContextProjectionImpl<projection, std::decay_t<FieldHandler>>
ContextProjection(FieldHandler&& field_handler) {
  return ContextProjectionImpl<projection, std::decay_t<FieldHandler>>(
      std::forward<FieldHandler>(field_handler));
}

template <auto first_projection, auto second_projection,
          auto... rest_projections, typename FieldHandler>
constexpr auto ContextProjection(FieldHandler&& field_handler) {
  return ContextProjection<first_projection>(
      ContextProjection<second_projection, rest_projections...>(
          std::forward<FieldHandler>(field_handler)));
}

// Implementation details follow.

namespace context_projection_internal {

template <typename T>
struct DecodeContextResult {};

template <typename Context>
struct DecodeContextResult<Context&>
    : std::type_identity<std::tuple<Context&>> {};

template <typename... Context>
struct DecodeContextResult<std::tuple<Context&...>>
    : std::type_identity<std::tuple<Context&...>> {};

template <auto projection, typename... Context>
struct InnerContextTuple
    : DecodeContextResult<
          std::invoke_result_t<decltype(projection), Context&...>> {};

}  // namespace context_projection_internal

template <size_t... indices>
struct ContextAtImpl {
  template <typename... Context>
  std::tuple<std::tuple_element_t<indices, std::tuple<Context&...>>...>
  operator()(Context&... context) const {
    const std::tuple<Context&...> context_tuple = {context...};
    return {std::get<indices>(context_tuple)...};
  }
};

template <auto projection, typename FieldHandler>
class ContextProjectionImpl {
 private:
  template <template <typename T, typename... InnerContext> class Predicate,
            typename InnerContextTuple>
  struct IsProjectedFieldHandlerImpl;

  template <template <typename T, typename... InnerContext> class Predicate,
            typename... InnerContext>
  struct IsProjectedFieldHandlerImpl<Predicate, std::tuple<InnerContext&...>>
      : Predicate<FieldHandler, InnerContext...> {};

  template <template <typename T, typename... InnerContext> class Predicate,
            typename... Context>
  struct IsProjectedFieldHandler
      : IsProjectedFieldHandlerImpl<
            Predicate, typename context_projection_internal::InnerContextTuple<
                           projection, Context...>::type> {};

 public:
  static constexpr int kFieldNumber = FieldHandler::kFieldNumber;

  template <typename FieldHandlerInitializer,
            std::enable_if_t<
                std::is_convertible_v<FieldHandlerInitializer&&, FieldHandler>,
                int> = 0>
  explicit constexpr ContextProjectionImpl(
      FieldHandlerInitializer&& field_handler)
      : field_handler_(std::forward<FieldHandlerInitializer>(field_handler)) {}

  ContextProjectionImpl(const ContextProjectionImpl& that) = default;
  ContextProjectionImpl& operator=(const ContextProjectionImpl& that) = default;

  ContextProjectionImpl(ContextProjectionImpl&& that) = default;
  ContextProjectionImpl& operator=(ContextProjectionImpl&& that) = default;

  template <
      typename... Context,
      std::enable_if_t<
          IsProjectedFieldHandler<
              serialized_message_reader_internal::IsStaticFieldHandlerForVarint,
              Context...>::value,
          int> = 0>
  absl::Status HandleVarint(uint64_t repr, Context&... context) const {
    return std::apply(
        [&](auto&... inner_context) {
          return field_handler_.HandleVarint(repr, inner_context...);
        },
        InnerContext(context...));
  }

  template <typename DependentFieldHandler = FieldHandler,
            std::enable_if_t<serialized_message_reader_internal::
                                 IsDynamicFieldHandlerForVarintSomeContext<
                                     DependentFieldHandler>::value,
                             int> = 0>
  auto AcceptVarint(int field_number) const {
    return field_handler_.AcceptVarint(field_number);
  }

  template <typename Accepted, typename... Context,
            std::enable_if_t<
                IsProjectedFieldHandler<serialized_message_reader_internal::
                                            IsDynamicFieldHandlerForVarint,
                                        Context...>::value,
                int> = 0>
  absl::Status DynamicHandleVarint(Accepted&& accepted, uint64_t repr,
                                   Context&... context) const {
    return std::apply(
        [&](auto&... inner_context) {
          return field_handler_.DynamicHandleVarint(
              std::forward<Accepted>(accepted), repr, inner_context...);
        },
        InnerContext(context...));
  }

  template <typename... Context,
            std::enable_if_t<
                IsProjectedFieldHandler<serialized_message_reader_internal::
                                            IsStaticFieldHandlerForFixed32,
                                        Context...>::value,
                int> = 0>
  absl::Status HandleFixed32(uint32_t repr, Context&... context) const {
    return std::apply(
        [&](auto&... inner_context) {
          return field_handler_.HandleFixed32(repr, inner_context...);
        },
        InnerContext(context...));
  }

  template <typename DependentFieldHandler = FieldHandler,
            std::enable_if_t<serialized_message_reader_internal::
                                 IsDynamicFieldHandlerForFixed32SomeContext<
                                     DependentFieldHandler>::value,
                             int> = 0>
  auto AcceptFixed32(int field_number) const {
    return field_handler_.AcceptFixed32(field_number);
  }

  template <typename Accepted, typename... Context,
            std::enable_if_t<
                IsProjectedFieldHandler<serialized_message_reader_internal::
                                            IsDynamicFieldHandlerForFixed32,
                                        Context...>::value,
                int> = 0>
  absl::Status DynamicHandleFixed32(Accepted&& accepted, uint32_t repr,
                                    Context&... context) const {
    return std::apply(
        [&](auto&... inner_context) {
          return field_handler_.DynamicHandleFixed32(
              std::forward<Accepted>(accepted), repr, inner_context...);
        },
        InnerContext(context...));
  }

  template <typename... Context,
            std::enable_if_t<
                IsProjectedFieldHandler<serialized_message_reader_internal::
                                            IsStaticFieldHandlerForFixed64,
                                        Context...>::value,
                int> = 0>
  absl::Status HandleFixed64(uint64_t repr, Context&... context) const {
    return std::apply(
        [&](auto&... inner_context) {
          return field_handler_.HandleFixed64(repr, inner_context...);
        },
        InnerContext(context...));
  }

  template <typename DependentFieldHandler = FieldHandler,
            std::enable_if_t<serialized_message_reader_internal::
                                 IsDynamicFieldHandlerForFixed64SomeContext<
                                     DependentFieldHandler>::value,
                             int> = 0>
  auto AcceptFixed64(int field_number) const {
    return field_handler_.AcceptFixed64(field_number);
  }

  template <typename Accepted, typename... Context,
            std::enable_if_t<
                IsProjectedFieldHandler<serialized_message_reader_internal::
                                            IsDynamicFieldHandlerForFixed64,
                                        Context...>::value,
                int> = 0>
  absl::Status DynamicHandleFixed64(Accepted&& accepted, uint64_t repr,
                                    Context&... context) const {
    return std::apply(
        [&](auto&... inner_context) {
          return field_handler_.DynamicHandleFixed64(
              std::forward<Accepted>(accepted), repr, inner_context...);
        },
        InnerContext(context...));
  }

  template <
      typename... Context,
      std::enable_if_t<IsProjectedFieldHandler<
                           serialized_message_reader_internal::
                               IsStaticFieldHandlerForLengthDelimitedFromReader,
                           Context...>::value,
                       int> = 0>
  absl::Status HandleLengthDelimitedFromReader(ReaderSpan<> repr,
                                               Context&... context) const {
    return std::apply(
        [&](auto&... inner_context) {
          return field_handler_.HandleLengthDelimitedFromReader(
              std::move(repr), inner_context...);
        },
        InnerContext(context...));
  }

  template <
      typename... Context,
      std::enable_if_t<IsProjectedFieldHandler<
                           serialized_message_reader_internal::
                               IsStaticFieldHandlerForLengthDelimitedFromCord,
                           Context...>::value,
                       int> = 0>
  absl::Status HandleLengthDelimitedFromCord(CordIteratorSpan repr,
                                             std::string& scratch,
                                             Context&... context) const {
    return std::apply(
        [&](auto&... inner_context) {
          return field_handler_.HandleLengthDelimitedFromCord(
              std::move(repr), scratch, inner_context...);
        },
        InnerContext(context...));
  }

  template <
      typename... Context,
      std::enable_if_t<IsProjectedFieldHandler<
                           serialized_message_reader_internal::
                               IsStaticFieldHandlerForLengthDelimitedFromString,
                           Context...>::value,
                       int> = 0>
  absl::Status HandleLengthDelimitedFromString(absl::string_view repr,
                                               Context&... context) const {
    return std::apply(
        [&](auto&... inner_context) {
          return field_handler_.HandleLengthDelimitedFromString(
              repr, inner_context...);
        },
        InnerContext(context...));
  }

  template <
      typename DependentFieldHandler = FieldHandler,
      std::enable_if_t<serialized_message_reader_internal::
                           IsDynamicFieldHandlerForLengthDelimitedSomeContext<
                               DependentFieldHandler>::value,
                       int> = 0>
  auto AcceptLengthDelimited(int field_number) const {
    return field_handler_.AcceptLengthDelimited(field_number);
  }

  template <typename Accepted, typename... Context,
            std::enable_if_t<
                IsProjectedFieldHandler<
                    serialized_message_reader_internal::
                        IsDynamicFieldHandlerForLengthDelimitedFromReader,
                    Context...>::value,
                int> = 0>
  absl::Status DynamicHandleLengthDelimitedFromReader(
      Accepted&& accepted, ReaderSpan<> repr, Context&... context) const {
    return std::apply(
        [&](auto&... inner_context) {
          return field_handler_.DynamicHandleLengthDelimitedFromReader(
              std::forward<Accepted>(accepted), std::move(repr),
              inner_context...);
        },
        InnerContext(context...));
  }

  template <
      typename Accepted, typename... Context,
      std::enable_if_t<IsProjectedFieldHandler<
                           serialized_message_reader_internal::
                               IsDynamicFieldHandlerForLengthDelimitedFromCord,
                           Context...>::value,
                       int> = 0>
  absl::Status DynamicHandleLengthDelimitedFromCord(Accepted&& accepted,
                                                    CordIteratorSpan repr,
                                                    std::string& scratch,
                                                    Context&... context) const {
    return std::apply(
        [&](auto&... inner_context) {
          return field_handler_.DynamicHandleLengthDelimitedFromCord(
              std::forward<Accepted>(accepted), std::move(repr), scratch,
              inner_context...);
        },
        InnerContext(context...));
  }

  template <typename Accepted, typename... Context,
            std::enable_if_t<
                IsProjectedFieldHandler<
                    serialized_message_reader_internal::
                        IsDynamicFieldHandlerForLengthDelimitedFromString,
                    Context...>::value,
                int> = 0>
  absl::Status DynamicHandleLengthDelimitedFromString(
      Accepted&& accepted, absl::string_view repr, Context&... context) const {
    return std::apply(
        [&](auto&... inner_context) {
          return field_handler_.DynamicHandleLengthDelimitedFromString(
              std::forward<Accepted>(accepted), repr, inner_context...);
        },
        InnerContext(context...));
  }

  template <typename... Context,
            std::enable_if_t<
                IsProjectedFieldHandler<serialized_message_reader_internal::
                                            IsStaticFieldHandlerForStartGroup,
                                        Context...>::value,
                int> = 0>
  absl::Status HandleStartGroup(Context&... context) const {
    return std::apply(
        [&](auto&... inner_context) {
          return field_handler_.HandleStartGroup(inner_context...);
        },
        InnerContext(context...));
  }

  template <typename DependentFieldHandler = FieldHandler,
            std::enable_if_t<serialized_message_reader_internal::
                                 IsDynamicFieldHandlerForStartGroupSomeContext<
                                     DependentFieldHandler>::value,
                             int> = 0>
  auto AcceptStartGroup(int field_number) const {
    return field_handler_.AcceptStartGroup(field_number);
  }

  template <typename Accepted, typename... Context,
            std::enable_if_t<
                IsProjectedFieldHandler<serialized_message_reader_internal::
                                            IsDynamicFieldHandlerForStartGroup,
                                        Context...>::value,
                int> = 0>
  absl::Status DynamicHandleStartGroup(Accepted&& accepted,
                                       Context&... context) const {
    return std::apply(
        [&](auto&... inner_context) {
          return field_handler_.DynamicHandleStartGroup(
              std::forward<Accepted>(accepted), inner_context...);
        },
        InnerContext(context...));
  }

  template <typename... Context,
            std::enable_if_t<
                IsProjectedFieldHandler<serialized_message_reader_internal::
                                            IsStaticFieldHandlerForEndGroup,
                                        Context...>::value,
                int> = 0>
  absl::Status HandleEndGroup(Context&... context) const {
    return std::apply(
        [&](auto&... inner_context) {
          return field_handler_.HandleEndGroup(inner_context...);
        },
        InnerContext(context...));
  }

  template <typename DependentFieldHandler = FieldHandler,
            std::enable_if_t<serialized_message_reader_internal::
                                 IsDynamicFieldHandlerForEndGroupSomeContext<
                                     DependentFieldHandler>::value,
                             int> = 0>
  auto AcceptEndGroup(int field_number) const {
    return field_handler_.AcceptEndGroup(field_number);
  }

  template <typename Accepted, typename... Context,
            std::enable_if_t<
                IsProjectedFieldHandler<serialized_message_reader_internal::
                                            IsDynamicFieldHandlerForEndGroup,
                                        Context...>::value,
                int> = 0>
  absl::Status DynamicHandleEndGroup(Accepted&& accepted,
                                     Context&... context) const {
    return std::apply(
        [&](auto&... inner_context) {
          return field_handler_.DynamicHandleEndGroup(
              std::forward<Accepted>(accepted), inner_context...);
        },
        InnerContext(context...));
  }

 private:
  template <typename... Context>
  static
      typename context_projection_internal::InnerContextTuple<projection,
                                                              Context...>::type
      InnerContext(Context&... context) {
    // If `projection` returns a reference, this returns a tuple with a single
    // element. If `projection` returns a tuple of references, this returns the
    // tuple while the braces are redundant.
    return {std::invoke(projection, context...)};
  }

  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS FieldHandler field_handler_;
};

}  // namespace riegeli

#endif  // RIEGELI_MESSAGES_CONTEXT_PROJECTION_H_
