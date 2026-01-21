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

#ifndef RIEGELI_MESSAGES_DYNAMIC_FIELD_HANDLER_H_
#define RIEGELI_MESSAGES_DYNAMIC_FIELD_HANDLER_H_

#include <cstdint>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/cord_iterator_span.h"
#include "riegeli/bytes/limiting_reader.h"
#include "riegeli/messages/serialized_message_reader2.h"
#include "riegeli/messages/serialized_message_reader_internal.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

template <typename FieldHandler>
class DynamicFieldHandlerType;

// A field handler for `SerializedMessageReader2` for a single field, with the
// field number specified at runtime.
//
// It is created from an unbound field handler.
//
// `DynamicFieldHandler` is similar to a `FieldHandlerMap` with a single
// registered field handler, but more efficient and without type erasure.
template <typename BaseFieldHandler>
constexpr DynamicFieldHandlerType<std::decay_t<BaseFieldHandler>>
DynamicFieldHandler(int field_number, BaseFieldHandler&& field_handler) {
  return DynamicFieldHandlerType<std::decay_t<BaseFieldHandler>>(
      field_number, std::forward<BaseFieldHandler>(field_handler));
}

// Implementation details follow.
template <typename BaseFieldHandler>
class DynamicFieldHandlerType {
 private:
  struct Accepted {};

  class MaybeAccepted {
   public:
    explicit MaybeAccepted(bool accepted) : accepted_(accepted) {}

    MaybeAccepted(const MaybeAccepted& that) = default;

    explicit operator bool() const { return accepted_; }

    Accepted operator*() const { return Accepted(); }

   private:
    bool accepted_;
  };

 public:
  template <
      typename BaseFieldHandlerInitializer,
      std::enable_if_t<std::is_convertible_v<BaseFieldHandlerInitializer&&,
                                             BaseFieldHandler>,
                       int> = 0>
  explicit constexpr DynamicFieldHandlerType(
      int field_number, BaseFieldHandlerInitializer&& base_field_handler)
      : field_number_(field_number),
        base_field_handler_(
            std::forward<BaseFieldHandlerInitializer>(base_field_handler)) {}

  // Implement the field handler protocol.

  static constexpr int kFieldNumber = kDynamicFieldNumber;

  ABSL_ATTRIBUTE_ALWAYS_INLINE
  MaybeAccepted AcceptVarint(int field_number) const {
    return MaybeAccepted(field_number == field_number_);
  }

  template <
      typename... Context,
      std::enable_if_t<
          std::conjunction_v<
              serialized_message_reader_internal::IsUnboundFieldHandler<
                  BaseFieldHandler, Context...>,
              serialized_message_reader_internal::IsStaticFieldHandlerForVarint<
                  BaseFieldHandler, Context...>>,
          int> = 0>
  absl::Status DynamicHandleVarint(Accepted, uint64_t repr,
                                   Context&... context) const {
    return base_field_handler_.HandleVarint(repr, context...);
  }

  ABSL_ATTRIBUTE_ALWAYS_INLINE
  MaybeAccepted AcceptFixed32(int field_number) const {
    return MaybeAccepted(field_number == field_number_);
  }

  template <
      typename... Context,
      std::enable_if_t<
          std::conjunction_v<
              serialized_message_reader_internal::IsUnboundFieldHandler<
                  BaseFieldHandler, Context...>,
              serialized_message_reader_internal::
                  IsStaticFieldHandlerForFixed32<BaseFieldHandler, Context...>>,
          int> = 0>
  absl::Status DynamicHandleFixed32(Accepted, uint32_t repr,
                                    Context&... context) const {
    return base_field_handler_.HandleFixed32(repr, context...);
  }

  ABSL_ATTRIBUTE_ALWAYS_INLINE
  MaybeAccepted AcceptFixed64(int field_number) const {
    return MaybeAccepted(field_number == field_number_);
  }

  template <
      typename... Context,
      std::enable_if_t<
          std::conjunction_v<
              serialized_message_reader_internal::IsUnboundFieldHandler<
                  BaseFieldHandler, Context...>,
              serialized_message_reader_internal::
                  IsStaticFieldHandlerForFixed64<BaseFieldHandler, Context...>>,
          int> = 0>
  absl::Status DynamicHandleFixed64(Accepted, uint64_t repr,
                                    Context&... context) const {
    return base_field_handler_.HandleFixed64(repr, context...);
  }

  ABSL_ATTRIBUTE_ALWAYS_INLINE MaybeAccepted
  AcceptLengthDelimited(int field_number) const {
    return MaybeAccepted(field_number == field_number_);
  }

  template <typename... Context,
            std::enable_if_t<
                std::conjunction_v<
                    serialized_message_reader_internal::IsUnboundFieldHandler<
                        BaseFieldHandler, Context...>,
                    serialized_message_reader_internal::
                        IsStaticFieldHandlerForLengthDelimitedFromReader<
                            BaseFieldHandler, Context...>>,
                int> = 0>
  absl::Status DynamicHandleLengthDelimitedFromReader(
      Accepted, ReaderSpan<> repr, Context&... context) const {
    return base_field_handler_.HandleLengthDelimitedFromReader(std::move(repr),
                                                               context...);
  }

  template <typename... Context,
            std::enable_if_t<
                std::conjunction_v<
                    serialized_message_reader_internal::IsUnboundFieldHandler<
                        BaseFieldHandler, Context...>,
                    serialized_message_reader_internal::
                        IsStaticFieldHandlerForLengthDelimitedFromCord<
                            BaseFieldHandler, Context...>>,
                int> = 0>
  absl::Status DynamicHandleLengthDelimitedFromCord(Accepted,
                                                    CordIteratorSpan repr,
                                                    std::string& scratch,
                                                    Context&... context) const {
    return base_field_handler_.HandleLengthDelimitedFromCord(
        std::move(repr), scratch, context...);
  }

  template <typename... Context,
            std::enable_if_t<
                std::conjunction_v<
                    serialized_message_reader_internal::IsUnboundFieldHandler<
                        BaseFieldHandler, Context...>,
                    serialized_message_reader_internal::
                        IsStaticFieldHandlerForLengthDelimitedFromString<
                            BaseFieldHandler, Context...>>,
                int> = 0>
  absl::Status DynamicHandleLengthDelimitedFromString(
      Accepted, absl::string_view repr, Context&... context) const {
    return base_field_handler_.HandleLengthDelimitedFromString(repr,
                                                               context...);
  }

  ABSL_ATTRIBUTE_ALWAYS_INLINE
  MaybeAccepted AcceptStartGroup(int field_number) const {
    return MaybeAccepted(field_number == field_number_);
  }

  template <typename... Context,
            std::enable_if_t<
                std::conjunction_v<
                    serialized_message_reader_internal::IsUnboundFieldHandler<
                        BaseFieldHandler, Context...>,
                    serialized_message_reader_internal::
                        IsStaticFieldHandlerForStartGroup<BaseFieldHandler,
                                                          Context...>>,
                int> = 0>
  absl::Status DynamicHandleStartGroup(Accepted, Context&... context) const {
    return base_field_handler_.HandleStartGroup(context...);
  }

  ABSL_ATTRIBUTE_ALWAYS_INLINE
  MaybeAccepted AcceptEndGroup(int field_number) const {
    return MaybeAccepted(field_number == field_number_);
  }

  template <typename... Context,
            std::enable_if_t<
                std::conjunction_v<
                    serialized_message_reader_internal::IsUnboundFieldHandler<
                        BaseFieldHandler, Context...>,
                    serialized_message_reader_internal::
                        IsStaticFieldHandlerForEndGroup<BaseFieldHandler,
                                                        Context...>>,
                int> = 0>
  absl::Status DynamicHandleEndGroup(Accepted, Context&... context) const {
    return base_field_handler_.HandleEndGroup(context...);
  }

 private:
  int field_number_;
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS BaseFieldHandler base_field_handler_;
};

}  // namespace riegeli

#endif  // RIEGELI_MESSAGES_DYNAMIC_FIELD_HANDLER_H_
