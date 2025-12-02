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

#ifndef RIEGELI_MESSAGES_SERIALIZED_MESSAGE_READER_INTERNAL_H_
#define RIEGELI_MESSAGES_SERIALIZED_MESSAGE_READER_INTERNAL_H_

#include <stddef.h>
#include <stdint.h>

#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "riegeli/bytes/limiting_reader.h"
#include "riegeli/bytes/reader.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli::serialized_message_reader_internal {

inline constexpr int kDynamicFieldNumber = -1;

inline constexpr int kUnboundFieldNumber = -2;

template <typename T, typename Enable = void>
struct IsFieldHandlerWithStaticFieldNumber : std::false_type {};

template <typename T>
struct IsFieldHandlerWithStaticFieldNumber<
    T, std::enable_if_t<(T::kFieldNumber > 0)>> : std::true_type {};

template <typename T, typename Enable = void>
struct IsFieldHandlerWithDynamicFieldNumber : std::false_type {};

template <typename T>
struct IsFieldHandlerWithDynamicFieldNumber<
    T, std::enable_if_t<T::kFieldNumber == kDynamicFieldNumber>>
    : std::true_type {};

template <typename T, typename Enable = void>
struct IsFieldHandlerWithUnboundFieldNumber : std::false_type {};

template <typename T>
struct IsFieldHandlerWithUnboundFieldNumber<
    T, std::enable_if_t<(T::kFieldNumber == kUnboundFieldNumber)>>
    : std::true_type {};

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
    : std::disjunction<IsStaticFieldHandlerFromString<T, Context...>,
                       IsDynamicFieldHandlerFromString<T, Context...>> {};

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

}  // namespace riegeli::serialized_message_reader_internal

#endif  // RIEGELI_MESSAGES_SERIALIZED_MESSAGE_READER_INTERNAL_H_
