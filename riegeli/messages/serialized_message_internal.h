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

#ifndef RIEGELI_MESSAGES_SERIALIZED_MESSAGE_INTERNAL_
#define RIEGELI_MESSAGES_SERIALIZED_MESSAGE_INTERNAL_

#include <stddef.h>
#include <stdint.h>

#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/meta/type_traits.h"
#include "absl/status/status.h"
#include "riegeli/base/type_erased_ref.h"
#include "riegeli/base/type_traits.h"

namespace riegeli {

class LimitingReaderBase;
class SerializedMessageWriter;

namespace serialized_message_internal {

template <typename Enable, typename Context, typename Action, typename... Args>
struct IsInvocableWithContextImpl : std::false_type {};

template <typename Context, typename Action, typename... Args>
struct IsInvocableWithContextImpl<std::enable_if_t<!std::is_void_v<Context>>,
                                  Context, Action, Args...>
    : is_invocable_r<absl::Status, Action, Args..., Context&> {};

template <typename Context, typename Action, typename... Args>
struct IsInvocableWithContext
    : IsInvocableWithContextImpl<void, Context, Action, Args...> {};

template <typename Action, typename... Args>
struct IsInvocableWithoutContext
    : is_invocable_r<absl::Status, Action, Args...> {};

template <typename Context, typename Action, typename... Args>
struct IsAction
    : absl::disjunction<IsInvocableWithContext<Context, Action, Args...>,
                        IsInvocableWithoutContext<Action, Args...>> {};

template <typename Context, typename Action, typename... Args,
          std::enable_if_t<
              IsInvocableWithContext<Context, Action, Args...>::value, int> = 0>
inline absl::Status InvokeAction(TypeErasedRef context, Action&& action,
                                 Args&&... args) {
  return riegeli::invoke(std::forward<Action>(action),
                         std::forward<Args>(args)..., context.Cast<Context&>());
}

template <typename Context, typename Action, typename... Args,
          std::enable_if_t<IsInvocableWithoutContext<Action, Args...>::value,
                           int> = 0>
inline absl::Status InvokeAction(ABSL_ATTRIBUTE_UNUSED TypeErasedRef context,
                                 Action&& action, Args&&... args) {
  return riegeli::invoke(std::forward<Action>(action),
                         std::forward<Args>(args)...);
}

template <typename Context, typename Action, typename... Args>
struct IsActionWithSrc
    : IsAction<Context, Action, Args..., LimitingReaderBase&> {};

template <typename Context, typename Action, typename... Args>
struct IsActionWithoutSrc : IsAction<Context, Action, Args...> {};

template <typename Context, typename Action, typename... Args>
struct IsActionWithOptionalSrc
    : absl::disjunction<IsActionWithSrc<Context, Action, Args...>,
                        IsActionWithoutSrc<Context, Action, Args...>> {};

template <
    typename Context, typename Action, typename... Args,
    std::enable_if_t<IsActionWithSrc<Context, Action, Args...>::value, int> = 0>
inline absl::Status InvokeActionWithSrc(LimitingReaderBase& src,
                                        TypeErasedRef context, Action&& action,
                                        Args&&... args) {
  return InvokeAction<Context>(context, std::forward<Action>(action),
                               std::forward<Args>(args)..., src);
}

template <typename Context, typename Action, typename... Args,
          std::enable_if_t<IsActionWithoutSrc<Context, Action, Args...>::value,
                           int> = 0>
inline absl::Status InvokeActionWithSrc(
    ABSL_ATTRIBUTE_UNUSED LimitingReaderBase& src, TypeErasedRef context,
    Action&& action, Args&&... args) {
  return InvokeAction<Context>(context, std::forward<Action>(action),
                               std::forward<Args>(args)...);
}

template <typename Context, typename Action, typename... Args>
struct IsActionWithDest
    : IsAction<Context, Action, Args..., SerializedMessageWriter&> {};

template <typename Context, typename Action, typename... Args>
struct IsActionWithoutDest : IsAction<Context, Action, Args...> {};

template <typename Context, typename Action, typename... Args>
struct IsActionWithOptionalDest
    : absl::disjunction<IsActionWithDest<Context, Action, Args...>,
                        IsActionWithoutDest<Context, Action, Args...>> {};

template <typename Context, typename Action, typename... Args,
          std::enable_if_t<IsActionWithDest<Context, Action, Args...>::value,
                           int> = 0>
inline absl::Status InvokeActionWithDest(SerializedMessageWriter& dest,
                                         TypeErasedRef context, Action&& action,
                                         Args&&... args) {
  return InvokeAction<Context>(context, std::forward<Action>(action),
                               std::forward<Args>(args)..., dest);
}

template <typename Context, typename Action, typename... Args,
          std::enable_if_t<IsActionWithoutDest<Context, Action, Args...>::value,
                           int> = 0>
inline absl::Status InvokeActionWithDest(
    ABSL_ATTRIBUTE_UNUSED SerializedMessageWriter& dest, TypeErasedRef context,
    Action&& action, Args&&... args) {
  return InvokeAction<Context>(context, std::forward<Action>(action),
                               std::forward<Args>(args)...);
}

template <typename Context, typename Action, typename... Args>
struct IsActionWithRequiredSrcAndOptionalDest
    : IsActionWithOptionalDest<Context, Action, Args..., LimitingReaderBase&> {
};

template <typename Context, typename Action, typename... Args>
struct IsActionWithOptionalSrcAndDest
    : absl::disjunction<
          IsActionWithRequiredSrcAndOptionalDest<Context, Action, Args...>,
          IsActionWithOptionalDest<Context, Action, Args...>> {};

template <typename Context, typename Action, typename... Args,
          std::enable_if_t<IsActionWithRequiredSrcAndOptionalDest<
                               Context, Action, Args...>::value,
                           int> = 0>
inline absl::Status InvokeActionWithSrcAndDest(LimitingReaderBase& src,
                                               SerializedMessageWriter& dest,
                                               TypeErasedRef context,
                                               Action&& action,
                                               Args&&... args) {
  return InvokeActionWithDest<Context>(dest, context,
                                       std::forward<Action>(action),
                                       std::forward<Args>(args)..., src);
}

template <
    typename Context, typename Action, typename... Args,
    std::enable_if_t<IsActionWithOptionalDest<Context, Action, Args...>::value,
                     int> = 0>
inline absl::Status InvokeActionWithSrcAndDest(
    ABSL_ATTRIBUTE_UNUSED LimitingReaderBase& src,
    SerializedMessageWriter& dest, TypeErasedRef context, Action&& action,
    Args&&... args) {
  return InvokeActionWithDest<Context>(
      dest, context, std::forward<Action>(action), std::forward<Args>(args)...);
}

}  // namespace serialized_message_internal

}  // namespace riegeli

#endif  // RIEGELI_MESSAGES_SERIALIZED_MESSAGE_INTERNAL_
