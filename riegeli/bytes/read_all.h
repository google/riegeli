// Copyright 2022 Google LLC
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

#ifndef RIEGELI_BYTES_READ_ALL_H_
#define RIEGELI_BYTES_READ_ALL_H_

#include <stddef.h>

#include <limits>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/meta/type_traits.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

namespace read_all_internal {

// Combines a status with the result of calling a function of type `Work` with
// the first parameter of type `absl::string_view`, and optionally the second
// parameter of type `const absl::Status&`.
//
// See `ReadAll()` below for details.

template <typename Work, typename Enable = void>
struct StringViewCallResult;

template <typename Work>
struct StringViewCallResult<Work, absl::void_t<decltype(std::declval<Work&&>()(
                                      std::declval<absl::string_view>()))>> {
 private:
  using WorkResult =
      decltype(std::declval<Work&&>()(std::declval<absl::string_view>()));

  using Maker = StatusOrMaker<WorkResult>;

 public:
  using type = typename Maker::type;

  static type Call(absl::Status&& status, Work&& work, absl::string_view dest) {
    return Maker::FromStatusOrWork(std::move(status), [&]() -> WorkResult {
      return std::forward<Work>(work)(dest);
    });
  }

  static void Update(type& result, const absl::Status& status) {
    return Maker::Update(result, status);
  }
};

template <typename Work>
struct StringViewCallResult<Work, absl::void_t<decltype(std::declval<Work&&>()(
                                      std::declval<absl::string_view>(),
                                      std::declval<const absl::Status&>()))>> {
 private:
  using WorkResult = decltype(std::declval<Work&&>()(
      std::declval<absl::string_view>(), std::declval<const absl::Status&>()));

  using Maker = StatusAndMaker<WorkResult>;

 public:
  using type = typename Maker::type;

  static type Call(absl::Status&& status, Work&& work, absl::string_view dest) {
    return Maker::FromStatusAndWork(status, [&]() -> WorkResult {
      return std::forward<Work>(work)(dest, status);
    });
  }

  static void Update(type& result, const absl::Status& status) {
    return Maker::Update(result, status);
  }
};

template <typename Work>
using StringViewCallResultT = typename StringViewCallResult<Work>::type;

}  // namespace read_all_internal

// Combines creating a `Reader` (optionally), reading all remaining data to
// `dest` (clearing any existing data in `dest`), and `VerifyEndAndClose()`
// (if the `Reader` is owned).
//
// If `length_read != nullptr` then sets `*length_read` to the length read.
// This is equal to the difference between `src.pos()` after and before the
// call.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the `Reader`. `Src` must support
// `Dependency<Reader*, Src&&>`, e.g. `Reader&` (not owned),
// `ChainReader<>` (owned), `std::unique_ptr<Reader>` (owned),
// `AnyDependency<Reader*>` (maybe owned).
//
// Reading to `absl::string_view` is supported in three ways:
//
//  1. With `Src` being restricted to `Reader&`, i.e. not owned.
//
//     The `absl::string_view` is valid until the next non-const operation on
//     the `Reader`.
//
//  2. With the `absl::string_view&` output parameter replaced with a function
//     to be called with a parameter of type `absl::string_view`. The function
//     is called with data read, but only if reading succeeded.
//
//     If the `Reader` is owned, it is closed after calling the function.
//     This invalidates the `absl::string_view`.
//
//     For `T` being the result type of the function, the result type of
//     `ReadAll()` generalizes `absl::StatusOr<T>` for types where that is not
//     applicable:
//      * `absl::StatusOr<const T>`           -> `absl::StatusOr<T>`
//      * `absl::StatusOr<T&>`                -> `absl::StatusOr<T*>`
//      * `absl::StatusOr<T&&>`               -> `absl::StatusOr<T*>`
//      * `absl::StatusOr<void>`              -> `absl::Status`
//      * `absl::StatusOr<absl::Status>`      -> `absl::Status`
//      * `absl::StatusOr<absl::StatusOr<T>>` -> `absl::StatusOr<T>`
//
//     The result of `ReadAll()` combines the status of reading, the result of
//     the function, and the status of closing: if one of these failed, returns
//     the first failed status, otherwise returns the result of the function.
//
//  2. With the `absl::string_view&` output parameter replaced with a function
//     to be called with the first parameter of type `absl::string_view` and the
//     second parameter of type `const absl::Status&`. The function is called
//     no matter whether reading succeeded, with possibly partial data and the
//     status of reading.
//
//     If the `Reader` is owned, it is closed after calling the function.
//     This invalidates the `absl::string_view`.
//
//     For `T` being the result type of the function, the result type of
//     `ReadAll()` generalizes `StatusAnd<T>` for types where that is not
//     applicable:
//      * `StatusAnd<void>` -> `absl::Status`
//
//     The result of `ReadAll()` includes the result of the function (as `value`
//     member, unless the function returns `void`) and combines the status of
//     reading with the status of closing (as `status` member, or the whole
//     result if the function returns `void`): if one of these failed, includes
//     the first failed status.
absl::Status ReadAll(Reader& src, absl::string_view& dest,
                     size_t max_length = std::numeric_limits<size_t>::max(),
                     size_t* length_read = nullptr);
absl::Status ReadAll(Reader& src, absl::string_view& dest, size_t* length_read);
template <typename Src, typename Work,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int> = 0>
read_all_internal::StringViewCallResultT<Work> ReadAll(
    Src&& src, Work&& work,
    size_t max_length = std::numeric_limits<size_t>::max(),
    size_t* length_read = nullptr);
template <typename Src, typename Work,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int> = 0>
read_all_internal::StringViewCallResultT<Work> ReadAll(Src&& src, Work&& work,
                                                       size_t* length_read);

// Combines creating a `Reader` (optionally), reading all remaining data to
// `dest` (clearing any existing data in `dest`), and `VerifyEndAndClose()`
// (if the `Reader` is owned).
//
// If `length_read != nullptr` then sets `*length_read` to the length read.
// This is equal to the difference between `src.pos()` after and before the
// call.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the `Reader`. `Src` must support
// `Dependency<Reader*, Src&&>`, e.g. `Reader&` (not owned),
// `ChainReader<>` (owned), `std::unique_ptr<Reader>` (owned),
// `AnyDependency<Reader*>` (maybe owned).
template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int> = 0>
absl::Status ReadAll(Src&& src, char* dest, size_t max_length,
                     size_t* length_read);
template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int> = 0>
absl::Status ReadAll(Src&& src, std::string& dest,
                     size_t max_length = std::numeric_limits<size_t>::max(),
                     size_t* length_read = nullptr);
template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int> = 0>
absl::Status ReadAll(Src&& src, std::string& dest, size_t* length_read);
template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int> = 0>
absl::Status ReadAll(Src&& src, Chain& dest,
                     size_t max_length = std::numeric_limits<size_t>::max(),
                     size_t* length_read = nullptr);
template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int> = 0>
absl::Status ReadAll(Src&& src, Chain& dest, size_t* length_read);
template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int> = 0>
absl::Status ReadAll(Src&& src, absl::Cord& dest,
                     size_t max_length = std::numeric_limits<size_t>::max(),
                     size_t* length_read = nullptr);
template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int> = 0>
absl::Status ReadAll(Src&& src, absl::Cord& dest, size_t* length_read);

// Combines creating a `Reader` (optionally), reading all remaining data to
// `dest` (appending to any existing data in `dest`), and `VerifyEndAndClose()`
// (if the `Reader` is owned).
//
// If `length_read != nullptr` then sets `*length_read` to the length read.
// This is equal to the difference between `pos()` after and before the call.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the `Reader`. `Src` must support
// `Dependency<Reader*, Src&&>`, e.g. `Reader&` (not owned),
// `ChainReader<>` (owned), `std::unique_ptr<Reader>` (owned),
// `AnyDependency<Reader*>` (maybe owned).
template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int> = 0>
absl::Status ReadAndAppendAll(
    Src&& src, std::string& dest,
    size_t max_length = std::numeric_limits<size_t>::max(),
    size_t* length_read = nullptr);
template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int> = 0>
absl::Status ReadAndAppendAll(Src&& src, std::string& dest,
                              size_t* length_read);
template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int> = 0>
absl::Status ReadAndAppendAll(
    Src&& src, Chain& dest,
    size_t max_length = std::numeric_limits<size_t>::max(),
    size_t* length_read = nullptr);
template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int> = 0>
absl::Status ReadAndAppendAll(Src&& src, Chain& dest, size_t* length_read);
template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int> = 0>
absl::Status ReadAndAppendAll(
    Src&& src, absl::Cord& dest,
    size_t max_length = std::numeric_limits<size_t>::max(),
    size_t* length_read = nullptr);
template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int> = 0>
absl::Status ReadAndAppendAll(Src&& src, absl::Cord& dest, size_t* length_read);

// Implementation details follow.

namespace read_all_internal {

absl::Status ReadAllImpl(Reader& src, absl::string_view& dest,
                         size_t max_length, size_t* length_read);
absl::Status ReadAllImpl(Reader& src, char* dest, size_t max_length,
                         size_t* length_read);
absl::Status ReadAllImpl(Reader& src, std::string& dest, size_t max_length,
                         size_t* length_read);
absl::Status ReadAllImpl(Reader& src, Chain& dest, size_t max_length,
                         size_t* length_read);
absl::Status ReadAllImpl(Reader& src, absl::Cord& dest, size_t max_length,
                         size_t* length_read);
absl::Status ReadAndAppendAllImpl(Reader& src, std::string& dest,
                                  size_t max_length, size_t* length_read);
absl::Status ReadAndAppendAllImpl(Reader& src, Chain& dest, size_t max_length,
                                  size_t* length_read);
absl::Status ReadAndAppendAllImpl(Reader& src, absl::Cord& dest,
                                  size_t max_length, size_t* length_read);

template <typename Src, typename Dest>
inline absl::Status ReadAllInternal(Src&& src, Dest& dest, size_t max_length,
                                    size_t* length_read) {
  Dependency<Reader*, Src&&> src_dep(std::forward<Src>(src));
  if (src_dep.is_owning()) src_dep->SetReadAllHint(true);
  absl::Status status = ReadAllImpl(*src_dep, dest, max_length, length_read);
  if (src_dep.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_dep->VerifyEndAndClose())) {
      status.Update(src_dep->status());
    }
  }
  return status;
}

template <typename Src, typename Dest>
inline absl::Status ReadAndAppendAllInternal(Src&& src, Dest& dest,
                                             size_t max_length,
                                             size_t* length_read) {
  Dependency<Reader*, Src&&> src_dep(std::forward<Src>(src));
  if (src_dep.is_owning()) src_dep->SetReadAllHint(true);
  absl::Status status =
      ReadAndAppendAllImpl(*src_dep, dest, max_length, length_read);
  if (src_dep.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_dep->VerifyEndAndClose())) {
      status.Update(src_dep->status());
    }
  }
  return status;
}

}  // namespace read_all_internal

inline absl::Status ReadAll(Reader& src, absl::string_view& dest,
                            size_t max_length, size_t* length_read) {
  return read_all_internal::ReadAllImpl(src, dest, max_length, length_read);
}

inline absl::Status ReadAll(Reader& src, absl::string_view& dest,
                            size_t* length_read) {
  return ReadAll(src, dest, std::numeric_limits<size_t>::max(), length_read);
}

template <typename Src, typename Work,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int>>
inline read_all_internal::StringViewCallResultT<Work> ReadAll(
    Src&& src, Work&& work, size_t max_length, size_t* length_read) {
  Dependency<Reader*, Src&&> src_dep(std::forward<Src>(src));
  if (src_dep.is_owning()) src_dep->SetReadAllHint(true);
  using ResultWithStatus = read_all_internal::StringViewCallResult<Work>;
  absl::string_view dest;
  absl::Status status =
      read_all_internal::ReadAllImpl(*src_dep, dest, max_length, length_read);
  typename ResultWithStatus::type result =
      ResultWithStatus::Call(std::move(status), std::forward<Work>(work), dest);
  if (src_dep.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_dep->VerifyEndAndClose())) {
      ResultWithStatus::Update(result, src_dep->status());
    }
  }
  return result;
}

template <typename Src, typename Work,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int>>
inline read_all_internal::StringViewCallResultT<Work> ReadAll(
    Src&& src, Work&& work, size_t* length_read) {
  return ReadAll(std::forward<Src>(src), std::forward<Work>(work),
                 std::numeric_limits<size_t>::max(), length_read);
}

template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int>>
absl::Status ReadAll(Src&& src, char* dest, size_t max_length,
                     size_t* length_read) {
  Dependency<Reader*, Src&&> src_dep(std::forward<Src>(src));
  if (src_dep.is_owning()) src_dep->SetReadAllHint(true);
  absl::Status status =
      read_all_internal::ReadAllImpl(*src_dep, dest, max_length, length_read);
  if (src_dep.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_dep->VerifyEndAndClose())) {
      status.Update(src_dep->status());
    }
  }
  return status;
}

template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int>>
inline absl::Status ReadAll(Src&& src, std::string& dest, size_t max_length,
                            size_t* length_read) {
  return read_all_internal::ReadAllInternal(std::forward<Src>(src), dest,
                                            max_length, length_read);
}

template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int>>
inline absl::Status ReadAll(Src&& src, std::string& dest, size_t* length_read) {
  return ReadAll(std::forward<Src>(src), dest,
                 std::numeric_limits<size_t>::max(), length_read);
}

template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int>>
inline absl::Status ReadAll(Src&& src, Chain& dest, size_t max_length,
                            size_t* length_read) {
  return read_all_internal::ReadAllInternal(std::forward<Src>(src), dest,
                                            max_length, length_read);
}

template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int>>
inline absl::Status ReadAll(Src&& src, Chain& dest, size_t* length_read) {
  return ReadAll(std::forward<Src>(src), dest,
                 std::numeric_limits<size_t>::max(), length_read);
}

template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int>>
inline absl::Status ReadAll(Src&& src, absl::Cord& dest, size_t max_length,
                            size_t* length_read) {
  return read_all_internal::ReadAllInternal(std::forward<Src>(src), dest,
                                            max_length, length_read);
}

template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int>>
inline absl::Status ReadAll(Src&& src, absl::Cord& dest, size_t* length_read) {
  return ReadAll(std::forward<Src>(src), dest,
                 std::numeric_limits<size_t>::max(), length_read);
}

template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int>>
inline absl::Status ReadAndAppendAll(Src&& src, std::string& dest,
                                     size_t max_length, size_t* length_read) {
  return read_all_internal::ReadAndAppendAllInternal(
      std::forward<Src>(src), dest, max_length, length_read);
}

template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int>>
inline absl::Status ReadAndAppendAll(Src&& src, std::string& dest,
                                     size_t* length_read) {
  return ReadAndAppendAll(std::forward<Src>(src), dest,
                          std::numeric_limits<size_t>::max(), length_read);
}

template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int>>
inline absl::Status ReadAndAppendAll(Src&& src, Chain& dest, size_t max_length,
                                     size_t* length_read) {
  return read_all_internal::ReadAndAppendAllInternal(
      std::forward<Src>(src), dest, max_length, length_read);
}

template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int>>
inline absl::Status ReadAndAppendAll(Src&& src, Chain& dest,
                                     size_t* length_read) {
  return ReadAndAppendAll(std::forward<Src>(src), dest,
                          std::numeric_limits<size_t>::max(), length_read);
}

template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int>>
inline absl::Status ReadAndAppendAll(Src&& src, absl::Cord& dest,
                                     size_t max_length, size_t* length_read) {
  return read_all_internal::ReadAndAppendAllInternal(
      std::forward<Src>(src), dest, max_length, length_read);
}

template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int>>
inline absl::Status ReadAndAppendAll(Src&& src, absl::Cord& dest,
                                     size_t* length_read) {
  return ReadAndAppendAll(std::forward<Src>(src), dest,
                          std::numeric_limits<size_t>::max(), length_read);
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_READ_ALL_H_
