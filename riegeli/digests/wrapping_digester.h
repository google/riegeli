// Copyright 2023 Google LLC
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

#ifndef RIEGELI_DIGESTS_WRAPPING_DIGESTER_H_
#define RIEGELI_DIGESTS_WRAPPING_DIGESTER_H_

#include <stddef.h>

#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/strings/string_view.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/types.h"
#include "riegeli/digests/digester.h"
#include "riegeli/digests/digester_handle.h"

namespace riegeli {

namespace wrapping_digester_internal {

template <typename From, typename To>
inline To ConvertDigest(From value) {
  return To(std::move(value));
}

}  // namespace wrapping_digester_internal

// Wraps an object providing and possibly owning a digester in a concrete class
// deriving from `Digester<DigestType>`. Possibly converts the digest returned
// by `Digest()` and/or changes its type. Propagates calls to `Close()` if the
// base digester is owned.
//
// `BaseDigester` must support `Dependency<DigesterBaseHandle, BaseDigester>`.
//
// `DigestType` is the new digest type, by default `DigestOf<BaseDigester>`,
// i.e. unchanged.
//
// `digest_converter` is a function used to convert a digest, by default using
// explicit constructor of `DigestType` from `DigestOf<BaseDigester>`.
template <typename BaseDigester, typename DigestType = DigestOf<BaseDigester>,
          DigestType (*digest_converter)(DigestOf<BaseDigester>) =
              wrapping_digester_internal::ConvertDigest<DigestOf<BaseDigester>,
                                                        DigestType>>
class WrappingDigester : public Digester<DigestType> {
 public:
  // Default-constructs the `BaseDigester`.
  template <
      typename DependentBaseDigester = BaseDigester,
      std::enable_if_t<
          std::is_default_constructible<DependentBaseDigester>::value, int> = 0>
  WrappingDigester() : base_(std::forward_as_tuple()) {}

  // Forwards constructor arguments to the `BaseDigester`.
  template <
      typename... Args,
      std::enable_if_t<std::is_constructible<BaseDigester, Args&&...>::value,
                       int> = true>
  explicit WrappingDigester(Args&&... args)
      : base_(std::forward_as_tuple(std::forward<Args>(args)...)) {}

  WrappingDigester(const WrappingDigester& that) = default;
  WrappingDigester& operator=(const WrappingDigester& that) = default;

  WrappingDigester(WrappingDigester&& that) = default;
  WrappingDigester& operator=(WrappingDigester&& that) = default;

 protected:
  void WriteImpl(absl::string_view src) override { base_.get().Write(src); }
  void WriteZerosImpl(riegeli::Position length) override {
    base_.get().WriteZeros(length);
  }
  void Done() override {
    if (base_.is_owning()) base_.get().Close();
  }
  DigestType DigestImpl() override {
    return digest_converter(base_.get().Digest());
  }

 private:
  Dependency<DigesterBaseHandle, BaseDigester> base_;
};

}  // namespace riegeli

#endif  // RIEGELI_DIGESTS_WRAPPING_DIGESTER_H_
