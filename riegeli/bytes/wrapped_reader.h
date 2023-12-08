// Copyright 2020 Google LLC
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

#ifndef RIEGELI_BYTES_WRAPPED_READER_H_
#define RIEGELI_BYTES_WRAPPED_READER_H_

#include <tuple>
#include <type_traits>

#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/wrapping_reader.h"

namespace riegeli {

template <typename Src = Reader*>
class WrappedReader : public WrappingReader<Src> {
 public:
  using WrappedReader::WrappingReader::WrappingReader;
};

#if __cpp_deduction_guides
explicit WrappedReader(Closed) -> WrappedReader<DeleteCtad<Closed>>;
template <typename Src>
explicit WrappedReader(const Src& src) -> WrappedReader<std::decay_t<Src>>;
template <typename Src>
explicit WrappedReader(Src&& src) -> WrappedReader<std::decay_t<Src>>;
template <typename... SrcArgs>
explicit WrappedReader(std::tuple<SrcArgs...> src_args)
    -> WrappedReader<DeleteCtad<std::tuple<SrcArgs...>>>;
#endif

}  // namespace riegeli

#endif  // RIEGELI_BYTES_WRAPPED_READER_H_
