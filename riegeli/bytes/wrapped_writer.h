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

#ifndef RIEGELI_BYTES_WRAPPED_WRITER_H_
#define RIEGELI_BYTES_WRAPPED_WRITER_H_

#include <tuple>
#include <type_traits>

#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/wrapping_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

template <typename Dest = Writer*>
class WrappedWriter : public WrappingWriter<Dest> {
 public:
  using WrappedWriter::WrappingWriter::WrappingWriter;
};

#if __cpp_deduction_guides
explicit WrappedWriter(Closed) -> WrappedWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit WrappedWriter(const Dest& dest) -> WrappedWriter<std::decay_t<Dest>>;
template <typename Dest>
explicit WrappedWriter(Dest&& dest) -> WrappedWriter<std::decay_t<Dest>>;
template <typename... DestArgs>
explicit WrappedWriter(std::tuple<DestArgs...> dest_args)
    -> WrappedWriter<DeleteCtad<std::tuple<DestArgs...>>>;
#endif

}  // namespace riegeli

#endif  // RIEGELI_BYTES_WRAPPED_WRITER_H_
