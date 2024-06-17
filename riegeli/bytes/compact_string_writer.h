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

#ifndef RIEGELI_BYTES_COMPACT_STRING_WRITER_H_
#define RIEGELI_BYTES_COMPACT_STRING_WRITER_H_

#include <stddef.h>

#include <type_traits>

#include "absl/meta/type_traits.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/compact_string.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/resizable_writer.h"

namespace riegeli {

namespace compact_string_internal {

// `ResizableTraits` for `CompactString`.
struct CompactStringResizableTraits {
  using Resizable = CompactString;
  static char* Data(Resizable& dest) { return dest.data(); }
  static size_t Size(const Resizable& dest) { return dest.size(); }
  static constexpr bool kIsStable = false;
  static bool Resize(Resizable& dest, size_t new_size, size_t used_size) {
    RIEGELI_ASSERT_LE(used_size, dest.size())
        << "Failed precondition of ResizableTraits::Resize(): "
           "used size exceeds old size";
    RIEGELI_ASSERT_LE(used_size, new_size)
        << "Failed precondition of ResizableTraits::Resize(): "
           "used size exceeds new size";
    dest.resize(new_size, used_size);
    return true;
  }
  static void GrowToCapacity(Resizable& dest) {
    dest.set_size(dest.capacity());
  }
  static bool Grow(Resizable& dest, size_t new_size, size_t used_size) {
    RIEGELI_ASSERT_LE(used_size, dest.size())
        << "Failed precondition of ResizableTraits::Grow(): "
           "used size exceeds old size";
    RIEGELI_ASSERT_LE(used_size, new_size)
        << "Failed precondition of ResizableTraits::Grow(): "
           "used size exceeds new size";
    dest.resize(new_size, used_size);
    GrowToCapacity(dest);
    return true;
  }
};

}  // namespace compact_string_internal

// Template parameter independent part of `CompactStringWriter`.
using CompactStringWriterBase = ResizableWriterBase;

// A `Writer` which appends to a `CompactString`, resizing it as necessary.
//
// It supports `Seek()` and `ReadMode()`.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the `CompactString` being written to. `Dest` must support
// `Dependency<CompactString*, Dest>`, e.g.
// `CompactString*` (not owned, default), `CompactString` (owned),
// `Any<CompactString*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as `CompactString`
// if there are no constructor arguments or the only argument is `Options`,
// otherwise as `InitializerTargetT` of the type of the first constructor
// argument, except that CTAD is deleted if the first constructor argument is a
// `CompactString&` or `const CompactString&` (to avoid writing to an
// unintentionally separate copy of an existing object). This requires C++17.
//
// The `CompactString` must not be accessed until the `CompactStringWriter` is
// closed or no longer used, except that it is allowed to read the
// `CompactString` immediately after `Flush()`.
template <typename Dest = CompactString*>
class CompactStringWriter
    : public ResizableWriter<
          compact_string_internal::CompactStringResizableTraits, Dest> {
 public:
  using CompactStringWriter::ResizableWriter::ResizableWriter;

  CompactStringWriter(CompactStringWriter&& that) = default;
  CompactStringWriter& operator=(CompactStringWriter&& that) = default;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit CompactStringWriter(Closed) -> CompactStringWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit CompactStringWriter(Dest&& dest,
                             CompactStringWriterBase::Options options =
                                 CompactStringWriterBase::Options())
    -> CompactStringWriter<std::conditional_t<
        absl::conjunction<std::is_lvalue_reference<Dest>,
                          std::is_convertible<std::remove_reference_t<Dest>*,
                                              const CompactString*>>::value,
        DeleteCtad<Dest&&>, InitializerTargetT<Dest>>>;
explicit CompactStringWriter(CompactStringWriterBase::Options options =
                                 CompactStringWriterBase::Options())
    -> CompactStringWriter<CompactString>;
#endif

}  // namespace riegeli

#endif  // RIEGELI_BYTES_COMPACT_STRING_WRITER_H_
