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

#ifndef RIEGELI_TENSORFLOW_IO_TSTRING_WRITER_H_
#define RIEGELI_TENSORFLOW_IO_TSTRING_WRITER_H_

#include <stddef.h>

#include <type_traits>

#include "absl/meta/type_traits.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/resizable_writer.h"
#include "tensorflow/core/platform/tstring.h"

namespace riegeli {
namespace tensorflow {

namespace tstring_internal {

// `ResizableTraits` for `tensorflow::tstring`.
struct TStringResizableTraits {
  using Resizable = ::tensorflow::tstring;
  static char* Data(Resizable& dest) { return dest.mdata(); }
  static size_t Size(const Resizable& dest) { return dest.size(); }
  static constexpr bool kIsStable = false;
  static bool Resize(Resizable& dest, size_t new_size, size_t used_size) {
    RIEGELI_ASSERT_LE(used_size, dest.size())
        << "Failed precondition of ResizableTraits::Resize(): "
           "used size exceeds old size";
    RIEGELI_ASSERT_LE(used_size, new_size)
        << "Failed precondition of ResizableTraits::Resize(): "
           "used size exceeds new size";
    Reserve(dest, new_size, used_size);
    dest.resize_uninitialized(new_size);
    return true;
  }
  static void GrowToCapacity(Resizable& dest) {
    dest.resize_uninitialized(dest.capacity());
  }
  static bool Grow(Resizable& dest, size_t new_size, size_t used_size) {
    RIEGELI_ASSERT_LE(used_size, dest.size())
        << "Failed precondition of ResizableTraits::Grow(): "
           "used size exceeds old size";
    RIEGELI_ASSERT_LE(used_size, new_size)
        << "Failed precondition of ResizableTraits::Grow(): "
           "used size exceeds new size";
    Reserve(dest, new_size, used_size);
    GrowToCapacity(dest);
    return true;
  }

 private:
  static void Reserve(Resizable& dest, size_t new_size, size_t used_size) {
    if (new_size > dest.capacity()) {
      dest.resize_uninitialized(used_size);
      dest.reserve(
          dest.capacity() <= ::tensorflow::tstring().capacity()
              ? new_size
              : UnsignedMax(new_size, dest.capacity() + dest.capacity() / 2));
    }
  }
};

}  // namespace tstring_internal

// Template parameter independent part of `TStringWriter`.
using TStringWriterBase = ResizableWriterBase;

// A `Writer` which appends to a `tensorflow::tstring`, resizing it as
// necessary.
//
// It supports `Seek()` and `ReadMode()`.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the `tensorflow::tstring` being written to. `Dest` must
// support `Dependency<tensorflow::tstring*, Dest>`, e.g.
// `tensorflow::tstring*` (not owned,  default), `tensorflow::tstring` (owned),
// `Any<tensorflow::tstring*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as
// `tensorflow::tstring` if there are no constructor arguments or the only
// argument is `Options`, otherwise as `TargetT` of the type of the first
// constructor argument, except that CTAD is deleted if the first constructor
// argument is a `tensorflow::tstring&` or `const tensorflow::tstring&` (to
// avoid writing to an unintentionally separate copy of an existing object).
// This requires C++17.
//
// The `tensorflow::tstring` must not be accessed until the `TStringWriter` is
// closed or no longer used, except that it is allowed to read the
// `tensorflow::tstring` immediately after `Flush()`.
template <typename Dest = ::tensorflow::tstring*>
class TStringWriter
    : public ResizableWriter<tstring_internal::TStringResizableTraits, Dest> {
 public:
  using TStringWriter::ResizableWriter::ResizableWriter;

  TStringWriter(TStringWriter&& that) = default;
  TStringWriter& operator=(TStringWriter&& that) = default;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit TStringWriter(Closed) -> TStringWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit TStringWriter(Dest&& dest, TStringWriterBase::Options options =
                                        TStringWriterBase::Options())
    -> TStringWriter<std::conditional_t<
        absl::conjunction<
            std::is_lvalue_reference<Dest>,
            std::is_convertible<std::remove_reference_t<Dest>*,
                                const ::tensorflow::tstring*>>::value,
        DeleteCtad<Dest&&>, TargetT<Dest>>>;
explicit TStringWriter(
    TStringWriterBase::Options options = TStringWriterBase::Options())
    -> TStringWriter<::tensorflow::tstring>;
#endif

}  // namespace tensorflow
}  // namespace riegeli

#endif  // RIEGELI_TENSORFLOW_IO_TSTRING_WRITER_H_
