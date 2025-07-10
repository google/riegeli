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

#ifndef RIEGELI_BYTES_STRINGIFY_WRITER_H_
#define RIEGELI_BYTES_STRINGIFY_WRITER_H_

#include <limits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/reset.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/prefix_limiting_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// A `Writer` which writes to a sink provided to `AbslStringify()`.
//
// The template parameter is the sink pointer type rather than the sink value
// type for consistency with other templated `Writer` classes parameterized by
// the type of the constructor argument. This must nevertheless be a pointer,
// not an arbitrary type supporting `Dependency`.
//
// `Dest` must support `->Append(absl::string_view)`.
template <typename Dest>
class StringifyWriter : public BufferedWriter {
 public:
  // Creates a closed `StringifyWriter`.
  explicit StringifyWriter(Closed) noexcept : BufferedWriter(kClosed) {}

  // Will write to `*dest`.
  explicit StringifyWriter(Dest dest ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : dest_(std::move(RIEGELI_EVAL_ASSERT_NOTNULL(dest))) {}

  StringifyWriter(StringifyWriter&& that) = default;
  StringifyWriter& operator=(StringifyWriter&& that) = default;

  // Makes `*this` equivalent to a newly constructed `StringifyWriter`. This
  // avoids constructing a temporary `StringifyWriter` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Dest dest);

  // Returns a pointer to the sink being written to. Unchanged by `Close()`.
  Dest& dest() ABSL_ATTRIBUTE_LIFETIME_BOUND { return dest_; }
  const Dest& dest() const ABSL_ATTRIBUTE_LIFETIME_BOUND { return dest_; }

 protected:
  bool WriteInternal(absl::string_view src) override;

 private:
  Dest dest_{};
};

// Specialization of `StringifyWriter<WriterStringifySink*>` which
// avoids wrapping a `Writer` in a `WriterStringifySink` and adapting it
// back to a `Writer`.
template <>
class StringifyWriter<WriterStringifySink*> : public PrefixLimitingWriter<> {
 public:
  // Creates a closed `StringifyWriter`.
  explicit StringifyWriter(Closed) noexcept : PrefixLimitingWriter(kClosed) {}

  // Will write to `*dest`.
  explicit StringifyWriter(
      WriterStringifySink* dest ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : PrefixLimitingWriter(RIEGELI_EVAL_ASSERT_NOTNULL(dest)->dest()),
        dest_(dest) {}

  StringifyWriter(StringifyWriter&& that) = default;
  StringifyWriter& operator=(StringifyWriter&& that) = default;

  // Makes `*this` equivalent to a newly constructed `StringifyWriter`. This
  // avoids constructing a temporary `StringifyWriter` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(WriterStringifySink* dest);

  // Returns a pointer to the sink being written to. Unchanged by `Close()`.
  WriterStringifySink*& dest() ABSL_ATTRIBUTE_LIFETIME_BOUND { return dest_; }
  WriterStringifySink* const& dest() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return dest_;
  }

 private:
  WriterStringifySink* dest_{};
};

explicit StringifyWriter(Closed) -> StringifyWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit StringifyWriter(Dest dest) -> StringifyWriter<Dest>;

// Implementation details follow.

template <typename Dest>
inline void StringifyWriter<Dest>::Reset(Closed) {
  BufferedWriter::Reset(kClosed);
  riegeli::Reset(dest_);
}

template <typename Dest>
inline void StringifyWriter<Dest>::Reset(Dest dest) {
  BufferedWriter::Reset();
  dest_ = std::move(dest);
}

template <typename Dest>
bool StringifyWriter<Dest>::WriteInternal(absl::string_view src) {
  RIEGELI_ASSERT(!src.empty())
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "nothing to write";
  RIEGELI_ASSERT_OK(*this)
      << "Failed precondition of BufferedWriter::WriteInternal()";
  if (ABSL_PREDICT_FALSE(src.size() >
                         std::numeric_limits<Position>::max() - start_pos())) {
    return FailOverflow();
  }
  dest_->Append(src);
  move_start_pos(src.size());
  return true;
}

inline void StringifyWriter<WriterStringifySink*>::Reset(Closed) {
  PrefixLimitingWriter::Reset(kClosed);
  dest_ = nullptr;
}

inline void StringifyWriter<WriterStringifySink*>::Reset(
    WriterStringifySink* dest) {
  PrefixLimitingWriter::Reset(dest->dest());
  dest_ = dest;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_STRINGIFY_WRITER_H_
