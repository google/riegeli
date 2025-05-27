// Copyright 2018 Google LLC
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

#ifndef PYTHON_RIEGELI_BYTES_PYTHON_WRITER_H_
#define PYTHON_RIEGELI_BYTES_PYTHON_WRITER_H_

// From https://docs.python.org/3/c-api/intro.html:
// Since Python may define some pre-processor definitions which affect the
// standard headers on some systems, you must include Python.h before any
// standard headers are included.
#include <Python.h>
// clang-format: do not reorder the above include.

#include <utility>

#include "absl/base/attributes.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "python/riegeli/base/utils.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/buffered_writer.h"

namespace riegeli::python {

// A `Writer` which writes to a Python binary I/O stream.
//
// The stream must support:
//  * `close()`          - for `Close()` if `Options::owns_dest()`
//  * `write(bytes)`
//  * `flush()`          - for `Flush()`
//  * `seekable()`
//  * `seek(int[, int])` - for `Seek()`, `Size()`, or `Truncate()`
//  * `tell()`           - for `Seek()`, `Size()`, or `Truncate()`
//  * `truncate()`       - for `Truncate()`
//
// `PythonWriter` supports random access if
// `Options::assumed_pos() == absl::nullopt` and the stream supports random
// access (this is checked by calling `seekable()`).
class PythonWriter : public BufferedWriter {
 public:
  class Options : public BufferOptionsBase<Options> {
   public:
    Options() noexcept {}

    // If `true`, `PythonWriter::Close()` closes the stream, and
    // `PythonWriter::Flush(flush_type)` flushes the stream even if `flush_type`
    // is `FlushType::kFromObject`.
    //
    // Default: `false`.
    Options& set_owns_dest(bool owns_dest) & ABSL_ATTRIBUTE_LIFETIME_BOUND {
      owns_dest_ = owns_dest;
      return *this;
    }
    Options&& set_owns_dest(bool owns_dest) && ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_owns_dest(owns_dest));
    }
    bool owns_dest() const { return owns_dest_; }

    // If `absl::nullopt`, the current position reported by `pos()` corresponds
    // to the current stream position if possible, otherwise 0 is assumed as the
    // initial position. Random access is supported if the stream supports
    // random access.
    //
    // If not `absl::nullopt`, this position is assumed initially, to be
    // reported by `pos()`. It does not need to correspond to the current stream
    // position. Random access is not supported.
    //
    // Default: `absl::nullopt`.
    Options& set_assumed_pos(absl::optional<Position> assumed_pos) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      assumed_pos_ = assumed_pos;
      return *this;
    }
    Options&& set_assumed_pos(absl::optional<Position> assumed_pos) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_assumed_pos(assumed_pos));
    }
    absl::optional<Position> assumed_pos() const { return assumed_pos_; }

   private:
    bool owns_dest_ = false;
    absl::optional<Position> assumed_pos_;
  };

  // Creates a closed `PythonWriter`.
  explicit PythonWriter(Closed) noexcept : BufferedWriter(kClosed) {}

  // Will write to `dest`.
  explicit PythonWriter(PyObject* dest, Options options = Options());

  PythonWriter(PythonWriter&& that) noexcept;
  PythonWriter& operator=(PythonWriter&& that) noexcept;

  // Returns a borrowed reference to the stream being written to.
  PyObject* dest() const ABSL_ATTRIBUTE_LIFETIME_BOUND { return dest_.get(); }

  const Exception& exception() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return exception_;
  }

  bool SupportsRandomAccess() override { return supports_random_access_; }

  // For implementing `tp_traverse` of objects containing `PythonWriter`.
  int Traverse(visitproc visit, void* arg);

 protected:
  void Done() override;
  bool WriteInternal(absl::string_view src) override;
  bool FlushImpl(FlushType flush_type) override;
  bool SeekBehindBuffer(Position new_pos) override;
  absl::optional<Position> SizeBehindBuffer() override;
  bool TruncateBehindBuffer(Position new_size) override;

 private:
  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation);
  absl::optional<Position> SizeInternal();

  PythonPtrLocking dest_;
  bool owns_dest_ = false;
  bool supports_random_access_ = false;
  absl::Status random_access_status_;
  Exception exception_;
  PythonPtrLocking write_function_;
  bool use_bytes_ = false;
};

inline PythonWriter::PythonWriter(PythonWriter&& that) noexcept
    : BufferedWriter(static_cast<BufferedWriter&&>(that)),
      dest_(std::move(that.dest_)),
      owns_dest_(that.owns_dest_),
      supports_random_access_(
          std::exchange(that.supports_random_access_, false)),
      random_access_status_(std::move(that.random_access_status_)),
      exception_(std::move(that.exception_)),
      write_function_(std::move(that.write_function_)),
      use_bytes_(that.use_bytes_) {}

inline PythonWriter& PythonWriter::operator=(PythonWriter&& that) noexcept {
  BufferedWriter::operator=(static_cast<BufferedWriter&&>(that));
  dest_ = std::move(that.dest_);
  owns_dest_ = that.owns_dest_;
  supports_random_access_ = std::exchange(that.supports_random_access_, false);
  random_access_status_ = std::move(that.random_access_status_);
  exception_ = std::move(that.exception_);
  write_function_ = std::move(that.write_function_);
  use_bytes_ = that.use_bytes_;
  return *this;
}

inline int PythonWriter::Traverse(visitproc visit, void* arg) {
  Py_VISIT(dest_.get());
  Py_VISIT(write_function_.get());
  return exception_.Traverse(visit, arg);
}

}  // namespace riegeli::python

#endif  // PYTHON_RIEGELI_BYTES_PYTHON_WRITER_H_
