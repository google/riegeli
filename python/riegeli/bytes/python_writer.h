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

#include <stddef.h>

#include <utility>

#include "absl/base/attributes.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "python/riegeli/base/utils.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/buffered_writer.h"

namespace riegeli {
namespace python {

// A `Writer` which writes to a Python binary I/O stream. It supports random
// access unless `Options::set_assumed_pos(pos)`.
//
// The file should support:
//  * `close()`          - for `Close()` unless `Options::set_close(false)`
//  * `write(bytes)`
//  * `flush()`          - for `Flush(FlushType::kFrom{Process,Machine})`
//  * `seek(int[, int])` - unless `Options::set_assumed_pos(pos)`,
//                         or for `Seek()`, `Size()`, or `Truncate()`
//  * `tell()`           - unless `Options::set_assumed_pos(pos)`,
//                         or for `Seek()`, `Size()`, or `Truncate()`
//  * `truncate()`       - for `Truncate()`
class PythonWriter : public BufferedWriter {
 public:
  class Options {
   public:
    Options() noexcept {}

    // If `true`, the file will be closed when the `PythonWriter` is closed.
    //
    // Default: `true`.
    Options& set_close(bool close) & {
      close_ = close;
      return *this;
    }
    Options&& set_close(bool close) && { return std::move(set_close(close)); }
    bool close() const { return close_; }

    // If `absl::nullopt`, `PythonWriter` will initially get the current file
    // position, and will set the final file position on `Close()`. The file
    // must be seekable.
    //
    // If not `absl::nullopt`, this file position will be assumed initially. The
    // file does not have to be seekable.
    //
    // Default: `absl::nullopt`.
    Options& set_assumed_pos(absl::optional<Position> assumed_pos) & {
      assumed_pos_ = assumed_pos;
      return *this;
    }
    Options&& set_assumed_pos(absl::optional<Position> assumed_pos) && {
      return std::move(set_assumed_pos(assumed_pos));
    }
    absl::optional<Position> assumed_pos() const { return assumed_pos_; }

    // Tunes how much data is buffered before writing to the file.
    //
    // Default: 64K
    Options& set_buffer_size(size_t buffer_size) & {
      RIEGELI_ASSERT_GT(buffer_size, 0u)
          << "Failed precondition of PythonWriter::Options::set_buffer_size(): "
             "zero buffer size";
      buffer_size_ = buffer_size;
      return *this;
    }
    Options&& set_buffer_size(size_t buffer_size) && {
      return std::move(set_buffer_size(buffer_size));
    }
    size_t buffer_size() const { return buffer_size_; }

   private:
    bool close_ = true;
    absl::optional<Position> assumed_pos_;
    size_t buffer_size_ = kDefaultBufferSize;
  };

  // Creates a closed `PythonWriter`.
  PythonWriter() noexcept {}

  // Will write to `dest`.
  explicit PythonWriter(PyObject* dest, Options options = Options());

  PythonWriter(PythonWriter&& that) noexcept;
  PythonWriter& operator=(PythonWriter&& that) noexcept;

  // Returns a borrowed reference to the file being written to.
  PyObject* dest() const { return dest_.get(); }

  const Exception& exception() const { return exception_; }

  bool Flush(FlushType flush_type) override;
  bool SupportsRandomAccess() const override { return random_access_; }
  bool Size(Position* size) override;
  bool SupportsTruncate() const override { return random_access_; }
  bool Truncate(Position new_size) override;

  // For implementing `tp_traverse` of objects containing `PythonWriter`.
  int Traverse(visitproc visit, void* arg);

 protected:
  void Done() override;
  bool WriteInternal(absl::string_view src) override;
  bool SeekSlow(Position new_pos) override;

 private:
  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation);
  bool SizeInternal(Position* size);

  PythonPtrLocking dest_;
  bool close_ = false;
  bool random_access_ = false;
  Exception exception_;
  PythonPtrLocking write_function_;
  bool use_bytes_ = false;
};

inline PythonWriter::PythonWriter(PythonWriter&& that) noexcept
    : BufferedWriter(std::move(that)),
      dest_(std::move(that.dest_)),
      close_(that.close_),
      random_access_(that.random_access_),
      exception_(std::move(that.exception_)),
      write_function_(std::move(that.write_function_)),
      use_bytes_(that.use_bytes_) {}

inline PythonWriter& PythonWriter::operator=(PythonWriter&& that) noexcept {
  BufferedWriter::operator=(std::move(that));
  dest_ = std::move(that.dest_);
  close_ = that.close_;
  random_access_ = that.random_access_;
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

}  // namespace python
}  // namespace riegeli

#endif  // PYTHON_RIEGELI_BYTES_PYTHON_WRITER_H_
