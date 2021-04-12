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

#ifndef PYTHON_RIEGELI_BYTES_PYTHON_READER_H_
#define PYTHON_RIEGELI_BYTES_PYTHON_READER_H_

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
#include "riegeli/bytes/buffered_reader.h"

namespace riegeli {
namespace python {

// A `Reader` which reads from a Python binary I/O stream.
//
// The stream must support:
//  * `close()`          - for `Close()` if `Options::owns_src()`
//  * `readinto1(memoryview)` or
//    `readinto(memoryview)` or
//    `read1(int)` or
//    `read(int)`
//  * `seekable()`
//  * `seek(int[, int])` - for `Seek()` or `Size()`
//  * `tell()`           - for `Seek()` or `Size()`
//
// `PythonReader` supports random access if
// `Options::assumed_pos() == absl::nullopt` and the stream supports random
// access (this is checked by calling `seekable()`).
//
// Warning: if random access is not supported and the stream is not owned,
// it will have an unpredictable amount of extra data consumed because of
// buffering.
class PythonReader : public BufferedReader {
 public:
  class Options {
   public:
    Options() noexcept {}

    // If `true`, `PythonReader::Close()` closes the stream.
    //
    // Default: `true`.
    Options& set_owns_src(bool owns_src) & {
      owns_src_ = owns_src;
      return *this;
    }
    Options&& set_owns_src(bool owns_src) && {
      return std::move(set_owns_src(owns_src));
    }
    bool owns_src() const { return owns_src_; }

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
    Options& set_assumed_pos(absl::optional<Position> assumed_pos) & {
      assumed_pos_ = assumed_pos;
      return *this;
    }
    Options&& set_assumed_pos(absl::optional<Position> assumed_pos) && {
      return std::move(set_assumed_pos(assumed_pos));
    }
    absl::optional<Position> assumed_pos() const { return assumed_pos_; }

    // Tunes how much data is buffered after reading from the stream.
    //
    // Default: `kDefaultBufferSize` (64K).
    Options& set_buffer_size(size_t buffer_size) & {
      RIEGELI_ASSERT_GT(buffer_size, 0u)
          << "Failed precondition of PythonReader::Options::set_buffer_size(): "
             "zero buffer size";
      buffer_size_ = buffer_size;
      return *this;
    }
    Options&& set_buffer_size(size_t buffer_size) && {
      return std::move(set_buffer_size(buffer_size));
    }
    size_t buffer_size() const { return buffer_size_; }

   private:
    bool owns_src_ = true;
    absl::optional<Position> assumed_pos_;
    size_t buffer_size_ = kDefaultBufferSize;
  };

  // Creates a closed `PythonReader`.
  PythonReader() noexcept {}

  // Will read from `src`.
  explicit PythonReader(PyObject* src, Options options = Options());

  PythonReader(PythonReader&& that) noexcept;
  PythonReader& operator=(PythonReader&& that) noexcept;

  // Returns a borrowed reference to the stream being read from.
  PyObject* src() const { return src_.get(); }

  const Exception& exception() const { return exception_; }

  bool Sync() override;
  bool SupportsRandomAccess() override { return supports_random_access_; }
  bool SupportsSize() override { return supports_random_access_; }
  absl::optional<Position> Size() override;

  // For implementing `tp_traverse` of objects containing `PythonReader`.
  int Traverse(visitproc visit, void* arg);

 protected:
  void Done() override;
  bool ReadInternal(size_t min_length, size_t max_length, char* dest) override;
  bool SeekSlow(Position new_pos) override;

 private:
  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation);
  bool SyncPos();
  absl::optional<Position> SizeInternal();

  PythonPtrLocking src_;
  bool owns_src_ = false;
  bool supports_random_access_ = false;
  Exception exception_;
  PythonPtrLocking read_function_;
  absl::string_view read_function_name_;
  bool use_bytes_ = false;
};

inline PythonReader::PythonReader(PythonReader&& that) noexcept
    : BufferedReader(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      src_(std::move(that.src_)),
      owns_src_(that.owns_src_),
      supports_random_access_(that.supports_random_access_),
      exception_(std::move(that.exception_)),
      read_function_(std::move(that.read_function_)),
      read_function_name_(that.read_function_name_),
      use_bytes_(that.use_bytes_) {}

inline PythonReader& PythonReader::operator=(PythonReader&& that) noexcept {
  BufferedReader::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  src_ = std::move(that.src_);
  owns_src_ = that.owns_src_;
  supports_random_access_ = that.supports_random_access_;
  exception_ = std::move(that.exception_);
  read_function_ = std::move(that.read_function_);
  read_function_name_ = that.read_function_name_;
  use_bytes_ = that.use_bytes_;
  return *this;
}

inline int PythonReader::Traverse(visitproc visit, void* arg) {
  Py_VISIT(src_.get());
  Py_VISIT(read_function_.get());
  return exception_.Traverse(visit, arg);
}

}  // namespace python
}  // namespace riegeli

#endif  // PYTHON_RIEGELI_BYTES_PYTHON_READER_H_
