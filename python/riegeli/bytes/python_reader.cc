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

// From https://docs.python.org/3/c-api/intro.html:
// Since Python may define some pre-processor definitions which affect the
// standard headers on some systems, you must include Python.h before any
// standard headers are included.
#define PY_SSIZE_T_CLEAN
#include <Python.h>
// clang-format: do not reorder the above include.

#include "python/riegeli/bytes/python_reader.h"
// clang-format: do not reorder the above include.

#include <stddef.h>

#include <cstring>
#include <limits>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "python/riegeli/base/utils.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/no_destructor.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffered_reader.h"

namespace riegeli {
namespace python {

PythonReader::PythonReader(PyObject* src, Options options)
    : BufferedReader(options.buffer_options()), owns_src_(options.owns_src()) {
  PythonLock::AssertHeld();
  Py_INCREF(src);
  src_.reset(src);
  if (options.assumed_pos() != absl::nullopt) {
    set_limit_pos(*options.assumed_pos());
    // `supports_random_access_` is left as `false`.
    static const NoDestructor<absl::Status> status(absl::UnimplementedError(
        "PythonReader::Options::assumed_pos() excludes random access"));
    random_access_status_ = *status;
  } else {
    static constexpr Identifier id_seekable("seekable");
    const PythonPtr seekable_result(
        PyObject_CallMethodObjArgs(src_.get(), id_seekable.get(), nullptr));
    if (ABSL_PREDICT_FALSE(seekable_result == nullptr)) {
      FailOperation("seekable()");
      return;
    }
    const int seekable_is_true = PyObject_IsTrue(seekable_result.get());
    if (ABSL_PREDICT_FALSE(seekable_is_true < 0)) {
      FailOperation("PyObject_IsTrue() after seekable()");
      return;
    }
    if (seekable_is_true == 0) {
      // Random access is not supported. Assume 0 as the initial position.
      // `supports_random_access_` is left as `false`.
      static const NoDestructor<absl::Status> status(absl::UnimplementedError(
          "seekable() is False which excludes random access"));
      random_access_status_ = *status;
      return;
    }
    static constexpr Identifier id_tell("tell");
    const PythonPtr tell_result(
        PyObject_CallMethodObjArgs(src_.get(), id_tell.get(), nullptr));
    if (ABSL_PREDICT_FALSE(tell_result == nullptr)) {
      FailOperation("tell()");
      return;
    }
    const absl::optional<Position> file_pos =
        PositionFromPython(tell_result.get());
    if (ABSL_PREDICT_FALSE(file_pos == absl::nullopt)) {
      FailOperation("PositionFromPython() after tell()");
      return;
    }
    set_limit_pos(*file_pos);
    supports_random_access_ = true;
  }
  BeginRun();
}

void PythonReader::Done() {
  BufferedReader::Done();
  random_access_status_ = absl::OkStatus();
  if (owns_src_ && src_ != nullptr) {
    PythonLock lock;
    static constexpr Identifier id_close("close");
    const PythonPtr close_result(
        PyObject_CallMethodObjArgs(src_.get(), id_close.get(), nullptr));
    if (ABSL_PREDICT_FALSE(close_result == nullptr)) FailOperation("close()");
  }
}

bool PythonReader::FailOperation(absl::string_view operation) {
  RIEGELI_ASSERT(is_open())
      << "Failed precondition of PythonReader::FailOperation(): "
         "Object closed";
  PythonLock::AssertHeld();
  if (ABSL_PREDICT_FALSE(!ok())) {
    // Ignore this error because `PythonReader` already failed.
    PyErr_Clear();
    return false;
  }
  exception_ = Exception::Fetch();
  return Fail(absl::UnknownError(
      absl::StrCat(operation, " failed: ", exception_.message())));
}

bool PythonReader::ReadInternal(size_t min_length, size_t max_length,
                                char* dest) {
  RIEGELI_ASSERT_GT(min_length, 0u)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "nothing to read";
  RIEGELI_ASSERT_GE(max_length, min_length)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "max_length < min_length";
  RIEGELI_ASSERT(ok())
      << "Failed precondition of BufferedReader::ReadInternal(): " << status();
  PythonLock lock;
  // Find a read function to use, preferring in order: `readinto1()`,
  // `readinto()`, `read1()`, `read()`.
  if (ABSL_PREDICT_FALSE(read_function_ == nullptr)) {
    static constexpr Identifier id_readinto1("readinto1");
    read_function_.reset(PyObject_GetAttr(src_.get(), id_readinto1.get()));
    read_function_name_ = "readinto1()";
    if (read_function_ == nullptr) {
      if (ABSL_PREDICT_FALSE(!PyErr_ExceptionMatches(PyExc_AttributeError))) {
        return FailOperation(read_function_name_);
      }
      PyErr_Clear();
      static constexpr Identifier id_readinto("readinto");
      read_function_.reset(PyObject_GetAttr(src_.get(), id_readinto.get()));
      read_function_name_ = "readinto()";
      if (read_function_ == nullptr) {
        if (ABSL_PREDICT_FALSE(!PyErr_ExceptionMatches(PyExc_AttributeError))) {
          return FailOperation(read_function_name_);
        }
        PyErr_Clear();
        use_bytes_ = true;
        static constexpr Identifier id_read1("read1");
        read_function_.reset(PyObject_GetAttr(src_.get(), id_read1.get()));
        read_function_name_ = "read1()";
        if (read_function_ == nullptr) {
          if (ABSL_PREDICT_FALSE(
                  !PyErr_ExceptionMatches(PyExc_AttributeError))) {
            return FailOperation(read_function_name_);
          }
          PyErr_Clear();
          static constexpr Identifier id_read("read");
          read_function_.reset(PyObject_GetAttr(src_.get(), id_read.get()));
          read_function_name_ = "read()";
          if (ABSL_PREDICT_FALSE(read_function_ == nullptr)) {
            return FailOperation(read_function_name_);
          }
        }
      }
    }
  }
  for (;;) {
    if (ABSL_PREDICT_FALSE(limit_pos() ==
                           std::numeric_limits<Position>::max())) {
      return FailOverflow();
    }
    const size_t length_to_read = UnsignedMin(
        max_length, std::numeric_limits<Position>::max() - limit_pos(),
        size_t{std::numeric_limits<Py_ssize_t>::max()});
    size_t length_read;
    if (!use_bytes_) {
      PythonPtr read_result;
      {
        // Prefer using `readinto1()` or `readinto()` to avoid copying memory.
        MemoryView memory_view;
        PyObject* const memory_view_object =
            memory_view.MutableToPython(absl::MakeSpan(dest, length_to_read));
        if (ABSL_PREDICT_FALSE(memory_view_object == nullptr)) {
          return FailOperation("MemoryView::MutableToPython()");
        }
        read_result.reset(PyObject_CallFunctionObjArgs(
            read_function_.get(), memory_view_object, nullptr));
        if (ABSL_PREDICT_FALSE(read_result == nullptr)) {
          return FailOperation(read_function_name_);
        }
        if (ABSL_PREDICT_FALSE(!memory_view.Release())) {
          return FailOperation("MemoryView::Release()");
        }
      }
      const absl::optional<size_t> length_read_opt =
          SizeFromPython(read_result.get());
      if (ABSL_PREDICT_FALSE(length_read_opt == absl::nullopt)) {
        return FailOperation(
            absl::StrCat("SizeFromPython() after ", read_function_name_));
      }
      length_read = *length_read_opt;
      if (ABSL_PREDICT_FALSE(length_read == 0)) return false;
      if (ABSL_PREDICT_FALSE(length_read > max_length)) {
        return Fail(absl::InternalError(
            absl::StrCat(read_function_name_, " read more than requested")));
      }
    } else {
      const PythonPtr length(SizeToPython(length_to_read));
      if (ABSL_PREDICT_FALSE(length == nullptr)) {
        return FailOperation("SizeToPython()");
      }
      const PythonPtr read_result(PyObject_CallFunctionObjArgs(
          read_function_.get(), length.get(), nullptr));
      if (ABSL_PREDICT_FALSE(read_result == nullptr)) {
        return FailOperation(read_function_name_);
      }
      Py_buffer buffer;
      if (ABSL_PREDICT_FALSE(PyObject_GetBuffer(read_result.get(), &buffer,
                                                PyBUF_CONTIG_RO) < 0)) {
        return FailOperation(
            absl::StrCat("PyObject_GetBuffer() after ", read_function_name_));
      }
      if (ABSL_PREDICT_FALSE(buffer.len == 0)) {
        PyBuffer_Release(&buffer);
        return false;
      }
      if (ABSL_PREDICT_FALSE(IntCast<size_t>(buffer.len) > max_length)) {
        PyBuffer_Release(&buffer);
        return Fail(absl::InternalError(
            absl::StrCat(read_function_name_, " read more than requested")));
      }
      std::memcpy(dest, buffer.buf, IntCast<size_t>(buffer.len));
      length_read = IntCast<size_t>(buffer.len);
      PyBuffer_Release(&buffer);
    }
    move_limit_pos(length_read);
    if (length_read >= min_length) return true;
    dest += length_read;
    min_length -= length_read;
    max_length -= length_read;
  }
}

bool PythonReader::SeekBehindBuffer(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of BufferedReader::SeekBehindBuffer(): "
         "position in the buffer, use Seek() instead";
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedReader::SeekBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!PythonReader::SupportsRandomAccess())) {
    if (ABSL_PREDICT_FALSE(new_pos < start_pos())) {
      if (ok()) Fail(random_access_status_);
      return false;
    }
    return BufferedReader::SeekBehindBuffer(new_pos);
  }
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  PythonLock lock;
  if (new_pos > limit_pos()) {
    // Seeking forwards.
    const absl::optional<Position> size = SizeInternal();
    if (ABSL_PREDICT_FALSE(size == absl::nullopt)) return false;
    if (ABSL_PREDICT_FALSE(new_pos > *size)) {
      // File ends.
      set_limit_pos(*size);
      return false;
    }
  }
  set_limit_pos(new_pos);
  const PythonPtr file_pos = PositionToPython(limit_pos());
  if (ABSL_PREDICT_FALSE(file_pos == nullptr)) {
    return FailOperation("PositionToPython()");
  }
  static constexpr Identifier id_seek("seek");
  const PythonPtr seek_result(PyObject_CallMethodObjArgs(
      src_.get(), id_seek.get(), file_pos.get(), nullptr));
  if (ABSL_PREDICT_FALSE(seek_result == nullptr)) {
    return FailOperation("seek()");
  }
  return true;
}

inline absl::optional<Position> PythonReader::SizeInternal() {
  RIEGELI_ASSERT(ok())
      << "Failed precondition of PythonReader::SizeInternal(): " << status();
  RIEGELI_ASSERT(PythonReader::SupportsRandomAccess())
      << "Failed precondition of PythonReader::SizeInternal(): "
         "random access not supported";
  PythonLock::AssertHeld();
  absl::string_view operation;
  const PythonPtr file_pos = PositionToPython(0);
  if (ABSL_PREDICT_FALSE(file_pos == nullptr)) {
    FailOperation("PositionToPython()");
    return absl::nullopt;
  }
  const PythonPtr whence = IntToPython(2);  // `io.SEEK_END`
  if (ABSL_PREDICT_FALSE(whence == nullptr)) {
    FailOperation("IntToPython()");
    return absl::nullopt;
  }
  static constexpr Identifier id_seek("seek");
  PythonPtr result(PyObject_CallMethodObjArgs(
      src_.get(), id_seek.get(), file_pos.get(), whence.get(), nullptr));
  if (result.get() == Py_None) {
    // Python2 `file.seek()` returns `None`, so `tell()` is needed to get the
    // new position. Python2 is dead, but some classes still behave like that.
    static constexpr Identifier id_tell("tell");
    result.reset(
        PyObject_CallMethodObjArgs(src_.get(), id_tell.get(), nullptr));
    operation = "tell()";
  } else {
    // `io.IOBase.seek()` returns the new position.
    operation = "seek()";
  }
  if (ABSL_PREDICT_FALSE(result == nullptr)) {
    FailOperation(operation);
    return absl::nullopt;
  }
  const absl::optional<Position> size = PositionFromPython(result.get());
  if (ABSL_PREDICT_FALSE(size == absl::nullopt)) {
    FailOperation(absl::StrCat("PositionFromPython() after ", operation));
    return absl::nullopt;
  }
  return *size;
}

absl::optional<Position> PythonReader::SizeImpl() {
  if (ABSL_PREDICT_FALSE(!PythonReader::SupportsRandomAccess())) {
    if (ok()) Fail(random_access_status_);
    return absl::nullopt;
  }
  if (ABSL_PREDICT_FALSE(!ok())) return absl::nullopt;
  PythonLock lock;
  const absl::optional<Position> size = SizeInternal();
  if (ABSL_PREDICT_FALSE(size == absl::nullopt)) return absl::nullopt;
  const PythonPtr file_pos = PositionToPython(limit_pos());
  if (ABSL_PREDICT_FALSE(file_pos == nullptr)) {
    FailOperation("PositionToPython()");
    return absl::nullopt;
  }
  static constexpr Identifier id_seek("seek");
  const PythonPtr seek_result(PyObject_CallMethodObjArgs(
      src_.get(), id_seek.get(), file_pos.get(), nullptr));
  if (ABSL_PREDICT_FALSE(seek_result == nullptr)) {
    FailOperation("seek()");
    return absl::nullopt;
  }
  return *size;
}

}  // namespace python
}  // namespace riegeli
