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

#include "absl/types/span.h"
#include "python/riegeli/bytes/python_reader.h"
// clang-format: do not reorder the above include.

#include <stddef.h>

#include <cstring>
#include <limits>
#include <memory>
#include <string>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "python/riegeli/base/utils.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/buffered_reader.h"

namespace riegeli {
namespace python {

PythonReader::PythonReader(PyObject* src, Options options)
    : BufferedReader(options.buffer_size()),
      close_(options.close()),
      random_access_(options.assumed_pos() == absl::nullopt) {
  PythonLock::AssertHeld();
  Py_INCREF(src);
  src_.reset(src);
  if (!random_access_) {
    set_limit_pos(*options.assumed_pos());
  } else {
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
  }
}

bool PythonReader::FailOperation(absl::string_view operation) {
  RIEGELI_ASSERT(is_open())
      << "Failed precondition of PythonReader::FailOperation(): "
         "Object closed";
  PythonLock::AssertHeld();
  if (ABSL_PREDICT_FALSE(!healthy())) {
    // Ignore this error because `PythonReader` already failed.
    PyErr_Clear();
    return false;
  }
  exception_ = Exception::Fetch();
  return Fail(absl::UnknownError(
      absl::StrCat(operation, " failed: ", exception_.message())));
}

bool PythonReader::SyncPos() {
  PythonLock lock;
  const PythonPtr file_pos = PositionToPython(pos());
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

void PythonReader::Done() {
  if (ABSL_PREDICT_TRUE(healthy()) && random_access_) SyncPos();
  BufferedReader::Done();
  if (close_ && src_ != nullptr) {
    PythonLock lock;
    static constexpr Identifier id_close("close");
    const PythonPtr close_result(
        PyObject_CallMethodObjArgs(src_.get(), id_close.get(), nullptr));
    if (ABSL_PREDICT_FALSE(close_result == nullptr)) FailOperation("close()");
  }
}

bool PythonReader::ReadInternal(size_t min_length, size_t max_length,
                                char* dest) {
  RIEGELI_ASSERT_GT(min_length, 0u)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "nothing to read";
  RIEGELI_ASSERT_GE(max_length, min_length)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "max_length < min_length";
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of BufferedReader::ReadInternal(): " << status();
  if (ABSL_PREDICT_FALSE(max_length >
                         std::numeric_limits<Position>::max() - limit_pos())) {
    return FailOverflow();
  }
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
    const size_t length_to_read =
        UnsignedMin(max_length, size_t{std::numeric_limits<Py_ssize_t>::max()});
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

bool PythonReader::Sync() {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (!random_access_) return true;
  const bool ok = SyncPos();
  set_limit_pos(pos());
  ClearBuffer();
  return ok;
}

bool PythonReader::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of Reader::SeekSlow(): "
         "position in the buffer, use Seek() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(!random_access_)) {
    return BufferedReader::SeekSlow(new_pos);
  }
  ClearBuffer();
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

absl::optional<Position> PythonReader::Size() {
  if (ABSL_PREDICT_FALSE(!healthy())) return absl::nullopt;
  if (ABSL_PREDICT_FALSE(!random_access_)) {
    Fail(absl::UnimplementedError("PythonReader::Size() not supported"));
    return absl::nullopt;
  }
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

inline absl::optional<Position> PythonReader::SizeInternal() {
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of PythonReader::SizeInternal(): " << status();
  RIEGELI_ASSERT(random_access_)
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
    // new position.
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

}  // namespace python
}  // namespace riegeli
