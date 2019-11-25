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

#include "python/riegeli/bytes/python_writer.h"
// clang-format: do not reorder the above include.

#include <stddef.h>

#include <limits>
#include <memory>

#include "absl/base/optimization.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "python/riegeli/base/utils.h"
#include "riegeli/base/base.h"
#include "riegeli/base/canonical_errors.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/buffered_writer.h"

namespace riegeli {
namespace python {

PythonWriter::PythonWriter(PyObject* dest, Options options)
    : BufferedWriter(options.buffer_size_),
      close_(options.close_),
      random_access_(!options.assumed_pos_.has_value()) {
  PythonLock::AssertHeld();
  Py_INCREF(dest);
  dest_.reset(dest);
  if (!random_access_) {
    set_start_pos(*options.assumed_pos_);
  } else {
    static constexpr Identifier id_tell("tell");
    const PythonPtr tell_result(
        PyObject_CallMethodObjArgs(dest_.get(), id_tell.get(), nullptr));
    if (ABSL_PREDICT_FALSE(tell_result == nullptr)) {
      FailOperation("tell()");
      return;
    }
    Position file_pos;
    if (ABSL_PREDICT_FALSE(!PositionFromPython(tell_result.get(), &file_pos))) {
      FailOperation("PositionFromPython() after tell()");
      return;
    }
    set_start_pos(file_pos);
  }
}

bool PythonWriter::FailOperation(absl::string_view operation) {
  PythonLock::AssertHeld();
  if (ABSL_PREDICT_FALSE(!healthy())) {
    // Ignore this error because `PythonWriter` already failed.
    PyErr_Clear();
    return false;
  }
  exception_ = Exception::Fetch();
  return Fail(
      UnknownError(absl::StrCat(operation, " failed: ", exception_.message())));
}

void PythonWriter::Done() {
  PushInternal();
  BufferedWriter::Done();
  if (close_ && dest_ != nullptr) {
    PythonLock lock;
    static constexpr Identifier id_close("close");
    const PythonPtr close_result(
        PyObject_CallMethodObjArgs(dest_.get(), id_close.get(), nullptr));
    if (ABSL_PREDICT_FALSE(close_result == nullptr)) FailOperation("close()");
  }
}

bool PythonWriter::WriteInternal(absl::string_view src) {
  RIEGELI_ASSERT(!src.empty())
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "nothing to write";
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of BufferedWriter::WriteInternal(): " << status();
  RIEGELI_ASSERT_EQ(written_to_buffer(), 0u)
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(src.size() >
                         std::numeric_limits<Position>::max() - start_pos())) {
    return FailOverflow();
  }
  PythonLock lock;
  if (ABSL_PREDICT_FALSE(write_function_ == nullptr)) {
    static constexpr Identifier id_write("write");
    write_function_.reset(PyObject_GetAttr(dest_.get(), id_write.get()));
    if (ABSL_PREDICT_FALSE(write_function_ == nullptr)) {
      return FailOperation("write()");
    }
  }
  do {
    size_t length_written;
    {
      const size_t length_to_write = UnsignedMin(
          src.size(), size_t{std::numeric_limits<Py_ssize_t>::max()});
      PythonPtr write_result;
      if (!use_bytes_) {
#if PY_MAJOR_VERSION >= 3
        // Prefer passing a `memoryview` to avoid copying memory.
        const PythonPtr memoryview(PyMemoryView_FromMemory(
            const_cast<char*>(src.data()), IntCast<Py_ssize_t>(length_to_write),
            PyBUF_READ));
        if (ABSL_PREDICT_FALSE(memoryview == nullptr)) {
          return FailOperation("PyMemoryView_FromMemory()");
        }
        write_result.reset(PyObject_CallFunctionObjArgs(
            write_function_.get(), memoryview.get(), nullptr));
        if (ABSL_PREDICT_FALSE(write_result == nullptr)) {
          if (!PyErr_ExceptionMatches(PyExc_TypeError)) {
            if (ABSL_PREDICT_FALSE(Py_REFCNT(memoryview.get()) > 1)) {
              // `write()` has stored a reference to the `memoryview`, but the
              // `memoryview` contains C++ pointers which are going to be
              // invalid. Call `memoryview.release()` to mark the `memoryview`
              // as invalid.
              PyObject* value;
              PyObject* type;
              PyObject* traceback;
              PyErr_Fetch(&value, &type, &traceback);
              static constexpr Identifier id_release("release");
              const PythonPtr release_result(PyObject_CallMethodObjArgs(
                  memoryview.get(), id_release.get(), nullptr));
              // Ignore errors from `release()` because `write()` failed first.
              PyErr_Restore(value, type, traceback);
            }
            return FailOperation("write()");
          }
          PyErr_Clear();
          use_bytes_ = true;
        }
        if (ABSL_PREDICT_FALSE(Py_REFCNT(memoryview.get()) > 1)) {
          // `write()` has stored a reference to the `memoryview`, but the
          // `memoryview` contains C++ pointers which are going to be invalid.
          // Call `memoryview.release()` to mark the `memoryview` as invalid.
          static constexpr Identifier id_release("release");
          const PythonPtr release_result(PyObject_CallMethodObjArgs(
              memoryview.get(), id_release.get(), nullptr));
          if (ABSL_PREDICT_FALSE(release_result == nullptr)) {
            return FailOperation("release()");
          }
        }
#else
        // Prefer passing a `buffer` to avoid copying memory.
        const PythonPtr buffer(
            PyBuffer_FromMemory(const_cast<char*>(src.data()),
                                IntCast<Py_ssize_t>(length_to_write)));
        if (ABSL_PREDICT_FALSE(buffer == nullptr)) {
          return FailOperation("PyBuffer_FromMemory()");
        }
        write_result.reset(PyObject_CallFunctionObjArgs(write_function_.get(),
                                                        buffer.get(), nullptr));
        if (ABSL_PREDICT_FALSE(write_result == nullptr)) {
          if (!PyErr_ExceptionMatches(PyExc_TypeError)) {
            return FailOperation("write()");
          }
          PyErr_Clear();
          use_bytes_ = true;
        }
#endif
      }
      if (use_bytes_) {
        // `write()` does not support `memoryview` / `buffer`. Use `bytes`.
        const PythonPtr bytes = BytesToPython(src.substr(0, length_to_write));
        if (ABSL_PREDICT_FALSE(bytes == nullptr)) {
          return FailOperation("BytesToPython()");
        }
        write_result.reset(PyObject_CallFunctionObjArgs(write_function_.get(),
                                                        bytes.get(), nullptr));
        if (ABSL_PREDICT_FALSE(write_result == nullptr)) {
          return FailOperation("write()");
        }
      }
      if (write_result.get() == Py_None) {
        // Python2 `file.write()` returns `None`, and would raise an exception
        // if less than the full length had been written.
        length_written = length_to_write;
      } else {
        // `io.IOBase.write()` returns the length written.
        if (ABSL_PREDICT_FALSE(
                !SizeFromPython(write_result.get(), &length_written))) {
          return FailOperation("SizeFromPython() after write()");
        }
      }
    }
    if (ABSL_PREDICT_FALSE(length_written > src.size())) {
      return Fail(InternalError("write() wrote more than requested"));
    }
    move_start_pos(length_written);
    src.remove_prefix(length_written);
  } while (!src.empty());
  return true;
}

bool PythonWriter::Flush(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!PushInternal())) return false;
  switch (flush_type) {
    case FlushType::kFromObject:
      return true;
    case FlushType::kFromProcess:
    case FlushType::kFromMachine:
      PythonLock lock;
      static constexpr Identifier id_flush("flush");
      const PythonPtr flush_result(
          PyObject_CallMethodObjArgs(dest_.get(), id_flush.get(), nullptr));
      if (ABSL_PREDICT_FALSE(flush_result == nullptr)) {
        return FailOperation("flush()");
      }
      return true;
  }
  RIEGELI_ASSERT_UNREACHABLE()
      << "Unknown flush type: " << static_cast<int>(flush_type);
}

bool PythonWriter::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > pos())
      << "Failed precondition of Writer::SeekSlow(): "
         "position in the buffer, use Seek() instead";
  if (ABSL_PREDICT_FALSE(!random_access_)) {
    return Fail(UnimplementedError("PythonWriter::Seek() not supported"));
  }
  if (ABSL_PREDICT_FALSE(!PushInternal())) return false;
  RIEGELI_ASSERT_EQ(written_to_buffer(), 0u)
      << "BufferedWriter::PushInternal() did not empty the buffer";
  PythonLock lock;
  if (new_pos >= start_pos()) {
    // Seeking forwards.
    Position size;
    if (ABSL_PREDICT_FALSE(!SizeInternal(&size))) return false;
    if (ABSL_PREDICT_FALSE(new_pos > size)) {
      // File ends.
      set_start_pos(size);
      return false;
    }
  }
  set_start_pos(new_pos);
  const PythonPtr file_pos = PositionToPython(start_pos());
  if (ABSL_PREDICT_FALSE(file_pos == nullptr)) {
    return FailOperation("PositionToPython()");
  }
  static constexpr Identifier id_seek("seek");
  const PythonPtr seek_result(PyObject_CallMethodObjArgs(
      dest_.get(), id_seek.get(), file_pos.get(), nullptr));
  if (ABSL_PREDICT_FALSE(seek_result == nullptr)) {
    return FailOperation("seek()");
  }
  return true;
}

bool PythonWriter::Size(Position* size) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(!random_access_)) {
    return Fail(UnimplementedError("PythonWriter::Size() not supported"));
  }
  PythonLock lock;
  if (ABSL_PREDICT_FALSE(!SizeInternal(size))) return false;
  const PythonPtr file_pos = PositionToPython(start_pos());
  if (ABSL_PREDICT_FALSE(file_pos == nullptr)) {
    return FailOperation("PositionToPython()");
  }
  static constexpr Identifier id_seek("seek");
  const PythonPtr seek_result(PyObject_CallMethodObjArgs(
      dest_.get(), id_seek.get(), file_pos.get(), nullptr));
  if (ABSL_PREDICT_FALSE(seek_result == nullptr)) {
    return FailOperation("seek()");
  }
  return true;
}

inline bool PythonWriter::SizeInternal(Position* size) {
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of PythonWriter::SizeInternal(): " << status();
  RIEGELI_ASSERT(random_access_)
      << "Failed precondition of PythonWriter::SizeInternal(): "
         "random access not supported";
  PythonLock::AssertHeld();
  absl::string_view operation;
  const PythonPtr file_pos = PositionToPython(0);
  if (ABSL_PREDICT_FALSE(file_pos == nullptr)) {
    return FailOperation("PositionToPython()");
  }
  const PythonPtr whence = IntToPython(2);  // `io.SEEK_END`
  if (ABSL_PREDICT_FALSE(whence == nullptr)) {
    return FailOperation("IntToPython()");
  }
  static constexpr Identifier id_seek("seek");
  PythonPtr result(PyObject_CallMethodObjArgs(
      dest_.get(), id_seek.get(), file_pos.get(), whence.get(), nullptr));
  if (result.get() == Py_None) {
    // Python2 `file.seek()` returns `None`.
    static constexpr Identifier id_tell("tell");
    result.reset(
        PyObject_CallMethodObjArgs(dest_.get(), id_tell.get(), nullptr));
    operation = "tell()";
  } else {
    // `io.IOBase.seek()` returns the new position.
    operation = "seek()";
  }
  if (ABSL_PREDICT_FALSE(result == nullptr)) return FailOperation(operation);
  Position file_size;
  if (ABSL_PREDICT_FALSE(!PositionFromPython(result.get(), &file_size))) {
    return FailOperation(
        absl::StrCat("PositionFromPython() after ", operation));
  }
  *size = UnsignedMax(file_size, pos());
  return true;
}

bool PythonWriter::Truncate(Position new_size) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(!random_access_)) {
    return Fail(UnimplementedError("PythonWriter::Truncate() not supported"));
  }
  if (ABSL_PREDICT_FALSE(!PushInternal())) return false;
  PythonLock lock;
  Position size;
  if (ABSL_PREDICT_FALSE(!SizeInternal(&size))) return false;
  if (ABSL_PREDICT_FALSE(new_size > size)) {
    // File ends.
    set_start_pos(size);
    return false;
  }
  {
    const PythonPtr file_pos = PositionToPython(new_size);
    if (ABSL_PREDICT_FALSE(file_pos == nullptr)) {
      return FailOperation("PositionToPython()");
    }
    static constexpr Identifier id_seek("seek");
    const PythonPtr seek_result(PyObject_CallMethodObjArgs(
        dest_.get(), id_seek.get(), file_pos.get(), nullptr));
    if (ABSL_PREDICT_FALSE(seek_result == nullptr)) {
      return FailOperation("seek()");
    }
  }
  set_start_pos(IntCast<Position>(new_size));
  static constexpr Identifier id_truncate("truncate");
  const PythonPtr truncate_result(
      PyObject_CallMethodObjArgs(dest_.get(), id_truncate.get(), nullptr));
  if (ABSL_PREDICT_FALSE(truncate_result == nullptr)) {
    return FailOperation("truncate()");
  }
  return true;
}

}  // namespace python
}  // namespace riegeli
