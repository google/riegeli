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

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/numeric/bits.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "python/riegeli/base/utils.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/global.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffered_writer.h"

namespace riegeli {
namespace python {

PythonWriter::PythonWriter(PyObject* dest, Options options)
    : BufferedWriter(options.buffer_options()),
      owns_dest_(options.owns_dest()) {
  PythonLock::AssertHeld();
  Py_INCREF(dest);
  dest_.reset(dest);
  if (options.assumed_pos() != absl::nullopt) {
    set_start_pos(*options.assumed_pos());
    // `supports_random_access_` is left as `false`.
    random_access_status_ = Global([] {
      return absl::UnimplementedError(
          "PythonWriter::Options::assumed_pos() excludes random access");
    });
  } else {
    static constexpr Identifier id_seekable("seekable");
    const PythonPtr seekable_result(
        PyObject_CallMethodObjArgs(dest_.get(), id_seekable.get(), nullptr));
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
      random_access_status_ = Global([] {
        return absl::UnimplementedError(
            "seekable() is False which excludes random access");
      });
      return;
    }
    static constexpr Identifier id_tell("tell");
    const PythonPtr tell_result(
        PyObject_CallMethodObjArgs(dest_.get(), id_tell.get(), nullptr));
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
    set_start_pos(*file_pos);
    supports_random_access_ = true;
  }
  BeginRun();
}

void PythonWriter::Done() {
  BufferedWriter::Done();
  random_access_status_ = absl::OkStatus();
  if (owns_dest_ && dest_ != nullptr) {
    PythonLock lock;
    static constexpr Identifier id_close("close");
    const PythonPtr close_result(
        PyObject_CallMethodObjArgs(dest_.get(), id_close.get(), nullptr));
    if (ABSL_PREDICT_FALSE(close_result == nullptr)) FailOperation("close()");
  }
}

inline bool PythonWriter::FailOperation(absl::string_view operation) {
  RIEGELI_ASSERT(is_open())
      << "Failed precondition of PythonWriter::FailOperation(): "
         "Object closed";
  PythonLock::AssertHeld();
  if (ABSL_PREDICT_FALSE(!ok())) {
    // Ignore this error because `PythonWriter` already failed.
    PyErr_Clear();
    return false;
  }
  exception_ = Exception::Fetch();
  return Fail(absl::UnknownError(
      absl::StrCat(operation, " failed: ", exception_.message())));
}

bool PythonWriter::WriteInternal(absl::string_view src) {
  RIEGELI_ASSERT(!src.empty())
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "nothing to write";
  RIEGELI_ASSERT_OK(*this)
      << "Failed precondition of BufferedWriter::WriteInternal()";
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
    const size_t length_to_write = UnsignedMin(
        src.size(),
        absl::bit_floor(size_t{std::numeric_limits<Py_ssize_t>::max()}));
    size_t length_written;
    {
      PythonPtr write_result;
      if (!use_bytes_) {
        // Prefer passing a `memoryview` to avoid copying memory.
        MemoryView memory_view;
        PyObject* const memory_view_object = memory_view.ToPython(
            absl::string_view(src.data(), length_to_write));
        if (ABSL_PREDICT_FALSE(memory_view_object == nullptr)) {
          return FailOperation("MemoryView::ToPython()");
        }
        write_result.reset(PyObject_CallFunctionObjArgs(
            write_function_.get(), memory_view_object, nullptr));
        if (ABSL_PREDICT_FALSE(write_result == nullptr)) {
          if (!PyErr_ExceptionMatches(PyExc_TypeError)) {
            return FailOperation("write()");
          }
          PyErr_Clear();
          use_bytes_ = true;
        }
        if (ABSL_PREDICT_FALSE(!memory_view.Release())) {
          return FailOperation("MemoryView::Release()");
        }
      }
      if (use_bytes_) {
        // `write()` does not support `memoryview`. Use `bytes`.
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
        // if less than the full length had been written. Python2 is dead, but
        // some classes still behave like that.
        length_written = length_to_write;
      } else {
        // `io.IOBase.write()` returns the length written.
        const absl::optional<size_t> length_written_opt =
            SizeFromPython(write_result.get());
        if (ABSL_PREDICT_FALSE(length_written_opt == absl::nullopt)) {
          return FailOperation("SizeFromPython() after write()");
        }
        length_written = *length_written_opt;
      }
    }
    if (ABSL_PREDICT_FALSE(length_written > length_to_write)) {
      return Fail(absl::InternalError("write() wrote more than requested"));
    }
    move_start_pos(length_written);
    src.remove_prefix(length_written);
  } while (!src.empty());
  return true;
}

bool PythonWriter::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!BufferedWriter::FlushImpl(flush_type))) return false;
  switch (flush_type) {
    case FlushType::kFromObject:
      if (!owns_dest_) return true;
      ABSL_FALLTHROUGH_INTENDED;
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
  RIEGELI_ASSUME_UNREACHABLE()
      << "Unknown flush type: " << static_cast<int>(flush_type);
}

bool PythonWriter::SeekBehindBuffer(Position new_pos) {
  RIEGELI_ASSERT_NE(new_pos, pos())
      << "Failed precondition of BufferedWriter::SeekBehindBuffer(): "
         "position unchanged, use Seek() instead";
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::SeekBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!PythonWriter::SupportsRandomAccess())) {
    if (ok()) Fail(random_access_status_);
    return false;
  }
  PythonLock lock;
  if (new_pos > start_pos()) {
    // Seeking forwards.
    const absl::optional<Position> size = SizeInternal();
    if (ABSL_PREDICT_FALSE(size == absl::nullopt)) return false;
    if (ABSL_PREDICT_FALSE(new_pos > *size)) {
      // File ends.
      set_start_pos(*size);
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

inline absl::optional<Position> PythonWriter::SizeInternal() {
  RIEGELI_ASSERT_OK(*this)
      << "Failed precondition of PythonWriter::SizeInternal()";
  RIEGELI_ASSERT(PythonWriter::SupportsRandomAccess())
      << "Failed precondition of PythonWriter::SizeInternal(): "
         "random access not supported";
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of PythonWriter::SizeInternal(): "
         "buffer not empty";
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
      dest_.get(), id_seek.get(), file_pos.get(), whence.get(), nullptr));
  if (result.get() == Py_None) {
    // Python2 `file.seek()` returns `None`. Python2 is dead, but some classes
    // still behave like that.
    static constexpr Identifier id_tell("tell");
    result.reset(
        PyObject_CallMethodObjArgs(dest_.get(), id_tell.get(), nullptr));
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

absl::optional<Position> PythonWriter::SizeBehindBuffer() {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::SizeBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!PythonWriter::SupportsRandomAccess())) {
    if (ok()) Fail(random_access_status_);
    return absl::nullopt;
  }
  if (ABSL_PREDICT_FALSE(!ok())) return absl::nullopt;
  PythonLock lock;
  const absl::optional<Position> size = SizeInternal();
  if (ABSL_PREDICT_FALSE(size == absl::nullopt)) return absl::nullopt;
  const PythonPtr file_pos = PositionToPython(start_pos());
  if (ABSL_PREDICT_FALSE(file_pos == nullptr)) {
    FailOperation("PositionToPython()");
    return absl::nullopt;
  }
  static constexpr Identifier id_seek("seek");
  const PythonPtr seek_result(PyObject_CallMethodObjArgs(
      dest_.get(), id_seek.get(), file_pos.get(), nullptr));
  if (ABSL_PREDICT_FALSE(seek_result == nullptr)) {
    FailOperation("seek()");
    return absl::nullopt;
  }
  return *size;
}

bool PythonWriter::TruncateBehindBuffer(Position new_size) {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::TruncateBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!PythonWriter::SupportsRandomAccess())) {
    if (ok()) Fail(random_access_status_);
    return false;
  }
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  PythonLock lock;
  const absl::optional<Position> size = SizeInternal();
  if (ABSL_PREDICT_FALSE(size == absl::nullopt)) return false;
  if (ABSL_PREDICT_FALSE(new_size > *size)) {
    // File ends.
    set_start_pos(*size);
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
  set_start_pos(new_size);
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
