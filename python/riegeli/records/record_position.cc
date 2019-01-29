// Copyright 2019 Google LLC
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

#include "python/riegeli/records/record_position.h"

#include <stdint.h>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/hash/hash.h"
#include "absl/strings/str_cat.h"
#include "python/riegeli/base/utils.h"
#include "riegeli/base/base.h"
#include "riegeli/records/record_position.h"

namespace riegeli {
namespace python {

namespace {

struct PyRecordPositionObject {
  // clang-format off
  PyObject_HEAD
  static_assert(true, "");  // clang-format workaround.
  // clang-format on

  PythonWrapped<FutureRecordPosition> record_position;
};

extern PyTypeObject PyRecordPosition_Type;

extern "C" void RecordPositionDestructor(PyRecordPositionObject* self) {
  PythonUnlocked([&] { self->record_position.reset(); });
  Py_TYPE(self)->tp_free(self);
}

extern "C" PyRecordPositionObject* RecordPositionNew(PyTypeObject* cls,
                                                     PyObject* args,
                                                     PyObject* kwargs) {
  static constexpr const char* keywords[] = {"chunk_begin", "record_index",
                                             nullptr};
  PyObject* chunk_begin_arg;
  PyObject* record_index_arg;
  if (ABSL_PREDICT_FALSE(!PyArg_ParseTupleAndKeywords(
          args, kwargs, "OO:RecordPosition", const_cast<char**>(keywords),
          &chunk_begin_arg, &record_index_arg))) {
    return nullptr;
  }
  Position chunk_begin;
  if (ABSL_PREDICT_FALSE(!PositionFromPython(chunk_begin_arg, &chunk_begin))) {
    return nullptr;
  }
  Position record_index;
  if (ABSL_PREDICT_FALSE(
          !PositionFromPython(record_index_arg, &record_index))) {
    return nullptr;
  }
  if (ABSL_PREDICT_FALSE(chunk_begin > std::numeric_limits<uint64_t>::max()) ||
      ABSL_PREDICT_FALSE(record_index >
                         std::numeric_limits<uint64_t>::max() - chunk_begin)) {
    PyErr_Format(PyExc_OverflowError, "RecordPosition overflow: %llu/%llu",
                 static_cast<unsigned long long>(chunk_begin),
                 static_cast<unsigned long long>(record_index));
    return nullptr;
  }
  std::unique_ptr<PyRecordPositionObject, Deleter> self(
      reinterpret_cast<PyRecordPositionObject*>(cls->tp_alloc(cls, 0)));
  if (ABSL_PREDICT_FALSE(self == nullptr)) return nullptr;
  self.get()->record_position.emplace(RecordPosition(
      IntCast<uint64_t>(chunk_begin), IntCast<uint64_t>(record_index)));
  return self.release();
}

extern "C" PyObject* RecordPositionChunkBegin(PyRecordPositionObject* self,
                                              void* closure) {
  const RecordPosition pos =
      PythonUnlocked([&] { return self->record_position->get(); });
  return PositionToPython(pos.chunk_begin()).release();
}

extern "C" PyObject* RecordPositionRecordIndex(PyRecordPositionObject* self,
                                               void* closure) {
  const RecordPosition pos =
      PythonUnlocked([&] { return self->record_position->get(); });
  return PositionToPython(pos.record_index()).release();
}

extern "C" PyObject* RecordPositionNumeric(PyRecordPositionObject* self,
                                           void* closure) {
  const RecordPosition pos =
      PythonUnlocked([&] { return self->record_position->get(); });
  return PositionToPython(pos.numeric()).release();
}

extern "C" PyObject* RecordPositionCompare(PyObject* a, PyObject* b, int op) {
  if (ABSL_PREDICT_FALSE(!PyObject_TypeCheck(a, &PyRecordPosition_Type)) ||
      ABSL_PREDICT_FALSE(!PyObject_TypeCheck(b, &PyRecordPosition_Type))) {
    Py_INCREF(Py_NotImplemented);
    return Py_NotImplemented;
  }
  RecordPosition a_pos;
  RecordPosition b_pos;
  PythonUnlocked([&] {
    a_pos =
        reinterpret_cast<PyRecordPositionObject*>(a)->record_position->get();
    b_pos =
        reinterpret_cast<PyRecordPositionObject*>(b)->record_position->get();
  });
  switch (op) {
    case Py_EQ:
      return PyBool_FromLong(a_pos == b_pos);
    case Py_NE:
      return PyBool_FromLong(a_pos != b_pos);
    case Py_LT:
      return PyBool_FromLong(a_pos < b_pos);
    case Py_GT:
      return PyBool_FromLong(a_pos > b_pos);
    case Py_LE:
      return PyBool_FromLong(a_pos <= b_pos);
    case Py_GE:
      return PyBool_FromLong(a_pos >= b_pos);
    default:
      Py_INCREF(Py_NotImplemented);
      return Py_NotImplemented;
  }
}

extern "C" Py_hash_t RecordPositionHash(PyRecordPositionObject* self) {
  const RecordPosition pos =
      PythonUnlocked([&] { return self->record_position->get(); });
  Py_hash_t hash = static_cast<Py_hash_t>(absl::Hash<RecordPosition>()(pos));
  if (ABSL_PREDICT_FALSE(hash == -1)) hash = -2;
  return hash;
}

extern "C" PyObject* RecordPositionStr(PyRecordPositionObject* self) {
  const RecordPosition pos =
      PythonUnlocked([&] { return self->record_position->get(); });
  return StringToPython(pos.ToString()).release();
}

extern "C" PyRecordPositionObject* RecordPositionFromStr(PyTypeObject* cls,
                                                         PyObject* args,
                                                         PyObject* kwargs) {
  static constexpr const char* keywords[] = {"serialized", nullptr};
  PyObject* serialized_arg;
  if (ABSL_PREDICT_FALSE(!PyArg_ParseTupleAndKeywords(
          args, kwargs, "O:from_str", const_cast<char**>(keywords),
          &serialized_arg))) {
    return nullptr;
  }
  TextOrBytes serialized;
  if (ABSL_PREDICT_FALSE(!serialized.FromPython(serialized_arg))) {
    return nullptr;
  }
  RecordPosition pos;
  if (ABSL_PREDICT_FALSE(!pos.FromString(serialized.data()))) {
    PyErr_SetString(PyExc_ValueError, "RecordPosition.from_str() failed");
    return nullptr;
  }
  std::unique_ptr<PyRecordPositionObject, Deleter> self(
      reinterpret_cast<PyRecordPositionObject*>(cls->tp_alloc(cls, 0)));
  if (ABSL_PREDICT_FALSE(self == nullptr)) return nullptr;
  self.get()->record_position.emplace(pos);
  return self.release();
}

extern "C" PyObject* RecordPositionRepr(PyRecordPositionObject* self) {
  const RecordPosition pos =
      PythonUnlocked([&] { return self->record_position->get(); });
  return StringToPython(absl::StrCat("RecordPosition(", pos.chunk_begin(), ", ",
                                     pos.record_index(), ")"))
      .release();
}

extern "C" PyObject* RecordPositionToBytes(PyRecordPositionObject* self,
                                           PyObject* args) {
  const RecordPosition pos =
      PythonUnlocked([&] { return self->record_position->get(); });
  return BytesToPython(pos.ToBytes()).release();
}

extern "C" PyRecordPositionObject* RecordPositionFromBytes(PyTypeObject* cls,
                                                           PyObject* args,
                                                           PyObject* kwargs) {
  static constexpr const char* keywords[] = {"serialized", nullptr};
  PyObject* serialized_arg;
  if (ABSL_PREDICT_FALSE(!PyArg_ParseTupleAndKeywords(
          args, kwargs, "O:from_bytes", const_cast<char**>(keywords),
          &serialized_arg))) {
    return nullptr;
  }
  BytesLike serialized;
  if (ABSL_PREDICT_FALSE(!serialized.FromPython(serialized_arg))) {
    return nullptr;
  }
  RecordPosition pos;
  if (ABSL_PREDICT_FALSE(!pos.FromBytes(serialized.data()))) {
    PyErr_SetString(PyExc_ValueError, "RecordPosition.from_bytes() failed");
    return nullptr;
  }
  std::unique_ptr<PyRecordPositionObject, Deleter> self(
      reinterpret_cast<PyRecordPositionObject*>(cls->tp_alloc(cls, 0)));
  if (ABSL_PREDICT_FALSE(self == nullptr)) return nullptr;
  self.get()->record_position.emplace(pos);
  return self.release();
}

const PyMethodDef RecordPositionMethods[] = {
    {"from_str", reinterpret_cast<PyCFunction>(RecordPositionFromStr),
     METH_VARARGS | METH_KEYWORDS | METH_CLASS,
     R"doc(
from_str(type, serialized: )doc" RIEGELI_TEXT_OR_BYTES R"doc() -> RecordPosition

Parses RecordPosition from its text format.

Args:
  serialized: Text std::string to parse.
)doc"},
    {"to_bytes", reinterpret_cast<PyCFunction>(RecordPositionToBytes),
     METH_NOARGS,
     R"doc(
to_bytes(self) -> bytes

Returns the RecordPosition serialized to its binary format.

Serialized byte strings have the same natural order as the corresponding
positions.
)doc"},
    {"from_bytes", reinterpret_cast<PyCFunction>(RecordPositionFromBytes),
     METH_VARARGS | METH_KEYWORDS | METH_CLASS, R"doc(
from_bytes(
    type, serialized: Union[bytes, bytearray, memoryview]) -> RecordPosition

Parses RecordPosition from its binary format.

Serialized byte strings have the same natural order as the corresponding
positions.

Args:
  serialized: Byte std::string to parse.
)doc"},
    {nullptr, nullptr, 0, nullptr},
};

const PyGetSetDef RecordPositionGetSet[] = {
    {const_cast<char*>("chunk_begin"),
     reinterpret_cast<getter>(RecordPositionChunkBegin), nullptr,
     const_cast<char*>(R"doc(
chunk_begin: int

File position of the beginning of the chunk containing the given record.
)doc"),
     nullptr},
    {const_cast<char*>("record_index"),
     reinterpret_cast<getter>(RecordPositionRecordIndex), nullptr,
     const_cast<char*>(R"doc(
record_index: int

Index of the record within the chunk.
)doc"),
     nullptr},
    {const_cast<char*>("numeric"),
     reinterpret_cast<getter>(RecordPositionNumeric), nullptr,
     const_cast<char*>(R"doc(
numeric: int

Converts RecordPosition to an integer scaled between 0 and file size.

Distinct RecordPositions of a valid file have distinct numeric values.
)doc"),
     nullptr},
    {nullptr, nullptr, nullptr, nullptr, nullptr}};

PyTypeObject PyRecordPosition_Type = {
    // clang-format off
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    // clang-format on
    "riegeli.records.record_position.RecordPosition",        // tp_name
    sizeof(PyRecordPositionObject),                          // tp_basicsize
    0,                                                       // tp_itemsize
    reinterpret_cast<destructor>(RecordPositionDestructor),  // tp_dealloc
    nullptr,                                                 // tp_print
    nullptr,                                                 // tp_getattr
    nullptr,                                                 // tp_setattr
#if PY_MAJOR_VERSION >= 3
    nullptr,  // tp_as_async
#else
    nullptr,  // tp_compare
#endif
    reinterpret_cast<reprfunc>(RecordPositionRepr),   // tp_repr
    nullptr,                                          // tp_as_number
    nullptr,                                          // tp_as_sequence
    nullptr,                                          // tp_as_mapping
    reinterpret_cast<hashfunc>(RecordPositionHash),   // tp_hash
    nullptr,                                          // tp_call
    reinterpret_cast<reprfunc>(RecordPositionStr),    // tp_str
    nullptr,                                          // tp_getattro
    nullptr,                                          // tp_setattro
    nullptr,                                          // tp_as_buffer
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,         // tp_flags
    R"doc(
RecordPosition(chunk_begin: int, record_index: int) -> RecordPosition

Represents a position in a Riegeli/records file.

There are two ways of expressing positions, both strictly monotonic:
 * RecordPosition - Faster for seeking.
 * int            - Scaled between 0 and file size.

RecordPosition can be converted to int by the numeric property.

Working with RecordPosition is recommended, unless it is needed to seek to an
approximate position interpolated along the file, e.g. for splitting the file
into shards, or unless the position must be expressed as an integer from the
range [0, file_size] in order to fit into a preexisting API.

Both RecordReader and RecordWriter return positions. A position from
RecordWriter can act as a future: accessing its contents for the first time
might block, waiting for pending operations to complete.
)doc",                                                // tp_doc
    nullptr,                                          // tp_traverse
    nullptr,                                          // tp_clear
    RecordPositionCompare,                            // tp_richcompare
    0,                                                // tp_weaklistoffset
    nullptr,                                          // tp_iter
    nullptr,                                          // tp_iternext
    const_cast<PyMethodDef*>(RecordPositionMethods),  // tp_methods
    nullptr,                                          // tp_members
    const_cast<PyGetSetDef*>(RecordPositionGetSet),   // tp_getset
    nullptr,                                          // tp_base
    nullptr,                                          // tp_dict
    nullptr,                                          // tp_descr_get
    nullptr,                                          // tp_descr_set
    0,                                                // tp_dictoffset
    nullptr,                                          // tp_init
    nullptr,                                          // tp_alloc
    reinterpret_cast<newfunc>(RecordPositionNew),     // tp_new
    nullptr,                                          // tp_free
    nullptr,                                          // tp_is_gc
    nullptr,                                          // tp_bases
    nullptr,                                          // tp_mro
    nullptr,                                          // tp_cache
    nullptr,                                          // tp_subclasses
    nullptr,                                          // tp_weaklist
    nullptr,                                          // tp_del
    0,                                                // tp_version_tag
#if PY_VERSION_HEX >= 0x030400a1
    nullptr,  // tp_finalize
#endif
};

PythonPtr RecordPositionToPython(FutureRecordPosition value) {
  PythonPtr self(PyRecordPosition_Type.tp_alloc(&PyRecordPosition_Type, 0));
  if (ABSL_PREDICT_FALSE(self == nullptr)) return nullptr;
  reinterpret_cast<PyRecordPositionObject*>(self.get())
      ->record_position.emplace(std::move(value));
  return self;
}

bool RecordPositionFromPython(PyObject* object, RecordPosition* value) {
  if (ABSL_PREDICT_FALSE(!PyObject_TypeCheck(object, &PyRecordPosition_Type))) {
    PyErr_Format(PyExc_TypeError, "Expected RecordPosition, not %s",
                 Py_TYPE(object)->tp_name);
    return false;
  }
  *value = PythonUnlocked([&] {
    return reinterpret_cast<PyRecordPositionObject*>(object)
        ->record_position->get();
  });
  return true;
}

const char* const kModuleName = "riegeli.records.record_position";
const char kModuleDoc[] =
    R"doc(Represents a position in a Riegeli/records file.)doc";

#if PY_MAJOR_VERSION >= 3
PyModuleDef kModuleDef = {
    PyModuleDef_HEAD_INIT,
    kModuleName,  // m_name
    kModuleDoc,   // m_doc
    -1,           // m_size
    nullptr,      // m_methods
    nullptr,      // m_slots
    nullptr,      // m_traverse
    nullptr,      // m_clear
    nullptr,      // m_free
};
#endif

PyObject* InitModule() {
  if (ABSL_PREDICT_FALSE(PyType_Ready(&PyRecordPosition_Type) < 0)) {
    return nullptr;
  }
#if PY_MAJOR_VERSION >= 3
  PythonPtr module(PyModule_Create(&kModuleDef));
#else
  PythonPtr module(Py_InitModule3(kModuleName, nullptr, kModuleDoc));
#endif
  if (ABSL_PREDICT_FALSE(module == nullptr)) return nullptr;
  Py_INCREF(&PyRecordPosition_Type);
  if (ABSL_PREDICT_FALSE(PyModule_AddObject(module.get(), "RecordPosition",
                                            reinterpret_cast<PyObject*>(
                                                &PyRecordPosition_Type)) < 0)) {
    return nullptr;
  }
  static constexpr RecordPositionApi kRecordPositionApi = {
      RecordPositionToPython,
      RecordPositionFromPython,
  };
  if (ABSL_PREDICT_FALSE(!ExportCapsule(
          module.get(), kRecordPositionCapsuleName, &kRecordPositionApi))) {
    return nullptr;
  }
  return module.release();
}

}  // namespace

#if PY_MAJOR_VERSION >= 3
PyMODINIT_FUNC PyInit_record_position() { return InitModule(); }
#else
PyMODINIT_FUNC initrecord_position() { InitModule(); }
#endif

}  // namespace python
}  // namespace riegeli
