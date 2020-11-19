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

#include "python/riegeli/base/utils.h"
// clang-format: do not reorder the above include.

#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/compare.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"

namespace riegeli {
namespace python {

Exception& Exception::operator=(const Exception& that) noexcept {
  PythonLock lock;
  Py_XINCREF(that.type_.get());
  type_.reset(that.type_.get());
  Py_XINCREF(that.value_.get());
  value_.reset(that.value_.get());
  Py_XINCREF(that.traceback_.get());
  traceback_.reset(that.traceback_.get());
  return *this;
}

Exception Exception::Fetch() {
  PythonLock::AssertHeld();
  PyObject* type;
  PyObject* value;
  PyObject* traceback;
  PyErr_Fetch(&type, &value, &traceback);
  PyErr_NormalizeException(&type, &value, &traceback);
  return Exception(type, value, traceback);
}

PyObject* Exception::Restore() const& {
  PythonLock::AssertHeld();
  Py_XINCREF(type_.get());
  Py_XINCREF(value_.get());
  Py_XINCREF(traceback_.get());
  PyErr_Restore(type_.get(), value_.get(), traceback_.get());
  return nullptr;
}

PyObject* Exception::Restore() && {
  PythonLock::AssertHeld();
  PyErr_Restore(type_.release(), value_.release(), traceback_.release());
  return nullptr;
}

std::string Exception::message() const {
  if (ok()) return "OK";
  PythonLock lock;
  RIEGELI_ASSERT(PyExceptionClass_Check(type_.get()))
      << "Expected an exception class, not " << Py_TYPE(type_.get())->tp_name;
  std::string message = PyExceptionClass_Name(type_.get());
  if (value_ == nullptr) return message;
  const PythonPtr str_result(PyObject_Str(value_.get()));
  if (ABSL_PREDICT_FALSE(str_result == nullptr)) {
    PyErr_Clear();
    absl::StrAppend(&message, ": <str() failed>");
    return message;
  }
  StrOrBytes str;
  if (ABSL_PREDICT_FALSE(!str.FromPython(str_result.get()))) {
    PyErr_Clear();
    absl::StrAppend(&message, ": <StrOrBytes::FromPython() failed>");
    return message;
  }
  if (!absl::string_view(str).empty()) {
    absl::StrAppend(&message, ": ", absl::string_view(str));
  }
  return message;
}

void SetRiegeliError(const absl::Status& status) {
  RIEGELI_ASSERT(!status.ok())
      << "Failed precondition of SetRiegeliError(): status not failed";
  PythonLock::AssertHeld();
  PythonPtr message = StringToPython(status.message());
  if (ABSL_PREDICT_FALSE(message == nullptr)) return;
  PyObject* type;
  switch (status.code()) {
#define HANDLE_CODE(name)                                     \
  case absl::StatusCode::k##name: {                           \
    static constexpr ImportedConstant k##name##Error(         \
        "riegeli.base.riegeli_error", #name "Error");         \
    if (ABSL_PREDICT_FALSE(!k##name##Error.Verify())) return; \
    type = k##name##Error.get();                              \
  } break

    // clang-format off
    HANDLE_CODE(Cancelled);
    default:
    HANDLE_CODE(Unknown);
    HANDLE_CODE(InvalidArgument);
    HANDLE_CODE(DeadlineExceeded);
    HANDLE_CODE(NotFound);
    HANDLE_CODE(AlreadyExists);
    HANDLE_CODE(PermissionDenied);
    HANDLE_CODE(Unauthenticated);
    HANDLE_CODE(ResourceExhausted);
    HANDLE_CODE(FailedPrecondition);
    HANDLE_CODE(Aborted);
    HANDLE_CODE(OutOfRange);
    HANDLE_CODE(Unimplemented);
    HANDLE_CODE(Internal);
    HANDLE_CODE(Unavailable);
    HANDLE_CODE(DataLoss);
      // clang-format on

#undef HANDLE_CODE
  }

  Py_INCREF(type);
  PyErr_Restore(type, message.release(), nullptr);
}

namespace internal {

namespace {

// A linked list of all objects of type `StaticObject` which have `value_`
// allocated, chained by their `next_` fields. This is used to free the objects
// on Python interpreter shutdown.
const StaticObject* all_static_objects = nullptr;

}  // namespace

void FreeStaticObjectsImpl() {
  const StaticObject* static_object =
      std::exchange(all_static_objects, nullptr);
  while (static_object != nullptr) {
    Py_DECREF(static_object->value_);
    static_object->value_ = nullptr;
    static_object = std::exchange(static_object->next_, nullptr);
  }
}

// `extern "C"` sets the C calling convention for compatibility with the Python
// API. Function is marked `static` to avoid making its symbol public, as
// `extern "C"` would trump anonymous namespace.
extern "C" {
static void FreeStaticObjects() { FreeStaticObjectsImpl(); }
}  // extern "C"

void StaticObject::RegisterThis() const {
  PythonLock::AssertHeld();
  if (all_static_objects == nullptr) {
    // This is the first registered `StaticObject` since `Py_Initialize()`.
    Py_AtExit(FreeStaticObjects);
  }
  next_ = std::exchange(all_static_objects, this);
}

bool ImportedCapsuleBase::ImportValue() const {
  // For some reason `PyImport_ImportModule()` is sometimes required before
  // `PyCapsule_Import()` for a module with a nested name.
  const size_t dot = absl::string_view(capsule_name_).rfind('.');
  RIEGELI_ASSERT_NE(dot, absl::string_view::npos)
      << "Capsule name does not contain a dot: " << capsule_name_;
  RIEGELI_CHECK(
      PyImport_ImportModule(std::string(capsule_name_, dot).c_str()) != nullptr)
      << Exception::Fetch().message();
  value_ = PyCapsule_Import(capsule_name_, false);
  return value_ != nullptr;
}

}  // namespace internal

bool Identifier::AllocateValue() const {
  value_ = StringToPython(name_).release();
  if (ABSL_PREDICT_FALSE(value_ == nullptr)) return false;
  PyUnicode_InternInPlace(&value_);
  RegisterThis();
  return true;
}

bool ImportedConstant::AllocateValue() const {
  const PythonPtr module_name = StringToPython(module_name_);
  if (ABSL_PREDICT_FALSE(module_name == nullptr)) return false;
  const PythonPtr module(PyImport_Import(module_name.get()));
  if (ABSL_PREDICT_FALSE(module == nullptr)) return false;
  const PythonPtr attr_name = StringToPython(attr_name_);
  if (ABSL_PREDICT_FALSE(attr_name == nullptr)) return false;
  value_ = PyObject_GetAttr(module.get(), attr_name.get());
  if (ABSL_PREDICT_FALSE(value_ == nullptr)) return false;
  RegisterThis();
  return true;
}

bool ExportCapsule(PyObject* module, const char* capsule_name,
                   const void* ptr) {
  PythonPtr capsule(
      PyCapsule_New(const_cast<void*>(ptr), capsule_name, nullptr));
  if (ABSL_PREDICT_FALSE(capsule == nullptr)) return false;
  const size_t dot = absl::string_view(capsule_name).rfind('.');
  RIEGELI_ASSERT_NE(dot, absl::string_view::npos)
      << "Capsule name does not contain a dot: " << capsule_name;
  RIEGELI_ASSERT(PyModule_Check(module))
      << "Expected a module, not " << Py_TYPE(module)->tp_name;
  RIEGELI_ASSERT_EQ(absl::string_view(PyModule_GetName(module)),
                    absl::string_view(capsule_name, dot))
      << "Module name mismatch";
  if (ABSL_PREDICT_FALSE(PyModule_AddObject(module, capsule_name + dot + 1,
                                            capsule.release()) < 0)) {
    return false;
  }
  return true;
}

bool StrOrBytes::FromPython(PyObject* object) {
  // TODO: Change this depending on how
  // https://bugs.python.org/issue35295 is resolved.
  if (PyUnicode_Check(object)) {
    Py_ssize_t length;
    const char* data = PyUnicode_AsUTF8AndSize(object, &length);
    if (ABSL_PREDICT_FALSE(data == nullptr)) return false;
    data_ = absl::string_view(data, IntCast<size_t>(length));
    return true;
  } else if (ABSL_PREDICT_FALSE(!PyBytes_Check(object))) {
    PyErr_Format(PyExc_TypeError, "Expected str or bytes, not %s",
                 Py_TYPE(object)->tp_name);
    return false;
  }
  data_ = absl::string_view(PyBytes_AS_STRING(object),
                            IntCast<size_t>(PyBytes_GET_SIZE(object)));
  return true;
}

PythonPtr ChainToPython(const Chain& value) {
  PythonPtr bytes(
      PyBytes_FromStringAndSize(nullptr, IntCast<Py_ssize_t>(value.size())));
  if (ABSL_PREDICT_FALSE(bytes == nullptr)) return nullptr;
  value.CopyTo(PyBytes_AS_STRING(bytes.get()));
  return bytes;
}

absl::optional<Chain> ChainFromPython(PyObject* object) {
  Py_buffer buffer;
  if (ABSL_PREDICT_FALSE(PyObject_GetBuffer(object, &buffer, PyBUF_CONTIG_RO) <
                         0)) {
    return absl::nullopt;
  }
  Chain result(absl::string_view(static_cast<const char*>(buffer.buf),
                                 IntCast<size_t>(buffer.len)));
  PyBuffer_Release(&buffer);
  return result;
}

PythonPtr SizeToPython(size_t value) {
  if (ABSL_PREDICT_FALSE(value >
                         std::numeric_limits<unsigned long long>::max())) {
    PyErr_Format(PyExc_OverflowError, "Size out of range: %zu", value);
    return nullptr;
  }
  return PythonPtr(
      PyLong_FromUnsignedLongLong(IntCast<unsigned long long>(value)));
}

absl::optional<size_t> SizeFromPython(PyObject* object) {
  const PythonPtr index(PyNumber_Index(object));
  if (ABSL_PREDICT_FALSE(index == nullptr)) return absl::nullopt;
  RIEGELI_ASSERT(PyLong_Check(index.get()))
      << "PyNumber_Index() returned an unexpected type: "
      << Py_TYPE(index.get())->tp_name;
  unsigned long long index_value = PyLong_AsUnsignedLongLong(index.get());
  if (ABSL_PREDICT_FALSE(index_value == static_cast<unsigned long long>(-1)) &&
      PyErr_Occurred()) {
    return absl::nullopt;
  }
  if (ABSL_PREDICT_FALSE(index_value > std::numeric_limits<size_t>::max())) {
    PyErr_Format(PyExc_OverflowError, "Size out of range: %llu", index_value);
    return absl::nullopt;
  }
  return IntCast<size_t>(index_value);
}

PythonPtr PositionToPython(Position value) {
  if (ABSL_PREDICT_FALSE(value >
                         std::numeric_limits<unsigned long long>::max())) {
    PyErr_Format(PyExc_OverflowError, "Position out of range: %ju",
                 uintmax_t{value});
    return nullptr;
  }
  return PythonPtr(
      PyLong_FromUnsignedLongLong(IntCast<unsigned long long>(value)));
}

absl::optional<Position> PositionFromPython(PyObject* object) {
  const PythonPtr index(PyNumber_Index(object));
  if (ABSL_PREDICT_FALSE(index == nullptr)) return absl::nullopt;
  RIEGELI_ASSERT(PyLong_Check(index.get()))
      << "PyNumber_Index() returned an unexpected type: "
      << Py_TYPE(index.get())->tp_name;
  const unsigned long long index_value = PyLong_AsUnsignedLongLong(index.get());
  if (ABSL_PREDICT_FALSE(index_value == static_cast<unsigned long long>(-1)) &&
      PyErr_Occurred()) {
    return absl::nullopt;
  }
  if (ABSL_PREDICT_FALSE(index_value > std::numeric_limits<Position>::max())) {
    PyErr_Format(PyExc_OverflowError, "Position out of range: %llu",
                 index_value);
    return absl::nullopt;
  }
  return IntCast<Position>(index_value);
}

absl::optional<absl::partial_ordering> PartialOrderingFromPython(
    PyObject* object) {
  if (object == Py_None) return absl::partial_ordering::unordered;
  const long long_value = PyLong_AsLong(object);
  if (ABSL_PREDICT_FALSE(long_value == -1) && PyErr_Occurred()) {
    return absl::nullopt;
  }
  return long_value < 0    ? absl::partial_ordering::less
         : long_value == 0 ? absl::partial_ordering::equivalent
                           : absl::partial_ordering::greater;
}

}  // namespace python
}  // namespace riegeli
