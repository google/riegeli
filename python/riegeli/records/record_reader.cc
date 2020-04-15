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
// clang-format: do not reorder the above include.

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/types/compare.h"
#include "absl/types/optional.h"
#include "python/riegeli/base/utils.h"
#include "python/riegeli/bytes/python_reader.h"
#include "python/riegeli/records/record_position.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/chunk_encoding/field_projection.h"
#include "riegeli/records/record_position.h"
#include "riegeli/records/record_reader.h"
#include "riegeli/records/skipped_region.h"

namespace riegeli {
namespace python {

namespace {

constexpr ImportedCapsule<RecordPositionApi> kRecordPositionApi(
    kRecordPositionCapsuleName);

extern "C" PyObject* GetRecordType(PyObject* self, PyObject* args,
                                   PyObject* kwargs) {
  static constexpr const char* keywords[] = {"metadata", nullptr};
  PyObject* metadata_arg;
  if (ABSL_PREDICT_FALSE(!PyArg_ParseTupleAndKeywords(
          args, kwargs, "O:get_record_type", const_cast<char**>(keywords),
          &metadata_arg))) {
    return nullptr;
  }
  // record_type_name = metadata.record_type_name
  static constexpr Identifier id_record_type_name("record_type_name");
  const PythonPtr record_type_name(
      PyObject_GetAttr(metadata_arg, id_record_type_name.get()));
  if (ABSL_PREDICT_FALSE(record_type_name == nullptr)) return nullptr;
  // if not record_type_name: return None
  const int record_type_name_is_true = PyObject_IsTrue(record_type_name.get());
  if (ABSL_PREDICT_FALSE(record_type_name_is_true < 0)) return nullptr;
  if (record_type_name_is_true == 0) Py_RETURN_NONE;
  // file_descriptors = metadata.file_descriptor
  static constexpr Identifier id_file_descriptor("file_descriptor");
  const PythonPtr file_descriptors(
      PyObject_GetAttr(metadata_arg, id_file_descriptor.get()));
  if (ABSL_PREDICT_FALSE(file_descriptors == nullptr)) return nullptr;
  // if not file_descriptors: return None
  const int file_descriptors_is_true = PyObject_IsTrue(file_descriptors.get());
  if (ABSL_PREDICT_FALSE(file_descriptors_is_true < 0)) return nullptr;
  if (file_descriptors_is_true == 0) Py_RETURN_NONE;
  // pool = DescriptorPool()
  static constexpr ImportedConstant kDescriptorPool(
      "google.protobuf.descriptor_pool", "DescriptorPool");
  if (ABSL_PREDICT_FALSE(!kDescriptorPool.Verify())) return nullptr;
  const PythonPtr pool(
      PyObject_CallFunctionObjArgs(kDescriptorPool.get(), nullptr));
  if (ABSL_PREDICT_FALSE(pool == nullptr)) return nullptr;
  // for file_descriptor in file_descriptors:
  //   pool.Add(file_descriptor)
  const PythonPtr iter(PyObject_GetIter(file_descriptors.get()));
  if (ABSL_PREDICT_FALSE(iter == nullptr)) return nullptr;
  while (const PythonPtr file_descriptor{PyIter_Next(iter.get())}) {
    static constexpr Identifier id_Add("Add");
    const PythonPtr add_result(PyObject_CallMethodObjArgs(
        pool.get(), id_Add.get(), file_descriptor.get(), nullptr));
    if (ABSL_PREDICT_FALSE(add_result == nullptr)) return nullptr;
  }
  if (ABSL_PREDICT_FALSE(PyErr_Occurred() != nullptr)) return nullptr;
  // message_descriptor = pool.FindMessageTypeByName(record_type_name)
  static constexpr Identifier id_FindMessageTypeByName("FindMessageTypeByName");
  const PythonPtr message_descriptor(
      PyObject_CallMethodObjArgs(pool.get(), id_FindMessageTypeByName.get(),
                                 record_type_name.get(), nullptr));
  if (ABSL_PREDICT_FALSE(message_descriptor == nullptr)) return nullptr;
  // factory = MessageFactory(pool)
  static constexpr ImportedConstant kMessageFactory(
      "google.protobuf.message_factory", "MessageFactory");
  if (ABSL_PREDICT_FALSE(!kMessageFactory.Verify())) return nullptr;
  const PythonPtr factory(
      PyObject_CallFunctionObjArgs(kMessageFactory.get(), pool.get(), nullptr));
  if (ABSL_PREDICT_FALSE(factory == nullptr)) return nullptr;
  // return factory.GetPrototype(message_descriptor)
  static constexpr Identifier id_GetPrototype("GetPrototype");
  return PyObject_CallMethodObjArgs(factory.get(), id_GetPrototype.get(),
                                    message_descriptor.get(), nullptr);
}

struct PyRecordReaderObject {
  // clang-format off
  PyObject_HEAD
  static_assert(true, "");  // clang-format workaround.
  // clang-format on

  PythonWrapped<RecordReader<PythonReader>> record_reader;
  PyObject* recovery;
  PythonWrapped<Exception> recovery_exception;
};

extern PyTypeObject PyRecordReader_Type;

struct PyRecordIterObject {
  // clang-format off
  PyObject_HEAD
  static_assert(true, "");  // clang-format workaround.
  // clang-format on

  PyObject* (*read_record)(PyRecordReaderObject* self, PyObject* args);
  PyRecordReaderObject* record_reader;
  PyObject* args;
};

extern PyTypeObject PyRecordIter_Type;

bool RecordReaderHasException(PyRecordReaderObject* self) {
  return self->recovery_exception.has_value() ||
         !self->record_reader->healthy();
}

void SetExceptionFromRecordReader(PyRecordReaderObject* self) {
  if (self->recovery_exception.has_value()) {
    self->recovery_exception->Restore();
    return;
  }
  RIEGELI_ASSERT(!self->record_reader->healthy())
      << "Failed precondition of SetExceptionFromRecordReader(): "
         "RecordReader healthy";
  if (!self->record_reader->src().exception().ok()) {
    self->record_reader->src().exception().Restore();
    return;
  }
  SetRiegeliError(self->record_reader->status());
}

extern "C" void RecordReaderDestructor(PyRecordReaderObject* self) {
  PyObject_GC_UnTrack(self);
  Py_TRASHCAN_SAFE_BEGIN(self);
  PythonUnlocked([&] { self->record_reader.reset(); });
  Py_XDECREF(self->recovery);
  self->recovery_exception.reset();
  Py_TYPE(self)->tp_free(self);
  Py_TRASHCAN_SAFE_END(self);
}

extern "C" int RecordReaderTraverse(PyRecordReaderObject* self, visitproc visit,
                                    void* arg) {
  Py_VISIT(self->recovery);
  if (self->recovery_exception.has_value()) {
    const int recovery_exception_result =
        self->recovery_exception->Traverse(visit, arg);
    if (ABSL_PREDICT_FALSE(recovery_exception_result != 0)) {
      return recovery_exception_result;
    }
  }
  if (self->record_reader.has_value()) {
    return self->record_reader->src().Traverse(visit, arg);
  }
  return 0;
}

extern "C" int RecordReaderClear(PyRecordReaderObject* self) {
  PythonUnlocked([&] { self->record_reader.reset(); });
  Py_CLEAR(self->recovery);
  self->recovery_exception.reset();
  return 0;
}

absl::optional<int> VerifyFieldNumber(long field_number_value) {
  static_assert(Field::kExistenceOnly == 0,
                "VerifyFieldNumber() assumes that Field::kExistenceOnly == 0");
  if (ABSL_PREDICT_FALSE(field_number_value < Field::kExistenceOnly ||
                         field_number_value > (1 << 29) - 1)) {
    PyErr_Format(PyExc_OverflowError, "Field number out of range: %ld",
                 field_number_value);
    return absl::nullopt;
  }
  return IntCast<int>(field_number_value);
}

absl::optional<int> FieldNumberFromPython(PyObject* object) {
#if PY_MAJOR_VERSION < 3
  if (ABSL_PREDICT_TRUE(PyInt_Check(object))) {
    const long field_number_value = PyInt_AS_LONG(object);
    return VerifyFieldNumber(field_number_value);
  }
#endif
  if (ABSL_PREDICT_FALSE(!PyLong_Check(object))) {
    PyErr_Format(PyExc_TypeError, "Expected int, not %s",
                 Py_TYPE(object)->tp_name);
    return absl::nullopt;
  }
  const long field_number_value = PyLong_AsLong(object);
  if (ABSL_PREDICT_FALSE(field_number_value == -1) && PyErr_Occurred()) {
    return absl::nullopt;
  }
  return VerifyFieldNumber(field_number_value);
}

absl::optional<FieldProjection> FieldProjectionFromPython(PyObject* object) {
  riegeli::FieldProjection field_projection;
  const PythonPtr field_iter(PyObject_GetIter(object));
  if (ABSL_PREDICT_FALSE(field_iter == nullptr)) return absl::nullopt;
  while (const PythonPtr field_object{PyIter_Next(field_iter.get())}) {
    riegeli::Field field;
    const PythonPtr field_number_iter(PyObject_GetIter(field_object.get()));
    if (ABSL_PREDICT_FALSE(field_number_iter == nullptr)) return absl::nullopt;
    while (const PythonPtr field_number_object{
        PyIter_Next(field_number_iter.get())}) {
      const absl::optional<int> field_number =
          FieldNumberFromPython(field_number_object.get());
      if (ABSL_PREDICT_FALSE(field_number == absl::nullopt))
        return absl::nullopt;
      field.AddFieldNumber(*field_number);
    }
    if (ABSL_PREDICT_FALSE(PyErr_Occurred() != nullptr)) return absl::nullopt;
    field_projection.AddField(std::move(field));
  }
  if (ABSL_PREDICT_FALSE(PyErr_Occurred() != nullptr)) return absl::nullopt;
  return field_projection;
}

extern "C" int RecordReaderInit(PyRecordReaderObject* self, PyObject* args,
                                PyObject* kwargs) {
  static constexpr const char* keywords[] = {
      "src",      "close", "assumed_pos", "buffer_size", "field_projection",
      "recovery", nullptr};
  PyObject* src_arg;
  PyObject* close_arg = nullptr;
  PyObject* assumed_pos_arg = nullptr;
  PyObject* buffer_size_arg = nullptr;
  PyObject* field_projection_arg = nullptr;
  PyObject* recovery_arg = nullptr;
  if (ABSL_PREDICT_FALSE(!PyArg_ParseTupleAndKeywords(
          args, kwargs,
          "O|"
#if PY_VERSION_HEX >= 0x03030000
          "$"
#endif
          "OOOOO:RecordReader",
          const_cast<char**>(keywords), &src_arg, &close_arg, &assumed_pos_arg,
          &buffer_size_arg, &field_projection_arg, &recovery_arg))) {
    return -1;
  }

  PythonReader::Options python_reader_options;
  if (close_arg != nullptr) {
    const int close_is_true = PyObject_IsTrue(close_arg);
    if (ABSL_PREDICT_FALSE(close_is_true < 0)) return -1;
    python_reader_options.set_close(close_is_true != 0);
  }
  if (assumed_pos_arg != nullptr && assumed_pos_arg != Py_None) {
    const absl::optional<Position> assumed_pos =
        PositionFromPython(assumed_pos_arg);
    if (ABSL_PREDICT_FALSE(assumed_pos == absl::nullopt)) return -1;
    python_reader_options.set_assumed_pos(*assumed_pos);
  }
  if (buffer_size_arg != nullptr) {
    const absl::optional<size_t> buffer_size = SizeFromPython(buffer_size_arg);
    if (ABSL_PREDICT_FALSE(buffer_size == absl::nullopt)) return -1;
    python_reader_options.set_buffer_size(*buffer_size);
  }

  RecordReaderBase::Options record_reader_options;
  if (field_projection_arg != nullptr && field_projection_arg != Py_None) {
    absl::optional<FieldProjection> field_projection =
        FieldProjectionFromPython(field_projection_arg);
    if (ABSL_PREDICT_FALSE(field_projection == absl::nullopt)) return -1;
    record_reader_options.set_field_projection(*std::move(field_projection));
  }
  if (recovery_arg != nullptr && recovery_arg != Py_None) {
    Py_INCREF(recovery_arg);
    Py_XDECREF(self->recovery);
    self->recovery = recovery_arg;
    record_reader_options.set_recovery(
        [self](const SkippedRegion& skipped_region) {
          PythonLock lock;
          const PythonPtr begin_object =
              PositionToPython(skipped_region.begin());
          if (ABSL_PREDICT_FALSE(begin_object == nullptr)) {
            self->recovery_exception.emplace(Exception::Fetch());
            return false;
          }
          const PythonPtr end_object = PositionToPython(skipped_region.end());
          if (ABSL_PREDICT_FALSE(end_object == nullptr)) {
            self->recovery_exception.emplace(Exception::Fetch());
            return false;
          }
          const PythonPtr message_object =
              StringToPython(skipped_region.message());
          if (ABSL_PREDICT_FALSE(message_object == nullptr)) {
            self->recovery_exception.emplace(Exception::Fetch());
            return false;
          }
          static constexpr ImportedConstant kSkippedRegion(
              "riegeli.records.skipped_region", "SkippedRegion");
          if (ABSL_PREDICT_FALSE(!kSkippedRegion.Verify())) {
            self->recovery_exception.emplace(Exception::Fetch());
            return false;
          }
          const PythonPtr skipped_region_object(PyObject_CallFunctionObjArgs(
              kSkippedRegion.get(), begin_object.get(), end_object.get(),
              message_object.get(), nullptr));
          if (ABSL_PREDICT_FALSE(skipped_region_object == nullptr)) {
            self->recovery_exception.emplace(Exception::Fetch());
            return false;
          }
          const PythonPtr recovery_result(PyObject_CallFunctionObjArgs(
              self->recovery, skipped_region_object.get(), nullptr));
          if (ABSL_PREDICT_FALSE(recovery_result == nullptr)) {
            if (PyErr_ExceptionMatches(PyExc_StopIteration)) {
              PyErr_Clear();
            } else {
              self->recovery_exception.emplace(Exception::Fetch());
            }
            return false;
          }
          return true;
        });
  }

  PythonReader python_reader(src_arg, std::move(python_reader_options));
  PythonUnlocked([&] {
    self->record_reader.emplace(std::move(python_reader),
                                std::move(record_reader_options));
  });
  if (ABSL_PREDICT_FALSE(!self->record_reader->healthy())) {
    self->record_reader->src().Close();
    SetExceptionFromRecordReader(self);
    return -1;
  }
  return 0;
}

extern "C" PyObject* RecordReaderSrc(PyRecordReaderObject* self,
                                     void* closure) {
  PyObject* const src = ABSL_PREDICT_FALSE(!self->record_reader.has_value())
                            ? Py_None
                            : self->record_reader->src().src();
  Py_INCREF(src);
  return src;
}

extern "C" PyObject* RecordReaderRepr(PyRecordReaderObject* self) {
  const PythonPtr format = StringToPython("<RecordReader src={!r}>");
  if (ABSL_PREDICT_FALSE(format == nullptr)) return nullptr;
  // return format.format(self.src)
  PyObject* const src = ABSL_PREDICT_FALSE(!self->record_reader.has_value())
                            ? Py_None
                            : self->record_reader->src().src();
  static constexpr Identifier id_format("format");
  return PyObject_CallMethodObjArgs(format.get(), id_format.get(), src,
                                    nullptr);
}

extern "C" PyObject* RecordReaderEnter(PyObject* self, PyObject* args) {
  // return self
  Py_INCREF(self);
  return self;
}

extern "C" PyObject* RecordReaderExit(PyRecordReaderObject* self,
                                      PyObject* args) {
  PyObject* exc_type;
  PyObject* exc_value;
  PyObject* traceback;
  if (ABSL_PREDICT_FALSE(!PyArg_ParseTuple(args, "OOO:__exit__", &exc_type,
                                           &exc_value, &traceback))) {
    return nullptr;
  }
  // self.close(), suppressing exceptions if exc_type != None.
  if (ABSL_PREDICT_TRUE(self->record_reader.has_value())) {
    const bool ok =
        PythonUnlocked([&] { return self->record_reader->Close(); });
    if (ABSL_PREDICT_FALSE(!ok) && exc_type == Py_None) {
      SetExceptionFromRecordReader(self);
      return nullptr;
    }
  }
  Py_RETURN_FALSE;
}

extern "C" PyObject* RecordReaderClose(PyRecordReaderObject* self,
                                       PyObject* args) {
  if (ABSL_PREDICT_TRUE(self->record_reader.has_value())) {
    const bool ok =
        PythonUnlocked([&] { return self->record_reader->Close(); });
    if (ABSL_PREDICT_FALSE(!ok)) {
      SetExceptionFromRecordReader(self);
      return nullptr;
    }
  }
  Py_RETURN_NONE;
}

extern "C" PyObject* RecordReaderCheckFileFormat(PyRecordReaderObject* self,
                                                 PyObject* args) {
  if (ABSL_PREDICT_FALSE(!self->record_reader.Verify())) return nullptr;
  const bool ok =
      PythonUnlocked([&] { return self->record_reader->CheckFileFormat(); });
  if (ABSL_PREDICT_FALSE(!ok)) {
    if (ABSL_PREDICT_FALSE(RecordReaderHasException(self))) {
      SetExceptionFromRecordReader(self);
      return nullptr;
    }
    Py_RETURN_FALSE;
  }
  Py_RETURN_TRUE;
}

extern "C" PyObject* RecordReaderReadMetadata(PyRecordReaderObject* self,
                                              PyObject* args) {
  if (ABSL_PREDICT_FALSE(!self->record_reader.Verify())) return nullptr;
  Chain metadata;
  const bool ok = PythonUnlocked(
      [&] { return self->record_reader->ReadSerializedMetadata(&metadata); });
  if (ABSL_PREDICT_FALSE(!ok)) {
    if (ABSL_PREDICT_FALSE(RecordReaderHasException(self))) {
      SetExceptionFromRecordReader(self);
      return nullptr;
    }
    Py_RETURN_NONE;
  }
  const PythonPtr serialized_metadata = ChainToPython(metadata);
  if (ABSL_PREDICT_FALSE(serialized_metadata == nullptr)) return nullptr;
  // return RecordsMetadata.FromString(serialized_metadata)
  static constexpr ImportedConstant kRecordsMetadata(
      "riegeli.records.records_metadata_pb2", "RecordsMetadata");
  if (ABSL_PREDICT_FALSE(!kRecordsMetadata.Verify())) return nullptr;
  static constexpr Identifier id_FromString("FromString");
  return PyObject_CallMethodObjArgs(kRecordsMetadata.get(), id_FromString.get(),
                                    serialized_metadata.get(), nullptr);
}

extern "C" PyObject* RecordReaderReadSerializedMetadata(
    PyRecordReaderObject* self, PyObject* args) {
  if (ABSL_PREDICT_FALSE(!self->record_reader.Verify())) return nullptr;
  Chain metadata;
  const bool ok = PythonUnlocked(
      [&] { return self->record_reader->ReadSerializedMetadata(&metadata); });
  if (ABSL_PREDICT_FALSE(!ok)) {
    if (ABSL_PREDICT_FALSE(RecordReaderHasException(self))) {
      SetExceptionFromRecordReader(self);
      return nullptr;
    }
    Py_RETURN_NONE;
  }
  return ChainToPython(metadata).release();
}

extern "C" PyObject* RecordReaderReadRecord(PyRecordReaderObject* self,
                                            PyObject* args) {
  if (ABSL_PREDICT_FALSE(!self->record_reader.Verify())) return nullptr;
  Chain record;
  const bool ok =
      PythonUnlocked([&] { return self->record_reader->ReadRecord(&record); });
  if (ABSL_PREDICT_FALSE(!ok)) {
    if (ABSL_PREDICT_FALSE(RecordReaderHasException(self))) {
      SetExceptionFromRecordReader(self);
      return nullptr;
    }
    Py_RETURN_NONE;
  }
  return ChainToPython(record).release();
}

extern "C" PyObject* RecordReaderReadRecordWithKey(PyRecordReaderObject* self,
                                                   PyObject* args) {
  if (ABSL_PREDICT_FALSE(!self->record_reader.Verify())) return nullptr;
  Chain record;
  RecordPosition key;
  const bool ok = PythonUnlocked(
      [&] { return self->record_reader->ReadRecord(&record, &key); });
  if (ABSL_PREDICT_FALSE(!ok)) {
    if (ABSL_PREDICT_FALSE(RecordReaderHasException(self))) {
      SetExceptionFromRecordReader(self);
      return nullptr;
    }
    Py_RETURN_NONE;
  }
  if (ABSL_PREDICT_FALSE(!kRecordPositionApi.Verify())) return nullptr;
  const PythonPtr key_object(
      kRecordPositionApi->RecordPositionToPython(FutureRecordPosition(key)));
  if (ABSL_PREDICT_FALSE(key_object == nullptr)) return nullptr;
  const PythonPtr record_object = ChainToPython(record);
  if (ABSL_PREDICT_FALSE(record_object == nullptr)) return nullptr;
  // return key, record
  return PyTuple_Pack(2, key_object.get(), record_object.get());
}

extern "C" PyObject* RecordReaderReadMessage(PyRecordReaderObject* self,
                                             PyObject* args, PyObject* kwargs) {
  static constexpr const char* keywords[] = {"message_type", nullptr};
  PyObject* message_type_arg;
  if (ABSL_PREDICT_FALSE(!PyArg_ParseTupleAndKeywords(
          args, kwargs, "O:read_message", const_cast<char**>(keywords),
          &message_type_arg))) {
    return nullptr;
  }
  if (ABSL_PREDICT_FALSE(!self->record_reader.Verify())) return nullptr;
  Chain record;
  const bool ok =
      PythonUnlocked([&] { return self->record_reader->ReadRecord(&record); });
  if (ABSL_PREDICT_FALSE(!ok)) {
    if (ABSL_PREDICT_FALSE(RecordReaderHasException(self))) {
      SetExceptionFromRecordReader(self);
      return nullptr;
    }
    Py_RETURN_NONE;
  }
  const PythonPtr record_object = ChainToPython(record);
  if (ABSL_PREDICT_FALSE(record_object == nullptr)) return nullptr;
  // return message_type.FromString(record)
  static constexpr Identifier id_FromString("FromString");
  return PyObject_CallMethodObjArgs(message_type_arg, id_FromString.get(),
                                    record_object.get(), nullptr);
}

extern "C" PyObject* RecordReaderReadMessageWithKey(PyRecordReaderObject* self,
                                                    PyObject* args,
                                                    PyObject* kwargs) {
  static constexpr const char* keywords[] = {"message_type", nullptr};
  PyObject* message_type_arg;
  if (ABSL_PREDICT_FALSE(!PyArg_ParseTupleAndKeywords(
          args, kwargs, "O:read_message_with_key", const_cast<char**>(keywords),
          &message_type_arg))) {
    return nullptr;
  }
  if (ABSL_PREDICT_FALSE(!self->record_reader.Verify())) return nullptr;
  Chain record;
  RecordPosition key;
  const bool ok = PythonUnlocked(
      [&] { return self->record_reader->ReadRecord(&record, &key); });
  if (ABSL_PREDICT_FALSE(!ok)) {
    if (ABSL_PREDICT_FALSE(RecordReaderHasException(self))) {
      SetExceptionFromRecordReader(self);
      return nullptr;
    }
    Py_RETURN_NONE;
  }
  if (ABSL_PREDICT_FALSE(!kRecordPositionApi.Verify())) return nullptr;
  const PythonPtr key_object(
      kRecordPositionApi->RecordPositionToPython(FutureRecordPosition(key)));
  if (ABSL_PREDICT_FALSE(key_object == nullptr)) return nullptr;
  const PythonPtr record_object = ChainToPython(record);
  if (ABSL_PREDICT_FALSE(record_object == nullptr)) return nullptr;
  // message = message_type.FromString(record)
  static constexpr Identifier id_FromString("FromString");
  const PythonPtr message(PyObject_CallMethodObjArgs(
      message_type_arg, id_FromString.get(), record_object.get(), nullptr));
  if (ABSL_PREDICT_FALSE(message == nullptr)) return nullptr;
  // return key, message
  return PyTuple_Pack(2, key_object.get(), message.get());
}

extern "C" PyRecordIterObject* RecordReaderReadRecords(
    PyRecordReaderObject* self, PyObject* args) {
  std::unique_ptr<PyRecordIterObject, Deleter> iter(
      PyObject_GC_New(PyRecordIterObject, &PyRecordIter_Type));
  if (ABSL_PREDICT_FALSE(iter == nullptr)) return nullptr;
  iter->read_record = [](PyRecordReaderObject* self, PyObject* args) {
    return RecordReaderReadRecord(self, args);
  };
  Py_INCREF(self);
  iter->record_reader = self;
  iter->args = nullptr;
  return iter.release();
}

extern "C" PyRecordIterObject* RecordReaderReadRecordsWithKeys(
    PyRecordReaderObject* self, PyObject* args) {
  std::unique_ptr<PyRecordIterObject, Deleter> iter(
      PyObject_GC_New(PyRecordIterObject, &PyRecordIter_Type));
  if (ABSL_PREDICT_FALSE(iter == nullptr)) return nullptr;
  iter->read_record = [](PyRecordReaderObject* self, PyObject* args) {
    return RecordReaderReadRecordWithKey(self, args);
  };
  Py_INCREF(self);
  iter->record_reader = self;
  iter->args = nullptr;
  return iter.release();
}

extern "C" PyRecordIterObject* RecordReaderReadMessages(
    PyRecordReaderObject* self, PyObject* args, PyObject* kwargs) {
  static constexpr const char* keywords[] = {"message_type", nullptr};
  PyObject* message_type_arg;
  if (ABSL_PREDICT_FALSE(!PyArg_ParseTupleAndKeywords(
          args, kwargs, "O:read_messages", const_cast<char**>(keywords),
          &message_type_arg))) {
    return nullptr;
  }
  std::unique_ptr<PyRecordIterObject, Deleter> iter(
      PyObject_GC_New(PyRecordIterObject, &PyRecordIter_Type));
  if (ABSL_PREDICT_FALSE(iter == nullptr)) return nullptr;
  iter->read_record = [](PyRecordReaderObject* self, PyObject* args) {
    return RecordReaderReadMessage(self, args, nullptr);
  };
  Py_INCREF(self);
  iter->record_reader = self;
  iter->args = PyTuple_Pack(1, message_type_arg);
  if (ABSL_PREDICT_FALSE(iter->args == nullptr)) return nullptr;
  return iter.release();
}

extern "C" PyRecordIterObject* RecordReaderReadMessagesWithKeys(
    PyRecordReaderObject* self, PyObject* args, PyObject* kwargs) {
  static constexpr const char* keywords[] = {"message_type", nullptr};
  PyObject* message_type_arg;
  if (ABSL_PREDICT_FALSE(!PyArg_ParseTupleAndKeywords(
          args, kwargs, "O:read_messages_with_keys",
          const_cast<char**>(keywords), &message_type_arg))) {
    return nullptr;
  }
  std::unique_ptr<PyRecordIterObject, Deleter> iter(
      PyObject_GC_New(PyRecordIterObject, &PyRecordIter_Type));
  if (ABSL_PREDICT_FALSE(iter == nullptr)) return nullptr;
  iter->read_record = [](PyRecordReaderObject* self, PyObject* args) {
    return RecordReaderReadMessageWithKey(self, args, nullptr);
  };
  Py_INCREF(self);
  iter->record_reader = self;
  iter->args = PyTuple_Pack(1, message_type_arg);
  if (ABSL_PREDICT_FALSE(iter->args == nullptr)) return nullptr;
  return iter.release();
}

extern "C" PyObject* RecordReaderSetFieldProjection(PyRecordReaderObject* self,
                                                    PyObject* args,
                                                    PyObject* kwargs) {
  static constexpr const char* keywords[] = {"field_projection", nullptr};
  PyObject* field_projection_arg;
  if (ABSL_PREDICT_FALSE(!PyArg_ParseTupleAndKeywords(
          args, kwargs, "O:set_field_projection", const_cast<char**>(keywords),
          &field_projection_arg))) {
    return nullptr;
  }
  absl::optional<FieldProjection> field_projection;
  if (field_projection_arg == Py_None) {
    field_projection = FieldProjection::All();
  } else {
    field_projection = FieldProjectionFromPython(field_projection_arg);
    if (ABSL_PREDICT_FALSE(field_projection == absl::nullopt)) return nullptr;
  }
  if (ABSL_PREDICT_FALSE(!self->record_reader.Verify())) return nullptr;
  const bool ok = PythonUnlocked([&] {
    return self->record_reader->SetFieldProjection(
        *std::move(field_projection));
  });
  if (ABSL_PREDICT_FALSE(!ok)) {
    SetExceptionFromRecordReader(self);
    return nullptr;
  }
  Py_RETURN_NONE;
}

extern "C" PyObject* RecordReaderPos(PyRecordReaderObject* self,
                                     void* closure) {
  if (ABSL_PREDICT_FALSE(!self->record_reader.Verify())) return nullptr;
  if (ABSL_PREDICT_FALSE(!kRecordPositionApi.Verify())) return nullptr;
  return kRecordPositionApi
      ->RecordPositionToPython(FutureRecordPosition(self->record_reader->pos()))
      .release();
}

extern "C" PyObject* RecordReaderSupportsRandomAccess(
    PyRecordReaderObject* self, void* closure) {
  if (ABSL_PREDICT_FALSE(!self->record_reader.Verify())) return nullptr;
  return PyBool_FromLong(self->record_reader->SupportsRandomAccess());
}

extern "C" PyObject* RecordReaderSeek(PyRecordReaderObject* self,
                                      PyObject* args, PyObject* kwargs) {
  static constexpr const char* keywords[] = {"pos", nullptr};
  PyObject* pos_arg;
  if (ABSL_PREDICT_FALSE(!PyArg_ParseTupleAndKeywords(
          args, kwargs, "O:seek", const_cast<char**>(keywords), &pos_arg))) {
    return nullptr;
  }
  if (ABSL_PREDICT_FALSE(!kRecordPositionApi.Verify())) return nullptr;
  const absl::optional<RecordPosition> pos =
      kRecordPositionApi->RecordPositionFromPython(pos_arg);
  if (ABSL_PREDICT_FALSE(pos == absl::nullopt)) return nullptr;
  if (ABSL_PREDICT_FALSE(!self->record_reader.Verify())) return nullptr;
  const bool ok =
      PythonUnlocked([&] { return self->record_reader->Seek(*pos); });
  if (ABSL_PREDICT_FALSE(!ok)) {
    SetExceptionFromRecordReader(self);
    return nullptr;
  }
  Py_RETURN_NONE;
}

extern "C" PyObject* RecordReaderSeekNumeric(PyRecordReaderObject* self,
                                             PyObject* args, PyObject* kwargs) {
  static constexpr const char* keywords[] = {"pos", nullptr};
  PyObject* pos_arg;
  if (ABSL_PREDICT_FALSE(!PyArg_ParseTupleAndKeywords(
          args, kwargs, "O:seek_numeric", const_cast<char**>(keywords),
          &pos_arg))) {
    return nullptr;
  }
  const absl::optional<Position> pos = PositionFromPython(pos_arg);
  if (ABSL_PREDICT_FALSE(pos == absl::nullopt)) return nullptr;
  if (ABSL_PREDICT_FALSE(!self->record_reader.Verify())) return nullptr;
  const bool ok =
      PythonUnlocked([&] { return self->record_reader->Seek(*pos); });
  if (ABSL_PREDICT_FALSE(!ok)) {
    SetExceptionFromRecordReader(self);
    return nullptr;
  }
  Py_RETURN_NONE;
}

extern "C" PyObject* RecordReaderSeekBack(PyRecordReaderObject* self,
                                          PyObject* args) {
  if (ABSL_PREDICT_FALSE(!self->record_reader.Verify())) return nullptr;
  const bool ok =
      PythonUnlocked([&] { return self->record_reader->SeekBack(); });
  if (ABSL_PREDICT_FALSE(!ok)) {
    if (ABSL_PREDICT_FALSE(RecordReaderHasException(self))) {
      SetExceptionFromRecordReader(self);
      return nullptr;
    }
    Py_RETURN_FALSE;
  }
  Py_RETURN_TRUE;
}

extern "C" PyObject* RecordReaderSize(PyRecordReaderObject* self,
                                      PyObject* args) {
  if (ABSL_PREDICT_FALSE(!self->record_reader.Verify())) return nullptr;
  const absl::optional<Position> size =
      PythonUnlocked([&] { return self->record_reader->Size(); });
  if (ABSL_PREDICT_FALSE(size == absl::nullopt)) {
    SetExceptionFromRecordReader(self);
    return nullptr;
  }
  return PositionToPython(*size).release();
}

extern "C" PyObject* RecordReaderSearch(PyRecordReaderObject* self,
                                        PyObject* args, PyObject* kwargs) {
  static constexpr const char* keywords[] = {"test", nullptr};
  PyObject* test_arg;
  if (ABSL_PREDICT_FALSE(!PyArg_ParseTupleAndKeywords(
          args, kwargs, "O:search", const_cast<char**>(keywords), &test_arg))) {
    return nullptr;
  }
  if (ABSL_PREDICT_FALSE(!self->record_reader.Verify())) return nullptr;
  absl::optional<Exception> test_exception;
  const bool ok = PythonUnlocked([&] {
    return self->record_reader->Search([&](RecordReaderBase*) {
      PythonLock lock;
      const PythonPtr test_result(
          PyObject_CallFunctionObjArgs(test_arg, self, nullptr));
      if (ABSL_PREDICT_FALSE(test_result == nullptr)) {
        test_exception.emplace(Exception::Fetch());
        return absl::partial_ordering::equivalent;
      }
      const absl::optional<absl::partial_ordering> ordering =
          PartialOrderingFromPython(test_result.get());
      if (ABSL_PREDICT_FALSE(ordering == absl::nullopt)) {
        test_exception.emplace(Exception::Fetch());
        return absl::partial_ordering::equivalent;
      }
      return *ordering;
    });
  });
  if (ABSL_PREDICT_FALSE(test_exception != absl::nullopt)) {
    test_exception->Restore();
    return nullptr;
  }
  if (ABSL_PREDICT_FALSE(!ok)) {
    SetExceptionFromRecordReader(self);
    return nullptr;
  }
  Py_RETURN_NONE;
}

extern "C" PyObject* RecordReaderSearchForRecord(PyRecordReaderObject* self,
                                                 PyObject* args,
                                                 PyObject* kwargs) {
  static constexpr const char* keywords[] = {"test", nullptr};
  PyObject* test_arg;
  if (ABSL_PREDICT_FALSE(!PyArg_ParseTupleAndKeywords(
          args, kwargs, "O:search_for_record", const_cast<char**>(keywords),
          &test_arg))) {
    return nullptr;
  }
  if (ABSL_PREDICT_FALSE(!self->record_reader.Verify())) return nullptr;
  absl::optional<Exception> test_exception;
  const bool ok = PythonUnlocked([&] {
    return self->record_reader->Search<Chain>([&](const Chain& record) {
      PythonLock lock;
      const PythonPtr record_object = ChainToPython(record);
      if (ABSL_PREDICT_FALSE(record_object == nullptr)) {
        test_exception.emplace(Exception::Fetch());
        return absl::partial_ordering::equivalent;
      }
      const PythonPtr test_result(
          PyObject_CallFunctionObjArgs(test_arg, record_object.get(), nullptr));
      if (ABSL_PREDICT_FALSE(test_result == nullptr)) {
        test_exception.emplace(Exception::Fetch());
        return absl::partial_ordering::equivalent;
      }
      const absl::optional<absl::partial_ordering> ordering =
          PartialOrderingFromPython(test_result.get());
      if (ABSL_PREDICT_FALSE(ordering == absl::nullopt)) {
        test_exception.emplace(Exception::Fetch());
        return absl::partial_ordering::equivalent;
      }
      return *ordering;
    });
  });
  if (ABSL_PREDICT_FALSE(test_exception != absl::nullopt)) {
    test_exception->Restore();
    return nullptr;
  }
  if (ABSL_PREDICT_FALSE(!ok)) {
    SetExceptionFromRecordReader(self);
    return nullptr;
  }
  Py_RETURN_NONE;
}

extern "C" PyObject* RecordReaderSearchForMessage(PyRecordReaderObject* self,
                                                  PyObject* args,
                                                  PyObject* kwargs) {
  static constexpr const char* keywords[] = {"message_type", "test", nullptr};
  PyObject* message_type_arg;
  PyObject* test_arg;
  if (ABSL_PREDICT_FALSE(!PyArg_ParseTupleAndKeywords(
          args, kwargs, "OO:search_for_message", const_cast<char**>(keywords),
          &message_type_arg, &test_arg))) {
    return nullptr;
  }
  if (ABSL_PREDICT_FALSE(!self->record_reader.Verify())) return nullptr;
  absl::optional<Exception> test_exception;
  const bool ok = PythonUnlocked([&] {
    return self->record_reader->Search<Chain>([&](const Chain& record) {
      PythonLock lock;
      const PythonPtr record_object = ChainToPython(record);
      if (ABSL_PREDICT_FALSE(record_object == nullptr)) {
        test_exception.emplace(Exception::Fetch());
        return absl::partial_ordering::equivalent;
      }
      // message = message_type.FromString(record)
      static constexpr Identifier id_FromString("FromString");
      const PythonPtr message_object(PyObject_CallMethodObjArgs(
          message_type_arg, id_FromString.get(), record_object.get(), nullptr));
      if (ABSL_PREDICT_FALSE(message_object == nullptr)) {
        test_exception.emplace(Exception::Fetch());
        return absl::partial_ordering::equivalent;
      }
      const PythonPtr test_result(PyObject_CallFunctionObjArgs(
          test_arg, message_object.get(), nullptr));
      if (ABSL_PREDICT_FALSE(test_result == nullptr)) {
        test_exception.emplace(Exception::Fetch());
        return absl::partial_ordering::equivalent;
      }
      const absl::optional<absl::partial_ordering> ordering =
          PartialOrderingFromPython(test_result.get());
      if (ABSL_PREDICT_FALSE(ordering == absl::nullopt)) {
        test_exception.emplace(Exception::Fetch());
        return absl::partial_ordering::equivalent;
      }
      return *ordering;
    });
  });
  if (ABSL_PREDICT_FALSE(test_exception != absl::nullopt)) {
    test_exception->Restore();
    return nullptr;
  }
  if (ABSL_PREDICT_FALSE(!ok)) {
    SetExceptionFromRecordReader(self);
    return nullptr;
  }
  Py_RETURN_NONE;
}

const PyMethodDef RecordReaderMethods[] = {
    {"__enter__", RecordReaderEnter, METH_NOARGS,
     R"doc(
__enter__(self) -> RecordReader

Returns self.
)doc"},
    {"__exit__", reinterpret_cast<PyCFunction>(RecordReaderExit), METH_VARARGS,
     R"doc(
__exit__(self, exc_type, exc_value, traceback) -> bool

Calls close().

Suppresses exceptions from close() if an exception is already in flight.

Args:
  exc_type: None or exception in flight (type).
  exc_value: None or exception in flight (value).
  traceback: None or exception in flight (traceback).
)doc"},
    {"close", reinterpret_cast<PyCFunction>(RecordReaderClose), METH_NOARGS,
     R"doc(
close(self) -> None

Indicates that reading is done.

Verifies that the file is not truncated at the current position, i.e. that it
either has more data or ends cleanly. Marks the RecordReader as closed,
disallowing further reading.

If the RecordReader was failed, raises the same exception again.

If the RecordReader was not failed but already closed, does nothing.
)doc"},
    {"check_file_format",
     reinterpret_cast<PyCFunction>(RecordReaderCheckFileFormat), METH_NOARGS,
     R"doc(
check_file_format(self) -> bool

Ensures that the file looks like a valid Riegeli/Records file.

Reading functions already check the file format. check_file_format() can verify
the file format before (or instead of) performing other operations.

This ignores the recovery function. If invalid file contents are skipped, then
checking the file format is meaningless: any file can be read.

Returns:
  True if this looks like a Riegeli/records file. False if the file ends before
  this could be determined.

Raises:
  RiegeliError: If this is not a Riegeli/records file.
)doc"},
    {"read_metadata", reinterpret_cast<PyCFunction>(RecordReaderReadMetadata),
     METH_NOARGS, R"doc(
read_metadata(self) -> Optional[RecordsMetadata]

Returns file metadata.

Record type in metadata can be conveniently interpreted by get_record_type().

read_metadata() must be called while the RecordReader is at the beginning of the
file (calling check_file_format() before is allowed).

Returns:
  File metadata as parsed RecordsMetadata message, or None at end of file.
)doc"},
    {"read_serialized_metadata",
     reinterpret_cast<PyCFunction>(RecordReaderReadSerializedMetadata),
     METH_NOARGS, R"doc(
read_serialized_metadata(self) -> Optional[bytes]

Returns file metadata.

This is like read_metadata(), but metadata is returned in the serialized form.
This is faster if the caller needs metadata already serialized.

Returns:
  File metadata as serialized RecordsMetadata message, or None at end of file.
)doc"},
    {"read_record", reinterpret_cast<PyCFunction>(RecordReaderReadRecord),
     METH_NOARGS, R"doc(
read_record(self) -> Optional[bytes]

Reads the next record.

Returns:
  The record read as bytes, or None at end of file.
)doc"},
    {"read_record_with_key",
     reinterpret_cast<PyCFunction>(RecordReaderReadRecordWithKey), METH_NOARGS,
     R"doc(
read_record_with_key(self) -> Optional[Tuple[RecordPosition, bytes]]

Reads the next record.

Returns:
  If successful, a tuple of canonical record position and the record read as
  bytes. Returns None at end of file.
)doc"},
    {"read_message", reinterpret_cast<PyCFunction>(RecordReaderReadMessage),
     METH_VARARGS | METH_KEYWORDS, R"doc(
read_message(self, message_type: Type[Message]) -> Optional[Message]

Reads the next record.

Args:
  message_type: Type of the message to parse the record as.

Returns:
  The record read as a parsed message, or None at end of file.
)doc"},
    {"read_message_with_key",
     reinterpret_cast<PyCFunction>(RecordReaderReadMessageWithKey),
     METH_VARARGS | METH_KEYWORDS, R"doc(
read_message_with_key(
    self, message_type: Type[Message]
) -> Optional[Tuple[RecordPosition, Message]]

Reads the next record.

Args:
  message_type: Type of the message to parse the record as.

Returns:
  If successful, a tuple of the canonical record position and the record read as
  a parsed message. Returns None at end of file.
)doc"},
    {"read_records", reinterpret_cast<PyCFunction>(RecordReaderReadRecords),
     METH_NOARGS, R"doc(
read_records(self) -> Iterator[bytes]

Returns an iterator which reads all remaining records.

Yields:
  The next record read as bytes.
)doc"},
    {"read_records_with_keys",
     reinterpret_cast<PyCFunction>(RecordReaderReadRecordsWithKeys),
     METH_NOARGS, R"doc(
read_records_with_keys(self) -> Iterator[Tuple[RecordPosition, bytes]]

Returns an iterator which reads all remaining records.

Yields:
  If successful, a tuple of canonical record position and the next record read
  as bytes.
)doc"},
    {"read_messages", reinterpret_cast<PyCFunction>(RecordReaderReadMessages),
     METH_VARARGS | METH_KEYWORDS, R"doc(
read_messages(self, message_type: Type[Message]) -> Iterator[Message]

Returns an iterator which reads all remaining records.

Yields:
  The next record read as parsed message.
)doc"},
    {"read_messages_with_keys",
     reinterpret_cast<PyCFunction>(RecordReaderReadMessagesWithKeys),
     METH_VARARGS | METH_KEYWORDS, R"doc(
read_messages_with_keys(
    self, message_type: Type[Message]
) -> Iterator[Tuple[RecordPosition, Message]]

Returns an iterator which reads all remaining records.

Yields:
  A tuple of the canonical record position and the next record read as parsed
  message.
)doc"},
    {"seek", reinterpret_cast<PyCFunction>(RecordReaderSeek),
     METH_VARARGS | METH_KEYWORDS, R"doc(
seek(self, pos: RecordPosition) -> None

Seeks to a position.

The position should have been obtained by pos for the same file.

Args:
  pos: Seek target.
)doc"},
    {"seek_numeric", reinterpret_cast<PyCFunction>(RecordReaderSeekNumeric),
     METH_VARARGS | METH_KEYWORDS, R"doc(
seek_numeric(self, pos: int) -> None

Seeks to a position.

The position can be any integer between 0 and file size. If it points between
records, it is interpreted as the next record.

Args:
  pos: Seek target.
)doc"},
    {"seek_back", reinterpret_cast<PyCFunction>(RecordReaderSeekBack),
     METH_NOARGS, R"doc(
seek_back(self) -> bool

Seeks back by one record.

Returns:
  If successful, True. Returns False at the beginning of the file.
)doc"},
    {"size", reinterpret_cast<PyCFunction>(RecordReaderSize), METH_NOARGS,
     R"doc(
size(self) -> int

Returns the size of the file in bytes.

This is the position corresponding to its end.
)doc"},
    {"search", reinterpret_cast<PyCFunction>(RecordReaderSearch),
     METH_VARARGS | METH_KEYWORDS,
     R"doc(
search(self, test: Callable[[RecordReader], Optional[int]]) -> None

Searches the file for a desired record, or for a desired position between
records, given that it is possible to determine whether a given record is before
or after the desired position.

The current position before calling search() does not matter.

Args:
  test: A function which takes the RecordReader as a parameter, seeked to some
    record, and returns an int or None:
     * < 0:  the current record is before the desired position
     * == 0: the current record is desired, searching can stop
     * > 0:  the current record is after the desired position
     * None: it could not be determined which is the case; the current record
             will be skipped

Preconditions:
 * all < 0 records precede all == 0 records
 * all < 0 records precede all > 0 records
 * all == 0 records precede all > 0 records

If there is some == 0 record, search() points to some == 0 record. Otherwise,
if there is some > 0 record, search() points to earliest > 0 record. Otherwise
search() points to the end of file.

To find the earliest == 0 record instead of an arbitrary one, test() can be
changed to return > 0 in place of == 0.

Further guarantees:
 * If a test() returns == 0, search() points back to the record before test()
   and returns.
 * If a test() returns < 0, test() will not be called again at earlier
   positions.
 * If a test() returns > 0, test() will not be called again at later positions.
 * test() will not be called again at the same position.

It follows that if a test() returns == 0 or > 0, search() points to the record
before the last test() call with one of these results. This allows to
communicate additional context of a == 0 or > 0 result by a side effect of
test().
)doc"},
    {"search_for_record",
     reinterpret_cast<PyCFunction>(RecordReaderSearchForRecord),
     METH_VARARGS | METH_KEYWORDS,
     R"doc(
search_for_record(self, test: Callable[[bytes], Optional[int]]) -> None

A variant of search() which reads a record before calling test(), instead of
letting test() read the record.

Args:
  test: A function which takes the record read as bytes as a parameter, and
    returns an int or None, like in search().
)doc"},
    {"search_for_message",
     reinterpret_cast<PyCFunction>(RecordReaderSearchForMessage),
     METH_VARARGS | METH_KEYWORDS,
     R"doc(
search_for_message(
    self, message_type: Type[Message],
    test: Callable[[Message], Optional[int]]
) -> None

A variant of search() which reads a record before calling test(), instead of
letting test() read the record.

Args:
  message_type: Type of the message to parse the record as.
  test: A function which takes the record read as a parsed message as a
    parameter, and returns an int or None, like in search().
)doc"},
    {nullptr, nullptr, 0, nullptr},
};

const PyGetSetDef RecordReaderGetSet[] = {
    {const_cast<char*>("src"), reinterpret_cast<getter>(RecordReaderSrc),
     nullptr, const_cast<char*>(R"doc(
src: BinaryIO

Binary IO stream being read from.
)doc"),
     nullptr},
    {const_cast<char*>("pos"), reinterpret_cast<getter>(RecordReaderPos),
     nullptr, const_cast<char*>(R"doc(
pos: RecordPosition

The current position.

pos.numeric returns the position as an int.

A position returned by pos before reading a record is not greater than the
canonical position returned by read_record_with_key() for that record, but
seeking to either position will read the same record.

pos is unchanged by close().
)doc"),
     nullptr},
    {const_cast<char*>("supports_random_access"),
     reinterpret_cast<getter>(RecordReaderSupportsRandomAccess), nullptr,
     const_cast<char*>(R"doc(
supports_random_access: bool

True if this RecordReader supports random access.

This includes seek(), seek_numeric(), and size().
)doc"),
     nullptr},
    {nullptr, nullptr, nullptr, nullptr, nullptr}};

PyTypeObject PyRecordReader_Type = {
    // clang-format off
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    // clang-format on
    "riegeli.records.record_reader.RecordReader",          // tp_name
    sizeof(PyRecordReaderObject),                          // tp_basicsize
    0,                                                     // tp_itemsize
    reinterpret_cast<destructor>(RecordReaderDestructor),  // tp_dealloc
    nullptr,                                               // tp_print
    nullptr,                                               // tp_getattr
    nullptr,                                               // tp_setattr
#if PY_MAJOR_VERSION >= 3
    nullptr,  // tp_as_async
#else
    nullptr,  // tp_compare
#endif
    reinterpret_cast<reprfunc>(RecordReaderRepr),  // tp_repr
    nullptr,                                       // tp_as_number
    nullptr,                                       // tp_as_sequence
    nullptr,                                       // tp_as_mapping
    nullptr,                                       // tp_hash
    nullptr,                                       // tp_call
    nullptr,                                       // tp_str
    nullptr,                                       // tp_getattro
    nullptr,                                       // tp_setattro
    nullptr,                                       // tp_as_buffer
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_GC,  // tp_flags
    R"doc(
def RecordReader(
    src: BinaryIO,
    *,
    close: bool = True,
    assumed_pos: Optional[int] = None,
    buffer_size: int = 64 << 10,
    field_projection: Optional[Iterable[Iterable[int]]] = None,
    recovery: Optional[Callable[[SkippedRegion], Any]] = None) -> RecordReader

Will read from the given file.

Args:
  src: Binary IO stream to read from.
  close: If True, src is owned, and close() or __exit__() will call src.close().
  assumed_pos: If None, src must support random access, RecordReader will
    support random access, and RecordReader will set the position of src on
    close(). If an int, it is enough that src supports sequential access, and
    this position will be assumed initially.
  buffer_size: Tunes how much data is buffered after reading from src.
  field_projection: If not None, the set of fields to be included in returned
    records, allowing to exclude the remaining fields (but does not guarantee
    that they will be excluded). Excluding data makes reading faster. Projection
    is effective if the file has been written with "transpose" in RecordWriter
    options. Additionally, "bucket_fraction" in RecordWriter options with a
    lower value can make reading with projection faster. A field projection is
    specified as an iterable of field paths. A field path is specified as an
    iterable of proto field numbers descending from the root message. A special
    field EXISTENCE_ONLY can be added to the end of the path; it preserves
    field existence but ignores its value; warning: for a repeated field this
    preserves the field count only if the field is not packed.
  recovery: If None, then invalid file contents cause RecordReader to raise
    RiegeliError. If not None, then invalid file contents cause RecordReader to
    skip over the invalid region and call this recovery function with a
    SkippedRegion as an argument. If the recovery function returns normally,
    reading continues. If the recovery function raises StopIteration, reading
    ends. If close() is called and file contents were truncated, the recovery
    function is called if set; the RecordReader remains closed.

The src argument should be a binary IO stream which supports:
 * close()          - for close() or __exit__() unless close is False
 * readinto1(memoryview) or readinto(memoryview) or read1(int) or read(int)
 * seek(int[, int]) - unless assumed_pos is not None,
                      or for seek(), seek_numeric(), or size()
 * tell()           - unless assumed_pos is not None,
                      or for seek(), seek_numeric(), or size()

Example values for src:
 * io.FileIO(filename, 'rb')
 * io.open(filename, 'rb') - better with buffering=0 or use io.FileIO()
 * open(filename, 'rb') - better with buffering=0 or use io.FileIO()
 * io.BytesIO(contents)
 * tf.io.gfile.GFile(filename, 'rb')

Warning: if close is False and assumed_pos is not None, src will have an
unpredictable amount of extra data consumed because of buffering.
)doc",                                                              // tp_doc
    reinterpret_cast<traverseproc>(RecordReaderTraverse),  // tp_traverse
    reinterpret_cast<inquiry>(RecordReaderClear),          // tp_clear
    nullptr,                                               // tp_richcompare
    0,                                                     // tp_weaklistoffset
    nullptr,                                               // tp_iter
    nullptr,                                               // tp_iternext
    const_cast<PyMethodDef*>(RecordReaderMethods),         // tp_methods
    nullptr,                                               // tp_members
    const_cast<PyGetSetDef*>(RecordReaderGetSet),          // tp_getset
    nullptr,                                               // tp_base
    nullptr,                                               // tp_dict
    nullptr,                                               // tp_descr_get
    nullptr,                                               // tp_descr_set
    0,                                                     // tp_dictoffset
    reinterpret_cast<initproc>(RecordReaderInit),          // tp_init
    nullptr,                                               // tp_alloc
    PyType_GenericNew,                                     // tp_new
    nullptr,                                               // tp_free
    nullptr,                                               // tp_is_gc
    nullptr,                                               // tp_bases
    nullptr,                                               // tp_mro
    nullptr,                                               // tp_cache
    nullptr,                                               // tp_subclasses
    nullptr,                                               // tp_weaklist
    nullptr,                                               // tp_del
    0,                                                     // tp_version_tag
#if PY_VERSION_HEX >= 0x030400a1
    nullptr,  // tp_finalize
#endif
};

extern "C" void RecordIterDestructor(PyRecordIterObject* self) {
  PyObject_GC_UnTrack(self);
  Py_TRASHCAN_SAFE_BEGIN(self);
  Py_XDECREF(self->record_reader);
  Py_XDECREF(self->args);
  Py_TYPE(self)->tp_free(self);
  Py_TRASHCAN_SAFE_END(self);
}

extern "C" int RecordIterTraverse(PyRecordIterObject* self, visitproc visit,
                                  void* arg) {
  Py_VISIT(self->record_reader);
  Py_VISIT(self->args);
  return 0;
}

extern "C" int RecordIterClear(PyRecordIterObject* self) {
  Py_CLEAR(self->record_reader);
  Py_CLEAR(self->args);
  return 0;
}

extern "C" PyObject* RecordIterNext(PyRecordIterObject* self) {
  PythonPtr read_record_result(
      self->read_record(self->record_reader, self->args));
  if (ABSL_PREDICT_FALSE(read_record_result.get() == Py_None)) return nullptr;
  return read_record_result.release();
}

PyTypeObject PyRecordIter_Type = {
    // clang-format off
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    // clang-format on
    "RecordIter",                                        // tp_name
    sizeof(PyRecordIterObject),                          // tp_basicsize
    0,                                                   // tp_itemsize
    reinterpret_cast<destructor>(RecordIterDestructor),  // tp_dealloc
    nullptr,                                             // tp_print
    nullptr,                                             // tp_getattr
    nullptr,                                             // tp_setattr
#if PY_MAJOR_VERSION >= 3
    nullptr,  // tp_as_async
#else
    nullptr,  // tp_compare
#endif
    nullptr,                                             // tp_repr
    nullptr,                                             // tp_as_number
    nullptr,                                             // tp_as_sequence
    nullptr,                                             // tp_as_mapping
    nullptr,                                             // tp_hash
    nullptr,                                             // tp_call
    nullptr,                                             // tp_str
    nullptr,                                             // tp_getattro
    nullptr,                                             // tp_setattro
    nullptr,                                             // tp_as_buffer
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,             // tp_flags
    nullptr,                                             // tp_doc
    reinterpret_cast<traverseproc>(RecordIterTraverse),  // tp_traverse
    nullptr,                                             // tp_clear
    nullptr,                                             // tp_richcompare
    0,                                                   // tp_weaklistoffset
    PyObject_SelfIter,                                   // tp_iter
    reinterpret_cast<iternextfunc>(RecordIterNext),      // tp_iternext
    nullptr,                                             // tp_methods
    nullptr,                                             // tp_members
    nullptr,                                             // tp_getset
    nullptr,                                             // tp_base
    nullptr,                                             // tp_dict
    nullptr,                                             // tp_descr_get
    nullptr,                                             // tp_descr_set
    0,                                                   // tp_dictoffset
    nullptr,                                             // tp_init
    nullptr,                                             // tp_alloc
    nullptr,                                             // tp_new
    nullptr,                                             // tp_free
    nullptr,                                             // tp_is_gc
    nullptr,                                             // tp_bases
    nullptr,                                             // tp_mro
    nullptr,                                             // tp_cache
    nullptr,                                             // tp_subclasses
    nullptr,                                             // tp_weaklist
    nullptr,                                             // tp_del
    0,                                                   // tp_version_tag
#if PY_VERSION_HEX >= 0x030400a1
    nullptr,  // tp_finalize
#endif
};

const char* const kModuleName = "riegeli.records.record_reader";
const char kModuleDoc[] = R"doc(Reads records from a Riegeli/records file.)doc";

const PyMethodDef kModuleMethods[] = {
    {"get_record_type", reinterpret_cast<PyCFunction>(GetRecordType),
     METH_VARARGS | METH_KEYWORDS,
     R"doc(
get_record_type(metadata: RecordsMetadata) -> Optional[Type[Message]]

Interprets record_type_name and file_descriptor from metadata.

Args:
  metadata: Riegeli/records file metadata, typically returned by
    RecordReader.read_metadata().

Returns:
  A generated message type corresponding to the type of records, or None if that
  information is not available in metadata.
)doc"},
    {nullptr, nullptr, 0, nullptr},
};

#if PY_MAJOR_VERSION >= 3
PyModuleDef kModuleDef = {
    PyModuleDef_HEAD_INIT,
    kModuleName,                               // m_name
    kModuleDoc,                                // m_doc
    -1,                                        // m_size
    const_cast<PyMethodDef*>(kModuleMethods),  // m_methods
    nullptr,                                   // m_slots
    nullptr,                                   // m_traverse
    nullptr,                                   // m_clear
    nullptr,                                   // m_free
};
#endif

PyObject* InitModule() {
  if (ABSL_PREDICT_FALSE(PyType_Ready(&PyRecordReader_Type) < 0)) {
    return nullptr;
  }
  if (ABSL_PREDICT_FALSE(PyType_Ready(&PyRecordIter_Type) < 0)) {
    return nullptr;
  }
#if PY_MAJOR_VERSION >= 3
  PythonPtr module(PyModule_Create(&kModuleDef));
#else
  PythonPtr module(Py_InitModule3(
      kModuleName, const_cast<PyMethodDef*>(kModuleMethods), kModuleDoc));
#endif
  if (ABSL_PREDICT_FALSE(module == nullptr)) return nullptr;
  PythonPtr existence_only = IntToPython(Field::kExistenceOnly);
  if (ABSL_PREDICT_FALSE(existence_only == nullptr)) return nullptr;
  if (ABSL_PREDICT_FALSE(PyModule_AddObject(module.get(), "EXISTENCE_ONLY",
                                            existence_only.release()) < 0)) {
    return nullptr;
  }
  Py_INCREF(&PyRecordReader_Type);
  if (ABSL_PREDICT_FALSE(PyModule_AddObject(module.get(), "RecordReader",
                                            reinterpret_cast<PyObject*>(
                                                &PyRecordReader_Type)) < 0)) {
    return nullptr;
  }
  return module.release();
}

}  // namespace

#if PY_MAJOR_VERSION >= 3
PyMODINIT_FUNC PyInit_record_reader() { return InitModule(); }
#else
PyMODINIT_FUNC initrecord_reader() { InitModule(); }
#endif

}  // namespace python
}  // namespace riegeli
