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

#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "python/riegeli/base/utils.h"
#include "python/riegeli/bytes/python_writer.h"
#include "python/riegeli/records/record_position.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/records/record_position.h"
#include "riegeli/records/record_writer.h"

namespace riegeli {
namespace python {

namespace {

constexpr ImportedCapsule<RecordPositionApi> kRecordPositionApi(
    kRecordPositionCapsuleName);

PyObject* PyFlushType_Type;

PythonPtr DefineFlushType() {
  static constexpr ImportedConstant kEnum("enum", "Enum");
  if (ABSL_PREDICT_FALSE(!kEnum.Verify())) return nullptr;
  static constexpr Identifier id_FlushType("FlushType");
  const PythonPtr values(Py_BuildValue(
      "((si)(si)(si))", "FROM_OBJECT", static_cast<int>(FlushType::kFromObject),
      "FROM_PROCESS", static_cast<int>(FlushType::kFromProcess), "FROM_MACHINE",
      static_cast<int>(FlushType::kFromMachine)));
  if (ABSL_PREDICT_FALSE(values == nullptr)) return nullptr;
  return PythonPtr(PyObject_CallFunctionObjArgs(kEnum.get(), id_FlushType.get(),
                                                values.get(), nullptr));
}

bool FlushTypeFromPython(PyObject* object, FlushType* value) {
  RIEGELI_ASSERT(PyFlushType_Type != nullptr)
      << "Python FlushType not defined yet";
  if (ABSL_PREDICT_FALSE(!PyObject_IsInstance(object, PyFlushType_Type))) {
    PyErr_Format(PyExc_TypeError, "Expected FlushType, not %s",
                 Py_TYPE(object)->tp_name);
    return false;
  }
  static constexpr Identifier id_value("value");
  const PythonPtr enum_value(PyObject_GetAttr(object, id_value.get()));
  if (ABSL_PREDICT_FALSE(enum_value == nullptr)) return false;
  const long long_value = PyLong_AsLong(enum_value.get());
  if (ABSL_PREDICT_FALSE(long_value == -1) && PyErr_Occurred()) return false;
  *value = static_cast<FlushType>(long_value);
  return true;
}

class FileDescriptorCollector {
 public:
  bool Init(PyObject* file_descriptors) {
    file_descriptors_ = file_descriptors;
    files_seen_.reset(PySet_New(nullptr));
    return files_seen_ != nullptr;
  }

  bool AddFile(PyObject* file_descriptor) {
    // name = file_descriptor.name
    static constexpr Identifier id_name("name");
    const PythonPtr name(PyObject_GetAttr(file_descriptor, id_name.get()));
    if (ABSL_PREDICT_FALSE(name == nullptr)) return false;
    // if name in self.files_seen: return
    const int contains = PySet_Contains(files_seen_.get(), name.get());
    if (ABSL_PREDICT_FALSE(contains < 0)) return false;
    if (contains != 0) return true;
    // self.files_seen.add(name)
    if (ABSL_PREDICT_FALSE(PySet_Add(files_seen_.get(), name.get()) < 0)) {
      return false;
    }
    // for dependency in file_descriptor.dependencies:
    //   self.add_file(dependency)
    static constexpr Identifier id_dependencies("dependencies");
    const PythonPtr dependencies(
        PyObject_GetAttr(file_descriptor, id_dependencies.get()));
    if (ABSL_PREDICT_FALSE(dependencies == nullptr)) return false;
    const PythonPtr iter(PyObject_GetIter(dependencies.get()));
    if (ABSL_PREDICT_FALSE(iter == nullptr)) return false;
    while (const PythonPtr dependency{PyIter_Next(iter.get())}) {
      if (ABSL_PREDICT_FALSE(!AddFile(dependency.get()))) return false;
    }
    if (ABSL_PREDICT_FALSE(PyErr_Occurred() != nullptr)) return false;
    // file_descriptor_proto = self.file_descriptors.add()
    static constexpr Identifier id_add("add");
    const PythonPtr file_descriptor_proto(
        PyObject_CallMethodObjArgs(file_descriptors_, id_add.get(), nullptr));
    if (ABSL_PREDICT_FALSE(file_descriptor_proto == nullptr)) return false;
    // file_descriptor.CopyToProto(file_descriptor_proto)
    static constexpr Identifier id_CopyToProto("CopyToProto");
    return PythonPtr(PyObject_CallMethodObjArgs(
               file_descriptor, id_CopyToProto.get(),
               file_descriptor_proto.get(), nullptr)) != nullptr;
  }

 private:
  PyObject* file_descriptors_;
  PythonPtr files_seen_;
};

// `extern "C"` sets the C calling convention for compatibility with the Python
// API. Functions are marked `static` to avoid making their symbols public, as
// `extern "C"` trumps anonymous namespace.
extern "C" {

static PyObject* SetRecordType(PyObject* self, PyObject* args,
                               PyObject* kwargs) {
  static constexpr const char* keywords[] = {"metadata", "message_type",
                                             nullptr};
  PyObject* metadata_arg;
  PyObject* message_type_arg;
  if (ABSL_PREDICT_FALSE(!PyArg_ParseTupleAndKeywords(
          args, kwargs, "OO:set_record_type", const_cast<char**>(keywords),
          &metadata_arg, &message_type_arg))) {
    return nullptr;
  }
  // message_descriptor = message_type.DESCRIPTOR
  static constexpr Identifier id_DESCRIPTOR("DESCRIPTOR");
  const PythonPtr message_descriptor(
      PyObject_GetAttr(message_type_arg, id_DESCRIPTOR.get()));
  if (ABSL_PREDICT_FALSE(message_descriptor == nullptr)) return nullptr;
  // metadata.record_type_name = message_descriptor.full_name
  static constexpr Identifier id_full_name("full_name");
  const PythonPtr full_name(
      PyObject_GetAttr(message_descriptor.get(), id_full_name.get()));
  if (ABSL_PREDICT_FALSE(full_name == nullptr)) return nullptr;
  static constexpr Identifier id_record_type_name("record_type_name");
  if (ABSL_PREDICT_FALSE(PyObject_SetAttr(metadata_arg,
                                          id_record_type_name.get(),
                                          full_name.get()) < 0)) {
    return nullptr;
  }
  // file_descriptors = metadata.file_descriptor
  static constexpr Identifier id_file_descriptor("file_descriptor");
  const PythonPtr file_descriptors(
      PyObject_GetAttr(metadata_arg, id_file_descriptor.get()));
  if (ABSL_PREDICT_FALSE(file_descriptors == nullptr)) return nullptr;
  // del file_descriptors[:]
  const PythonPtr slice(PySlice_New(nullptr, nullptr, nullptr));
  if (ABSL_PREDICT_FALSE(slice == nullptr)) return nullptr;
  if (ABSL_PREDICT_FALSE(PyObject_DelItem(file_descriptors.get(), slice.get()) <
                         0)) {
    return nullptr;
  }
  // file_descriptor = message_descriptor.file
  static constexpr Identifier id_file("file");
  const PythonPtr file_descriptor(
      PyObject_GetAttr(message_descriptor.get(), id_file.get()));
  if (ABSL_PREDICT_FALSE(file_descriptor == nullptr)) return nullptr;
  // FileDescriptorCollector(file_descriptors).add_file(file_descriptor)
  FileDescriptorCollector collector;
  if (ABSL_PREDICT_FALSE(!collector.Init(file_descriptors.get()))) {
    return nullptr;
  }
  if (ABSL_PREDICT_FALSE(!collector.AddFile(file_descriptor.get()))) {
    return nullptr;
  }
  Py_RETURN_NONE;
}

}  // extern "C"

struct PyRecordWriterObject {
  // clang-format off
  PyObject_HEAD
  static_assert(true, "");  // clang-format workaround.
  // clang-format on

  PythonWrapped<RecordWriter<PythonWriter>> record_writer;
};

extern PyTypeObject PyRecordWriter_Type;

void SetExceptionFromRecordWriter(PyRecordWriterObject* self) {
  RIEGELI_ASSERT(!self->record_writer->ok())
      << "Failed precondition of SetExceptionFromRecordWriter(): "
         "RecordWriter OK";
  if (!self->record_writer->dest().exception().ok()) {
    self->record_writer->dest().exception().Restore();
    return;
  }
  SetRiegeliError(self->record_writer->status());
}

extern "C" {

static void RecordWriterDestructor(PyRecordWriterObject* self) {
  PyObject_GC_UnTrack(self);
  Py_TRASHCAN_SAFE_BEGIN(self);
  PythonUnlocked([&] { self->record_writer.reset(); });
  Py_TYPE(self)->tp_free(self);
  Py_TRASHCAN_SAFE_END(self);
}

static int RecordWriterTraverse(PyRecordWriterObject* self, visitproc visit,
                                void* arg) {
  if (self->record_writer.has_value()) {
    return self->record_writer->dest().Traverse(visit, arg);
  }
  return 0;
}

static int RecordWriterClear(PyRecordWriterObject* self) {
  PythonUnlocked([&] { self->record_writer.reset(); });
  return 0;
}

static int RecordWriterInit(PyRecordWriterObject* self, PyObject* args,
                            PyObject* kwargs) {
  static constexpr const char* keywords[] = {"dest",
                                             "owns_dest",
                                             "assumed_pos",
                                             "min_buffer_size",
                                             "max_buffer_size",
                                             "buffer_size",
                                             "options",
                                             "metadata",
                                             "serialized_metadata",
                                             nullptr};
  PyObject* dest_arg;
  PyObject* owns_dest_arg = nullptr;
  PyObject* assumed_pos_arg = nullptr;
  PyObject* min_buffer_size_arg = nullptr;
  PyObject* max_buffer_size_arg = nullptr;
  PyObject* buffer_size_arg = nullptr;
  PyObject* options_arg = nullptr;
  PyObject* metadata_arg = nullptr;
  PyObject* serialized_metadata_arg = nullptr;
  if (ABSL_PREDICT_FALSE(!PyArg_ParseTupleAndKeywords(
          args, kwargs, "O|$OOOOOOOO:RecordWriter",
          const_cast<char**>(keywords), &dest_arg, &owns_dest_arg,
          &assumed_pos_arg, &min_buffer_size_arg, &max_buffer_size_arg,
          &buffer_size_arg, &options_arg, &metadata_arg,
          &serialized_metadata_arg))) {
    return -1;
  }

  PythonWriter::Options python_writer_options;
  if (owns_dest_arg != nullptr) {
    const int owns_dest_is_true = PyObject_IsTrue(owns_dest_arg);
    if (ABSL_PREDICT_FALSE(owns_dest_is_true < 0)) return -1;
    python_writer_options.set_owns_dest(owns_dest_is_true != 0);
  }
  if (assumed_pos_arg != nullptr && assumed_pos_arg != Py_None) {
    const absl::optional<Position> assumed_pos =
        PositionFromPython(assumed_pos_arg);
    if (ABSL_PREDICT_FALSE(assumed_pos == absl::nullopt)) return -1;
    python_writer_options.set_assumed_pos(*assumed_pos);
  }
  if (buffer_size_arg != nullptr && buffer_size_arg != Py_None) {
    min_buffer_size_arg = buffer_size_arg;
    max_buffer_size_arg = buffer_size_arg;
  }
  if (min_buffer_size_arg != nullptr) {
    const absl::optional<size_t> min_buffer_size =
        SizeFromPython(min_buffer_size_arg);
    if (ABSL_PREDICT_FALSE(min_buffer_size == absl::nullopt)) return -1;
    python_writer_options.set_min_buffer_size(*min_buffer_size);
  }
  if (max_buffer_size_arg != nullptr) {
    const absl::optional<size_t> max_buffer_size =
        SizeFromPython(max_buffer_size_arg);
    if (ABSL_PREDICT_FALSE(max_buffer_size == absl::nullopt)) return -1;
    python_writer_options.set_max_buffer_size(*max_buffer_size);
  }

  RecordWriterBase::Options record_writer_options;
  if (options_arg != nullptr) {
    StrOrBytes options;
    if (ABSL_PREDICT_FALSE(!options.FromPython(options_arg))) return -1;
    {
      const absl::Status status =
          record_writer_options.FromString(absl::string_view(options));
      if (ABSL_PREDICT_FALSE(!status.ok())) {
        SetRiegeliError(status);
        return -1;
      }
    }
  }
  if (metadata_arg != nullptr && metadata_arg != Py_None) {
    static constexpr Identifier id_SerializeToString("SerializeToString");
    const PythonPtr serialized_metadata_str(PyObject_CallMethodObjArgs(
        metadata_arg, id_SerializeToString.get(), nullptr));
    if (ABSL_PREDICT_FALSE(serialized_metadata_str == nullptr)) return -1;
    absl::optional<Chain> serialized_metadata =
        ChainFromPython(serialized_metadata_str.get());
    if (ABSL_PREDICT_FALSE(serialized_metadata == absl::nullopt)) return -1;
    record_writer_options.set_serialized_metadata(
        *std::move(serialized_metadata));
  }
  if (serialized_metadata_arg != nullptr &&
      serialized_metadata_arg != Py_None) {
    absl::optional<Chain> serialized_metadata =
        ChainFromPython(serialized_metadata_arg);
    if (ABSL_PREDICT_FALSE(serialized_metadata == absl::nullopt)) return -1;
    if (record_writer_options.serialized_metadata() != absl::nullopt) {
      PyErr_SetString(PyExc_TypeError,
                      "RecordWriter() got conflicting keyword arguments "
                      "'metadata' and 'serialized_metadata'");
      return -1;
    }
    record_writer_options.set_serialized_metadata(
        *std::move(serialized_metadata));
  }

  PythonWriter python_writer(dest_arg, std::move(python_writer_options));
  PythonUnlocked([&] {
    self->record_writer.emplace(std::move(python_writer),
                                std::move(record_writer_options));
  });
  if (ABSL_PREDICT_FALSE(!self->record_writer->ok())) {
    self->record_writer->dest().Close();
    SetExceptionFromRecordWriter(self);
    return -1;
  }
  return 0;
}

static PyObject* RecordWriterDest(PyRecordWriterObject* self, void* closure) {
  PyObject* const dest = ABSL_PREDICT_FALSE(!self->record_writer.has_value())
                             ? Py_None
                             : self->record_writer->dest().dest();
  Py_INCREF(dest);
  return dest;
}

static PyObject* RecordWriterRepr(PyRecordWriterObject* self) {
  const PythonPtr format = StringToPython("<RecordWriter dest={!r}>");
  if (ABSL_PREDICT_FALSE(format == nullptr)) return nullptr;
  // return format.format(self.dest)
  PyObject* const dest = ABSL_PREDICT_FALSE(!self->record_writer.has_value())
                             ? Py_None
                             : self->record_writer->dest().dest();
  static constexpr Identifier id_format("format");
  return PyObject_CallMethodObjArgs(format.get(), id_format.get(), dest,
                                    nullptr);
}

static PyObject* RecordWriterEnter(PyObject* self, PyObject* args) {
  // return self
  Py_INCREF(self);
  return self;
}

static PyObject* RecordWriterExit(PyRecordWriterObject* self, PyObject* args) {
  PyObject* exc_type;
  PyObject* exc_value;
  PyObject* traceback;
  if (ABSL_PREDICT_FALSE(!PyArg_ParseTuple(args, "OOO:__exit__", &exc_type,
                                           &exc_value, &traceback))) {
    return nullptr;
  }
  // self.close(), suppressing exceptions if exc_type != None.
  if (ABSL_PREDICT_TRUE(self->record_writer.has_value())) {
    const bool close_ok =
        PythonUnlocked([&] { return self->record_writer->Close(); });
    if (ABSL_PREDICT_FALSE(!close_ok) && exc_type == Py_None) {
      SetExceptionFromRecordWriter(self);
      return nullptr;
    }
  }
  Py_RETURN_FALSE;
}

static PyObject* RecordWriterClose(PyRecordWriterObject* self, PyObject* args) {
  if (ABSL_PREDICT_TRUE(self->record_writer.has_value())) {
    const bool close_ok =
        PythonUnlocked([&] { return self->record_writer->Close(); });
    if (ABSL_PREDICT_FALSE(!close_ok)) {
      SetExceptionFromRecordWriter(self);
      return nullptr;
    }
  }
  Py_RETURN_NONE;
}

static PyObject* RecordWriterWriteRecord(PyRecordWriterObject* self,
                                         PyObject* args, PyObject* kwargs) {
  static constexpr const char* keywords[] = {"record", nullptr};
  PyObject* record_arg;
  if (ABSL_PREDICT_FALSE(!PyArg_ParseTupleAndKeywords(
          args, kwargs, "O:write_record", const_cast<char**>(keywords),
          &record_arg))) {
    return nullptr;
  }
  BytesLike record;
  if (ABSL_PREDICT_FALSE(!record.FromPython(record_arg))) return nullptr;
  if (ABSL_PREDICT_FALSE(!self->record_writer.Verify())) return nullptr;
  const bool write_record_ok = PythonUnlocked([&] {
    return self->record_writer->WriteRecord(absl::string_view(record));
  });
  if (ABSL_PREDICT_FALSE(!write_record_ok)) {
    SetExceptionFromRecordWriter(self);
    return nullptr;
  }
  Py_RETURN_NONE;
}

static PyObject* RecordWriterWriteMessage(PyRecordWriterObject* self,
                                          PyObject* args, PyObject* kwargs) {
  static constexpr const char* keywords[] = {"record", nullptr};
  PyObject* record_arg;
  if (ABSL_PREDICT_FALSE(!PyArg_ParseTupleAndKeywords(
          args, kwargs, "O:write_message", const_cast<char**>(keywords),
          &record_arg))) {
    return nullptr;
  }
  // self.write_record(record.SerializeToString())
  static constexpr Identifier id_SerializeToString("SerializeToString");
  const PythonPtr serialized_object(PyObject_CallMethodObjArgs(
      record_arg, id_SerializeToString.get(), nullptr));
  if (ABSL_PREDICT_FALSE(serialized_object == nullptr)) return nullptr;
  BytesLike serialized;
  if (ABSL_PREDICT_FALSE(!serialized.FromPython(serialized_object.get()))) {
    return nullptr;
  }
  if (ABSL_PREDICT_FALSE(!self->record_writer.Verify())) return nullptr;
  const bool write_record_ok = PythonUnlocked([&] {
    return self->record_writer->WriteRecord(absl::string_view(serialized));
  });
  if (ABSL_PREDICT_FALSE(!write_record_ok)) {
    SetExceptionFromRecordWriter(self);
    return nullptr;
  }
  Py_RETURN_NONE;
}

static PyObject* RecordWriterWriteRecords(PyRecordWriterObject* self,
                                          PyObject* args, PyObject* kwargs) {
  static constexpr const char* keywords[] = {"records", nullptr};
  PyObject* records_arg;
  if (ABSL_PREDICT_FALSE(!PyArg_ParseTupleAndKeywords(
          args, kwargs, "O:write_records", const_cast<char**>(keywords),
          &records_arg))) {
    return nullptr;
  }
  // for record in records:
  //   self.write_record(record)
  const PythonPtr iter(PyObject_GetIter(records_arg));
  if (ABSL_PREDICT_FALSE(iter == nullptr)) return nullptr;
  while (const PythonPtr record_object{PyIter_Next(iter.get())}) {
    BytesLike record;
    if (ABSL_PREDICT_FALSE(!record.FromPython(record_object.get()))) {
      return nullptr;
    }
    if (ABSL_PREDICT_FALSE(!self->record_writer.Verify())) return nullptr;
    const bool write_record_ok = PythonUnlocked([&] {
      return self->record_writer->WriteRecord(absl::string_view(record));
    });
    if (ABSL_PREDICT_FALSE(!write_record_ok)) {
      SetExceptionFromRecordWriter(self);
      return nullptr;
    }
  }
  if (ABSL_PREDICT_FALSE(PyErr_Occurred() != nullptr)) return nullptr;
  Py_RETURN_NONE;
}

static PyObject* RecordWriterWriteMessages(PyRecordWriterObject* self,
                                           PyObject* args, PyObject* kwargs) {
  static constexpr const char* keywords[] = {"records", nullptr};
  PyObject* records_arg;
  if (ABSL_PREDICT_FALSE(!PyArg_ParseTupleAndKeywords(
          args, kwargs, "O:write_messages", const_cast<char**>(keywords),
          &records_arg))) {
    return nullptr;
  }
  // for record in records:
  //   self.write_record(record.SerializeToString())
  const PythonPtr iter(PyObject_GetIter(records_arg));
  if (ABSL_PREDICT_FALSE(iter == nullptr)) return nullptr;
  while (const PythonPtr record_object{PyIter_Next(iter.get())}) {
    static constexpr Identifier id_SerializeToString("SerializeToString");
    const PythonPtr serialized_object(PyObject_CallMethodObjArgs(
        record_object.get(), id_SerializeToString.get(), nullptr));
    if (ABSL_PREDICT_FALSE(serialized_object == nullptr)) return nullptr;
    BytesLike serialized;
    if (ABSL_PREDICT_FALSE(!serialized.FromPython(serialized_object.get()))) {
      return nullptr;
    }
    if (ABSL_PREDICT_FALSE(!self->record_writer.Verify())) return nullptr;
    const bool write_record_ok = PythonUnlocked([&] {
      return self->record_writer->WriteRecord(absl::string_view(serialized));
    });
    if (ABSL_PREDICT_FALSE(!write_record_ok)) {
      SetExceptionFromRecordWriter(self);
      return nullptr;
    }
  }
  if (ABSL_PREDICT_FALSE(PyErr_Occurred() != nullptr)) return nullptr;
  Py_RETURN_NONE;
}

static PyObject* RecordWriterFlush(PyRecordWriterObject* self, PyObject* args,
                                   PyObject* kwargs) {
  static constexpr const char* keywords[] = {"flush_type", nullptr};
  PyObject* flush_type_arg = nullptr;
  if (ABSL_PREDICT_FALSE(!PyArg_ParseTupleAndKeywords(
          args, kwargs, "|O:flush", const_cast<char**>(keywords),
          &flush_type_arg))) {
    return nullptr;
  }
  FlushType flush_type = FlushType::kFromProcess;
  if (flush_type_arg != nullptr) {
    if (ABSL_PREDICT_FALSE(!FlushTypeFromPython(flush_type_arg, &flush_type))) {
      return nullptr;
    }
  }
  if (ABSL_PREDICT_FALSE(!self->record_writer.Verify())) return nullptr;
  const bool flush_ok =
      PythonUnlocked([&] { return self->record_writer->Flush(flush_type); });
  if (ABSL_PREDICT_FALSE(!flush_ok)) {
    SetExceptionFromRecordWriter(self);
    return nullptr;
  }
  Py_RETURN_NONE;
}

static PyObject* RecordWriterLastPos(PyRecordWriterObject* self,
                                     void* closure) {
  if (ABSL_PREDICT_FALSE(!self->record_writer.Verify())) return nullptr;
  if (ABSL_PREDICT_FALSE(!kRecordPositionApi.Verify())) return nullptr;
  if (ABSL_PREDICT_FALSE(!self->record_writer->last_record_is_valid())) {
    SetRiegeliError(absl::FailedPreconditionError("No record was written"));
    return nullptr;
  }
  return kRecordPositionApi
      ->RecordPositionToPython(self->record_writer->LastPos())
      .release();
}

static PyObject* RecordWriterPos(PyRecordWriterObject* self, void* closure) {
  if (ABSL_PREDICT_FALSE(!self->record_writer.Verify())) return nullptr;
  if (ABSL_PREDICT_FALSE(!kRecordPositionApi.Verify())) return nullptr;
  return kRecordPositionApi->RecordPositionToPython(self->record_writer->Pos())
      .release();
}

static PyObject* RecordWriterEstimatedSize(PyRecordWriterObject* self,
                                           PyObject* args) {
  if (ABSL_PREDICT_FALSE(!self->record_writer.Verify())) return nullptr;
  return PositionToPython(self->record_writer->EstimatedSize()).release();
}

}  // extern "C"

const PyMethodDef RecordWriterMethods[] = {
    {"__enter__", RecordWriterEnter, METH_NOARGS,
     R"doc(
__enter__(self) -> RecordWriter

Returns self.
)doc"},
    {"__exit__", reinterpret_cast<PyCFunction>(RecordWriterExit), METH_VARARGS,
     R"doc(
__exit__(self, exc_type, exc_value, traceback) -> bool

Calls close().

Suppresses exceptions from close() if an exception is already in flight.

Args:
  exc_type: None or exception in flight (type).
  exc_value: None or exception in flight (value).
  traceback: None or exception in flight (traceback).
)doc"},
    {"close", reinterpret_cast<PyCFunction>(RecordWriterClose), METH_NOARGS,
     R"doc(
close(self) -> None

Indicates that writing is done.

Writes buffered data to the file. Marks the RecordWriter as closed,
disallowing further writing.

If the RecordWriter was failed, raises the same exception again.

If the RecordWriter was not failed but already closed, does nothing.
)doc"},
    {"write_record", reinterpret_cast<PyCFunction>(RecordWriterWriteRecord),
     METH_VARARGS | METH_KEYWORDS, R"doc(
write_record(self, record: Union[bytes, bytearray, memoryview]) -> None

Writes the next record.

Args:
  record: Record to write as a bytes-like object.
)doc"},
    {"write_message", reinterpret_cast<PyCFunction>(RecordWriterWriteMessage),
     METH_VARARGS | METH_KEYWORDS, R"doc(
write_message(self, record: Message) -> None

Writes the next record.

Args:
  record: Record to write as a proto message.
)doc"},
    {"write_records", reinterpret_cast<PyCFunction>(RecordWriterWriteRecords),
     METH_VARARGS | METH_KEYWORDS, R"doc(
write_records(
    self, records: Iterable[Union[bytes, bytearray, memoryview]]) -> None

Writes a number of records.

Args:
  records: Records to write as an iterable of bytes-like objects.
)doc"},
    {"write_messages", reinterpret_cast<PyCFunction>(RecordWriterWriteMessages),
     METH_VARARGS | METH_KEYWORDS, R"doc(
write_messages(self, records: Iterable[Message]) -> None

Writes a number of records.

Args:
  records: Records to write as an iterable of proto messages.
)doc"},
    {"flush", reinterpret_cast<PyCFunction>(RecordWriterFlush),
     METH_VARARGS | METH_KEYWORDS, R"doc(
flush(self, flush_type: FlushType = FlushType.FROM_PROCESS) -> None

Finalizes any open chunk and pushes buffered data to the destination.
If parallelism was used in options, waits for any background writing to
complete.

This makes data written so far visible, but in contrast to close(),
keeps the possibility to write more data later. What exactly does it mean
for data to be visible depends on the destination.

This degrades compression density if used too often.

Args:
  flush_type: The scope of objects to flush and the intended data durability
  (without a guarantee).
   * FlushType.FROM_OBJECT:  Makes data written so far visible in other
                             objects, propagating flushing through owned
                             dependencies of the given writer.
   * FlushType.FROM_PROCESS: Makes data written so far visible outside
                             the process, propagating flushing through
                             dependencies of the given writer.
                             This is the default.
   * FlushType.FROM_MACHINE: Makes data written so far visible outside
                             the process and durable in case of operating
                             system crash, propagating flushing through
                             dependencies of the given writer.
)doc"},
    {"estimated_size", reinterpret_cast<PyCFunction>(RecordWriterEstimatedSize),
     METH_NOARGS,
     R"doc(
estimated_size(self) -> int

Returns an estimation of the file size if no more data is written, without
affecting data representation (i.e. without closing the current chunk) and
without blocking.

This is an underestimation because pending work is not taken into account:
 * The currently open chunk.
 * If parallelism was used in options, chunks being encoded in background.

The exact file size can be found by flush(FlushType.FROM_OBJECT) which closes
the currently open chunk, and pos.chunk_begin (record_index == 0 after flushing)
which might need to wait for some background work to complete.
)doc"},
    {nullptr, nullptr, 0, nullptr},
};

const PyGetSetDef RecordWriterGetSet[] = {
    {const_cast<char*>("dest"), reinterpret_cast<getter>(RecordWriterDest),
     nullptr, const_cast<char*>(R"doc(
dest: BinaryIO

Binary IO stream being written to.
)doc"),
     nullptr},
    {const_cast<char*>("last_pos"),
     reinterpret_cast<getter>(RecordWriterLastPos), nullptr,
     const_cast<char*>(R"doc(
last_pos: RecordPosition

The canonical position of the last record written.

The canonical position is the largest among all equivalent positions.
Seeking to any equivalent position leads to reading the same record.

last_pos.numeric returns the position as an int.

Precondition:
  a record was successfully written and there was no intervening call to
  close() or flush().
)doc"),
     nullptr},
    {const_cast<char*>("pos"), reinterpret_cast<getter>(RecordWriterPos),
     nullptr, const_cast<char*>(R"doc(
pos: RecordPosition

A position of the next record (or the end of file if there is no next record).

A position which is not canonical can be smaller than the equivalent canonical
position. Seeking to any equivalent position leads to reading the same record.

pos.numeric returns the position as an int.

After opening the file, close(), or flush(), pos is the canonical position of
the next record, and pos.record_index == 0.
)doc"),
     nullptr},
    {nullptr, nullptr, nullptr, nullptr, nullptr}};

PyTypeObject PyRecordWriter_Type = {
    // clang-format off
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    // clang-format on
    "riegeli.records.record_writer.RecordWriter",          // tp_name
    sizeof(PyRecordWriterObject),                          // tp_basicsize
    0,                                                     // tp_itemsize
    reinterpret_cast<destructor>(RecordWriterDestructor),  // tp_dealloc
#if PY_VERSION_HEX >= 0x03080000
    0,  // tp_vectorcall_offset
#else
    nullptr,  // tp_print
#endif
    nullptr,                                       // tp_getattr
    nullptr,                                       // tp_setattr
    nullptr,                                       // tp_as_async
    reinterpret_cast<reprfunc>(RecordWriterRepr),  // tp_repr
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
RecordWriter(
    dest: BinaryIO,
    *,
    owns_dest: bool = True,
    assumed_pos: Optional[int] = None,
    min_buffer_size: int = 4 << 10,
    max_buffer_size: int = 64 << 10,
    buffer_size: Optional[int],
    options: Union[str, bytes] = '',
    metadata: Optional[RecordsMetadata] = None,
    serialized_metadata: Union[bytes, bytearray, memoryview] = b''
) -> RecordWriter

Will write to the given file.

Args:
  dest: Binary IO stream to write to.
  owns_dest: If True, dest is owned, close() or __exit__() calls dest.close(),
    and flush(flush_type) calls dest.flush() even if flush_type is
    FlushType.FROM_OBJECT.
  assumed_pos: If None, dest must support random access. If an int, it is enough
    that dest supports sequential access, and this position will be assumed
    initially.
  min_buffer_size: Tunes the minimal buffer size, which determines how much data
    at a time is typically written to dest. The actual buffer size changes
    between min_buffer_size and max_buffer_size depending on the access pattern.
  max_buffer_size: Tunes the maximal buffer size, which determines how much data
    at a time is typically written to dest. The actual buffer size changes
    between min_buffer_size and max_buffer_size depending on the access pattern.
  buffer_size: If not None, a shortcut for setting min_buffer_size and
    max_buffer_size to the same value.
  options: Compression and other writing options. See below.
  metadata: If not None, file metadata to be written at the beginning (if
    metadata has any fields set). Metadata are written only when the file is
    written from the beginning, not when it is appended to. Record type in
    metadata can be conveniently set by set_record_type().
  serialized_metadata: If not empty, like metadata, but metadata are passed
    serialized as a bytes-like object. This is faster if the caller has metadata
    already serialized. This conflicts with metadata.

The dest argument should be a binary IO stream which supports:
 * close()          - for close() or __exit__() if owns_dest
 * write(bytes)
 * flush()          - for flush()
 * seek(int[, int]) - if assumed_pos is None
 * tell()           - if assumed_pos is None

Example values for dest (possibly with 'ab' instead of 'wb' for appending):
 * io.FileIO(filename, 'wb')
 * io.open(filename, 'wb') - better with buffering=0, or use io.FileIO() instead
 * open(filename, 'wb')    - better with buffering=0, or use io.FileIO() instead
 * io.BytesIO()            - use owns_dest=False to access dest after closing
                             the RecordWriter
 * tf.io.gfile.GFile(filename, 'wb')

Options are documented at
https://github.com/google/riegeli/blob/master/doc/record_writer_options.md
)doc",                                                              // tp_doc
    reinterpret_cast<traverseproc>(RecordWriterTraverse),  // tp_traverse
    reinterpret_cast<inquiry>(RecordWriterClear),          // tp_clear
    nullptr,                                               // tp_richcompare
    0,                                                     // tp_weaklistoffset
    nullptr,                                               // tp_iter
    nullptr,                                               // tp_iternext
    const_cast<PyMethodDef*>(RecordWriterMethods),         // tp_methods
    nullptr,                                               // tp_members
    const_cast<PyGetSetDef*>(RecordWriterGetSet),          // tp_getset
    nullptr,                                               // tp_base
    nullptr,                                               // tp_dict
    nullptr,                                               // tp_descr_get
    nullptr,                                               // tp_descr_set
    0,                                                     // tp_dictoffset
    reinterpret_cast<initproc>(RecordWriterInit),          // tp_init
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
    nullptr,                                               // tp_finalize
};

const char* const kModuleName = "riegeli.records.record_writer";
const char kModuleDoc[] = R"doc(Writes records to a Riegeli/records file.)doc";

const PyMethodDef kModuleMethods[] = {
    {"set_record_type", reinterpret_cast<PyCFunction>(SetRecordType),
     METH_VARARGS | METH_KEYWORDS,
     R"doc(
set_record_type(metadata: RecordsMetadata, message_type: Type[Message]) -> None

Sets record_type_name and file_descriptor in metadata.

Args:
  metadata: Riegeli/records file metadata being filled, typically will become
    the metadata argument of RecordWriter().
  message_type: Promised type of records, typically the argument type of
    RecordWriter.write_message().
)doc"},
    {nullptr, nullptr, 0, nullptr},
};

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

PyObject* InitModule() {
  if (ABSL_PREDICT_FALSE(PyType_Ready(&PyRecordWriter_Type) < 0)) {
    return nullptr;
  }
  PythonPtr module(PyModule_Create(&kModuleDef));
  if (ABSL_PREDICT_FALSE(module == nullptr)) return nullptr;
  PyFlushType_Type = DefineFlushType().release();
  if (ABSL_PREDICT_FALSE(PyFlushType_Type == nullptr)) return nullptr;
  if (ABSL_PREDICT_FALSE(PyModule_AddObject(module.get(), "FlushType",
                                            PyFlushType_Type) < 0)) {
    return nullptr;
  }
  Py_INCREF(&PyRecordWriter_Type);
  if (ABSL_PREDICT_FALSE(PyModule_AddObject(module.get(), "RecordWriter",
                                            reinterpret_cast<PyObject*>(
                                                &PyRecordWriter_Type)) < 0)) {
    return nullptr;
  }
  return module.release();
}

}  // namespace

PyMODINIT_FUNC PyInit_record_writer() { return InitModule(); }

}  // namespace python
}  // namespace riegeli
