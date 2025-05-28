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

#ifndef PYTHON_RIEGELI_RECORDS_RECORD_POSITION_H_
#define PYTHON_RIEGELI_RECORDS_RECORD_POSITION_H_

// From https://docs.python.org/3/c-api/intro.html:
// Since Python may define some pre-processor definitions which affect the
// standard headers on some systems, you must include Python.h before any
// standard headers are included.
#include <Python.h>
// clang-format: do not reorder the above include.

#include <optional>

#include "python/riegeli/base/utils.h"
#include "riegeli/records/record_position.h"

namespace riegeli::python {

// Access the API thus:
// ```
// static constexpr ImportedCapsule<RecordPositionApi> kRecordPositionApi(
//    kRecordPositionCapsuleName);
// ```

struct RecordPositionApi {
  PythonPtr (*RecordPositionToPython)(FutureRecordPosition value);
  std::optional<RecordPosition> (*RecordPositionFromPython)(PyObject* object);
};

inline constexpr const char* kRecordPositionCapsuleName =
    "riegeli.records.record_position._CPPAPI";

}  // namespace riegeli::python

#endif  // PYTHON_RIEGELI_RECORDS_RECORD_POSITION_H_
