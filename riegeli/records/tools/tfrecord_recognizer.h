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

#ifndef RIEGELI_RECORDS_TOOLS_TFRECORD_DETECTOR_H_
#define RIEGELI_RECORDS_TOOLS_TFRECORD_DETECTOR_H_

#include "riegeli/base/base.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/reader.h"
#include "tensorflow/core/lib/io/record_reader.h"

namespace riegeli {

class TFRecordRecognizer : public Object {
 public:
  explicit TFRecordRecognizer(Reader* byte_reader);

  // Ensures that the file looks like a valid TFRecord file.
  //
  // Updates `*record_reader_options` on success.
  //
  // Return values:
  //  * `true`                      - success
  //                                  (`*record_reader_options` is updated)
  //  * `false` (when `healthy()`)  - source ends
  //  * `false` (when `!healthy()`) - failure
  bool CheckFileFormat(
      tensorflow::io::RecordReaderOptions* record_reader_options);

 private:
  Reader* byte_reader_;
};

// Implementation details follow.

inline TFRecordRecognizer::TFRecordRecognizer(Reader* byte_reader)
    : Object(kInitiallyOpen),
      byte_reader_(RIEGELI_ASSERT_NOTNULL(byte_reader)) {}

}  // namespace riegeli

#endif  // RIEGELI_RECORDS_TOOLS_TFRECORD_DETECTOR_H_
