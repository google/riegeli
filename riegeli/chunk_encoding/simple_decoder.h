// Copyright 2017 Google LLC
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

#ifndef RIEGELI_CHUNK_ENCODING_SIMPLE_DECODER_H_
#define RIEGELI_CHUNK_ENCODING_SIMPLE_DECODER_H_

#include <stddef.h>
#include <stdint.h>
#include <vector>

#include "riegeli/base/base.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/chunk_encoding/decompressor.h"

namespace riegeli {

class SimpleDecoder final : public Object {
 public:
  SimpleDecoder() noexcept : Object(State::kClosed) {}

  SimpleDecoder(const SimpleDecoder&) = delete;
  SimpleDecoder& operator=(const SimpleDecoder&) = delete;

  // Resets the SimpleDecoder and parses the chunk.
  //
  // Makes concatenated record values available for reading from reader().
  // Sets *limits to sorted record end positions.
  //
  // src is not owned by this SimpleDecoder and must be kept alive but not
  // accessed until closing the SimpleDecoder.
  //
  // Return values:
  //  * true  - success (healthy())
  //  * false - failure (!healthy())
  bool Reset(Reader* src, uint64_t num_records, uint64_t decoded_data_size,
             std::vector<size_t>* limits);

  // Returns the Reader from which concatenated record values should be read.
  //
  // Precondition: healthy()
  Reader* reader() const;

  // Verifies that the concatenated record values end at the current position,
  // failing the SimpleDecoder if not. Closes the SimpleDecoder.
  //
  // Return values:
  //  * true  - success (concatenated messages end at the former current
  //            position)
  //  * false - failure (concatenated messages do not end at the former current
  //            position or the SimpleDecoder was not healthy before closing)
  bool VerifyEndAndClose();

 protected:
  void Done() override;

 private:
  internal::Decompressor values_decompressor_;
};

// Implementation details follow.

inline Reader* SimpleDecoder::reader() const {
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of SimpleDecoder::reader(): " << message();
  return values_decompressor_.reader();
}

}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_SIMPLE_DECODER_H_
