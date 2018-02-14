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
#include <memory>
#include <vector>

#include "riegeli/base/object.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/chunk_encoding/types.h"

namespace riegeli {

class SimpleDecoder final : public Object {
 public:
  SimpleDecoder() noexcept : Object(State::kOpen) {}

  SimpleDecoder(const SimpleDecoder&) = delete;
  SimpleDecoder& operator=(const SimpleDecoder&) = delete;

  // Resets the SimpleDecoder and parses the chunk.
  //
  // Makes concatenated messages available for reading from reader().
  // Sets *boundaries to positions between messages:
  // boundaries->size() == num_records + 1, message[i] is in reader()
  // between positions (*boundaries)[i] and (*boundaries)[i + 1],
  // boundaries->front() == 0, and boundaries->back() == decoded_data_size.
  //
  // src is not owned by this SimpleDecoder and must be kept alive but not
  // accessed until closing the SimpleDecoder.
  //
  // Return values:
  //  * true  - success (healthy())
  //  * false - failure (!healthy())
  bool Reset(Reader* src, uint64_t num_records, uint64_t decoded_data_size,
             std::vector<size_t>* boundaries);

  // Concatenated messages are available for reading from reader() after Reset()
  // returns true.
  Reader* reader() const { return values_decompressor_.reader(); }

  // Verifies that the concatenated messages end at the current position,
  // failing the SimpleDecoder if not. Closes the SimpleDecoder.
  bool VerifyEndAndClose();

 protected:
  void Done() override;

 private:
  class Decompressor final : public Object {
   public:
    Decompressor() : Object(State::kClosed) {}

    Decompressor(const Decompressor&) = delete;
    Decompressor& operator=(const Decompressor&) = delete;

    bool Reset(Reader* src, CompressionType compression_type);

    Reader* reader() const { return reader_; }

    bool VerifyEndAndClose();

   protected:
    void Done() override;

   private:
    std::unique_ptr<Reader> owned_reader_;
    Reader* reader_ = nullptr;
  };

  Decompressor values_decompressor_;
};

}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_SIMPLE_DECODER_H_
