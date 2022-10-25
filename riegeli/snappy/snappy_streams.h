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

#ifndef RIEGELI_SNAPPY_SNAPPY_STREAMS_H_
#define RIEGELI_SNAPPY_SNAPPY_STREAMS_H_

#include <stddef.h>

#include "riegeli/base/assert.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"
#include "snappy-sinksource.h"

namespace riegeli {
namespace snappy_internal {

// Adapts a `Writer` to a `snappy::Sink`.
class WriterSnappySink : public snappy::Sink {
 public:
  explicit WriterSnappySink(Writer* dest)
      : dest_(RIEGELI_ASSERT_NOTNULL(dest)) {}

  WriterSnappySink(const WriterSnappySink&) = delete;
  WriterSnappySink& operator=(const WriterSnappySink&) = delete;

  void Append(const char* src, size_t length) override;
  char* GetAppendBuffer(size_t length, char* scratch) override;
  void AppendAndTakeOwnership(char* src, size_t length,
                              void (*deleter)(void*, const char*, size_t),
                              void* deleter_arg) override;
  char* GetAppendBufferVariable(size_t min_length, size_t recommended_length,
                                char* scratch, size_t scratch_length,
                                size_t* result_length) override;

 private:
  Writer* dest_;
};

// Adapts a `Reader` to a `snappy::Source`.
class ReaderSnappySource : public snappy::Source {
 public:
  explicit ReaderSnappySource(Reader* src, Position size)
      : src_(RIEGELI_ASSERT_NOTNULL(src)), size_(size) {}

  ReaderSnappySource(const ReaderSnappySource&) = delete;
  ReaderSnappySource& operator=(const ReaderSnappySource&) = delete;

  size_t Available() const override;
  const char* Peek(size_t* length) override;
  void Skip(size_t length) override;

 private:
  Reader* src_;
  Position size_;
};

}  // namespace snappy_internal
}  // namespace riegeli

#endif  // RIEGELI_SNAPPY_SNAPPY_STREAMS_H_
