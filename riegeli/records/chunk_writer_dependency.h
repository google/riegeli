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

#ifndef RIEGELI_RECORDS_CHUNK_WRITER_DEPENDENCY_H_
#define RIEGELI_RECORDS_CHUNK_WRITER_DEPENDENCY_H_

#include <type_traits>
#include <utility>

#include "riegeli/base/dependency.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/records/chunk_writer.h"

namespace riegeli {

// Specialization of Dependency<ChunkWriter*, M> adapted from
// Dependency<Writer*, M> by wrapping M in DefaultChunkWriter<M>.
template <typename M>
class Dependency<
    ChunkWriter*, M,
    typename std::enable_if<IsValidDependency<Writer*, M>::value>::type> {
 public:
  Dependency() noexcept {}

  explicit Dependency(const M& value) : chunk_writer_(value) {}
  explicit Dependency(M&& value) : chunk_writer_(std::move(value)) {}

  Dependency(Dependency&& that) noexcept
      : chunk_writer_(std::move(that.chunk_writer_)) {}
  Dependency& operator=(Dependency&& that) noexcept {
    chunk_writer_ = std::move(that.chunk_writer_);
    return *this;
  }

  M& manager() { return chunk_writer_.dest(); }
  const M& manager() const { return chunk_writer_.dest(); }

  ChunkWriter* ptr() { return &chunk_writer_; }
  const ChunkWriter* ptr() const { return &chunk_writer_; }
  ChunkWriter& operator*() { return *ptr(); }
  const ChunkWriter& operator*() const { return *ptr(); }
  ChunkWriter* operator->() { return ptr(); }
  const ChunkWriter* operator->() const { return ptr(); }

  static constexpr bool kIsOwning() { return true; }
  static constexpr bool kIsStable() { return false; }

 private:
  DefaultChunkWriter<M> chunk_writer_;
};

}  // namespace riegeli

#endif  // RIEGELI_RECORDS_CHUNK_WRITER_DEPENDENCY_H_
