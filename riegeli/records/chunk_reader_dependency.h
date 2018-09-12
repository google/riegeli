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

#ifndef RIEGELI_RECORDS_CHUNK_READER_DEPENDENCY_H_
#define RIEGELI_RECORDS_CHUNK_READER_DEPENDENCY_H_

#include <type_traits>
#include <utility>

#include "riegeli/base/dependency.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/records/chunk_reader.h"

namespace riegeli {

// Specialization of Dependency<ChunkReader*, M> adapted from
// Dependency<Reader*, M> by wrapping M in DefaultChunkReader<M>.
template <typename M>
class Dependency<
    ChunkReader*, M,
    typename std::enable_if<IsValidDependency<Reader*, M>::value>::type> {
 public:
  Dependency() noexcept {}

  explicit Dependency(const M& value) : chunk_reader_(value) {}
  explicit Dependency(M&& value) : chunk_reader_(std::move(value)) {}

  Dependency(Dependency&& src) noexcept
      : chunk_reader_(std::move(src.chunk_reader_)) {}
  Dependency& operator=(Dependency&& src) noexcept {
    chunk_reader_ = std::move(src.chunk_reader_);
    return *this;
  }

  M& manager() { return chunk_reader_.src(); }
  const M& manager() const { return chunk_reader_.src(); }

  ChunkReader* ptr() { return &chunk_reader_; }
  const ChunkReader* ptr() const { return &chunk_reader_; }
  ChunkReader& operator*() { return *ptr(); }
  const ChunkReader& operator*() const { return *ptr(); }
  ChunkReader* operator->() { return ptr(); }
  const ChunkReader* operator->() const { return ptr(); }

  static constexpr bool kIsOwning() { return true; }
  static constexpr bool kIsStable() { return false; }

 private:
  DefaultChunkReader<M> chunk_reader_;
};

}  // namespace riegeli

#endif  // RIEGELI_RECORDS_CHUNK_READER_DEPENDENCY_H_
