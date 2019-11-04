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

#include <tuple>
#include <type_traits>
#include <utility>

#include "riegeli/base/dependency.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/records/chunk_reader.h"

namespace riegeli {

// Specialization of `Dependency<ChunkReader*, M>` adapted from
// `Dependency<Reader*, M>` by wrapping `M` in `DefaultChunkReader<M>`.
template <typename M>
class Dependency<ChunkReader*, M,
                 std::enable_if_t<IsValidDependency<Reader*, M>::value>> {
 public:
  Dependency() noexcept {}

  explicit Dependency(const M& manager) : chunk_reader_(manager) {}
  explicit Dependency(M&& manager) : chunk_reader_(std::move(manager)) {}

  template <typename... MArgs>
  explicit Dependency(std::tuple<MArgs...> manager_args)
      : chunk_reader_(std::move(manager_args)) {}

  Dependency(Dependency&& that) noexcept
      : chunk_reader_(std::move(that.chunk_reader_)) {}
  Dependency& operator=(Dependency&& that) noexcept {
    chunk_reader_ = std::move(that.chunk_reader_);
    return *this;
  }

  void Reset() { chunk_reader_.Reset(); }

  void Reset(const M& manager) { chunk_reader_.Reset(manager); }
  void Reset(M&& manager) { chunk_reader_.Reset(std::move(manager)); }

  template <typename... MArgs>
  void Reset(std::tuple<MArgs...> manager_args) {
    chunk_reader_.Reset(std::move(manager_args));
  }

  M& manager() { return chunk_reader_.src(); }
  const M& manager() const { return chunk_reader_.src(); }

  DefaultChunkReader<M>* get() { return &chunk_reader_; }
  const DefaultChunkReader<M>* get() const { return &chunk_reader_; }
  DefaultChunkReader<M>& operator*() { return *get(); }
  const DefaultChunkReader<M>& operator*() const { return *get(); }
  DefaultChunkReader<M>* operator->() { return get(); }
  const DefaultChunkReader<M>* operator->() const { return get(); }

  bool is_owning() const { return true; }
  static constexpr bool kIsStable() { return false; }

 private:
  DefaultChunkReader<M> chunk_reader_;
};

}  // namespace riegeli

#endif  // RIEGELI_RECORDS_CHUNK_READER_DEPENDENCY_H_
