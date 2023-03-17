// Copyright 2020 Google LLC
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

#ifndef RIEGELI_BROTLI_BROTLI_ALLOCATOR_H_
#define RIEGELI_BROTLI_BROTLI_ALLOCATOR_H_

// IWYU pragma: private, include "riegeli/brotli/brotli_reader.h"
// IWYU pragma: private, include "riegeli/brotli/brotli_writer.h"

#include <stddef.h>

#include <type_traits>
#include <utility>

#include "brotli/types.h"
#include "riegeli/base/intrusive_ref_count.h"

namespace riegeli {

namespace brotli_internal {

// `extern "C"` sets the C calling convention for compatibility with the Brotli
// API.
extern "C" {
void* RiegeliBrotliAllocFunc(void* opaque, size_t size);
void RiegeliBrotliFreeFunc(void* opaque, void* ptr);
}  // extern "C"

}  // namespace brotli_internal

// Memory allocator used by the Brotli engine.
//
// `BrotliAllocator` adapts C++ functors to C function pointers required by the
// Brotli engine.
class BrotliAllocator {
 public:
  // Specifies the default allocator chosen by the Brotli engine.
  BrotliAllocator() = default;

  BrotliAllocator(const BrotliAllocator& that) = default;
  BrotliAllocator& operator=(const BrotliAllocator& that) = default;

  BrotliAllocator(BrotliAllocator&& that) = default;
  BrotliAllocator& operator=(BrotliAllocator&& that) = default;

  // Specifies functions to allocate and free a block of memory.
  //
  // Arguments should be functions of the following types, or equivalent
  // functors:
  //   `void* alloc_functor(size_t size)`
  //   `void free_functor(void* ptr)`
  template <typename AllocFunctor, typename FreeFunctor>
  explicit BrotliAllocator(AllocFunctor&& alloc_functor,
                           FreeFunctor&& free_functor);

  // Returns parameters for `Brotli{Encoder,Decoder}CreateInstance()`.
  brotli_alloc_func alloc_func() const;
  brotli_free_func free_func() const;
  void* opaque() const;

 private:
  friend void* brotli_internal::RiegeliBrotliAllocFunc(void* opaque,
                                                       size_t size);
  friend void brotli_internal::RiegeliBrotliFreeFunc(void* opaque, void* ptr);

  class Interface;

  template <typename AllocFunctor, typename FreeFunctor>
  class Implementation;

  RefCountedPtr<const Interface> impl_;
};

// Implementation details follow.

class BrotliAllocator::Interface : public RefCountedBase<Interface> {
 public:
  virtual ~Interface();

  virtual void* Alloc(size_t size) const = 0;
  virtual void Free(void* ptr) const = 0;
};

template <typename AllocFunctor, typename FreeFunctor>
class BrotliAllocator::Implementation : public Interface {
 public:
  template <typename AllocFunctorArg, typename FreeFunctorArg>
  explicit Implementation(AllocFunctorArg&& alloc_functor,
                          FreeFunctorArg&& free_functor)
      : alloc_functor_(std::forward<AllocFunctorArg>(alloc_functor)),
        free_functor_(std::forward<FreeFunctorArg>(free_functor)) {}

  void* Alloc(size_t size) const override { return alloc_functor_(size); }
  void Free(void* ptr) const override { free_functor_(ptr); }

 private:
  AllocFunctor alloc_functor_;
  FreeFunctor free_functor_;
};

template <typename AllocFunctor, typename FreeFunctor>
inline BrotliAllocator::BrotliAllocator(AllocFunctor&& alloc_functor,
                                        FreeFunctor&& free_functor)
    : impl_(MakeRefCounted<const Implementation<std::decay_t<AllocFunctor>,
                                                std::decay_t<FreeFunctor>>>(
          std::forward<AllocFunctor>(alloc_functor),
          std::forward<FreeFunctor>(free_functor))) {}

inline brotli_alloc_func BrotliAllocator::alloc_func() const {
  return impl_ == nullptr ? nullptr : brotli_internal::RiegeliBrotliAllocFunc;
}

inline brotli_free_func BrotliAllocator::free_func() const {
  return impl_ == nullptr ? nullptr : brotli_internal::RiegeliBrotliFreeFunc;
}

inline void* BrotliAllocator::opaque() const {
  return const_cast<void*>(static_cast<const void*>(impl_.get()));
}

}  // namespace riegeli

#endif  // RIEGELI_BROTLI_BROTLI_ALLOCATOR_H_
