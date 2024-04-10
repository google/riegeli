// Copyright 2021 Google LLC
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

#ifndef RIEGELI_BYTES_READER_FACTORY_H_
#define RIEGELI_BYTES_READER_FACTORY_H_

#include <memory>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/base/thread_annotations.h"
#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/object.h"
#include "riegeli/base/stable_dependency.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

// Template parameter independent part of `ReaderFactory`.
class ReaderFactoryBase : public Object {
 public:
  class Options : public BufferOptionsBase<Options> {};

  // Returns the original `Reader`. Unchanged by `Close()`.
  virtual Reader* SrcReader() const = 0;

  // Returns the original position of the original `Reader`.
  Position pos() const { return initial_pos_; }

  // Returns a `Reader` which reads from the same source as the original
  // `Reader`, but has an independent current position, starting from
  // `initial_pos`, defaulting to `pos()`.
  //
  // If the source ends before `initial_pos`, the position of the new `Reader`
  // is set to the end. The resulting `Reader` supports `Seek()` and
  // `NewReader()`. Calling `NewReader()` on the new `Reader` is equivalent to
  // calling it on this `ReaderFactory` again.
  //
  // The new `Reader` does not own the source, even if the original `Reader`
  // does. The original `Reader` must not be accessed until the new `Reader` is
  // closed or no longer used.
  //
  // Returns `nullptr` only if `!ok()` before `NewReader()` was called.
  //
  // `NewReader()` is const and thus may be called concurrently.
  std::unique_ptr<Reader> NewReader(Position initial_pos) const;
  std::unique_ptr<Reader> NewReader() const { return NewReader(pos()); }

 protected:
  using Object::Object;

  ReaderFactoryBase(ReaderFactoryBase&& that) noexcept;
  ReaderFactoryBase& operator=(ReaderFactoryBase&& that) noexcept;

  void Reset(Closed);
  void Reset();
  void Initialize(BufferOptions buffer_options, Reader* src);

  void Done() override;
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;

 private:
  class ConcurrentReader;

  struct Shared {
    explicit Shared(BufferOptions buffer_options, Reader* reader)
        : buffer_options(buffer_options), reader(reader) {}

    BufferOptions buffer_options;
    absl::Mutex mutex;
    Reader* reader ABSL_GUARDED_BY(mutex);
  };

  Position initial_pos_ = 0;
  // If `shared_ == nullptr`, then `!is_open()` or `Reader::NewReader()` is
  // used. If `shared_ != nullptr`, then `ConcurrentReader` emulation is used.
  std::unique_ptr<Shared> shared_;
};

// `ReaderFactory` exposes `Reader::NewReader()`, or provides its emulation
// for `Reader` classes which do not support `NewReader()`. This allows for
// interleaved or concurrent reading of several regions of the same source.
//
// The original `Reader` must support random access.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the original `Reader`. `Src` must support
// `Dependency<Reader*, Src>`, e.g. `Reader*` (not owned, default),
// `ChainReader<>` (owned), `std::unique_ptr<Reader>` (owned),
// `AnyDependency<Reader*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as
// `InitializerTargetT` of the type of the first constructor argument.
// This requires C++17.
//
// The original `Reader` must not be accessed until the `ReaderFactory` is
// closed or no longer used.
template <typename Src = Reader*>
class ReaderFactory : public ReaderFactoryBase {
 public:
  // Creates a closed `ReaderFactory`.
  explicit ReaderFactory(Closed) noexcept : ReaderFactoryBase(kClosed) {}

  // Will read from the original `Reader` provided by `src`.
  explicit ReaderFactory(Initializer<Src> src, Options options = Options());

  ReaderFactory(ReaderFactory&& that) noexcept;
  ReaderFactory& operator=(ReaderFactory&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `ReaderFactory`. This
  // avoids constructing a temporary `ReaderFactory` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Src> src,
                                          Options options = Options());

  // Returns the object providing and possibly owning the original `Reader`.
  // Unchanged by `Close()`.
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  Reader* SrcReader() const override { return src_.get(); }

 protected:
  void Done() override;

 private:
  // The object providing and possibly owning the original `Reader`.
  StableDependency<Reader*, Src> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit ReaderFactory(Closed) -> ReaderFactory<DeleteCtad<Closed>>;
template <typename Src>
explicit ReaderFactory(Src&& src, ReaderFactoryBase::Options options =
                                      ReaderFactoryBase::Options())
    -> ReaderFactory<InitializerTargetT<Src>>;
#endif

// Implementation details follow.

inline ReaderFactoryBase::ReaderFactoryBase(ReaderFactoryBase&& that) noexcept
    : Object(static_cast<Object&&>(that)),
      initial_pos_(that.initial_pos_),
      shared_(std::move(that.shared_)) {}

inline ReaderFactoryBase& ReaderFactoryBase::operator=(
    ReaderFactoryBase&& that) noexcept {
  Object::operator=(static_cast<Object&&>(that));
  initial_pos_ = that.initial_pos_;
  shared_ = std::move(that.shared_);
  return *this;
}

inline void ReaderFactoryBase::Reset(Closed) {
  Object::Reset(kClosed);
  initial_pos_ = 0;
  shared_.reset();
}

inline void ReaderFactoryBase::Reset() {
  Object::Reset();
  // `initial_pos_` will be set by `Initialize()`.
  shared_.reset();
}

template <typename Src>
inline ReaderFactory<Src>::ReaderFactory(Initializer<Src> src, Options options)
    : src_(std::move(src)) {
  Initialize(options.buffer_options(), src_.get());
}

template <typename Src>
inline ReaderFactory<Src>::ReaderFactory(ReaderFactory&& that) noexcept
    : ReaderFactoryBase(static_cast<ReaderFactoryBase&&>(that)),
      src_(std::move(that.src_)) {}

template <typename Src>
inline ReaderFactory<Src>& ReaderFactory<Src>::operator=(
    ReaderFactory&& that) noexcept {
  ReaderFactoryBase::operator=(static_cast<ReaderFactoryBase&&>(that));
  src_ = std::move(that.src_);
  return *this;
}

template <typename Src>
inline void ReaderFactory<Src>::Reset(Closed) {
  ReaderFactoryBase::Reset(kClosed);
  src_.Reset();
}

template <typename Src>
inline void ReaderFactory<Src>::Reset(Initializer<Src> src, Options options) {
  ReaderFactoryBase::Reset();
  src_.Reset(std::move(src));
  Initialize(options.buffer_options(), src_.get());
}

template <typename Src>
void ReaderFactory<Src>::Done() {
  ReaderFactoryBase::Done();
  if (src_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) {
      FailWithoutAnnotation(src_->status());
    }
  }
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_READER_FACTORY_H_
