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

#include <stddef.h>

#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/base/thread_annotations.h"
#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"
#include "riegeli/base/base.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/stable_dependency.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

// Template parameter independent part of `ReaderFactory`.
class ReaderFactoryBase : public Object {
 public:
  class Options {
   public:
    Options() noexcept {}

    // Tunes how much data is buffered after reading from the original Reader.
    //
    // Default: `kDefaultBufferSize` (64K).
    Options& set_buffer_size(size_t buffer_size) & {
      RIEGELI_ASSERT_GT(buffer_size, 0u)
          << "Failed precondition of "
             "ReaderFactoryBase::Options::set_buffer_size(): "
             "zero buffer size";
      buffer_size_ = buffer_size;
      return *this;
    }
    Options&& set_buffer_size(size_t buffer_size) && {
      return std::move(set_buffer_size(buffer_size));
    }
    size_t buffer_size() const { return buffer_size_; }

   private:
    size_t buffer_size_ = kDefaultBufferSize;
  };

  // Returns the original `Reader`. Unchanged by `Close()`.
  virtual Reader* src_reader() = 0;
  virtual const Reader* src_reader() const = 0;

  // Returns a `Reader` which reads from the same source as the original
  // `Reader`, but has an independent current position, starting from
  // `initial_pos`.
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

 protected:
  using Object::Object;

  ReaderFactoryBase(ReaderFactoryBase&& that) noexcept;
  ReaderFactoryBase& operator=(ReaderFactoryBase&& that) noexcept;

  void Reset(Closed);
  void Reset();
  void Initialize(size_t buffer_size, Reader* src);

  void Done() override;
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;

 private:
  class ConcurrentReader;

  struct Shared {
    explicit Shared(size_t buffer_size, Reader* reader)
        : buffer_size(buffer_size), reader(reader) {}

    size_t buffer_size;
    absl::Mutex mutex;
    Reader* reader ABSL_GUARDED_BY(mutex);
  };

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
// `std::unique_ptr<Reader>` (owned), `ChainReader<>` (owned).
//
// By relying on CTAD the template argument can be deduced as the value type of
// the first constructor argument. This requires C++17.
//
// The original `Reader` must not be accessed until the `ReaderFactory` is
// closed or no longer used.
template <typename Src = Reader*>
class ReaderFactory : public ReaderFactoryBase {
 public:
  // Creates a closed `ReaderFactory`.
  explicit ReaderFactory(Closed) noexcept : ReaderFactoryBase(kClosed) {}

  // Will read from the original `Reader` provided by `src`.
  explicit ReaderFactory(const Src& src, Options options = Options());
  explicit ReaderFactory(Src&& src, Options options = Options());

  // Will read from the original `Reader` provided by a `Src` constructed from
  // elements of `src_args`. This avoids constructing a temporary `Src` and
  // moving from it.
  template <typename... SrcArgs>
  explicit ReaderFactory(std::tuple<SrcArgs...> src_args,
                         Options options = Options());

  ReaderFactory(ReaderFactory&& that) noexcept;
  ReaderFactory& operator=(ReaderFactory&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `ReaderFactory`. This
  // avoids constructing a temporary `ReaderFactory` and moving from it.
  void Reset(Closed);
  void Reset(const Src& src, Options options = Options());
  void Reset(Src&& src, Options options = Options());
  template <typename... SrcArgs>
  void Reset(std::tuple<SrcArgs...> src_args, Options options = Options());

  // Returns the object providing and possibly owning the original `Reader`.
  // Unchanged by `Close()`.
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  Reader* src_reader() override { return src_.get(); }
  const Reader* src_reader() const override { return src_.get(); }

 protected:
  void Done() override;

 private:
  // The object providing and possibly owning the original `Reader`.
  StableDependency<Reader*, Src> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit ReaderFactory(Closed)->ReaderFactory<DeleteCtad<Closed>>;
template <typename Src>
explicit ReaderFactory(const Src& src, ReaderFactoryBase::Options options =
                                           ReaderFactoryBase::Options())
    -> ReaderFactory<std::decay_t<Src>>;
template <typename Src>
explicit ReaderFactory(Src&& src, ReaderFactoryBase::Options options =
                                      ReaderFactoryBase::Options())
    -> ReaderFactory<std::decay_t<Src>>;
template <typename... SrcArgs>
explicit ReaderFactory(
    std::tuple<SrcArgs...> src_args,
    ReaderFactoryBase::Options options = ReaderFactoryBase::Options())
    -> ReaderFactory<DeleteCtad<std::tuple<SrcArgs...>>>;
#endif

// Implementation details follow.

inline ReaderFactoryBase::ReaderFactoryBase(ReaderFactoryBase&& that) noexcept
    : Object(static_cast<Object&&>(that)), shared_(std::move(that.shared_)) {}

inline ReaderFactoryBase& ReaderFactoryBase::operator=(
    ReaderFactoryBase&& that) noexcept {
  Object::operator=(static_cast<Object&&>(that));
  shared_ = std::move(that.shared_);
  return *this;
}

inline void ReaderFactoryBase::Reset(Closed) {
  Object::Reset(kClosed);
  shared_.reset();
}

inline void ReaderFactoryBase::Reset() {
  Object::Reset();
  shared_.reset();
}

template <typename Src>
inline ReaderFactory<Src>::ReaderFactory(const Src& src, Options options)
    : src_(src) {
  Initialize(options.buffer_size(), src_.get());
}

template <typename Src>
inline ReaderFactory<Src>::ReaderFactory(Src&& src, Options options)
    : src_(std::move(src)) {
  Initialize(options.buffer_size(), src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline ReaderFactory<Src>::ReaderFactory(std::tuple<SrcArgs...> src_args,
                                         Options options)
    : src_(std::move(src_args)) {
  Initialize(options.buffer_size(), src_.get());
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
inline void ReaderFactory<Src>::Reset(const Src& src, Options options) {
  ReaderFactoryBase::Reset();
  src_.Reset(src);
  Initialize(options.buffer_size(), src_.get());
}

template <typename Src>
inline void ReaderFactory<Src>::Reset(Src&& src, Options options) {
  ReaderFactoryBase::Reset();
  src_.Reset(std::move(src));
  Initialize(options.buffer_size(), src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline void ReaderFactory<Src>::Reset(std::tuple<SrcArgs...> src_args,
                                      Options options) {
  ReaderFactoryBase::Reset();
  src_.Reset(std::move(src_args));
  Initialize(options.buffer_size(), src_.get());
}

template <typename Src>
void ReaderFactory<Src>::Done() {
  ReaderFactoryBase::Done();
  if (src_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) {
      FailWithoutAnnotation(src_->status());
    }
  }
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_READER_FACTORY_H_
