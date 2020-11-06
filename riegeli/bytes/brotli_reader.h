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

#ifndef RIEGELI_BYTES_BROTLI_READER_H_
#define RIEGELI_BYTES_BROTLI_READER_H_

#include <stddef.h>

#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "brotli/decode.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/resetter.h"
#include "riegeli/bytes/brotli_allocator.h"
#include "riegeli/bytes/pullable_reader.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

// Template parameter independent part of `BrotliReader`.
class BrotliReaderBase : public PullableReader {
 public:
  class Options {
   public:
    Options() noexcept {}

    // Memory allocator used by the Brotli engine.
    //
    // Default: `BrotliAllocator()`.
    Options& set_allocator(const BrotliAllocator& allocator) & {
      allocator_ = allocator;
      return *this;
    }
    Options& set_allocator(BrotliAllocator&& allocator) & {
      allocator_ = std::move(allocator);
      return *this;
    }
    Options&& set_allocator(const BrotliAllocator& allocator) && {
      return std::move(set_allocator(allocator));
    }
    Options&& set_allocator(BrotliAllocator&& allocator) && {
      return std::move(set_allocator(std::move(allocator)));
    }
    BrotliAllocator& allocator() & { return allocator_; }
    const BrotliAllocator& allocator() const& { return allocator_; }
    BrotliAllocator&& allocator() && { return std::move(allocator_); }
    const BrotliAllocator&& allocator() const&& {
      return std::move(allocator_);
    }

   private:
    BrotliAllocator allocator_;
  };

  // Returns the compressed `Reader`. Unchanged by `Close()`.
  virtual Reader* src_reader() = 0;
  virtual const Reader* src_reader() const = 0;

  // `BrotliReaderBase` overrides `Reader::Fail()` to annotate the status with
  // the current position, clarifying that this is the uncompressed position.
  // A status propagated from `*src_reader()` might carry annotation with the
  // compressed position.
  using PullableReader::Fail;
  ABSL_ATTRIBUTE_COLD bool Fail(absl::Status status) override;

  // Returns `true` if the source is truncated (without a clean end of the
  // compressed stream) at the current position. In such case, if the source
  // does not grow, `Close()` will fail.
  bool truncated() const { return truncated_; }

 protected:
  BrotliReaderBase() noexcept : PullableReader(kInitiallyClosed) {}

  explicit BrotliReaderBase(BrotliAllocator&& allocator);

  BrotliReaderBase(BrotliReaderBase&& that) noexcept;
  BrotliReaderBase& operator=(BrotliReaderBase&& that) noexcept;

  void Reset();
  void Reset(BrotliAllocator&& allocator);
  void Initialize(Reader* src);

  void Done() override;
  bool PullSlow(size_t min_length, size_t recommended_length) override;

 private:
  struct BrotliDecoderStateDeleter {
    void operator()(BrotliDecoderState* ptr) const {
      BrotliDecoderDestroyInstance(ptr);
    }
  };

  BrotliAllocator allocator_;
  // If `true`, the source is truncated (without a clean end of the compressed
  // stream) at the current position. If the source does not grow, `Close()`
  // will fail.
  bool truncated_ = false;
  // If `healthy()` but `decompressor_ == nullptr` then all data have been
  // decompressed.
  std::unique_ptr<BrotliDecoderState, BrotliDecoderStateDeleter> decompressor_;

  // Invariant if scratch is not used:
  //   `start()` and `limit()` point to the buffer returned by
  //   `BrotliDecoderTakeOutput()` or are both `nullptr`
};

// A `Reader` which decompresses data with Brotli after getting it from another
// `Reader`.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the compressed `Reader`. `Src` must support
// `Dependency<Reader*, Src>`, e.g. `Reader*` (not owned, default),
// `std::unique_ptr<Reader>` (owned), `ChainReader<>` (owned).
//
// The compressed `Reader` must not be accessed until the `BrotliReader` is
// closed or no longer used.
template <typename Src = Reader*>
class BrotliReader : public BrotliReaderBase {
 public:
  // Creates a closed `BrotliReader`.
  BrotliReader() noexcept {}

  // Will read from the compressed `Reader` provided by `src`.
  explicit BrotliReader(const Src& src, Options options = Options());
  explicit BrotliReader(Src&& src, Options options = Options());

  // Will read from the compressed `Reader` provided by a `Src` constructed from
  // elements of `src_args`. This avoids constructing a temporary `Src` and
  // moving from it.
  template <typename... SrcArgs>
  explicit BrotliReader(std::tuple<SrcArgs...> src_args,
                        Options options = Options());

  BrotliReader(BrotliReader&& that) noexcept;
  BrotliReader& operator=(BrotliReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `BrotliReader`. This avoids
  // constructing a temporary `BrotliReader` and moving from it.
  void Reset();
  void Reset(const Src& src, Options options = Options());
  void Reset(Src&& src, Options options = Options());
  template <typename... SrcArgs>
  void Reset(std::tuple<SrcArgs...> src_args, Options options = Options());

  // Returns the object providing and possibly owning the compressed `Reader`.
  // Unchanged by `Close()`.
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  Reader* src_reader() override { return src_.get(); }
  const Reader* src_reader() const override { return src_.get(); }

  void VerifyEnd() override;

 protected:
  void Done() override;

 private:
  // The object providing and possibly owning the compressed `Reader`.
  Dependency<Reader*, Src> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
template <typename Src>
BrotliReader(Src&& src,
             BrotliReaderBase::Options options = BrotliReaderBase::Options())
    -> BrotliReader<std::decay_t<Src>>;
template <typename... SrcArgs>
BrotliReader(std::tuple<SrcArgs...> src_args,
             BrotliReaderBase::Options options = BrotliReaderBase::Options())
    -> BrotliReader<void>;  // Delete.
#endif

// Implementation details follow.

inline BrotliReaderBase::BrotliReaderBase(BrotliAllocator&& allocator)
    : PullableReader(kInitiallyOpen), allocator_(std::move(allocator)) {}

inline BrotliReaderBase::BrotliReaderBase(BrotliReaderBase&& that) noexcept
    : PullableReader(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      allocator_(std::move(that.allocator_)),
      truncated_(that.truncated_),
      decompressor_(std::move(that.decompressor_)) {}

inline BrotliReaderBase& BrotliReaderBase::operator=(
    BrotliReaderBase&& that) noexcept {
  PullableReader::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  allocator_ = std::move(that.allocator_);
  truncated_ = that.truncated_;
  decompressor_ = std::move(that.decompressor_);
  return *this;
}

inline void BrotliReaderBase::Reset() {
  PullableReader::Reset(kInitiallyClosed);
  truncated_ = false;
  decompressor_.reset();
  allocator_ = BrotliAllocator();
}

inline void BrotliReaderBase::Reset(BrotliAllocator&& allocator) {
  PullableReader::Reset(kInitiallyOpen);
  truncated_ = false;
  decompressor_.reset();
  allocator_ = std::move(allocator);
}

template <typename Src>
inline BrotliReader<Src>::BrotliReader(const Src& src, Options options)
    : BrotliReaderBase(std::move(options.allocator())), src_(src) {
  Initialize(src_.get());
}

template <typename Src>
inline BrotliReader<Src>::BrotliReader(Src&& src, Options options)
    : BrotliReaderBase(std::move(options.allocator())), src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline BrotliReader<Src>::BrotliReader(std::tuple<SrcArgs...> src_args,
                                       Options options)
    : BrotliReaderBase(std::move(options.allocator())),
      src_(std::move(src_args)) {
  Initialize(src_.get());
}

template <typename Src>
inline BrotliReader<Src>::BrotliReader(BrotliReader&& that) noexcept
    : BrotliReaderBase(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      src_(std::move(that.src_)) {}

template <typename Src>
inline BrotliReader<Src>& BrotliReader<Src>::operator=(
    BrotliReader&& that) noexcept {
  BrotliReaderBase::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  src_ = std::move(that.src_);
  return *this;
}

template <typename Src>
inline void BrotliReader<Src>::Reset() {
  BrotliReaderBase::Reset();
  src_.Reset();
}

template <typename Src>
inline void BrotliReader<Src>::Reset(const Src& src, Options options) {
  BrotliReaderBase::Reset(std::move(options.allocator()));
  src_.Reset(src);
  Initialize(src_.get());
}

template <typename Src>
inline void BrotliReader<Src>::Reset(Src&& src, Options options) {
  BrotliReaderBase::Reset(std::move(options.allocator()));
  src_.Reset(std::move(src));
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline void BrotliReader<Src>::Reset(std::tuple<SrcArgs...> src_args,
                                     Options options) {
  BrotliReaderBase::Reset(std::move(options.allocator()));
  src_.Reset(std::move(src_args));
  Initialize(src_.get());
}

template <typename Src>
void BrotliReader<Src>::Done() {
  BrotliReaderBase::Done();
  if (src_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) Fail(*src_);
  }
}

template <typename Src>
void BrotliReader<Src>::VerifyEnd() {
  BrotliReaderBase::VerifyEnd();
  if (src_.is_owning() && ABSL_PREDICT_TRUE(healthy())) src_->VerifyEnd();
}

template <typename Src>
struct Resetter<BrotliReader<Src>> : ResetterByReset<BrotliReader<Src>> {};

}  // namespace riegeli

#endif  // RIEGELI_BYTES_BROTLI_READER_H_
