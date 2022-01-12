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

#ifndef RIEGELI_BROTLI_BROTLI_READER_H_
#define RIEGELI_BROTLI_BROTLI_READER_H_

#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "brotli/decode.h"
#include "riegeli/base/base.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/brotli/brotli_allocator.h"
#include "riegeli/brotli/brotli_dictionary.h"
#include "riegeli/bytes/pullable_reader.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

// Template parameter independent part of `BrotliReader`.
class BrotliReaderBase : public PullableReader {
 public:
  class Options {
   public:
    Options() noexcept {}

    // Shared Brotli dictionary. The same dictionary must have been used for
    // compression.
    //
    // Default: `BrotliDictionary()`.
    Options& set_dictionary(const BrotliDictionary& dictionary) & {
      dictionary_ = dictionary;
      return *this;
    }
    Options& set_dictionary(BrotliDictionary&& dictionary) & {
      dictionary_ = std::move(dictionary);
      return *this;
    }
    Options&& set_dictionary(const BrotliDictionary& dictionary) && {
      return std::move(set_dictionary(dictionary));
    }
    Options&& set_dictionary(BrotliDictionary&& dictionary) && {
      return std::move(set_dictionary(std::move(dictionary)));
    }
    BrotliDictionary& dictionary() { return dictionary_; }
    const BrotliDictionary& dictionary() const { return dictionary_; }

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
    BrotliAllocator& allocator() { return allocator_; }
    const BrotliAllocator& allocator() const { return allocator_; }

   private:
    BrotliDictionary dictionary_;
    BrotliAllocator allocator_;
  };

  // Returns the compressed `Reader`. Unchanged by `Close()`.
  virtual Reader* src_reader() = 0;
  virtual const Reader* src_reader() const = 0;

  // Returns `true` if the source is truncated (without a clean end of the
  // compressed stream) at the current position. In such case, if the source
  // does not grow, `Close()` will fail.
  bool truncated() const { return truncated_; }

  bool SupportsRewind() override;
  bool SupportsNewReader() override;

 protected:
  explicit BrotliReaderBase(Closed) noexcept : PullableReader(kClosed) {}

  explicit BrotliReaderBase(BrotliDictionary&& dictionary,
                            BrotliAllocator&& allocator);

  BrotliReaderBase(BrotliReaderBase&& that) noexcept;
  BrotliReaderBase& operator=(BrotliReaderBase&& that) noexcept;

  void Reset(Closed);
  void Reset(BrotliDictionary&& dictionary, BrotliAllocator&& allocator);
  void Initialize(Reader* src);
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateOverSrc(absl::Status status);

  void Done() override;
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;
  bool PullBehindScratch() override;
  bool SeekBehindScratch(Position new_pos) override;
  std::unique_ptr<Reader> NewReaderImpl(Position initial_pos) override;

 private:
  struct BrotliDecoderStateDeleter {
    void operator()(BrotliDecoderState* ptr) const {
      BrotliDecoderDestroyInstance(ptr);
    }
  };

  void InitializeDecompressor();

  BrotliDictionary dictionary_;
  BrotliAllocator allocator_;
  // If `true`, the source is truncated (without a clean end of the compressed
  // stream) at the current position. If the source does not grow, `Close()`
  // will fail.
  bool truncated_ = false;
  Position initial_compressed_pos_ = 0;
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
// By relying on CTAD the template argument can be deduced as the value type of
// the first constructor argument. This requires C++17.
//
// The compressed `Reader` must not be accessed until the `BrotliReader` is
// closed or no longer used.
template <typename Src = Reader*>
class BrotliReader : public BrotliReaderBase {
 public:
  // Creates a closed `BrotliReader`.
  explicit BrotliReader(Closed) noexcept : BrotliReaderBase(kClosed) {}

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
  void Reset(Closed);
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
explicit BrotliReader(Closed)->BrotliReader<DeleteCtad<Closed>>;
template <typename Src>
explicit BrotliReader(const Src& src, BrotliReaderBase::Options options =
                                          BrotliReaderBase::Options())
    -> BrotliReader<std::decay_t<Src>>;
template <typename Src>
explicit BrotliReader(
    Src&& src, BrotliReaderBase::Options options = BrotliReaderBase::Options())
    -> BrotliReader<std::decay_t<Src>>;
template <typename... SrcArgs>
explicit BrotliReader(
    std::tuple<SrcArgs...> src_args,
    BrotliReaderBase::Options options = BrotliReaderBase::Options())
    -> BrotliReader<DeleteCtad<std::tuple<SrcArgs...>>>;
#endif

// Implementation details follow.

inline BrotliReaderBase::BrotliReaderBase(BrotliDictionary&& dictionary,
                                          BrotliAllocator&& allocator)
    : dictionary_(std::move(dictionary)), allocator_(std::move(allocator)) {}

inline BrotliReaderBase::BrotliReaderBase(BrotliReaderBase&& that) noexcept
    : PullableReader(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      dictionary_(std::move(that.dictionary_)),
      allocator_(std::move(that.allocator_)),
      truncated_(that.truncated_),
      initial_compressed_pos_(that.initial_compressed_pos_),
      decompressor_(std::move(that.decompressor_)) {}

inline BrotliReaderBase& BrotliReaderBase::operator=(
    BrotliReaderBase&& that) noexcept {
  PullableReader::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  dictionary_ = std::move(that.dictionary_);
  allocator_ = std::move(that.allocator_);
  truncated_ = that.truncated_;
  initial_compressed_pos_ = that.initial_compressed_pos_;
  decompressor_ = std::move(that.decompressor_);
  return *this;
}

inline void BrotliReaderBase::Reset(Closed) {
  PullableReader::Reset(kClosed);
  truncated_ = false;
  initial_compressed_pos_ = 0;
  decompressor_.reset();
  dictionary_ = BrotliDictionary();
  allocator_ = BrotliAllocator();
}

inline void BrotliReaderBase::Reset(BrotliDictionary&& dictionary,
                                    BrotliAllocator&& allocator) {
  PullableReader::Reset();
  truncated_ = false;
  initial_compressed_pos_ = 0;
  decompressor_.reset();
  dictionary_ = std::move(dictionary);
  allocator_ = std::move(allocator);
}

template <typename Src>
inline BrotliReader<Src>::BrotliReader(const Src& src, Options options)
    : BrotliReaderBase(std::move(options.dictionary()),
                       std::move(options.allocator())),
      src_(src) {
  Initialize(src_.get());
}

template <typename Src>
inline BrotliReader<Src>::BrotliReader(Src&& src, Options options)
    : BrotliReaderBase(std::move(options.dictionary()),
                       std::move(options.allocator())),
      src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline BrotliReader<Src>::BrotliReader(std::tuple<SrcArgs...> src_args,
                                       Options options)
    : BrotliReaderBase(std::move(options.dictionary()),
                       std::move(options.allocator())),
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
inline void BrotliReader<Src>::Reset(Closed) {
  BrotliReaderBase::Reset(kClosed);
  src_.Reset();
}

template <typename Src>
inline void BrotliReader<Src>::Reset(const Src& src, Options options) {
  BrotliReaderBase::Reset(std::move(options.dictionary()),
                          std::move(options.allocator()));
  src_.Reset(src);
  Initialize(src_.get());
}

template <typename Src>
inline void BrotliReader<Src>::Reset(Src&& src, Options options) {
  BrotliReaderBase::Reset(std::move(options.dictionary()),
                          std::move(options.allocator()));
  src_.Reset(std::move(src));
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline void BrotliReader<Src>::Reset(std::tuple<SrcArgs...> src_args,
                                     Options options) {
  BrotliReaderBase::Reset(std::move(options.dictionary()),
                          std::move(options.allocator()));
  src_.Reset(std::move(src_args));
  Initialize(src_.get());
}

template <typename Src>
void BrotliReader<Src>::Done() {
  BrotliReaderBase::Done();
  if (src_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) {
      FailWithoutAnnotation(AnnotateOverSrc(src_->status()));
    }
  }
}

template <typename Src>
void BrotliReader<Src>::VerifyEnd() {
  BrotliReaderBase::VerifyEnd();
  if (src_.is_owning() && ABSL_PREDICT_TRUE(healthy())) src_->VerifyEnd();
}

}  // namespace riegeli

#endif  // RIEGELI_BROTLI_BROTLI_READER_H_
