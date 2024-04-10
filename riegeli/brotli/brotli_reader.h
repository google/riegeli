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

#include <stddef.h>

#include <memory>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "brotli/decode.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/brotli/brotli_allocator.h"   // IWYU pragma: export
#include "riegeli/brotli/brotli_dictionary.h"  // IWYU pragma: export
#include "riegeli/bytes/pullable_reader.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

// Template parameter independent part of `BrotliReader`.
class BrotliReaderBase : public PullableReader {
 public:
  class Options {
   public:
    Options() noexcept {}

    // Shared Brotli dictionary. The same dictionary must have been used
    // for compression. If no dictionary was used for compression, then no
    // dictionary must be supplied for decompression.
    //
    // Default: `BrotliDictionary()`.
    Options& set_dictionary(BrotliDictionary dictionary) & {
      dictionary_ = std::move(dictionary);
      return *this;
    }
    Options&& set_dictionary(BrotliDictionary dictionary) && {
      return std::move(set_dictionary(std::move(dictionary)));
    }
    BrotliDictionary& dictionary() { return dictionary_; }
    const BrotliDictionary& dictionary() const { return dictionary_; }

    // Memory allocator used by the Brotli engine.
    //
    // Default: `BrotliAllocator()`.
    Options& set_allocator(BrotliAllocator allocator) & {
      allocator_ = std::move(allocator);
      return *this;
    }
    Options&& set_allocator(BrotliAllocator allocator) && {
      return std::move(set_allocator(std::move(allocator)));
    }
    BrotliAllocator& allocator() { return allocator_; }
    const BrotliAllocator& allocator() const { return allocator_; }

   private:
    BrotliDictionary dictionary_;
    BrotliAllocator allocator_;
  };

  // Returns the compressed `Reader`. Unchanged by `Close()`.
  virtual Reader* SrcReader() const = 0;

  // Returns `true` if the source is truncated (without a clean end of the
  // compressed stream) at the current position. In such case, if the source
  // does not grow, `Close()` will fail.
  bool truncated() const { return truncated_ && available() == 0; }

  bool ToleratesReadingAhead() override;
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
  bool PullBehindScratch(size_t recommended_length) override;
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
  // If `ok()` but `decompressor_ == nullptr` then all data have been
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
// `ChainReader<>` (owned), `std::unique_ptr<Reader>` (owned),
// `AnyDependency<Reader*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as
// `InitializerTargetT` of the type of the first constructor argument.
// This requires C++17.
//
// The compressed `Reader` must not be accessed until the `BrotliReader` is
// closed or no longer used.
template <typename Src = Reader*>
class BrotliReader : public BrotliReaderBase {
 public:
  // Creates a closed `BrotliReader`.
  explicit BrotliReader(Closed) noexcept : BrotliReaderBase(kClosed) {}

  // Will read from the compressed `Reader` provided by `src`.
  explicit BrotliReader(Initializer<Src> src, Options options = Options());

  BrotliReader(BrotliReader&& that) noexcept;
  BrotliReader& operator=(BrotliReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `BrotliReader`. This avoids
  // constructing a temporary `BrotliReader` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Src> src,
                                          Options options = Options());

  // Returns the object providing and possibly owning the compressed `Reader`.
  // Unchanged by `Close()`.
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  Reader* SrcReader() const override { return src_.get(); }

 protected:
  void Done() override;
  void SetReadAllHintImpl(bool read_all_hint) override;
  void VerifyEndImpl() override;

 private:
  // The object providing and possibly owning the compressed `Reader`.
  Dependency<Reader*, Src> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit BrotliReader(Closed) -> BrotliReader<DeleteCtad<Closed>>;
template <typename Src>
explicit BrotliReader(
    Src&& src, BrotliReaderBase::Options options = BrotliReaderBase::Options())
    -> BrotliReader<InitializerTargetT<Src>>;
#endif

// Implementation details follow.

inline BrotliReaderBase::BrotliReaderBase(BrotliDictionary&& dictionary,
                                          BrotliAllocator&& allocator)
    : dictionary_(std::move(dictionary)), allocator_(std::move(allocator)) {}

inline BrotliReaderBase::BrotliReaderBase(BrotliReaderBase&& that) noexcept
    : PullableReader(static_cast<PullableReader&&>(that)),
      dictionary_(std::move(that.dictionary_)),
      allocator_(std::move(that.allocator_)),
      truncated_(that.truncated_),
      initial_compressed_pos_(that.initial_compressed_pos_),
      decompressor_(std::move(that.decompressor_)) {}

inline BrotliReaderBase& BrotliReaderBase::operator=(
    BrotliReaderBase&& that) noexcept {
  PullableReader::operator=(static_cast<PullableReader&&>(that));
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
inline BrotliReader<Src>::BrotliReader(Initializer<Src> src, Options options)
    : BrotliReaderBase(std::move(options.dictionary()),
                       std::move(options.allocator())),
      src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
inline BrotliReader<Src>::BrotliReader(BrotliReader&& that) noexcept
    : BrotliReaderBase(static_cast<BrotliReaderBase&&>(that)),
      src_(std::move(that.src_)) {}

template <typename Src>
inline BrotliReader<Src>& BrotliReader<Src>::operator=(
    BrotliReader&& that) noexcept {
  BrotliReaderBase::operator=(static_cast<BrotliReaderBase&&>(that));
  src_ = std::move(that.src_);
  return *this;
}

template <typename Src>
inline void BrotliReader<Src>::Reset(Closed) {
  BrotliReaderBase::Reset(kClosed);
  src_.Reset();
}

template <typename Src>
inline void BrotliReader<Src>::Reset(Initializer<Src> src, Options options) {
  BrotliReaderBase::Reset(std::move(options.dictionary()),
                          std::move(options.allocator()));
  src_.Reset(std::move(src));
  Initialize(src_.get());
}

template <typename Src>
void BrotliReader<Src>::Done() {
  BrotliReaderBase::Done();
  if (src_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) {
      FailWithoutAnnotation(AnnotateOverSrc(src_->status()));
    }
  }
}

template <typename Src>
void BrotliReader<Src>::SetReadAllHintImpl(bool read_all_hint) {
  if (src_.IsOwning()) src_->SetReadAllHint(read_all_hint);
}

template <typename Src>
void BrotliReader<Src>::VerifyEndImpl() {
  BrotliReaderBase::VerifyEndImpl();
  if (src_.IsOwning() && ABSL_PREDICT_TRUE(ok())) src_->VerifyEnd();
}

}  // namespace riegeli

#endif  // RIEGELI_BROTLI_BROTLI_READER_H_
