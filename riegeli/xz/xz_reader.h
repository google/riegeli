// Copyright 2023 Google LLC
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

#ifndef RIEGELI_XZ_XZ_READER_H_
#define RIEGELI_XZ_XZ_READER_H_

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "lzma.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/object.h"
#include "riegeli/base/recycling_pool.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

// Template parameter independent part of `XzReader`.
class XzReaderBase : public BufferedReader {
 public:
  // Specifies what container format to expect.
  enum class Container {
    kXz,        // Xz container (recommended).
    kLzma,      // Lzma container (legacy file format).
    kXzOrLzma,  // Xz or Lzma container.
  };

  class Options : public BufferOptionsBase<Options> {
   public:
    Options() noexcept {}

    // What container format to expect.
    //
    // Default: `Container::kXzOrLzma`.
    static constexpr Container kDefaultContainer = Container::kXzOrLzma;
    Options& set_container(Container container) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      container_ = container;
      return *this;
    }
    Options&& set_container(Container container) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_container(container));
    }
    Container container() const { return container_; }

    // If `true`, concatenated compressed streams are decoded to concatenation
    // of their decompressed contents. An empty compressed stream is decoded to
    // empty decompressed contents.
    //
    // If `false`, exactly one compressed stream is consumed.
    //
    // `concatenate()` is supported only for `Container::kXz` and
    // `Container::kXzOrLzma` (if the actual format is `kXz`)
    //
    // Default: `false`.
    Options& set_concatenate(bool concatenate) & ABSL_ATTRIBUTE_LIFETIME_BOUND {
      concatenate_ = concatenate;
      return *this;
    }
    Options&& set_concatenate(bool concatenate) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_concatenate(concatenate));
    }
    bool concatenate() const { return concatenate_; }

    // Options for a global `RecyclingPool` of decompression contexts.
    //
    // They tune the amount of memory which is kept to speed up creation of new
    // decompression sessions, and usage of a background thread to clean it.
    //
    // Default: `RecyclingPoolOptions()`.
    Options& set_recycling_pool_options(
        const RecyclingPoolOptions& recycling_pool_options) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      recycling_pool_options_ = recycling_pool_options;
      return *this;
    }
    Options&& set_recycling_pool_options(
        const RecyclingPoolOptions& recycling_pool_options) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_recycling_pool_options(recycling_pool_options));
    }
    const RecyclingPoolOptions& recycling_pool_options() const
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return recycling_pool_options_;
    }

   private:
    Container container_ = kDefaultContainer;
    bool concatenate_ = false;
    RecyclingPoolOptions recycling_pool_options_;
  };

  // Returns the compressed `Reader`. Unchanged by `Close()`.
  virtual Reader* SrcReader() const ABSL_ATTRIBUTE_LIFETIME_BOUND = 0;

  // Returns `true` if the source is truncated (without a clean end of the
  // compressed stream) at the current position. In such case, if the source
  // does not grow, `Close()` will fail.
  //
  // Precondition: `Options::concatenate()` was `false`.
  bool truncated() const {
    RIEGELI_ASSERT_EQ(flags_ & LZMA_CONCATENATED, 0u)
        << "Failed precondition of XzReaderBase::truncated(): "
           "Options::concatenate() is true";
    return truncated_ && available() == 0;
  }

  bool ToleratesReadingAhead() override;
  bool SupportsRewind() override;
  bool SupportsNewReader() override;

 protected:
  explicit XzReaderBase(Closed) noexcept : BufferedReader(kClosed) {}

  explicit XzReaderBase(BufferOptions buffer_options, Container container,
                        uint32_t flags,
                        const RecyclingPoolOptions& recycling_pool_options);

  XzReaderBase(XzReaderBase&& that) noexcept;
  XzReaderBase& operator=(XzReaderBase&& that) noexcept;

  void Reset(Closed);
  void Reset(BufferOptions buffer_options, Container container, uint32_t flags,
             const RecyclingPoolOptions& recycling_pool_options);
  static int GetWindowBits(const Options& options);
  void Initialize(Reader* src);
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateOverSrc(absl::Status status);

  void Done() override;
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;
  bool ReadInternal(size_t min_length, size_t max_length, char* dest) override;
  void ExactSizeReached() override;
  bool SeekBehindBuffer(Position new_pos) override;
  std::unique_ptr<Reader> NewReaderImpl(Position initial_pos) override;

 private:
  struct LzmaStreamDeleter {
    void operator()(lzma_stream* ptr) const {
      lzma_end(ptr);
      delete ptr;
    }
  };

  struct LzmaStreamKey : WithEqual<LzmaStreamKey> {
    LzmaStreamKey() = default;
    explicit LzmaStreamKey(Container container) : container(container) {}

    friend bool operator==(LzmaStreamKey a, LzmaStreamKey b) {
      return a.container == b.container;
    }
    template <typename HashState>
    friend HashState AbslHashValue(HashState hash_state, LzmaStreamKey self) {
      return HashState::combine(std::move(hash_state), self.container);
    }

    Container container;
  };

  void InitializeDecompressor();
  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation,
                                         lzma_ret liblzma_code);

  Container container_ = Options::kDefaultContainer;
  uint32_t flags_ = 0;
  RecyclingPoolOptions recycling_pool_options_;
  // If `true`, the source is truncated (without a clean end of the compressed
  // stream) at the current position. If the source does not grow, `Close()`
  // will fail.
  bool truncated_ = false;
  Position initial_compressed_pos_ = 0;
  // If `ok()` but `decompressor_ == nullptr` then all data have been
  // decompressed, `exact_size() == limit_pos()`, and `ReadInternal()` must not
  // be called again.
  KeyedRecyclingPool<lzma_stream, LzmaStreamKey, LzmaStreamDeleter>::Handle
      decompressor_;
};

// A `Reader` which decompresses data with Xz (LZMA) after getting it from
// another `Reader`.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the compressed `Reader`. `Src` must support
// `Dependency<Reader*, Src>`, e.g. `Reader*` (not owned, default),
// `ChainReader<>` (owned), `std::unique_ptr<Reader>` (owned),
// `Any<Reader*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as `TargetT` of the
// type of the first constructor argument.
//
// The compressed `Reader` must not be accessed until the `XzReader` is closed
// or no longer used.
template <typename Src = Reader*>
class XzReader : public XzReaderBase {
 public:
  // Creates a closed `XzReader`.
  explicit XzReader(Closed) noexcept : XzReaderBase(kClosed) {}

  // Will read from the compressed `Reader` provided by `src`.
  explicit XzReader(Initializer<Src> src, Options options = Options());

  XzReader(XzReader&&) = default;
  XzReader& operator=(XzReader&&) = default;

  // Makes `*this` equivalent to a newly constructed `XzReader`. This avoids
  // constructing a temporary `XzReader` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Src> src,
                                          Options options = Options());

  // Returns the object providing and possibly owning the compressed `Reader`.
  // Unchanged by `Close()`.
  Src& src() ABSL_ATTRIBUTE_LIFETIME_BOUND { return src_.manager(); }
  const Src& src() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return src_.manager();
  }
  Reader* SrcReader() const ABSL_ATTRIBUTE_LIFETIME_BOUND override {
    return src_.get();
  }

 protected:
  void Done() override;
  void SetReadAllHintImpl(bool read_all_hint) override;
  void VerifyEndImpl() override;

 private:
  // The object providing and possibly owning the compressed `Reader`.
  Dependency<Reader*, Src> src_;
};

explicit XzReader(Closed) -> XzReader<DeleteCtad<Closed>>;
template <typename Src>
explicit XzReader(Src&& src,
                  XzReaderBase::Options options = XzReaderBase::Options())
    -> XzReader<TargetT<Src>>;

// Returns `true` if the data look like they have been Xz-compressed with
// `Container::kXz`.
//
// The current position of `src` is unchanged.
bool RecognizeXz(Reader& src);

// Implementation details follow.

inline XzReaderBase::XzReaderBase(
    BufferOptions buffer_options, Container container, uint32_t flags,
    const RecyclingPoolOptions& recycling_pool_options)
    : BufferedReader(buffer_options),
      container_(container),
      flags_(flags),
      recycling_pool_options_(recycling_pool_options) {}

inline XzReaderBase::XzReaderBase(XzReaderBase&& that) noexcept
    : BufferedReader(static_cast<BufferedReader&&>(that)),
      container_(that.container_),
      flags_(that.flags_),
      recycling_pool_options_(that.recycling_pool_options_),
      truncated_(that.truncated_),
      initial_compressed_pos_(that.initial_compressed_pos_),
      decompressor_(std::move(that.decompressor_)) {}

inline XzReaderBase& XzReaderBase::operator=(XzReaderBase&& that) noexcept {
  BufferedReader::operator=(static_cast<BufferedReader&&>(that));
  container_ = that.container_;
  flags_ = that.flags_;
  recycling_pool_options_ = that.recycling_pool_options_;
  truncated_ = that.truncated_;
  initial_compressed_pos_ = that.initial_compressed_pos_;
  decompressor_ = std::move(that.decompressor_);
  return *this;
}

inline void XzReaderBase::Reset(Closed) {
  BufferedReader::Reset(kClosed);
  container_ = Options::kDefaultContainer;
  flags_ = 0;
  recycling_pool_options_ = RecyclingPoolOptions();
  truncated_ = false;
  initial_compressed_pos_ = 0;
  decompressor_.reset();
}

inline void XzReaderBase::Reset(
    BufferOptions buffer_options, Container container, uint32_t flags,
    const RecyclingPoolOptions& recycling_pool_options) {
  BufferedReader::Reset(buffer_options);
  container_ = container;
  flags_ = flags;
  recycling_pool_options_ = recycling_pool_options;
  truncated_ = false;
  initial_compressed_pos_ = 0;
  decompressor_.reset();
}

template <typename Src>
inline XzReader<Src>::XzReader(Initializer<Src> src, Options options)
    : XzReaderBase(options.buffer_options(), options.container(),
                   options.concatenate() ? LZMA_CONCATENATED : 0,
                   options.recycling_pool_options()),
      src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
inline void XzReader<Src>::Reset(Closed) {
  XzReaderBase::Reset(kClosed);
  src_.Reset();
}

template <typename Src>
inline void XzReader<Src>::Reset(Initializer<Src> src, Options options) {
  XzReaderBase::Reset(options.buffer_options(), options.container(),
                      options.concatenate() ? LZMA_CONCATENATED : 0,
                      options.recycling_pool_options());
  src_.Reset(std::move(src));
  Initialize(src_.get());
}

template <typename Src>
void XzReader<Src>::Done() {
  XzReaderBase::Done();
  if (src_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) {
      FailWithoutAnnotation(AnnotateOverSrc(src_->status()));
    }
  }
}

template <typename Src>
void XzReader<Src>::SetReadAllHintImpl(bool read_all_hint) {
  XzReaderBase::SetReadAllHintImpl(read_all_hint);
  if (src_.IsOwning()) src_->SetReadAllHint(read_all_hint);
}

template <typename Src>
void XzReader<Src>::VerifyEndImpl() {
  XzReaderBase::VerifyEndImpl();
  if (src_.IsOwning() && ABSL_PREDICT_TRUE(ok())) src_->VerifyEnd();
}

}  // namespace riegeli

#endif  // RIEGELI_XZ_XZ_READER_H_
