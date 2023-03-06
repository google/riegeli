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
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "lzma.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/dependency.h"
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
    Options& set_container(Container container) & {
      container_ = container;
      return *this;
    }
    Options&& set_container(Container container) && {
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
    Options& set_concatenate(bool concatenate) & {
      concatenate_ = concatenate;
      return *this;
    }
    Options&& set_concatenate(bool concatenate) && {
      return std::move(set_concatenate(concatenate));
    }
    bool concatenate() const { return concatenate_; }

   private:
    Container container_ = kDefaultContainer;
    bool concatenate_ = false;
  };

  // Returns the compressed `Reader`. Unchanged by `Close()`.
  virtual Reader* SrcReader() = 0;
  virtual const Reader* SrcReader() const = 0;

  // Returns `true` if the source is truncated (without a clean end of the
  // compressed stream) at the current position. In such case, if the source
  // does not grow, `Close()` will fail.
  //
  // Precondition: `Options::concatenate()` was `false`.
  bool truncated() const {
    RIEGELI_ASSERT((flags_ & LZMA_CONCATENATED) == 0)
        << "Failed precondition of XzReaderBase::truncated(): "
           "Options::concatenate() is true";
    return truncated_ && available() == 0;
  }

  bool ToleratesReadingAhead() override;
  bool SupportsRewind() override;
  bool SupportsNewReader() override;

 protected:
  explicit XzReaderBase(Closed) noexcept : BufferedReader(kClosed) {}

  explicit XzReaderBase(const BufferOptions& buffer_options,
                        Container container, uint32_t flags);

  XzReaderBase(XzReaderBase&& that) noexcept;
  XzReaderBase& operator=(XzReaderBase&& that) noexcept;

  void Reset(Closed);
  void Reset(const BufferOptions& buffer_options, Container container,
             uint32_t flags);
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

  struct LzmaStreamKey {
    friend bool operator==(LzmaStreamKey a, LzmaStreamKey b) {
      return a.container == b.container;
    }
    friend bool operator!=(LzmaStreamKey a, LzmaStreamKey b) {
      return a.container != b.container;
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
// `AnyDependency<Reader*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as the value type of
// the first constructor argument. This requires C++17.
//
// The compressed `Reader` must not be accessed until the `XzReader` is closed
// or no longer used.
template <typename Src = Reader*>
class XzReader : public XzReaderBase {
 public:
  // Creates a closed `XzReader`.
  explicit XzReader(Closed) noexcept : XzReaderBase(kClosed) {}

  // Will read from the compressed `Reader` provided by `src`.
  explicit XzReader(const Src& src, Options options = Options());
  explicit XzReader(Src&& src, Options options = Options());

  // Will read from the compressed `Reader` provided by a `Src` constructed from
  // elements of `src_args`. This avoids constructing a temporary `Src` and
  // moving from it.
  template <typename... SrcArgs>
  explicit XzReader(std::tuple<SrcArgs...> src_args,
                    Options options = Options());

  XzReader(XzReader&&) noexcept;
  XzReader& operator=(XzReader&&) noexcept;

  // Makes `*this` equivalent to a newly constructed `XzReader`. This avoids
  // constructing a temporary `XzReader` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(const Src& src,
                                          Options options = Options());
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Src&& src,
                                          Options options = Options());
  template <typename... SrcArgs>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(std::tuple<SrcArgs...> src_args,
                                          Options options = Options());

  // Returns the object providing and possibly owning the compressed `Reader`.
  // Unchanged by `Close()`.
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  Reader* SrcReader() override { return src_.get(); }
  const Reader* SrcReader() const override { return src_.get(); }

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
explicit XzReader(Closed) -> XzReader<DeleteCtad<Closed>>;
template <typename Src>
explicit XzReader(const Src& src,
                  XzReaderBase::Options options = XzReaderBase::Options())
    -> XzReader<std::decay_t<Src>>;
template <typename Src>
explicit XzReader(Src&& src,
                  XzReaderBase::Options options = XzReaderBase::Options())
    -> XzReader<std::decay_t<Src>>;
template <typename... SrcArgs>
explicit XzReader(std::tuple<SrcArgs...> src_args,
                  XzReaderBase::Options options = XzReaderBase::Options())
    -> XzReader<DeleteCtad<std::tuple<SrcArgs...>>>;
#endif

// Returns `true` if the data look like they have been Xz-compressed with
// `Container::kXz`.
//
// The current position of `src` is unchanged.
bool RecognizeXz(Reader& src);

// Implementation details follow.

inline XzReaderBase::XzReaderBase(const BufferOptions& buffer_options,
                                  Container container, uint32_t flags)
    : BufferedReader(buffer_options), container_(container), flags_(flags) {}

inline XzReaderBase::XzReaderBase(XzReaderBase&& that) noexcept
    : BufferedReader(static_cast<BufferedReader&&>(that)),
      container_(that.container_),
      flags_(that.flags_),
      truncated_(that.truncated_),
      initial_compressed_pos_(that.initial_compressed_pos_),
      decompressor_(std::move(that.decompressor_)) {}

inline XzReaderBase& XzReaderBase::operator=(XzReaderBase&& that) noexcept {
  BufferedReader::operator=(static_cast<BufferedReader&&>(that));
  container_ = that.container_;
  flags_ = that.flags_;
  truncated_ = that.truncated_;
  initial_compressed_pos_ = that.initial_compressed_pos_;
  decompressor_ = std::move(that.decompressor_);
  return *this;
}

inline void XzReaderBase::Reset(Closed) {
  BufferedReader::Reset(kClosed);
  container_ = Options::kDefaultContainer;
  flags_ = 0;
  truncated_ = false;
  initial_compressed_pos_ = 0;
  decompressor_.reset();
}

inline void XzReaderBase::Reset(const BufferOptions& buffer_options,
                                Container container, uint32_t flags) {
  BufferedReader::Reset(buffer_options);
  container_ = container;
  flags_ = flags;
  truncated_ = false;
  initial_compressed_pos_ = 0;
  decompressor_.reset();
}

template <typename Src>
inline XzReader<Src>::XzReader(const Src& src, Options options)
    : XzReaderBase(options.buffer_options(), options.container(),
                   options.concatenate() ? LZMA_CONCATENATED : 0),
      src_(src) {
  Initialize(src_.get());
}

template <typename Src>
inline XzReader<Src>::XzReader(Src&& src, Options options)
    : XzReaderBase(options.buffer_options(), options.container(),
                   options.concatenate() ? LZMA_CONCATENATED : 0),
      src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline XzReader<Src>::XzReader(std::tuple<SrcArgs...> src_args, Options options)
    : XzReaderBase(options.buffer_options(), options.container(),
                   options.concatenate() ? LZMA_CONCATENATED : 0),
      src_(std::move(src_args)) {
  Initialize(src_.get());
}

template <typename Src>
inline XzReader<Src>::XzReader(XzReader&& that) noexcept
    : XzReaderBase(static_cast<XzReaderBase&&>(that)),
      src_(std::move(that.src_)) {}

template <typename Src>
inline XzReader<Src>& XzReader<Src>::operator=(XzReader&& that) noexcept {
  XzReaderBase::operator=(static_cast<XzReaderBase&&>(that));
  src_ = std::move(that.src_);
  return *this;
}

template <typename Src>
inline void XzReader<Src>::Reset(Closed) {
  XzReaderBase::Reset(kClosed);
  src_.Reset();
}

template <typename Src>
inline void XzReader<Src>::Reset(const Src& src, Options options) {
  XzReaderBase::Reset(options.buffer_options(), options.container(),
                      options.concatenate() ? LZMA_CONCATENATED : 0);
  src_.Reset(src);
  Initialize(src_.get());
}

template <typename Src>
inline void XzReader<Src>::Reset(Src&& src, Options options) {
  XzReaderBase::Reset(options.buffer_options(), options.container(),
                      options.concatenate() ? LZMA_CONCATENATED : 0);
  src_.Reset(std::move(src));
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline void XzReader<Src>::Reset(std::tuple<SrcArgs...> src_args,
                                 Options options) {
  XzReaderBase::Reset(options.buffer_options(), options.container(),
                      options.concatenate() ? LZMA_CONCATENATED : 0);
  src_.Reset(std::move(src_args));
  Initialize(src_.get());
}

template <typename Src>
void XzReader<Src>::Done() {
  XzReaderBase::Done();
  if (src_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) {
      FailWithoutAnnotation(AnnotateOverSrc(src_->status()));
    }
  }
}

template <typename Src>
void XzReader<Src>::SetReadAllHintImpl(bool read_all_hint) {
  XzReaderBase::SetReadAllHintImpl(read_all_hint);
  if (src_.is_owning()) src_->SetReadAllHint(read_all_hint);
}

template <typename Src>
void XzReader<Src>::VerifyEndImpl() {
  XzReaderBase::VerifyEndImpl();
  if (src_.is_owning() && ABSL_PREDICT_TRUE(ok())) src_->VerifyEnd();
}

}  // namespace riegeli

#endif  // RIEGELI_XZ_XZ_READER_H_
