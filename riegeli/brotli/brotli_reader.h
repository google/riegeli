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
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "brotli/decode.h"
#include "brotli/shared_dictionary.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/brotli/brotli_allocator.h"
#include "riegeli/bytes/pullable_reader.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

// Template parameter independent part of `BrotliReader`.
class BrotliReaderBase : public PullableReader {
 public:
  // Stores Shared Brotli dictionaries for decompression.
  //
  // A `Dictionaries` object can own the dictionary data, or can hold pointers
  // to unowned dictionary data which must not be changed until the last
  // `BrotliReader` using these dictionaries is closed or no longer used. If the
  // same sequence of dictionaries is needed for multiple decompression
  // sessions, the `Dictionaries` object can be reused.
  //
  // Copying a `Dictionaries` object is cheap, sharing the actual dictionaries.
  class Dictionaries {
   public:
    static constexpr size_t kMaxDictionaries = SHARED_BROTLI_MAX_COMPOUND_DICTS;

    Dictionaries() noexcept {}

    // Clears all dictionaries.
    Dictionaries& clear() & {
      dictionaries_.clear();
      return *this;
    }
    Dictionaries&& clear() && { return std::move(clear()); }

    // Adds a raw dictionary (data which should contain sequences that are
    // commonly seen in the data being compressed). Up to `kMaxDictionaries` can
    // be added.
    //
    // `std::string&&` is accepted with a template to avoid implicit conversions
    // to `std::string` which can be ambiguous against `absl::string_view`
    // (e.g. `const char*`).
    Dictionaries& add_raw(absl::string_view data) & {
      dictionaries_.emplace_back(
          BROTLI_SHARED_DICTIONARY_RAW, data,
          std::integral_constant<Ownership, Ownership::kCopied>());
      return *this;
    }
    template <typename Src,
              std::enable_if_t<std::is_same<Src, std::string>::value, int> = 0>
    Dictionaries& add_raw(Src&& data) & {
      // `std::move(data)` is correct and `std::forward<Src>(data)` is not
      // necessary: `Src` is always `std::string`, never an lvalue reference.
      dictionaries_.emplace_back(BROTLI_SHARED_DICTIONARY_RAW, std::move(data));
      return *this;
    }
    Dictionaries&& add_raw(absl::string_view data) && {
      return std::move(add_raw(data));
    }
    template <typename Src,
              std::enable_if_t<std::is_same<Src, std::string>::value, int> = 0>
    Dictionaries&& add_raw(Src&& data) && {
      // `std::move(data)` is correct and `std::forward<Src>(data)` is not
      // necessary: `Src` is always `std::string`, never an lvalue reference.
      return std::move(add_raw(std::move(data)));
    }

    // Like `add_raw()`, but does not take ownership of `data`, which must not
    // be changed until the last `BrotliReader` using these dictionaries is
    // closed or no longer used.
    Dictionaries& add_raw_unowned(absl::string_view data) & {
      dictionaries_.emplace_back(
          BROTLI_SHARED_DICTIONARY_RAW, data,
          std::integral_constant<Ownership, Ownership::kUnowned>());
      return *this;
    }
    Dictionaries&& add_raw_unowned(absl::string_view data) && {
      return std::move(add_raw_unowned(data));
    }

    // Sets a serialized dictionary (prepared by shared_brotli_encode_dictionary
    // tool).
    //
    // `std::string&&` is accepted with a template to avoid implicit conversions
    // to `std::string` which can be ambiguous against `absl::string_view`
    // (e.g. `const char*`).
    Dictionaries& set_serialized(absl::string_view data) & {
      clear();
      dictionaries_.emplace_back(
          BROTLI_SHARED_DICTIONARY_SERIALIZED, data,
          std::integral_constant<Ownership, Ownership::kCopied>());
      return *this;
    }
    template <typename Src,
              std::enable_if_t<std::is_same<Src, std::string>::value, int> = 0>
    Dictionaries& set_serialized(Src&& data) & {
      clear();
      // `std::move(data)` is correct and `std::forward<Src>(data)` is not
      // necessary: `Src` is always `std::string`, never an lvalue reference.
      dictionaries_.emplace_back(BROTLI_SHARED_DICTIONARY_SERIALIZED,
                                 std::move(data));
      return *this;
    }
    Dictionaries&& set_serialized(absl::string_view data) && {
      return std::move(set_serialized(data));
    }
    template <typename Src,
              std::enable_if_t<std::is_same<Src, std::string>::value, int> = 0>
    Dictionaries&& set_serialized(Src&& data) && {
      // `std::move(data)` is correct and `std::forward<Src>(data)` is not
      // necessary: `Src` is always `std::string`, never an lvalue reference.
      return std::move(set_serialized(std::move(data)));
    }

    // Like `set_serialized()`, but does not take ownership of `data`, which
    // must not be changed until the last `BrotliReader` using these
    // dictionaries is closed or no longer used.
    Dictionaries& set_serialized_unowned(absl::string_view data) & {
      clear();
      dictionaries_.emplace_back(
          BROTLI_SHARED_DICTIONARY_SERIALIZED, data,
          std::integral_constant<Ownership, Ownership::kUnowned>());
      return *this;
    }
    Dictionaries&& set_serialized_unowned(absl::string_view data) && {
      return std::move(set_serialized_unowned(data));
    }

    // Returns `true` if no dictionaries are present.
    bool empty() const { return dictionaries_.empty(); }

   private:
    friend class BrotliReaderBase;

    enum class Ownership { kCopied, kUnowned };

    class Dictionary {
     public:
      // Owns a copy of `data`.
      explicit Dictionary(BrotliSharedDictionaryType type,
                          absl::string_view data,
                          std::integral_constant<Ownership, Ownership::kCopied>)
          : type_(type),
            owned_data_(std::make_shared<const std::string>(data)),
            data_(*owned_data_) {}
      // Owns moved `data`.
      explicit Dictionary(BrotliSharedDictionaryType type, std::string&& data)
          : type_(type),
            owned_data_(std::make_shared<const std::string>(std::move(data))),
            data_(*owned_data_) {}
      // Does not take ownership of `data`, which must not be changed until the
      // last `BrotliReader` using these dictionaries is closed or no longer
      // used.
      explicit Dictionary(
          BrotliSharedDictionaryType type, absl::string_view data,
          std::integral_constant<Ownership, Ownership::kUnowned>)
          : type_(type), data_(data) {}

      BrotliSharedDictionaryType type() const { return type_; }
      absl::string_view data() const { return data_; }

     private:
      BrotliSharedDictionaryType type_;
      std::shared_ptr<const std::string> owned_data_;
      absl::string_view data_;
    };

    explicit Dictionaries(std::vector<Dictionary> dictionaries)
        : dictionaries_(std::move(dictionaries)) {}

    std::vector<Dictionary> dictionaries_;
  };

  class Options {
   public:
    Options() noexcept {}

    // Shared Brotli dictionaries. The same sequence of dictionaries must have
    // been used for compression.
    //
    // Default: `Dictionaries()`.
    Options& set_dictionaries(const Dictionaries& dictionaries) & {
      dictionaries_ = dictionaries;
      return *this;
    }
    Options& set_dictionaries(Dictionaries&& dictionaries) & {
      dictionaries_ = std::move(dictionaries);
      return *this;
    }
    Options&& set_dictionaries(const Dictionaries& dictionaries) && {
      return std::move(set_dictionaries(dictionaries));
    }
    Options&& set_dictionaries(Dictionaries&& dictionaries) && {
      return std::move(set_dictionaries(std::move(dictionaries)));
    }
    Dictionaries& dictionaries() { return dictionaries_; }
    const Dictionaries& dictionaries() const { return dictionaries_; }

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
    Dictionaries dictionaries_;
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

  explicit BrotliReaderBase(Dictionaries&& dictionaries,
                            BrotliAllocator&& allocator);

  BrotliReaderBase(BrotliReaderBase&& that) noexcept;
  BrotliReaderBase& operator=(BrotliReaderBase&& that) noexcept;

  void Reset(Closed);
  void Reset(Dictionaries&& dictionaries, BrotliAllocator&& allocator);
  void Initialize(Reader* src);

  void Done() override;
  // `BrotliReaderBase` overrides `Reader::DefaultAnnotateStatus()` to annotate
  // the status with the current position, clarifying that this is the
  // uncompressed position. A status propagated from `*src_reader()` might carry
  // annotation with the compressed position.
  ABSL_ATTRIBUTE_COLD void DefaultAnnotateStatus() override;
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

  std::vector<Dictionaries::Dictionary> dictionaries_;
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

inline BrotliReaderBase::BrotliReaderBase(Dictionaries&& dictionaries,
                                          BrotliAllocator&& allocator)
    : dictionaries_(std::move(dictionaries.dictionaries_)),
      allocator_(std::move(allocator)) {}

inline BrotliReaderBase::BrotliReaderBase(BrotliReaderBase&& that) noexcept
    : PullableReader(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      dictionaries_(std::move(that.dictionaries_)),
      allocator_(std::move(that.allocator_)),
      truncated_(that.truncated_),
      initial_compressed_pos_(that.initial_compressed_pos_),
      decompressor_(std::move(that.decompressor_)) {}

inline BrotliReaderBase& BrotliReaderBase::operator=(
    BrotliReaderBase&& that) noexcept {
  PullableReader::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  dictionaries_ = std::move(that.dictionaries_);
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
  dictionaries_ = std::vector<Dictionaries::Dictionary>();
  allocator_ = BrotliAllocator();
}

inline void BrotliReaderBase::Reset(Dictionaries&& dictionaries,
                                    BrotliAllocator&& allocator) {
  PullableReader::Reset();
  truncated_ = false;
  initial_compressed_pos_ = 0;
  decompressor_.reset();
  dictionaries_ = std::move(dictionaries.dictionaries_);
  allocator_ = std::move(allocator);
}

template <typename Src>
inline BrotliReader<Src>::BrotliReader(const Src& src, Options options)
    : BrotliReaderBase(std::move(options.dictionaries()),
                       std::move(options.allocator())),
      src_(src) {
  Initialize(src_.get());
}

template <typename Src>
inline BrotliReader<Src>::BrotliReader(Src&& src, Options options)
    : BrotliReaderBase(std::move(options.dictionaries()),
                       std::move(options.allocator())),
      src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline BrotliReader<Src>::BrotliReader(std::tuple<SrcArgs...> src_args,
                                       Options options)
    : BrotliReaderBase(std::move(options.dictionaries()),
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
  BrotliReaderBase::Reset(std::move(options.dictionaries()),
                          std::move(options.allocator()));
  src_.Reset(src);
  Initialize(src_.get());
}

template <typename Src>
inline void BrotliReader<Src>::Reset(Src&& src, Options options) {
  BrotliReaderBase::Reset(std::move(options.dictionaries()),
                          std::move(options.allocator()));
  src_.Reset(std::move(src));
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline void BrotliReader<Src>::Reset(std::tuple<SrcArgs...> src_args,
                                     Options options) {
  BrotliReaderBase::Reset(std::move(options.dictionaries()),
                          std::move(options.allocator()));
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

}  // namespace riegeli

#endif  // RIEGELI_BROTLI_BROTLI_READER_H_
