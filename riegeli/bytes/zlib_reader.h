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

#ifndef RIEGELI_BYTES_ZLIB_READER_H_
#define RIEGELI_BYTES_ZLIB_READER_H_

#include <stddef.h>
#include <memory>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/dependency.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/reader.h"
#include "zlib.h"

namespace riegeli {

// Template parameter invariant part of ZlibReader.
class ZlibReaderBase : public BufferedReader {
 public:
  class Options {
   public:
    Options() noexcept {}

    // Parameter interpreted by inflateInit2() which specifies the acceptable
    // base two logarithm of the maximum window size, and which kinds of a
    // header are expected.
    //
    // A negative window_bits means no header, with the negated window_bits
    // specifying the actual number of bits in range 8..15.
    //
    // Otherwise 0 means that any number of bits is acceptable (up to 15),
    // a value in range 8..15 means that up to this number of window bits is
    // acceptable, and further window_bits can be incremented to specify which
    // kinds of a header are expected:
    //  * bits + 0  - zlib header
    //  * bits + 16 - gzip header
    //  * bits + 32 - zlib or gzip header
    Options& set_window_bits(int window_bits) & {
      window_bits_ = window_bits;
      return *this;
    }
    Options&& set_window_bits(int window_bits) && {
      return std::move(set_window_bits(window_bits));
    }

    Options& set_buffer_size(size_t buffer_size) & {
      RIEGELI_ASSERT_GT(buffer_size, 0u)
          << "Failed precondition of "
             "ZlibReaderBase::Options::set_buffer_size(): "
             "zero buffer size";
      buffer_size_ = buffer_size;
      return *this;
    }
    Options&& set_buffer_size(size_t buffer_size) && {
      return std::move(set_buffer_size(buffer_size));
    }

   private:
    template <typename Src>
    friend class ZlibReader;

    int window_bits_ = 32;
    size_t buffer_size_ = kDefaultBufferSize();
  };

  // Returns the compressed Reader. Unchanged by Close().
  virtual Reader* src_reader() = 0;
  virtual const Reader* src_reader() const = 0;

 protected:
  ZlibReaderBase() noexcept {}

  explicit ZlibReaderBase(size_t buffer_size) noexcept
      : BufferedReader(buffer_size) {}

  ZlibReaderBase(ZlibReaderBase&& that) noexcept;
  ZlibReaderBase& operator=(ZlibReaderBase&& that) noexcept;

  ~ZlibReaderBase();

  void Initialize(int window_bits);
  void Done() override;
  bool PullSlow() override;
  bool ReadInternal(char* dest, size_t min_length, size_t max_length) override;

 private:
  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation);

  // If true, the source is truncated (without a clean end of the compressed
  // stream) at the current position. If the source does not grow, Close() will
  // fail.
  bool truncated_ = false;
  bool decompressor_present_ = false;
  z_stream decompressor_;
};

// A Reader which decompresses data with zlib after getting it from another
// Reader.
//
// The Src template parameter specifies the type of the object providing and
// possibly owning the compressed Reader. Src must support
// Dependency<Reader*, Src>, e.g. Reader* (not owned, default),
// unique_ptr<Reader> (owned), ChainReader<> (owned).
//
// The compressed Reader must not be accessed until the ZlibReader is closed or
// no longer used.
template <typename Src = Reader*>
class ZlibReader : public ZlibReaderBase {
 public:
  // Creates a closed ZlibReader.
  ZlibReader() noexcept {}

  // Will read from the compressed Reader provided by src.
  explicit ZlibReader(Src src, Options options = Options());

  ZlibReader(ZlibReader&&) noexcept;
  ZlibReader& operator=(ZlibReader&&) noexcept;

  // Returns the object providing and possibly owning the compressed Reader.
  // Unchanged by Close().
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  Reader* src_reader() override { return src_.ptr(); }
  const Reader* src_reader() const override { return src_.ptr(); }

 protected:
  void Done() override;
  void VerifyEnd() override;

 private:
  // The object providing and possibly owning the compressed Reader.
  Dependency<Reader*, Src> src_;
};

// Implementation details follow.

inline ZlibReaderBase::ZlibReaderBase(ZlibReaderBase&& that) noexcept
    : BufferedReader(std::move(that)),
      truncated_(riegeli::exchange(that.truncated_, false)),
      decompressor_present_(
          riegeli::exchange(that.decompressor_present_, false)),
      decompressor_(that.decompressor_) {}

inline ZlibReaderBase& ZlibReaderBase::operator=(
    ZlibReaderBase&& that) noexcept {
  // Exchange that.decompressor_present_ early to support self-assignment.
  const bool decompressor_present =
      riegeli::exchange(that.decompressor_present_, false);
  if (decompressor_present_) {
    const int result = inflateEnd(&decompressor_);
    RIEGELI_ASSERT_EQ(result, Z_OK) << "inflateEnd() failed";
  }
  BufferedReader::operator=(std::move(that));
  truncated_ = riegeli::exchange(that.truncated_, false);
  decompressor_present_ = decompressor_present;
  decompressor_ = that.decompressor_;
  return *this;
}

inline ZlibReaderBase::~ZlibReaderBase() {
  if (decompressor_present_) {
    const int result = inflateEnd(&decompressor_);
    RIEGELI_ASSERT_EQ(result, Z_OK) << "inflateEnd() failed";
  }
}

template <typename Src>
ZlibReader<Src>::ZlibReader(Src src, Options options)
    : ZlibReaderBase(options.buffer_size_), src_(std::move(src)) {
  RIEGELI_ASSERT(src_.ptr() != nullptr)
      << "Failed precondition of ZlibReader<Src>::ZlibReader(Src): "
         "null Reader pointer";
  Initialize(options.window_bits_);
}

template <typename Src>
ZlibReader<Src>::ZlibReader(ZlibReader&& that) noexcept
    : ZlibReaderBase(std::move(that)), src_(std::move(that.src_)) {}

template <typename Src>
ZlibReader<Src>& ZlibReader<Src>::operator=(ZlibReader&& that) noexcept {
  ZlibReaderBase::operator=(std::move(that));
  src_ = std::move(that.src_);
  return *this;
}

template <typename Src>
void ZlibReader<Src>::Done() {
  ZlibReaderBase::Done();
  if (src_.kIsOwning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) Fail(*src_);
  }
}

template <typename Src>
void ZlibReader<Src>::VerifyEnd() {
  ZlibReaderBase::VerifyEnd();
  if (src_.kIsOwning()) src_->VerifyEnd();
}

extern template class ZlibReader<Reader*>;
extern template class ZlibReader<std::unique_ptr<Reader>>;

}  // namespace riegeli

#endif  // RIEGELI_BYTES_ZLIB_READER_H_
