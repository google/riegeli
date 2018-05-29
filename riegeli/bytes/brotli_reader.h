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

#include <memory>
#include <utility>

#include "brotli/decode.h"
#include "riegeli/base/base.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

// A Reader which decompresses data with Brotli after getting it from another
// Reader.
class BrotliReader final : public Reader {
 public:
  class Options {};

  // Creates a closed BrotliReader.
  BrotliReader() noexcept : Reader(State::kClosed) {}

  // Will read Brotli-compressed stream from the Reader which is owned by this
  // BrotliReader and will be closed and deleted when the BrotliReader is
  // closed.
  explicit BrotliReader(std::unique_ptr<Reader> src,
                        Options options = Options());

  // Will read Brotli-compressed stream from the Reader which is not owned by
  // this BrotliReader and must be kept alive but not accessed until closing the
  // BrotliReader.
  explicit BrotliReader(Reader* src, Options options = Options());

  BrotliReader(BrotliReader&& src) noexcept;
  BrotliReader& operator=(BrotliReader&& src) noexcept;

  // Returns the Reader the compressed stream is being read from. Unchanged by
  // Close().
  Reader* src() const { return src_; }

 protected:
  void Done() override;
  bool PullSlow() override;

 private:
  struct BrotliDecoderStateDeleter {
    void operator()(BrotliDecoderState* ptr) const {
      BrotliDecoderDestroyInstance(ptr);
    }
  };

  std::unique_ptr<Reader> owned_src_;
  // Invariant: if healthy() then src_ != nullptr
  Reader* src_ = nullptr;
  // If healthy() but decompressor_ == nullptr then all data have been
  // decompressed.
  std::unique_ptr<BrotliDecoderState, BrotliDecoderStateDeleter> decompressor_;

  // Invariant:
  //   cursor_ and limit_ point inside the buffer returned by
  //   BrotliDecoderTakeOutput() or are both nullptr
};

// Implementation details follow.

inline BrotliReader::BrotliReader(std::unique_ptr<Reader> src, Options options)
    : BrotliReader(src.get(), options) {
  owned_src_ = std::move(src);
}

inline BrotliReader::BrotliReader(BrotliReader&& src) noexcept
    : Reader(std::move(src)),
      owned_src_(std::move(src.owned_src_)),
      src_(riegeli::exchange(src.src_, nullptr)),
      decompressor_(std::move(src.decompressor_)) {}

inline BrotliReader& BrotliReader::operator=(BrotliReader&& src) noexcept {
  Reader::operator=(std::move(src));
  owned_src_ = std::move(src.owned_src_);
  src_ = riegeli::exchange(src.src_, nullptr);
  decompressor_ = std::move(src.decompressor_);
  return *this;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_BROTLI_READER_H_
