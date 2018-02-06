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

#ifndef RIEGELI_BYTES_CHAIN_WRITER_H_
#define RIEGELI_BYTES_CHAIN_WRITER_H_

#include <stddef.h>
#include <string>
#include <utility>

#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/string_view.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// A Writer which appends to a Chain.
class ChainWriter final : public Writer {
 public:
  class Options {
   public:
    // Not defaulted because of a C++ defect:
    // https://stackoverflow.com/questions/17430377
    constexpr Options() noexcept {}

    // Announce in advance the destination size. This may reduce Chain memory
    // usage.
    //
    // If the size hint turns out to not match reality, nothing breaks.
    Options& set_size_hint(Position size_hint) & {
      size_hint_ = size_hint;
      return *this;
    }
    Options&& set_size_hint(Position size_hint) && {
      return std::move(set_size_hint(size_hint));
    }

   private:
    friend class ChainWriter;

    Position size_hint_ = 0;
  };

  // Creates a closed ChainWriter.
  ChainWriter() noexcept;

  // Will write to the Chain which is not owned by this ChainWriter and must be
  // kept alive but not accessed until closing the ChainWriter, except that it
  // is allowed to read it directly after Flush().
  explicit ChainWriter(Chain* dest, Options options = Options());

  ChainWriter(ChainWriter&& src) noexcept;
  ChainWriter& operator=(ChainWriter&& src) noexcept;

  ~ChainWriter();

  bool Flush(FlushType flush_type) override;

 protected:
  void Done() override;
  bool PushSlow() override;
  bool WriteSlow(string_view src) override;
  bool WriteSlow(std::string&& src) override;
  bool WriteSlow(const Chain& src) override;
  bool WriteSlow(Chain&& src) override;

 private:
  // Discards uninitialized space from the end of *dest_, so that it contains
  // only actual data written. Invalidates buffer pointers and start_pos_.
  void DiscardBuffer();

  // Appends some uninitialized space to *dest_ if this can be done without
  // allocation. Sets buffer pointers to the uninitialized space and restores
  // start_pos_.
  void MakeBuffer();

  // If healthy(), the Chain being written to, with uninitialized space appended
  // (possibly empty); cursor_ points to the uninitialized space, except that it
  // can be nullptr if the uninitialized space is empty.
  //
  // Invariant: if healthy() then dest_ != nullptr
  Chain* dest_ = nullptr;
  size_t size_hint_ = 0;

  // Invariants if healthy():
  //   limit_ == nullptr || limit_ == dest_->blocks().back().data() +
  //                                  dest_->blocks().back().size()
  //   limit_pos() == dest_->size()
};

}  // namespace riegeli

#endif  // RIEGELI_BYTES_CHAIN_WRITER_H_
