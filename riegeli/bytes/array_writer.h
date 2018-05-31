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

#ifndef RIEGELI_BYTES_ARRAY_WRITER_H_
#define RIEGELI_BYTES_ARRAY_WRITER_H_

#include <stddef.h>
#include <memory>
#include <utility>

#include "absl/types/span.h"
#include "riegeli/base/base.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// A Writer which writes to an array with a known size limit.
class ArrayWriter : public Writer {
 public:
  // Creates a closed ArrayWriter.
  ArrayWriter() noexcept : Writer(State::kClosed) {}

  // Will write to an array which is owned by this ArrayWriter, available as
  // dest().
  ArrayWriter(OwnsDest, size_t max_size);

  // Will write to the array which is not owned by this ArrayWriter and must be
  // kept alive but not accessed until closing the ArrayWriter, except that it
  // is allowed to read it directly after Flush().
  ArrayWriter(char* dest, size_t max_size);

  ArrayWriter(ArrayWriter&& src) noexcept;
  ArrayWriter& operator=(ArrayWriter&& src) noexcept;

  // Returns written data in a prefix of the original array. Valid only after
  // Close() or Flush().
  absl::Span<char> dest() const { return dest_; }

  bool Flush(FlushType flush_type) override;
  bool SupportsTruncate() const override { return true; }
  bool Truncate(Position new_size) override;

 protected:
  void Done() override;
  bool PushSlow() override;

 private:
  std::unique_ptr<char[]> owned_dest_;
  // Written data. Valid only after Close() or Flush().
  absl::Span<char> dest_;

  // Invariant: if healthy() then start_pos_ == 0
};

// Implementation details follow.

inline ArrayWriter::ArrayWriter(OwnsDest, size_t max_size)
    : Writer(State::kOpen), owned_dest_(new char[max_size]) {
  start_ = owned_dest_.get();
  cursor_ = start_;
  limit_ = start_ + max_size;
}

inline ArrayWriter::ArrayWriter(char* dest, size_t max_size)
    : Writer(State::kOpen) {
  start_ = dest;
  cursor_ = dest;
  limit_ = dest + max_size;
}

inline ArrayWriter::ArrayWriter(ArrayWriter&& src) noexcept
    : Writer(std::move(src)),
      owned_dest_(std::move(src.owned_dest_)),
      dest_(riegeli::exchange(src.dest_, absl::Span<char>())) {}

inline ArrayWriter& ArrayWriter::operator=(ArrayWriter&& src) noexcept {
  Writer::operator=(std::move(src));
  owned_dest_ = std::move(src.owned_dest_);
  dest_ = riegeli::exchange(src.dest_, absl::Span<char>());
  return *this;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_ARRAY_WRITER_H_
