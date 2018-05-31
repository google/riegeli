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

#ifndef RIEGELI_BYTES_ARRAY_BACKWARD_WRITER_H_
#define RIEGELI_BYTES_ARRAY_BACKWARD_WRITER_H_

#include <stddef.h>
#include <memory>
#include <utility>

#include "absl/types/span.h"
#include "riegeli/base/base.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/backward_writer.h"

namespace riegeli {

// A BackwardWriter which writes to an array with a known size limit.
class ArrayBackwardWriter : public BackwardWriter {
 public:
  // Creates a closed ArrayBackwardWriter.
  ArrayBackwardWriter() noexcept : BackwardWriter(State::kClosed) {}

  // Will write to an array which is owned by this ArrayBackwardWriter,
  // available as dest().
  ArrayBackwardWriter(OwnsDest, size_t max_size);

  // Will write to the array which is not owned by this ArrayBackwardWriter and
  // must be kept alive but not accessed until closing the ArrayBackwardWriter.
  // dest points to the beginning of the array.
  ArrayBackwardWriter(char* dest, size_t max_size);

  ArrayBackwardWriter(ArrayBackwardWriter&& src) noexcept;
  ArrayBackwardWriter& operator=(ArrayBackwardWriter&& src) noexcept;

  // Returns written data in a prefix of the original array. Valid only after
  // Close().
  absl::Span<char> dest() const { return dest_; }

  bool SupportsTruncate() const override { return true; }
  bool Truncate(Position new_size) override;

 protected:
  void Done() override;
  bool PushSlow() override;

 private:
  std::unique_ptr<char[]> owned_dest_;
  // Written data. Valid only after Close().
  absl::Span<char> dest_;

  // Invariant: if healthy() then start_pos_ == 0
};

// Implementation details follow.

inline ArrayBackwardWriter::ArrayBackwardWriter(OwnsDest, size_t max_size)
    : BackwardWriter(State::kOpen), owned_dest_(new char[max_size]) {
  limit_ = owned_dest_.get();
  start_ = limit_ + max_size;
  cursor_ = start_;
}

inline ArrayBackwardWriter::ArrayBackwardWriter(char* dest, size_t max_size)
    : BackwardWriter(State::kOpen) {
  limit_ = dest;
  start_ = limit_ + max_size;
  cursor_ = start_;
}

inline ArrayBackwardWriter::ArrayBackwardWriter(
    ArrayBackwardWriter&& src) noexcept
    : BackwardWriter(std::move(src)),
      owned_dest_(std::move(src.owned_dest_)),
      dest_(riegeli::exchange(src.dest_, absl::Span<char>())) {}

inline ArrayBackwardWriter& ArrayBackwardWriter::operator=(
    ArrayBackwardWriter&& src) noexcept {
  BackwardWriter::operator=(std::move(src));
  owned_dest_ = std::move(src.owned_dest_);
  dest_ = riegeli::exchange(src.dest_, absl::Span<char>());
  return *this;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_ARRAY_BACKWARD_WRITER_H_
