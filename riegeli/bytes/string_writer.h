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

#ifndef RIEGELI_BYTES_STRING_WRITER_H_
#define RIEGELI_BYTES_STRING_WRITER_H_

#include <stddef.h>
#include <string>
#include <utility>

#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// A Writer which appends to a string, resizing it as necessary.
class StringWriter : public Writer {
 public:
  class Options {
   public:
    Options() noexcept {}

    // Announce in advance the destination size. This may improve performance
    // and reduce memory usage.
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
    friend class StringWriter;

    Position size_hint_ = 0;
  };

  // Creates a closed StringWriter.
  StringWriter() noexcept : Writer(State::kClosed) {}

  // Will write to a string which is owned by this StringWriter, available as
  // dest().
  explicit StringWriter(OwnsDest, Options options = Options());

  // Will write to the string which is not owned by this StringWriter and must
  // be kept alive but not accessed until closing the StringWriter, except that
  // it is allowed to read it directly after Flush().
  explicit StringWriter(std::string* dest, Options options = Options());

  StringWriter(StringWriter&& src) noexcept;
  StringWriter& operator=(StringWriter&& src) noexcept;

  // Returns the string being written to. Unchanged by Close().
  std::string& dest() const { return *dest_; }

  bool Flush(FlushType flush_type) override;
  bool SupportsTruncate() const override { return true; }
  bool Truncate(Position new_size) override;

 protected:
  void Done() override;
  bool PushSlow() override;
  bool WriteSlow(std::string&& src) override;
  bool WriteSlow(absl::string_view src) override;
  bool WriteSlow(const Chain& src) override;

 private:
  // Discards uninitialized space from the end of *dest_, so that it contains
  // only actual data written. Invalidates buffer pointers.
  void DiscardBuffer();

  // Appends some uninitialized space to *dest_ if this can be done without
  // reallocation. Sets buffer pointers to the uninitialized space.
  void MakeBuffer(size_t cursor_pos);
  void MakeBuffer() { return MakeBuffer(dest_->size()); }

  std::string owned_dest_;
  // The string being written to, with uninitialized space appended (possibly
  // empty); cursor_ points to the uninitialized space.
  //
  // Invariant: dest_ != nullptr
  std::string* dest_ = &owned_dest_;

  // Invariants if healthy():
  //   start_ == &(*dest_)[0]
  //   buffer_size() == dest_->size()
  //   start_pos_ == 0
};

// Implementation details follow.

inline StringWriter::StringWriter(OwnsDest, Options options)
    : StringWriter(&owned_dest_, options) {}

inline StringWriter::StringWriter(std::string* dest, Options options)
    : Writer(State::kOpen), dest_(RIEGELI_ASSERT_NOTNULL(dest)) {
  const size_t size_hint = UnsignedMin(options.size_hint_, dest->max_size());
  if (dest->capacity() < size_hint) dest_->reserve(size_hint);
  start_ = &(*dest_)[0];
  cursor_ = start_ + dest_->size();
  limit_ = cursor_;
}

inline StringWriter::StringWriter(StringWriter&& src) noexcept
    : Writer(std::move(src)),
      owned_dest_(riegeli::exchange(src.owned_dest_, std::string())),
      dest_(src.dest_ == &src.owned_dest_
                ? &owned_dest_
                : riegeli::exchange(src.dest_, &src.owned_dest_)) {
  if (dest_ == &owned_dest_ && ABSL_PREDICT_TRUE(start_ != nullptr)) {
    // *dest_ was moved, which invalidated buffer pointers.
    const size_t cursor_index = written_to_buffer();
    const size_t limit_index = buffer_size();
    start_ = &owned_dest_[0];
    cursor_ = start_ + cursor_index;
    limit_ = start_ + limit_index;
  }
}

inline StringWriter& StringWriter::operator=(StringWriter&& src) noexcept {
  Writer::operator=(std::move(src));
  owned_dest_ = riegeli::exchange(src.owned_dest_, std::string());
  dest_ = src.dest_ == &src.owned_dest_
              ? &owned_dest_
              : riegeli::exchange(src.dest_, &src.owned_dest_);
  if (dest_ == &owned_dest_ && ABSL_PREDICT_TRUE(start_ != nullptr)) {
    // *dest_ was moved, which invalidated buffer pointers.
    const size_t cursor_index = written_to_buffer();
    const size_t limit_index = buffer_size();
    start_ = &owned_dest_[0];
    cursor_ = start_ + cursor_index;
    limit_ = start_ + limit_index;
  }
  return *this;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_STRING_WRITER_H_
