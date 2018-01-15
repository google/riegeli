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

#include <string>
#include <utility>

#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/string_view.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// StringWriter::Options.
class StringWriterOptions {
 public:
  // Announce in advance the destination size. This may improve performance and
  // reduce memory usage.
  //
  // If the size hint turns out to not match reality, nothing breaks.
  StringWriterOptions& set_size_hint(Position size_hint) & {
    size_hint_ = size_hint;
    return *this;
  }
  StringWriterOptions&& set_size_hint(Position size_hint) && {
    return std::move(set_size_hint(size_hint));
  }

 private:
  friend class StringWriter;

  Position size_hint_ = 0;
};

// A Writer which appends to a string, resizing it as necessary.
//
// Functions of this Writer fail only when it is closed.
class StringWriter final : public Writer {
 public:
  using Options = StringWriterOptions;

  // Creates a cancelled StringWriter.
  StringWriter();

  // Will write to the string which is not owned by this StringWriter and must
  // be kept alive but not accessed until closing the StringWriter, except that
  // it is allowed to read it directly after Flush().
  explicit StringWriter(std::string* dest, Options options = Options());

  StringWriter(StringWriter&& src) noexcept;
  StringWriter& operator=(StringWriter&& src) noexcept;

  ~StringWriter();

  bool Flush(FlushType flush_type) override;

 protected:
  void Done() override;
  bool PushSlow() override;
  bool WriteSlow(string_view src) override;
  bool WriteSlow(const Chain& src) override;

 private:
  // Discards uninitialized space from the end of *dest_, so that it contains
  // only actual data written. Invalidates buffer pointers.
  void DiscardBuffer();

  // Appends some uninitialized space to *dest_ if this can be done without
  // reallocation. Sets buffer pointers to the uninitialized space.
  void MakeBuffer();

  // If healthy(), the string being written to, with uninitialized space
  // appended (possibly empty); cursor_ points to the uninitialized space.
  //
  // Invariant: if healthy() then dest_ != nullptr
  std::string* dest_;

  // Invariants if healthy():
  //   start_ == &(*dest_)[0]
  //   limit_ == &(*dest_)[dest_->size()]
  //   start_pos_ == 0
};

}  // namespace riegeli

#endif  // RIEGELI_BYTES_STRING_WRITER_H_
