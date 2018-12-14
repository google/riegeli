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
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Template parameter invariant part of StringWriter.
class StringWriterBase : public Writer {
 public:
  class Options {
   public:
    Options() noexcept {}

    // Announces in advance the destination size. This may improve performance
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
    template <typename Dest>
    friend class StringWriter;

    Position size_hint_ = 0;
  };

  // Returns the string being written to. Unchanged by Close().
  virtual std::string* dest_string() = 0;
  virtual const std::string* dest_string() const = 0;

  bool Flush(FlushType flush_type) override;
  bool SupportsTruncate() const override { return true; }
  bool Truncate(Position new_size) override;

 protected:
  explicit StringWriterBase(State state) noexcept : Writer(state) {}

  StringWriterBase(StringWriterBase&& that) noexcept;
  StringWriterBase& operator=(StringWriterBase&& that) noexcept;

  void Done() override;
  bool PushSlow() override;
  using Writer::WriteSlow;
  bool WriteSlow(absl::string_view src) override;
  bool WriteSlow(std::string&& src) override;
  bool WriteSlow(const Chain& src) override;

 private:
  // Discards uninitialized space from the end of *dest, so that it contains
  // only actual data written.
  void SyncBuffer(std::string* dest);

  // Appends some uninitialized space to *dest if this can be done without
  // reallocation.
  void MakeBuffer(std::string* dest);

  // Invariants if healthy():
  //   start_ == &(*dest_string())[0]
  //   buffer_size() == dest_string()->size()
  //   start_pos_ == 0
};

// A Writer which appends to a string, resizing it as necessary.
//
// The Dest template parameter specifies the type of the object providing and
// possibly owning the string being written to. Dest must support
// Dependency<string*, Src>, e.g. string* (not owned, default), string (owned).
//
// The string must not be accessed until the StringWriter is closed or no longer
// used, except that it is allowed to read the string immediately after Flush().
template <typename Dest = std::string*>
class StringWriter : public StringWriterBase {
 public:
  // Creates a closed StringWriter.
  StringWriter() noexcept : StringWriterBase(State::kClosed) {}

  // Will append to the string provided by dest.
  explicit StringWriter(Dest dest, Options options = Options());

  StringWriter(StringWriter&& that) noexcept;
  StringWriter& operator=(StringWriter&& that) noexcept;

  // Returns the object providing and possibly owning the string being written
  // to. Unchanged by Close().
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  std::string* dest_string() override { return dest_.ptr(); }
  const std::string* dest_string() const override { return dest_.ptr(); }

 private:
  void MoveDest(StringWriter&& that);

  // The object providing and possibly owning the string being written to, with
  // uninitialized space appended (possibly empty); cursor_ points to the
  // uninitialized space.
  Dependency<std::string*, Dest> dest_;
};

// Implementation details follow.

inline StringWriterBase::StringWriterBase(StringWriterBase&& that) noexcept
    : Writer(std::move(that)) {}

inline StringWriterBase& StringWriterBase::operator=(
    StringWriterBase&& that) noexcept {
  Writer::operator=(std::move(that));
  return *this;
}

template <typename Dest>
inline StringWriter<Dest>::StringWriter(Dest dest, Options options)
    : StringWriterBase(State::kOpen), dest_(std::move(dest)) {
  RIEGELI_ASSERT(dest_.ptr() != nullptr)
      << "Failed precondition of StringWriter<Dest>::StringWriter(Dest): "
         "null string pointer";
  const size_t size_hint = UnsignedMin(options.size_hint_, dest_->max_size());
  if (dest_->capacity() < size_hint) dest_->reserve(size_hint);
  start_ = &(*dest_)[0];
  cursor_ = start_ + dest_->size();
  limit_ = cursor_;
}

template <typename Dest>
inline StringWriter<Dest>::StringWriter(StringWriter&& that) noexcept
    : StringWriterBase(std::move(that)) {
  MoveDest(std::move(that));
}

template <typename Dest>
inline StringWriter<Dest>& StringWriter<Dest>::operator=(
    StringWriter&& that) noexcept {
  StringWriterBase::operator=(std::move(that));
  MoveDest(std::move(that));
  return *this;
}

template <typename Dest>
inline void StringWriter<Dest>::MoveDest(StringWriter&& that) {
  if (dest_.kIsStable()) {
    dest_ = std::move(that.dest_);
  } else {
    const size_t cursor_index = written_to_buffer();
    dest_ = std::move(that.dest_);
    if (start_ != nullptr) {
      start_ = &(*dest_)[0];
      cursor_ = start_ + cursor_index;
      limit_ = start_ + dest_->size();
    }
  }
}

extern template class StringWriter<std::string*>;
extern template class StringWriter<std::string>;

}  // namespace riegeli

#endif  // RIEGELI_BYTES_STRING_WRITER_H_
