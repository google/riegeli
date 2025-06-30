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

#ifndef RIEGELI_RECORDS_SKIPPED_REGION_H_
#define RIEGELI_RECORDS_SKIPPED_REGION_H_

#include <iosfwd>
#include <string>
#include <utility>

#include "absl/strings/string_view.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/string_ref.h"
#include "riegeli/base/types.h"

namespace riegeli {

// Details about a skipped region of invalid file contents.
class SkippedRegion {
 public:
  SkippedRegion() = default;

  // Creates a `SkippedRegion` with the given region location and message
  // explaining why the region is invalid.
  explicit SkippedRegion(Position begin, Position end,
                         StringInitializer message);

  SkippedRegion(const SkippedRegion& that) = default;
  SkippedRegion& operator=(const SkippedRegion& that) = default;

  SkippedRegion(SkippedRegion&&) = default;
  SkippedRegion& operator=(SkippedRegion&&) = default;

  // File position of the beginning of the skipped region, inclusive.
  Position begin() const { return begin_; }
  // File position of the end of the skipped region, exclusive.
  Position end() const { return end_; }

  // Length of the skipped region, in bytes.
  Position length() const { return end_ - begin_; }

  // Message explaining why the region is invalid.
  absl::string_view message() const { return message_; }

  // Formats `SkippedRegion` as string: "[<begin>..<end>): <message>".
  std::string ToString() const;

  // Default stringification by `absl::StrCat()` etc.
  //
  // Writes `src.ToString()` to `dest`.
  template <typename Sink>
  friend void AbslStringify(Sink& dest, const SkippedRegion& src) {
    dest.Append(src.ToString());
  }

  // Writes `src.ToString()` to `out`.
  friend std::ostream& operator<<(std::ostream& dest,
                                  const SkippedRegion& src) {
    src.Output(dest);
    return dest;
  }

 private:
  void Output(std::ostream& dest) const;

  Position begin_ = 0;
  Position end_ = 0;
  std::string message_;
};

// Implementation details follow.

inline SkippedRegion::SkippedRegion(Position begin, Position end,
                                    StringInitializer message)
    : begin_(begin), end_(end), message_(std::move(message)) {
  RIEGELI_ASSERT_LE(begin, end)
      << "Failed precondition of SkippedRegion::SkippedRegion: "
         "positions in the wrong order";
}

}  // namespace riegeli

#endif  // RIEGELI_RECORDS_SKIPPED_REGION_H_
