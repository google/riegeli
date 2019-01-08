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

#include "absl/utility/utility.h"
#include "riegeli/base/base.h"

namespace riegeli {

// Details about a skipped region of invalid file contents.
class SkippedRegion {
 public:
  SkippedRegion() noexcept {}

  // Creates a SkippedRegion with the given region location and message
  // explaining why the region is invalid.
  explicit SkippedRegion(Position begin, Position end, std::string message);

  SkippedRegion(const SkippedRegion& that);
  SkippedRegion& operator=(const SkippedRegion& that);

  SkippedRegion(SkippedRegion&&) noexcept;
  SkippedRegion& operator=(SkippedRegion&&) noexcept;

  // File position of the beginning of the skipped region, inclusive.
  Position begin() const { return begin_; }
  // File position of the end of the skipped region, exclusive.
  Position end() const { return end_; }

  // Length of the skipped region, in bytes.
  Position length() const { return end_ - begin_; }

  // Message explaining why the region is invalid.
  const std::string& message() const { return message_; }

  // Formats SkippedRegion as string: "[<begin>, <end>): <message>".
  std::string ToString() const;

  // Same as: out << skipped_region.ToString()
  friend std::ostream& operator<<(std::ostream& out,
                                  const SkippedRegion& skipped_region);

 private:
  Position begin_ = 0;
  Position end_ = 0;
  std::string message_;
};

// Implementation details follow.

inline SkippedRegion::SkippedRegion(Position begin, Position end,
                                    std::string message)
    : begin_(begin), end_(end), message_(std::move(message)) {
  RIEGELI_ASSERT_LE(begin, end)
      << "Failed precondition of SkippedRegion::SkippedRegion: "
         "positions in the wrong order";
}

inline SkippedRegion::SkippedRegion(const SkippedRegion& that)
    : begin_(that.begin_), end_(that.end_), message_(that.message_) {}

inline SkippedRegion& SkippedRegion::operator=(const SkippedRegion& that) {
  begin_ = that.begin_;
  end_ = that.end_;
  message_ = that.message_;
  return *this;
}

inline SkippedRegion::SkippedRegion(SkippedRegion&& that) noexcept
    : begin_(absl::exchange(that.begin_, 0)),
      end_(absl::exchange(that.end_, 0)),
      message_(absl::exchange(that.message_, std::string())) {}

inline SkippedRegion& SkippedRegion::operator=(SkippedRegion&& that) noexcept {
  begin_ = absl::exchange(that.begin_, 0);
  end_ = absl::exchange(that.end_, 0);
  message_ = absl::exchange(that.message_, std::string());
  return *this;
}

}  // namespace riegeli

#endif  // RIEGELI_RECORDS_SKIPPED_REGION_H_
