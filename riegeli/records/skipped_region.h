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

#include "riegeli/base/base.h"

namespace riegeli {

class SkippedRegion {
 public:
  constexpr SkippedRegion() noexcept {}

  explicit SkippedRegion(Position begin, Position end);

  SkippedRegion(const SkippedRegion& that) noexcept;
  SkippedRegion& operator=(const SkippedRegion& that) noexcept;

  Position begin() const { return begin_; }
  Position end() const { return end_; }

  Position length() const { return end_ - begin_; }

  std::string ToString() const;

 private:
  Position begin_ = 0;
  Position end_ = 0;
};

bool operator==(SkippedRegion a, SkippedRegion b);
bool operator!=(SkippedRegion a, SkippedRegion b);

std::ostream& operator<<(std::ostream& out, SkippedRegion skipped_region);

// Implementation details follow.

inline SkippedRegion::SkippedRegion(Position begin, Position end)
    : begin_(begin), end_(end) {
  RIEGELI_ASSERT_LE(begin, end)
      << "Failed precondition of SkippedRegion::SkippedRegion: "
         "positions in the wrong order";
}

inline SkippedRegion::SkippedRegion(const SkippedRegion& that) noexcept
    : begin_(that.begin_), end_(that.end_) {}

inline SkippedRegion& SkippedRegion::operator=(
    const SkippedRegion& that) noexcept {
  begin_ = that.begin_;
  end_ = that.end_;
  return *this;
}

inline bool operator==(SkippedRegion a, SkippedRegion b) {
  return a.begin() == b.begin() && a.end() == b.end();
}

inline bool operator!=(SkippedRegion a, SkippedRegion b) {
  return a.begin() != b.begin() || a.end() != b.end();
}

}  // namespace riegeli

#endif  // RIEGELI_RECORDS_SKIPPED_REGION_H_
