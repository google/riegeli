// Copyright 2023 Google LLC
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

#include "riegeli/bytes/file_options.h"

#include <memory>

#include "file/base/options.pb.h"

namespace riegeli {
namespace file_internal {

FileOptions::FileOptions(const FileOptions& that)
    : file_options_(
          that.file_options_ == nullptr
              ? nullptr
              : std::make_unique<file::Options>(*that.file_options_)) {}

FileOptions& FileOptions::operator=(const FileOptions& that) {
  file_options_ = that.file_options_ == nullptr
                      ? nullptr
                      : std::make_unique<file::Options>(*that.file_options_);
  return *this;
}

}  // namespace file_internal
}  // namespace riegeli
