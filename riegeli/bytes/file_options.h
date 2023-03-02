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

#ifndef RIEGELI_BYTES_FILE_OPTIONS_H_
#define RIEGELI_BYTES_FILE_OPTIONS_H_

#include <memory>
#include <utility>

#include "file/base/options.h"
#include "file/base/options.pb.h"

namespace riegeli {
namespace file_internal {

// Storage for `file::Options` optimized for `file::Defaults()`.
class FileOptions {
 public:
  FileOptions() = default;

  FileOptions(const FileOptions& that);
  FileOptions& operator=(const FileOptions& that);

  FileOptions(FileOptions&& that) = default;
  FileOptions& operator=(FileOptions&& that) = default;

  bool defaults() const { return file_options_ == nullptr; }

  void set(const file::Options& file_options) {
    if (&file_options == &file::Defaults()) {
      file_options_ = nullptr;
    } else {
      file_options_ = std::make_unique<file::Options>(file_options);
    }
  }
  void set(file::Options&& file_options) {
    file_options_ = std::make_unique<file::Options>(std::move(file_options));
  }

  const file::Options& get() const {
    if (file_options_ == nullptr) return file::Defaults();
    return *file_options_;
  }

  file::Options& mutable_get() {
    if (file_options_ == nullptr) {
      file_options_ = std::make_unique<file::Options>();
    }
    return *file_options_;
  }

 private:
  // `nullptr` means `file::Defaults()`.
  std::unique_ptr<file::Options> file_options_;
};

}  // namespace file_internal
}  // namespace riegeli

#endif  // RIEGELI_BYTES_FILE_OPTIONS_H_
