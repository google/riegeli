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

#ifndef RIEGELI_BYTES_READER_UTILS_H_
#define RIEGELI_BYTES_READER_UTILS_H_

#include <stddef.h>

#include <limits>
#include <string>

#include "absl/base/attributes.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

ABSL_DEPRECATED("Use Reader::ReadAll() instead")
inline bool ReadAll(Reader& src, absl::string_view& dest,
                    size_t max_size = std::numeric_limits<size_t>::max()) {
  return src.ReadAll(dest, max_size);
}

ABSL_DEPRECATED("Use Reader::ReadAll() instead")
inline bool ReadAll(Reader& src, std::string& dest,
                    size_t max_size = std::numeric_limits<size_t>::max()) {
  return src.ReadAll(dest, max_size);
}

ABSL_DEPRECATED("Use Reader::ReadAll() instead")
inline bool ReadAll(Reader& src, Chain& dest,
                    size_t max_size = std::numeric_limits<size_t>::max()) {
  return src.ReadAll(dest, max_size);
}

ABSL_DEPRECATED("Use Reader::ReadAll() instead")
inline bool ReadAll(Reader& src, absl::Cord& dest,
                    size_t max_size = std::numeric_limits<size_t>::max()) {
  return src.ReadAll(dest, max_size);
}

ABSL_DEPRECATED("Use Reader::ReadAndAppendAll() instead")
inline bool ReadAndAppendAll(
    Reader& src, std::string& dest,
    size_t max_size = std::numeric_limits<size_t>::max()) {
  return src.ReadAndAppendAll(dest, max_size);
}

ABSL_DEPRECATED("Use Reader::ReadAndAppendAll() instead")
inline bool ReadAndAppendAll(
    Reader& src, Chain& dest,
    size_t max_size = std::numeric_limits<size_t>::max()) {
  return src.ReadAndAppendAll(dest, max_size);
}

ABSL_DEPRECATED("Use Reader::ReadAndAppendAll() instead")
inline bool ReadAndAppendAll(
    Reader& src, absl::Cord& dest,
    size_t max_size = std::numeric_limits<size_t>::max()) {
  return src.ReadAndAppendAll(dest, max_size);
}

ABSL_DEPRECATED("Use Reader::CopyAll() instead")
inline bool CopyAll(Reader& src, Writer& dest,
                    Position max_size = std::numeric_limits<Position>::max()) {
  return src.CopyAll(dest, max_size);
}

ABSL_DEPRECATED("Use Reader::CopyAll() instead")
inline bool CopyAll(Reader& src, BackwardWriter& dest,
                    size_t max_size = std::numeric_limits<size_t>::max()) {
  return src.CopyAll(dest, max_size);
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_READER_UTILS_H_
