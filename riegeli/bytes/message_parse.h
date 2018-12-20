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

#ifndef RIEGELI_BYTES_MESSAGE_PARSE_H_
#define RIEGELI_BYTES_MESSAGE_PARSE_H_

#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

// Reads a message in binary format from the given Reader. If successful, the
// entire input will be consumed. Fails if some required fields are missing.
//
// The Src template parameter specifies the type of the object providing and
// possibly owning the Reader. Src must support Dependency<Reader*, Src>,
// e.g. Reader* (not owned, default), unique_ptr<Reader> (owned),
// ChainReader<> (owned).
//
// Return values:
//  * true  - success (*dest is filled)
//  * false - failure (*dest is unspecified, *error_message is set)
template <typename Src = Reader*>
bool ParseFromReader(google::protobuf::MessageLite* dest, Src src,
                     std::string* error_message = nullptr);

// Reads a message in binary format from the given Chain. If successful, the
// entire input will be consumed. Fails if some required fields are missing.
//
// Return values:
//  * true  - success (*dest is filled)
//  * false - failure (*dest is unspecified, *error_message is set)
bool ParseFromChain(google::protobuf::MessageLite* dest, const Chain& src,
                    std::string* error_message = nullptr);

// Implementation details follow.

namespace internal {

bool ParseFromReaderImpl(google::protobuf::MessageLite* dest, Reader* src,
                         std::string* error_message);

}  // namespace internal

template <typename Src>
inline bool ParseFromReader(google::protobuf::MessageLite* dest, Src src,
                            std::string* error_message) {
  Dependency<Reader*, Src> src_dep(std::move(src));
  if (ABSL_PREDICT_FALSE(
          !internal::ParseFromReaderImpl(dest, src_dep.ptr(), error_message))) {
    return false;
  }
  if (src_dep.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_dep->Close())) {
      if (error_message != nullptr)
        *error_message = std::string(src_dep->message());
      return false;
    }
  }
  return true;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_MESSAGE_PARSE_H_
