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

#ifndef RIEGELI_BYTES_MESSAGE_SERIALIZE_H_
#define RIEGELI_BYTES_MESSAGE_SERIALIZE_H_

#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Writes the message in binary format to the given Writer. Fails if some
// required fields are missing.
//
// The Dest template parameter specifies the type of the object providing and
// possibly owning the Writer. Dest must support Dependency<Writer*, Dest>,
// e.g. Writer* (not owned, default), unique_ptr<Writer> (owned),
// ChainWriter<> (owned).
//
// Return values:
//  * true  - success
//  * false - failure (*error_message is set)
template <typename Dest = Writer*>
bool SerializeToWriter(const google::protobuf::MessageLite& src, Dest dest,
                       std::string* error_message = nullptr);

// Writes the message in binary format to the given Chain, clearing it first.
// Fails if some required fields are missing.
//
// Return values:
//  * true  - success
//  * false - failure (*error_message is set)
bool SerializeToChain(const google::protobuf::MessageLite& src, Chain* dest,
                      std::string* error_message = nullptr);

// Implementation details follow.

namespace internal {

bool SerializeToWriterImpl(const google::protobuf::MessageLite& src,
                           Writer* dest, std::string* error_message);

}  // namespace internal

template <typename Dest>
inline bool SerializeToWriter(const google::protobuf::MessageLite& src,
                              Dest dest, std::string* error_message) {
  Dependency<Writer*, Dest> dest_dep(std::move(dest));
  if (ABSL_PREDICT_FALSE(!internal::SerializeToWriterImpl(src, dest_dep.ptr(),
                                                          error_message))) {
    return false;
  }
  if (dest_dep.kIsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_dep->Close())) {
      if (error_message != nullptr)
        *error_message = std::string(dest_dep->message());
      return false;
    }
  }
  return true;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_MESSAGE_SERIALIZE_H_
