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

#include <utility>

#include "absl/base/optimization.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/status.h"
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
// Returns status:
//  * status.ok()  - success (dest is written to)
//  * !status.ok() - failure (dest is unspecified)
template <typename Dest = Writer*>
Status SerializeToWriter(const google::protobuf::MessageLite& src, Dest dest);

// Writes the message in binary format to the given Chain, clearing it first.
// Fails if some required fields are missing.
//
// Returns status:
//  * status.ok()  - success (*dest is filled)
//  * !status.ok() - failure (*dest is unspecified)
Status SerializeToChain(const google::protobuf::MessageLite& src, Chain* dest);

// Implementation details follow.

namespace internal {

Status SerializeToWriterImpl(const google::protobuf::MessageLite& src,
                             Writer* dest);

}  // namespace internal

template <typename Dest>
inline Status SerializeToWriter(const google::protobuf::MessageLite& src,
                                Dest dest) {
  Dependency<Writer*, Dest> dest_dep(std::move(dest));
  const Status status = internal::SerializeToWriterImpl(src, dest_dep.ptr());
  if (ABSL_PREDICT_TRUE(status.ok()) && dest_dep.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_dep->Close())) return dest_dep->status();
  }
  return status;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_MESSAGE_SERIALIZE_H_
