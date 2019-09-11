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

#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/optimization.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Writes the message in binary format to the given Writer.
//
// SerializePartialToWriter() allows missing required fields.
//
// The Dest template parameter specifies the type of the object providing and
// possibly owning the Writer. Dest must support Dependency<Writer*, Dest>,
// e.g. Writer* (not owned, default), unique_ptr<Writer> (owned),
// ChainWriter<> (owned).
//
// With a dest_args parameter, writes to a Dest constructed from elements of
// dest_args. This avoids constructing a temporary Dest and moving from it.
//
// Returns status:
//  * status.ok()  - success (dest is written to)
//  * !status.ok() - failure (dest is unspecified)
template <typename Dest>
Status SerializeToWriter(const google::protobuf::MessageLite& src, Dest&& dest);
template <typename Dest, typename... DestArgs>
Status SerializeToWriter(const google::protobuf::MessageLite& src,
                         std::tuple<DestArgs...> dest_args);
template <typename Dest>
Status SerializeToPartialWriter(const google::protobuf::MessageLite& src,
                                Dest&& dest);
template <typename Dest, typename... DestArgs>
Status SerializeToPartialWriter(const google::protobuf::MessageLite& src,
                                std::tuple<DestArgs...> dest_args);

// Writes the message in binary format to the given Chain, clearing it first.
//
// SerializePartialToChain() allows missing required fields.
//
// Returns status:
//  * status.ok()  - success (*dest is filled)
//  * !status.ok() - failure (*dest is unspecified)
Status SerializeToChain(const google::protobuf::MessageLite& src, Chain* dest);
Status SerializePartialToChain(const google::protobuf::MessageLite& src,
                               Chain* dest);

// Implementation details follow.

namespace internal {

Status SerializeToWriterImpl(const google::protobuf::MessageLite& src,
                             Writer* dest);
Status SerializePartialToWriterImpl(const google::protobuf::MessageLite& src,
                                    Writer* dest);

}  // namespace internal

template <typename Dest>
inline Status SerializeToWriter(const google::protobuf::MessageLite& src,
                                Dest&& dest) {
  Dependency<Writer*, std::decay_t<Dest>> dest_dep(std::forward<Dest>(dest));
  Status status = internal::SerializeToWriterImpl(src, dest_dep.get());
  if (dest_dep.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_dep->Close())) {
      if (ABSL_PREDICT_TRUE(status.ok())) status = dest_dep->status();
    }
  }
  return status;
}

template <typename Dest, typename... DestArgs>
inline Status SerializeToWriter(const google::protobuf::MessageLite& src,
                                std::tuple<DestArgs...> dest_args) {
  Dependency<Writer*, Dest> dest_dep(std::move(dest_args));
  Status status = internal::SerializeToWriterImpl(src, dest_dep.get());
  if (dest_dep.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_dep->Close())) {
      if (ABSL_PREDICT_TRUE(status.ok())) status = dest_dep->status();
    }
  }
  return status;
}

template <typename Dest>
inline Status SerializePartialToWriter(const google::protobuf::MessageLite& src,
                                       Dest&& dest) {
  Dependency<Writer*, std::decay_t<Dest>> dest_dep(std::forward<Dest>(dest));
  Status status = internal::SerializePartialToWriterImpl(src, dest_dep.get());
  if (dest_dep.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_dep->Close())) {
      if (ABSL_PREDICT_TRUE(status.ok())) status = dest_dep->status();
    }
  }
  return status;
}

template <typename Dest, typename... DestArgs>
inline Status SerializePartialToWriter(const google::protobuf::MessageLite& src,
                                       std::tuple<DestArgs...> dest_args) {
  Dependency<Writer*, Dest> dest_dep(std::move(dest_args));
  Status status = internal::SerializePartialToWriterImpl(src, dest_dep.get());
  if (dest_dep.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_dep->Close())) {
      if (ABSL_PREDICT_TRUE(status.ok())) status = dest_dep->status();
    }
  }
  return status;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_MESSAGE_SERIALIZE_H_
