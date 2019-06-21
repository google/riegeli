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

#include <tuple>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/meta/type_traits.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

// Reads a message in binary format from the given Reader. If successful, the
// entire input will be consumed.
//
// ParsePartialFromReader() allows missing required fields.
//
// The Src template parameter specifies the type of the object providing and
// possibly owning the Reader. Src must support Dependency<Reader*, Src>,
// e.g. Reader* (not owned, default), unique_ptr<Reader> (owned),
// ChainReader<> (owned).
//
// With a src_args parameter, reads from a Src constructed from elements of
// src_args. This avoids constructing a temporary Src and moving from it.
//
// Returns status:
//  * status.ok()  - success (*dest is filled)
//  * !status.ok() - failure (*dest is unspecified)
template <typename Src>
Status ParseFromReader(google::protobuf::MessageLite* dest, Src&& src);
template <typename Src, typename... SrcArgs>
Status ParseFromReader(google::protobuf::MessageLite* dest,
                       std::tuple<SrcArgs...> src_args);
template <typename Src>
Status ParsePartialFromReader(google::protobuf::MessageLite* dest, Src&& src);
template <typename Src, typename... SrcArgs>
Status ParsePartialFromReader(google::protobuf::MessageLite* dest,
                              std::tuple<SrcArgs...> src_args);

// Reads a message in binary format from the given Chain. If successful, the
// entire input will be consumed.
//
// ParsePartialFromChain() allows missing required fields.
//
// Returns status:
//  * status.ok()  - success (*dest is filled)
//  * !status.ok() - failure (*dest is unspecified, *error_message is set)
Status ParseFromChain(google::protobuf::MessageLite* dest, const Chain& src);
Status ParsePartialFromChain(google::protobuf::MessageLite* dest,
                             const Chain& src);

// Implementation details follow.

namespace internal {

Status ParseFromReaderImpl(google::protobuf::MessageLite* dest, Reader* src);
Status ParsePartialFromReaderImpl(google::protobuf::MessageLite* dest,
                                  Reader* src);

Status ParseFromReaderUsingInputStream(google::protobuf::MessageLite* dest,
                                       Reader* src);
Status ParsePartialFromReaderUsingInputStream(
    google::protobuf::MessageLite* dest, Reader* src);

}  // namespace internal

template <typename Src>
inline Status ParseFromReader(google::protobuf::MessageLite* dest, Src&& src) {
  Dependency<Reader*, absl::decay_t<Src>> src_dep(std::forward<Src>(src));
  Status status = internal::ParseFromReaderImpl(dest, src_dep.get());
  if (src_dep.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_dep->Close())) {
      if (ABSL_PREDICT_TRUE(status.ok())) status = src_dep->status();
    }
  }
  return status;
}

template <typename Src, typename... SrcArgs>
inline Status ParseFromReader(google::protobuf::MessageLite* dest,
                              std::tuple<SrcArgs...> src_args) {
  Dependency<Reader*, Src> src_dep(std::move(src_args));
  Status status = internal::ParseFromReaderImpl(dest, src_dep.get());
  if (src_dep.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_dep->Close())) {
      if (ABSL_PREDICT_TRUE(status.ok())) status = src_dep->status();
    }
  }
  return status;
}

template <typename Src>
inline Status ParsePartialFromReader(google::protobuf::MessageLite* dest,
                                     Src&& src) {
  Dependency<Reader*, absl::decay_t<Src>> src_dep(std::forward<Src>(src));
  Status status = internal::ParsePartialFromReaderImpl(dest, src_dep.get());
  if (src_dep.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_dep->Close())) {
      if (ABSL_PREDICT_TRUE(status.ok())) status = src_dep->status();
    }
  }
  return status;
}

template <typename Src, typename... SrcArgs>
inline Status ParsePartialFromReader(google::protobuf::MessageLite* dest,
                                     std::tuple<SrcArgs...> src_args) {
  Dependency<Reader*, Src> src_dep(std::move(src_args));
  Status status = internal::ParsePartialFromReaderImpl(dest, src_dep.get());
  if (src_dep.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_dep->Close())) {
      if (ABSL_PREDICT_TRUE(status.ok())) status = src_dep->status();
    }
  }
  return status;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_MESSAGE_PARSE_H_
