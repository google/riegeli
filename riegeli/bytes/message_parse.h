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
#include <type_traits>
#include <utility>

#include "absl/base/optimization.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

class ParseOptions {
 public:
  ParseOptions() noexcept {}

  // If `false`, missing required fields cause a failure.
  //
  // If `true`, missing required fields result in a partial parsed message,
  // not having these fields.
  //
  // Default: `false`
  ParseOptions& set_partial(bool partial) & {
    partial_ = partial;
    return *this;
  }
  ParseOptions&& set_partial(bool partial) && {
    return std::move(set_partial(partial));
  }
  bool partial() const { return partial_; }

 private:
  bool partial_ = false;
};

// Reads a message in binary format from the given `Reader`. If successful, the
// entire input will be consumed.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the `Reader`. `Src` must support `Dependency<Reader*, Src>`,
// e.g. `Reader*` (not owned), `std::unique_ptr<Reader>` (owned),
// `ChainReader<>` (owned).
//
// With a `src_args` parameter, reads from a `Src` constructed from elements of
// `src_args`. This avoids constructing a temporary `Src` and moving from it.
//
// Returns status:
//  * `status.ok()`  - success (`*dest` is filled)
//  * `!status.ok()` - failure (`*dest` is unspecified)
template <typename Src>
Status ParseFromReader(const Src& src, google::protobuf::MessageLite* dest,
                       ParseOptions options = ParseOptions());
template <typename Src>
Status ParseFromReader(Src&& src, google::protobuf::MessageLite* dest,
                       ParseOptions options = ParseOptions());
template <typename Src, typename... SrcArgs>
Status ParseFromReader(std::tuple<SrcArgs...> src_args,
                       google::protobuf::MessageLite* dest,
                       ParseOptions options = ParseOptions());

// Reads a message in binary format from the given `Chain`. If successful, the
// entire input will be consumed.
//
// Returns status:
//  * `status.ok()`  - success (`*dest` is filled)
//  * `!status.ok()` - failure (`*dest` is unspecified)
Status ParseFromChain(const Chain& src, google::protobuf::MessageLite* dest,
                      ParseOptions options = ParseOptions());

// Implementation details follow.

namespace internal {

Status ParseFromReaderImpl(Reader* src, google::protobuf::MessageLite* dest,
                           ParseOptions options);

template <typename Src>
inline Status ParseFromReaderImpl(Dependency<Reader*, Src> src,
                                  google::protobuf::MessageLite* dest,
                                  ParseOptions options) {
  Status status = ParseFromReaderImpl(src.get(), dest, options);
  if (src.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src->Close())) {
      if (ABSL_PREDICT_TRUE(status.ok())) status = src->status();
    }
  }
  return status;
}

}  // namespace internal

template <typename Src>
inline Status ParseFromReader(const Src& src,
                              google::protobuf::MessageLite* dest,
                              ParseOptions options) {
  return internal::ParseFromReaderImpl(Dependency<Reader*, Src>(src), dest,
                                       options);
}

template <typename Src>
inline Status ParseFromReader(Src&& src, google::protobuf::MessageLite* dest,
                              ParseOptions options) {
  return internal::ParseFromReaderImpl(
      Dependency<Reader*, std::decay_t<Src>>(std::forward<Src>(src)), dest,
      options);
}

template <typename Src, typename... SrcArgs>
inline Status ParseFromReader(std::tuple<SrcArgs...> src_args,
                              google::protobuf::MessageLite* dest,
                              ParseOptions options) {
  return internal::ParseFromReaderImpl(
      Dependency<Reader*, Src>(std::move(src_args)), dest, options);
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_MESSAGE_PARSE_H_
