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

#include <stddef.h>

#include "google/protobuf/message_lite.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Writes the message to the given Writer. All required fields must be set.
bool SerializeToWriter(const google::protobuf::MessageLite& message,
                       Writer* output);
// Like SerializeToWriter(), but allows missing required fields.
bool SerializePartialToWriter(const google::protobuf::MessageLite& message,
                              Writer* output);

// Serializes the message and store it in the given Chain. All required fields
// must be set. The Chain is cleared on failure.
bool SerializeToChain(const google::protobuf::MessageLite& message,
                      Chain* output);
// Like SerializeToChain(), but allows missing required fields.
bool SerializePartialToChain(const google::protobuf::MessageLite& message,
                             Chain* output);

// Makes a Chain encoding the message. Is equivalent to calling
// SerializeToChain() on a Chain and using that. Returns an empty Chain
// if SerializeToChain() would have returned an error.
Chain SerializeAsChain(const google::protobuf::MessageLite& message);
// Like SerializeAsChain(), but allows missing required fields.
Chain SerializePartialAsChain(const google::protobuf::MessageLite& message);

// Like SerializeToChain(), but appends to the data to the Chain's existing
// contents. All required fields must be set.
bool AppendToChain(const google::protobuf::MessageLite& message, Chain* output,
                   size_t size_hint = 0);
// Like AppendToChain(), but allows missing required fields.
bool AppendPartialToChain(const google::protobuf::MessageLite& message,
                          Chain* output, size_t size_hint = 0);

}  // namespace riegeli

#endif  // RIEGELI_BYTES_MESSAGE_SERIALIZE_H_
