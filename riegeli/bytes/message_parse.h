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

#include "google/protobuf/message_lite.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

// Reads a message from the given Reader. If successful, the entire input will
// be consumed.
bool ParseFromReader(google::protobuf::MessageLite* message, Reader* input);
// Like ParseFromReader(), but accepts messages that are missing required
// fields.
bool ParsePartialFromReader(google::protobuf::MessageLite* message, Reader* input);

// Parses a message contained in an array of bytes.
bool ParseFromStringView(google::protobuf::MessageLite* message, absl::string_view data);
// Like ParseFromStringView(), but accepts messages that are missing required
// fields.
bool ParsePartialFromStringView(google::protobuf::MessageLite* message,
                                absl::string_view data);

// Parses a message contained in a Chain.
bool ParseFromChain(google::protobuf::MessageLite* message, const Chain& data);
// Like ParseFromChain(), but accepts messages that are missing required fields.
bool ParsePartialFromChain(google::protobuf::MessageLite* message, const Chain& data);

}  // namespace riegeli

#endif  // RIEGELI_BYTES_MESSAGE_PARSE_H_
