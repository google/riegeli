// Copyright 2022 Google LLC
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

#include "riegeli/messages/text_print_message.h"

#include <string>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/message.h"
#include "google/protobuf/text_format.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/chain_writer.h"
#include "riegeli/bytes/cord_writer.h"
#include "riegeli/bytes/string_writer.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/messages/serialize_message.h"

namespace riegeli {

namespace text_print_message_internal {

absl::Status TextPrintMessageImpl(const google::protobuf::Message& src,
                                  Writer& dest,
                                  const TextPrintOptions& options) {
  RIEGELI_ASSERT(options.partial() || src.IsInitialized())
      << "Failed to text-print message of type " << src.GetTypeName()
      << " because it is missing required fields: "
      << src.InitializationErrorString();
  if (options.header()) {
    const google::protobuf::Descriptor* const descriptor = src.GetDescriptor();
    dest.Write("# proto-file: ", descriptor->file()->name(),
               "\n"
               "# proto-message: ",
               descriptor->containing_type() != nullptr
                   ? descriptor->full_name()
                   : descriptor->name(),
               "\n\n");
  }
  WriterOutputStream output_stream(&dest);
  const bool print_ok = options.printer().Print(src, &output_stream);
  if (ABSL_PREDICT_FALSE(!dest.ok())) return dest.status();
  RIEGELI_ASSERT(print_ok)
      << "Failed to text-print message of type " << src.GetTypeName()
      << ": TextFormat::Printer::Print() failed for an unknown reason";
  return absl::OkStatus();
}

}  // namespace text_print_message_internal

absl::Status TextPrintMessage(const google::protobuf::Message& src,
                              std::string& dest,
                              const TextPrintOptions& options) {
  return TextPrintMessage(src, StringWriter<>(&dest), options);
}

absl::Status TextPrintMessage(const google::protobuf::Message& src, Chain& dest,
                              const TextPrintOptions& options) {
  return TextPrintMessage(src, ChainWriter<>(&dest), options);
}

absl::Status TextPrintMessage(const google::protobuf::Message& src,
                              absl::Cord& dest,
                              const TextPrintOptions& options) {
  return TextPrintMessage(src, CordWriter<>(&dest), options);
}

}  // namespace riegeli
