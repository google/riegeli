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

#include "riegeli/bytes/message_serialize.h"

#include "google/protobuf/io/zero_copy_stream.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/chain_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

namespace {

// WriterOutputStream adapts a Writer to a ZeroCopyOutputStream.
class WriterOutputStream : public google::protobuf::io::ZeroCopyOutputStream {
 public:
  explicit WriterOutputStream(Writer* dest)
      : dest_(RIEGELI_ASSERT_NOTNULL(dest)), initial_pos_(dest_->pos()) {}

  bool Next(void** data, int* size) override {
    if (RIEGELI_UNLIKELY(!dest_->Push())) return false;
    *data = dest_->cursor();
    *size = dest_->available();
    dest_->set_cursor(dest_->limit());
    return true;
  }
  void BackUp(int count) override {
    dest_->set_cursor(dest_->cursor() - count);
  }
  google::protobuf::int64 ByteCount() const override { return dest_->pos() - initial_pos_; }

 private:
  Writer* dest_;
  Position initial_pos_;
};

}  // namespace

bool SerializeToWriter(const google::protobuf::MessageLite& message, Writer* output) {
  WriterOutputStream output_stream(output);
  return message.SerializeToZeroCopyStream(&output_stream);
}

bool SerializePartialToWriter(const google::protobuf::MessageLite& message,
                              Writer* output) {
  WriterOutputStream output_stream(output);
  return message.SerializePartialToZeroCopyStream(&output_stream);
}

bool SerializeToChain(const google::protobuf::MessageLite& message, Chain* output) {
  output->Clear();
  return AppendToChain(message, output);
}

bool SerializePartialToChain(const google::protobuf::MessageLite& message,
                             Chain* output) {
  output->Clear();
  return AppendPartialToChain(message, output);
}

Chain SerializeAsChain(const google::protobuf::MessageLite& message) {
  Chain result;
  AppendToChain(message, &result);
  return result;
}

Chain SerializePartialAsChain(const google::protobuf::MessageLite& message) {
  Chain result;
  AppendPartialToChain(message, &result);
  return result;
}

bool AppendToChain(const google::protobuf::MessageLite& message, Chain* output) {
  ChainWriter output_writer(
      output, ChainWriter::Options().set_size_hint(output->size() +
                                                   message.ByteSizeLong()));
  if (RIEGELI_UNLIKELY(!SerializeToWriter(message, &output_writer))) {
    return false;
  }
  return output_writer.Close();
}

bool AppendPartialToChain(const google::protobuf::MessageLite& message, Chain* output) {
  ChainWriter output_writer(
      output, ChainWriter::Options().set_size_hint(output->size() +
                                                   message.ByteSizeLong()));
  if (RIEGELI_UNLIKELY(!SerializePartialToWriter(message, &output_writer))) {
    return false;
  }
  return output_writer.Close();
}

}  // namespace riegeli
