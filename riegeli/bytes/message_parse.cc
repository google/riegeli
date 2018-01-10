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

#include "riegeli/bytes/message_parse.h"

#include "google/protobuf/io/zero_copy_stream.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/string_view.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

namespace {

// ReaderInputStream adapts a Reader to a ZeroCopyInputStream.
class ReaderInputStream : public google::protobuf::io::ZeroCopyInputStream {
 public:
  explicit ReaderInputStream(Reader* src)
      : src_(RIEGELI_ASSERT_NOTNULL(src)), initial_pos_(src_->pos()) {}

  bool Next(const void** data, int* size) override {
    if (RIEGELI_UNLIKELY(!src_->Pull())) return false;
    *data = src_->cursor();
    *size = src_->available();
    src_->set_cursor(src_->limit());
    return true;
  }
  void BackUp(int count) override { src_->set_cursor(src_->cursor() - count); }
  bool Skip(int count) override { return src_->Skip(count); }
  google::protobuf::int64 ByteCount() const override { return src_->pos() - initial_pos_; }

 private:
  Reader* src_;
  Position initial_pos_;
};

}  // namespace

bool ParseFromReader(google::protobuf::MessageLite* message, Reader* input) {
  ReaderInputStream input_stream(input);
  return message->ParseFromZeroCopyStream(&input_stream);
}

bool ParsePartialFromReader(google::protobuf::MessageLite* message, Reader* input) {
  ReaderInputStream input_stream(input);
  return message->ParsePartialFromZeroCopyStream(&input_stream);
}

bool ParseFromStringView(google::protobuf::MessageLite* message, string_view data) {
  return message->ParseFromArray(data.data(), data.size());
}

bool ParsePartialFromStringView(google::protobuf::MessageLite* message,
                                string_view data) {
  return message->ParsePartialFromArray(data.data(), data.size());
}

bool ParseFromChain(google::protobuf::MessageLite* message, const Chain& data) {
  ChainReader data_reader(&data);
  if (RIEGELI_UNLIKELY(!ParseFromReader(message, &data_reader))) return false;
  return data_reader.Close();
}

bool ParsePartialFromChain(google::protobuf::MessageLite* message, const Chain& data) {
  ChainReader data_reader(&data);
  if (RIEGELI_UNLIKELY(!ParsePartialFromReader(message, &data_reader))) {
    return false;
  }
  return data_reader.Close();
}

}  // namespace riegeli
