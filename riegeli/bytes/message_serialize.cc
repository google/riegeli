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

#include <stddef.h>
#include <limits>

#include "absl/base/optimization.h"
#include "google/protobuf/io/zero_copy_stream.h"
#include "google/protobuf/message_lite.h"
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

  bool Next(void** data, int* size) override;
  void BackUp(int length) override;
  google::protobuf::int64 ByteCount() const override;

 private:
  Position relative_pos() const;

  Writer* dest_;
  // Invariants:
  //   dest_->pos() >= initial_pos_
  //   dest_->pos() - initial_pos_ <=
  //   numeric_limits<google::protobuf::int64>::max()
  Position initial_pos_;
};

inline Position WriterOutputStream::relative_pos() const {
  RIEGELI_ASSERT_GE(dest_->pos(), initial_pos_)
      << "Failed invariant of WriterOutputStream: "
         "current position smaller than initial position";
  const Position pos = dest_->pos() - initial_pos_;
  RIEGELI_ASSERT_LE(
      pos, Position{std::numeric_limits<google::protobuf::int64>::max()})
      << "Failed invariant of WriterOutputStream: relative position overflow";
  return pos;
}

bool WriterOutputStream::Next(void** data, int* size) {
  const Position pos = relative_pos();
  if (ABSL_PREDICT_FALSE(
          pos ==
          Position{std::numeric_limits<google::protobuf::int64>::max()})) {
    return false;
  }
  if (ABSL_PREDICT_FALSE(!dest_->Push())) return false;
  *data = dest_->cursor();
  *size = IntCast<int>(UnsignedMin(
      dest_->available(), size_t{std::numeric_limits<int>::max()},
      Position{std::numeric_limits<google::protobuf::int64>::max()} - pos));
  dest_->set_cursor(dest_->cursor() + *size);
  return true;
}

void WriterOutputStream::BackUp(int length) {
  RIEGELI_ASSERT_GE(length, 0)
      << "Failed precondition of ZeroCopyOutputStream::BackUp(): "
         "negative length";
  RIEGELI_ASSERT_LE(IntCast<size_t>(length), dest_->written_to_buffer())
      << "Failed precondition of ZeroCopyOutputStream::BackUp(): "
         "length larger than the amount of buffered data";
  dest_->set_cursor(dest_->cursor() - length);
}

google::protobuf::int64 WriterOutputStream::ByteCount() const {
  return IntCast<google::protobuf::int64>(relative_pos());
}

}  // namespace

bool SerializeToWriter(const google::protobuf::MessageLite& message,
                       Writer* output) {
  WriterOutputStream output_stream(output);
  return message.SerializeToZeroCopyStream(&output_stream);
}

bool SerializePartialToWriter(const google::protobuf::MessageLite& message,
                              Writer* output) {
  WriterOutputStream output_stream(output);
  return message.SerializePartialToZeroCopyStream(&output_stream);
}

bool SerializeToChain(const google::protobuf::MessageLite& message,
                      Chain* output) {
  output->Clear();
  return AppendToChain(message, output, message.ByteSizeLong());
}

bool SerializePartialToChain(const google::protobuf::MessageLite& message,
                             Chain* output) {
  output->Clear();
  return AppendPartialToChain(message, output, message.ByteSizeLong());
}

Chain SerializeAsChain(const google::protobuf::MessageLite& message) {
  Chain result;
  AppendToChain(message, &result, message.ByteSizeLong());
  return result;
}

Chain SerializePartialAsChain(const google::protobuf::MessageLite& message) {
  Chain result;
  AppendPartialToChain(message, &result, message.ByteSizeLong());
  return result;
}

bool AppendToChain(const google::protobuf::MessageLite& message, Chain* output,
                   size_t size_hint) {
  RIEGELI_CHECK_LE(message.ByteSizeLong(),
                   std::numeric_limits<size_t>::max() - output->size())
      << "Failed precondition of AppendToChain(): Chain size overflow";
  ChainWriter output_writer(output,
                            ChainWriter::Options().set_size_hint(size_hint));
  if (ABSL_PREDICT_FALSE(!SerializeToWriter(message, &output_writer))) {
    return false;
  }
  return output_writer.Close();
}

bool AppendPartialToChain(const google::protobuf::MessageLite& message,
                          Chain* output, size_t size_hint) {
  RIEGELI_CHECK_LE(message.ByteSizeLong(),
                   std::numeric_limits<size_t>::max() - output->size())
      << "Failed precondition of AppendPartialToChain(): Chain size overflow";
  ChainWriter output_writer(output,
                            ChainWriter::Options().set_size_hint(size_hint));
  if (ABSL_PREDICT_FALSE(!SerializePartialToWriter(message, &output_writer))) {
    return false;
  }
  return output_writer.Close();
}

}  // namespace riegeli
