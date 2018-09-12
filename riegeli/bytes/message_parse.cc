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

#include <stddef.h>
#include <limits>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "google/protobuf/io/zero_copy_stream.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

namespace {

// ReaderInputStream adapts a Reader to a ZeroCopyInputStream.
class ReaderInputStream : public google::protobuf::io::ZeroCopyInputStream {
 public:
  explicit ReaderInputStream(Reader* src)
      : src_(RIEGELI_ASSERT_NOTNULL(src)), initial_pos_(src_->pos()) {}

  bool Next(const void** data, int* size) override;
  void BackUp(int length) override;
  bool Skip(int length) override;
  google::protobuf::int64 ByteCount() const override;

 private:
  Position relative_pos() const;

  Reader* src_;
  // Invariants:
  //   src_->pos() >= initial_pos_
  //   src_->pos() - initial_pos_ <=
  //   numeric_limits<google::protobuf::int64>::max()
  Position initial_pos_;
};

inline Position ReaderInputStream::relative_pos() const {
  RIEGELI_ASSERT_GE(src_->pos(), initial_pos_)
      << "Failed invariant of ReaderInputStream: "
         "current position smaller than initial position";
  const Position pos = src_->pos() - initial_pos_;
  RIEGELI_ASSERT_LE(
      pos, Position{std::numeric_limits<google::protobuf::int64>::max()})
      << "Failed invariant of ReaderInputStream: "
         "relative position overflow";
  return pos;
}

bool ReaderInputStream::Next(const void** data, int* size) {
  const Position pos = relative_pos();
  if (ABSL_PREDICT_FALSE(
          pos ==
          Position{std::numeric_limits<google::protobuf::int64>::max()})) {
    return false;
  }
  if (ABSL_PREDICT_FALSE(!src_->Pull())) return false;
  *data = src_->cursor();
  *size = IntCast<int>(UnsignedMin(
      src_->available(), size_t{std::numeric_limits<int>::max()},
      Position{std::numeric_limits<google::protobuf::int64>::max()} - pos));
  src_->set_cursor(src_->cursor() + *size);
  return true;
}

void ReaderInputStream::BackUp(int length) {
  RIEGELI_ASSERT_GE(length, 0)
      << "Failed precondition of ZeroCopyInputStream::BackUp(): "
         "negative length";
  RIEGELI_ASSERT_LE(IntCast<size_t>(length), src_->read_from_buffer())
      << "Failed precondition of ZeroCopyInputStream::BackUp(): "
         "length larger than the amount of buffered data";
  src_->set_cursor(src_->cursor() - length);
}

bool ReaderInputStream::Skip(int length) {
  RIEGELI_ASSERT_GE(length, 0)
      << "Failed precondition of ZeroCopyInputStream::Skip(): negative length";
  const Position max_length =
      Position{std::numeric_limits<google::protobuf::int64>::max()} -
      relative_pos();
  if (ABSL_PREDICT_FALSE(IntCast<size_t>(length) > max_length)) {
    src_->Skip(max_length);
    return false;
  }
  return src_->Skip(IntCast<size_t>(length));
}

google::protobuf::int64 ReaderInputStream::ByteCount() const {
  return IntCast<google::protobuf::int64>(relative_pos());
}

}  // namespace

bool ParseFromReader(google::protobuf::MessageLite* message, Reader* input) {
  ReaderInputStream input_stream(input);
  return message->ParseFromZeroCopyStream(&input_stream);
}

bool ParsePartialFromReader(google::protobuf::MessageLite* message,
                            Reader* input) {
  ReaderInputStream input_stream(input);
  return message->ParsePartialFromZeroCopyStream(&input_stream);
}

bool ParseFromStringView(google::protobuf::MessageLite* message,
                         absl::string_view data) {
  if (ABSL_PREDICT_FALSE(data.size() >
                         size_t{std::numeric_limits<int>::max()})) {
    return false;
  }
  return message->ParseFromArray(data.data(), IntCast<int>(data.size()));
}

bool ParsePartialFromStringView(google::protobuf::MessageLite* message,
                                absl::string_view data) {
  if (ABSL_PREDICT_FALSE(data.size() >
                         size_t{std::numeric_limits<int>::max()})) {
    return false;
  }
  return message->ParsePartialFromArray(data.data(), IntCast<int>(data.size()));
}

bool ParseFromChain(google::protobuf::MessageLite* message, const Chain& data) {
  ChainReader<> data_reader(&data);
  if (ABSL_PREDICT_FALSE(!ParseFromReader(message, &data_reader))) return false;
  return data_reader.Close();
}

bool ParsePartialFromChain(google::protobuf::MessageLite* message,
                           const Chain& data) {
  ChainReader<> data_reader(&data);
  if (ABSL_PREDICT_FALSE(!ParsePartialFromReader(message, &data_reader))) {
    return false;
  }
  return data_reader.Close();
}

}  // namespace riegeli
