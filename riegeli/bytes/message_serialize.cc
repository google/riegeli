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
#include <stdint.h>

#include <limits>
#include <string>
#include <tuple>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/chain_writer.h"
#include "riegeli/bytes/cord_writer.h"
#include "riegeli/bytes/string_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

namespace internal {

absl::Status SerializeToWriterImpl(const google::protobuf::MessageLite& src,
                                   Writer* dest, SerializeOptions options) {
  RIEGELI_ASSERT(options.partial() || src.IsInitialized())
      << "Failed to serialize message of type " << src.GetTypeName()
      << " because it is missing required fields: "
      << src.InitializationErrorString();
  const size_t size = options.GetByteSize(src);
  if (ABSL_PREDICT_FALSE(size > size_t{std::numeric_limits<int>::max()})) {
    return absl::ResourceExhaustedError(absl::StrCat(
        "Failed to serialize message of type ", src.GetTypeName(),
        " because it exceeds maximum protobuf size of 2GB: ", size));
  }
  WriterOutputStream output_stream(dest);
  google::protobuf::io::CodedOutputStream coded_stream(&output_stream);
  coded_stream.SetSerializationDeterministic(options.deterministic());
  src.SerializeWithCachedSizes(&coded_stream);
  if (ABSL_PREDICT_FALSE(coded_stream.HadError())) {
    RIEGELI_ASSERT(!dest->healthy())
        << "Failed to serialize message of type " << src.GetTypeName()
        << ": SerializeWithCachedSizes() failed for an unknown reason";
    return dest->status();
  }
  RIEGELI_ASSERT_EQ(size, src.ByteSizeLong())
      << src.GetTypeName() << " was modified concurrently during serialization";
  RIEGELI_ASSERT_EQ(coded_stream.ByteCount(), size)
      << "Byte size calculation and serialization were inconsistent. This "
         "may indicate a bug in protocol buffers or it may be caused by "
         "concurrent modification of "
      << src.GetTypeName();
  return absl::OkStatus();
}

}  // namespace internal

absl::Status SerializeToString(const google::protobuf::MessageLite& src,
                               std::string* dest, SerializeOptions options) {
  dest->clear();
  const size_t size = options.GetByteSize(src);
  return SerializeToWriter<StringWriter<>>(
      src,
      std::forward_as_tuple(dest,
                            StringWriterBase::Options().set_size_hint(size)),
      options);
}

absl::Status SerializeToChain(const google::protobuf::MessageLite& src,
                              Chain* dest, SerializeOptions options) {
  dest->Clear();
  const size_t size = options.GetByteSize(src);
  return SerializeToWriter<ChainWriter<>>(
      src,
      std::forward_as_tuple(dest,
                            ChainWriterBase::Options().set_size_hint(size)),
      options);
}

absl::Status SerializeToCord(const google::protobuf::MessageLite& src,
                             absl::Cord* dest, SerializeOptions options) {
  dest->Clear();
  const size_t size = options.GetByteSize(src);
  return SerializeToWriter<CordWriter<>>(
      src,
      std::forward_as_tuple(dest,
                            CordWriterBase::Options().set_size_hint(size)),
      options);
}

inline Position WriterOutputStream::relative_pos() const {
  RIEGELI_ASSERT_GE(dest_->pos(), initial_pos_)
      << "Failed invariant of WriterOutputStream: "
         "current position smaller than initial position";
  const Position pos = dest_->pos() - initial_pos_;
  RIEGELI_ASSERT_LE(pos, Position{std::numeric_limits<int64_t>::max()})
      << "Failed invariant of WriterOutputStream: relative position overflow";
  return pos;
}

bool WriterOutputStream::Next(void** data, int* size) {
  const Position pos = relative_pos();
  if (ABSL_PREDICT_FALSE(pos ==
                         Position{std::numeric_limits<int64_t>::max()})) {
    return false;
  }
  if (ABSL_PREDICT_FALSE(!dest_->Push())) return false;
  *data = dest_->cursor();
  *size = SaturatingIntCast<int>(UnsignedMin(
      dest_->available(), Position{std::numeric_limits<int64_t>::max()} - pos));
  dest_->move_cursor(IntCast<size_t>(*size));
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

int64_t WriterOutputStream::ByteCount() const {
  return IntCast<int64_t>(relative_pos());
}

}  // namespace riegeli
