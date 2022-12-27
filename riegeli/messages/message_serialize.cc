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

#include "riegeli/messages/message_serialize.h"

#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <string>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/chain_writer.h"
#include "riegeli/bytes/cord_writer.h"
#include "riegeli/bytes/string_writer.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/varint/varint_writing.h"

namespace riegeli {

namespace {

ABSL_ATTRIBUTE_COLD inline absl::Status FailSizeOverflow(
    const google::protobuf::MessageLite& src, size_t size) {
  return absl::ResourceExhaustedError(
      absl::StrCat("Failed to serialize message of type ", src.GetTypeName(),
                   " because its size must be smaller than 2GiB: ", size));
}

ABSL_ATTRIBUTE_COLD inline absl::Status FailSizeOverflow(
    const google::protobuf::MessageLite& src, Writer& dest, size_t size) {
  return dest.AnnotateStatus(FailSizeOverflow(src, size));
}

inline absl::Status SerializeToWriterUsingStream(
    const google::protobuf::MessageLite& src, Writer& dest, bool deterministic,
    size_t size) {
  WriterOutputStream output_stream(&dest);
  google::protobuf::io::CodedOutputStream coded_stream(&output_stream);
  coded_stream.SetSerializationDeterministic(deterministic);
  src.SerializeWithCachedSizes(&coded_stream);
  // Flush `coded_stream` before checking `dest.ok()`.
  coded_stream.Trim();
  if (ABSL_PREDICT_FALSE(!dest.ok())) return dest.status();
  RIEGELI_ASSERT(!coded_stream.HadError())
      << "Failed to serialize message of type " << src.GetTypeName()
      << ": SerializeWithCachedSizes() failed for an unknown reason";
  RIEGELI_ASSERT_EQ(src.ByteSizeLong(), size)
      << src.GetTypeName() << " was modified concurrently during serialization";
  RIEGELI_ASSERT_EQ(IntCast<size_t>(coded_stream.ByteCount()), size)
      << "Byte size calculation and serialization were inconsistent. This "
         "may indicate a bug in protocol buffers or it may be caused by "
         "concurrent modification of "
      << src.GetTypeName();
  return absl::OkStatus();
}

inline absl::Status SerializeToWriterHavingSize(
    const google::protobuf::MessageLite& src, Writer& dest, bool deterministic,
    size_t size) {
  if (size <= kMaxBytesToCopy &&
      deterministic == google::protobuf::io::CodedOutputStream::
                           IsDefaultSerializationDeterministic()) {
    // The data are small, so making a flat output is harmless.
    // `SerializeWithCachedSizesToArray()` is faster than
    // `SerializeWithCachedSizes()`.
    if (ABSL_PREDICT_FALSE(!dest.Push(size))) return dest.status();
    char* const cursor =
        reinterpret_cast<char*>(src.SerializeWithCachedSizesToArray(
            reinterpret_cast<uint8_t*>(dest.cursor())));
    RIEGELI_ASSERT_EQ(src.ByteSizeLong(), size)
        << src.GetTypeName()
        << " was modified concurrently during serialization";
    RIEGELI_ASSERT_EQ(PtrDistance(dest.cursor(), cursor), size)
        << "Byte size calculation and serialization were inconsistent. This "
           "may indicate a bug in protocol buffers or it may be caused by "
           "concurrent modification of "
        << src.GetTypeName();
    dest.set_cursor(cursor);
    return absl::OkStatus();
  }
  return SerializeToWriterUsingStream(src, dest, deterministic, size);
}

}  // namespace

namespace messages_internal {

absl::Status SerializeToWriterImpl(const google::protobuf::MessageLite& src,
                                   Writer& dest, SerializeOptions options,
                                   bool set_write_hint) {
  RIEGELI_ASSERT(options.partial() || src.IsInitialized())
      << "Failed to serialize message of type " << src.GetTypeName()
      << " because it is missing required fields: "
      << src.InitializationErrorString();
  const size_t size = options.GetByteSize(src);
  if (ABSL_PREDICT_FALSE(size >
                         uint32_t{std::numeric_limits<int32_t>::max()})) {
    return FailSizeOverflow(src, dest, size);
  }
  if (set_write_hint) dest.SetWriteSizeHint(size);
  return SerializeToWriterHavingSize(src, dest, options.deterministic(), size);
}

}  // namespace messages_internal

absl::Status SerializeLengthPrefixedToWriter(
    const google::protobuf::MessageLite& src, Writer& dest,
    SerializeOptions options) {
  RIEGELI_ASSERT(options.partial() || src.IsInitialized())
      << "Failed to serialize message of type " << src.GetTypeName()
      << " because it is missing required fields: "
      << src.InitializationErrorString();
  const size_t size = options.GetByteSize(src);
  if (ABSL_PREDICT_FALSE(size >
                         uint32_t{std::numeric_limits<int32_t>::max()})) {
    return FailSizeOverflow(src, dest, size);
  }
  if (ABSL_PREDICT_FALSE(!WriteVarint32(IntCast<uint32_t>(size), dest))) {
    return dest.status();
  }
  return SerializeToWriterHavingSize(src, dest, options.deterministic(), size);
}

absl::Status SerializeToString(const google::protobuf::MessageLite& src,
                               std::string& dest, SerializeOptions options) {
  RIEGELI_ASSERT(options.partial() || src.IsInitialized())
      << "Failed to serialize message of type " << src.GetTypeName()
      << " because it is missing required fields: "
      << src.InitializationErrorString();
  const size_t size = options.GetByteSize(src);
  if (ABSL_PREDICT_FALSE(size >
                         uint32_t{std::numeric_limits<int32_t>::max()})) {
    return FailSizeOverflow(src, size);
  }
  if (options.deterministic() == google::protobuf::io::CodedOutputStream::
                                     IsDefaultSerializationDeterministic()) {
    // Creating a string, which is necessarily flat.
    // `SerializeWithCachedSizesToArray()` is faster than
    // `SerializeWithCachedSizes()`.
    dest.clear();
    dest.resize(size);
    char* const cursor =
        reinterpret_cast<char*>(src.SerializeWithCachedSizesToArray(
            reinterpret_cast<uint8_t*>(&dest[0])));
    RIEGELI_ASSERT_EQ(src.ByteSizeLong(), size)
        << src.GetTypeName()
        << " was modified concurrently during serialization";
    RIEGELI_ASSERT_EQ(PtrDistance(dest.data(), cursor), size)
        << "Byte size calculation and serialization were inconsistent. This "
           "may indicate a bug in protocol buffers or it may be caused by "
           "concurrent modification of "
        << src.GetTypeName();
    return absl::OkStatus();
  }
  riegeli::StringWriter<> writer(&dest);
  writer.SetWriteSizeHint(size);
  const absl::Status status =
      SerializeToWriterUsingStream(src, writer, options.deterministic(), size);
  if (!writer.Close()) {
    RIEGELI_ASSERT_UNREACHABLE()
        << "A StringWriter has no reason to fail: " << writer.status();
  }
  return status;
}

absl::Status SerializeToChain(const google::protobuf::MessageLite& src,
                              Chain& dest, SerializeOptions options) {
  RIEGELI_ASSERT(options.partial() || src.IsInitialized())
      << "Failed to serialize message of type " << src.GetTypeName()
      << " because it is missing required fields: "
      << src.InitializationErrorString();
  const size_t size = options.GetByteSize(src);
  if (ABSL_PREDICT_FALSE(size >
                         uint32_t{std::numeric_limits<int32_t>::max()})) {
    return FailSizeOverflow(src, size);
  }
  if (size <= kMaxBytesToCopy &&
      options.deterministic() == google::protobuf::io::CodedOutputStream::
                                     IsDefaultSerializationDeterministic()) {
    // The data are small, so making a flat output is harmless.
    // `SerializeWithCachedSizesToArray()` is faster than
    // `SerializeWithCachedSizes()`.
    dest.Clear();
    const absl::Span<char> buffer =
        dest.AppendFixedBuffer(size, Chain::Options().set_size_hint(size));
    char* const cursor =
        reinterpret_cast<char*>(src.SerializeWithCachedSizesToArray(
            reinterpret_cast<uint8_t*>(buffer.data())));
    RIEGELI_ASSERT_EQ(src.ByteSizeLong(), size)
        << src.GetTypeName()
        << " was modified concurrently during serialization";
    RIEGELI_ASSERT_EQ(PtrDistance(buffer.data(), cursor), size)
        << "Byte size calculation and serialization were inconsistent. This "
           "may indicate a bug in protocol buffers or it may be caused by "
           "concurrent modification of "
        << src.GetTypeName();
    return absl::OkStatus();
  }
  riegeli::ChainWriter<> writer(&dest);
  writer.SetWriteSizeHint(size);
  const absl::Status status =
      SerializeToWriterUsingStream(src, writer, options.deterministic(), size);
  if (!writer.Close()) {
    RIEGELI_ASSERT_UNREACHABLE()
        << "A ChainWriter has no reason to fail: " << writer.status();
  }
  return status;
}

absl::Status SerializeToCord(const google::protobuf::MessageLite& src,
                             absl::Cord& dest, SerializeOptions options) {
  RIEGELI_ASSERT(options.partial() || src.IsInitialized())
      << "Failed to serialize message of type " << src.GetTypeName()
      << " because it is missing required fields: "
      << src.InitializationErrorString();
  const size_t size = options.GetByteSize(src);
  if (ABSL_PREDICT_FALSE(size >
                         uint32_t{std::numeric_limits<int32_t>::max()})) {
    return FailSizeOverflow(src, size);
  }
  if (size <= kMaxBytesToCopy &&
      options.deterministic() == google::protobuf::io::CodedOutputStream::
                                     IsDefaultSerializationDeterministic()) {
    // The data are small, so making a flat output is harmless.
    // `SerializeWithCachedSizesToArray()` is faster than
    // `SerializeWithCachedSizes()`.
    Buffer buffer(size);
    char* const cursor =
        reinterpret_cast<char*>(src.SerializeWithCachedSizesToArray(
            reinterpret_cast<uint8_t*>(buffer.data())));
    RIEGELI_ASSERT_EQ(src.ByteSizeLong(), size)
        << src.GetTypeName()
        << " was modified concurrently during serialization";
    RIEGELI_ASSERT_EQ(PtrDistance(buffer.data(), cursor), size)
        << "Byte size calculation and serialization were inconsistent. This "
           "may indicate a bug in protocol buffers or it may be caused by "
           "concurrent modification of "
        << src.GetTypeName();
    dest = std::move(buffer).ToCord(buffer.data(), size);
    return absl::OkStatus();
  }
  riegeli::CordWriter<> writer(&dest);
  writer.SetWriteSizeHint(size);
  const absl::Status status =
      SerializeToWriterUsingStream(src, writer, options.deterministic(), size);
  if (!writer.Close()) {
    RIEGELI_ASSERT_UNREACHABLE()
        << "A CordWriter has no reason to fail: " << writer.status();
  }
  return status;
}

bool WriterOutputStream::Next(void** data, int* size) {
  if (ABSL_PREDICT_FALSE(dest_->pos() >=
                         Position{std::numeric_limits<int64_t>::max()})) {
    return false;
  }
  const Position max_length =
      Position{std::numeric_limits<int64_t>::max()} - dest_->pos();
  if (ABSL_PREDICT_FALSE(!dest_->Push())) return false;
  *data = dest_->cursor();
  *size = SaturatingIntCast<int>(UnsignedMin(dest_->available(), max_length));
  dest_->move_cursor(IntCast<size_t>(*size));
  return true;
}

void WriterOutputStream::BackUp(int length) {
  RIEGELI_ASSERT_GE(length, 0)
      << "Failed precondition of ZeroCopyOutputStream::BackUp(): "
         "negative length";
  RIEGELI_ASSERT_LE(IntCast<size_t>(length), dest_->start_to_cursor())
      << "Failed precondition of ZeroCopyOutputStream::BackUp(): "
         "length larger than the amount of buffered data";
  dest_->set_cursor(dest_->cursor() - length);
}

int64_t WriterOutputStream::ByteCount() const {
  return SaturatingIntCast<int64_t>(dest_->pos());
}

}  // namespace riegeli
