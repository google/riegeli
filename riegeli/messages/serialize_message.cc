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

#include "riegeli/messages/serialize_message.h"

#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <string>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/cord_buffer.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/compact_string.h"
#include "riegeli/base/string_utils.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/array_writer.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/chain_writer.h"
#include "riegeli/bytes/cord_writer.h"
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

ABSL_ATTRIBUTE_COLD inline absl::Status FailSizeOverflow(
    const google::protobuf::MessageLite& src, BackwardWriter& dest,
    size_t size) {
  return dest.AnnotateStatus(FailSizeOverflow(src, size));
}

inline absl::Status SerializeMessageUsingStream(
    const google::protobuf::MessageLite& src, Writer& dest, bool deterministic,
    size_t size) {
  WriterOutputStream output_stream(&dest);
  google::protobuf::io::CodedOutputStream coded_stream(&output_stream);
  coded_stream.SetSerializationDeterministic(deterministic);
  src.SerializeWithCachedSizes(&coded_stream);
  RIEGELI_ASSERT_EQ(src.ByteSizeLong(), size)
      << src.GetTypeName() << " was modified concurrently during serialization";
  // Flush `coded_stream` before checking `dest.ok()`.
  coded_stream.Trim();
  if (ABSL_PREDICT_FALSE(!dest.ok())) return dest.status();
  RIEGELI_ASSERT(!coded_stream.HadError())
      << "Failed to serialize message of type " << src.GetTypeName()
      << ": SerializeWithCachedSizes() failed for an unknown reason";
  RIEGELI_ASSERT_EQ(IntCast<size_t>(coded_stream.ByteCount()), size)
      << "Byte size calculation and serialization were inconsistent. This "
         "may indicate a bug in protocol buffers or it may be caused by "
         "concurrent modification of "
      << src.GetTypeName();
  return absl::OkStatus();
}

inline absl::Status SerializeMessageHavingSize(
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
  return SerializeMessageUsingStream(src, dest, deterministic, size);
}

inline absl::Status SerializeMessageHavingSize(
    const google::protobuf::MessageLite& src, BackwardWriter& dest,
    bool deterministic, size_t size) {
  if (size <= kMaxBytesToCopy &&
      deterministic == google::protobuf::io::CodedOutputStream::
                           IsDefaultSerializationDeterministic()) {
    // The data are small, so making a flat output is harmless.
    // `SerializeWithCachedSizesToArray()` is faster than
    // `SerializeWithCachedSizes()`.
    if (ABSL_PREDICT_FALSE(!dest.Push(size))) return dest.status();
    dest.move_cursor(size);
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
    return absl::OkStatus();
  }
  riegeli::CordWriter cord_writer;
  if (absl::Status status =
          SerializeMessageUsingStream(src, cord_writer, deterministic, size);
      ABSL_PREDICT_FALSE(!status.ok())) {
    return status;
  }
  if (ABSL_PREDICT_FALSE(!cord_writer.Close())) return cord_writer.status();
  if (ABSL_PREDICT_FALSE(!dest.Write(std::move(cord_writer.dest())))) {
    return dest.status();
  }
  return absl::OkStatus();
}

}  // namespace

namespace serialize_message_internal {

absl::Status SerializeMessageImpl(const google::protobuf::MessageLite& src,
                                  Writer& dest, SerializeMessageOptions options,
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
  return SerializeMessageHavingSize(src, dest, options.deterministic(), size);
}

absl::Status SerializeMessageImpl(const google::protobuf::MessageLite& src,
                                  BackwardWriter& dest,
                                  SerializeMessageOptions options,
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
  return SerializeMessageHavingSize(src, dest, options.deterministic(), size);
}

}  // namespace serialize_message_internal

absl::Status SerializeLengthPrefixedMessage(
    const google::protobuf::MessageLite& src, Writer& dest,
    SerializeMessageOptions options) {
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
  return SerializeMessageHavingSize(src, dest, options.deterministic(), size);
}

absl::Status SerializeMessage(const google::protobuf::MessageLite& src,
                              std::string& dest,
                              SerializeMessageOptions options) {
  RIEGELI_ASSERT(options.partial() || src.IsInitialized())
      << "Failed to serialize message of type " << src.GetTypeName()
      << " because it is missing required fields: "
      << src.InitializationErrorString();
  const size_t size = options.GetByteSize(src);
  if (ABSL_PREDICT_FALSE(size >
                         uint32_t{std::numeric_limits<int32_t>::max()})) {
    return FailSizeOverflow(src, size);
  }
  dest.clear();
  ResizeStringAmortized(dest, size);
  if (options.deterministic() == google::protobuf::io::CodedOutputStream::
                                     IsDefaultSerializationDeterministic()) {
    // Creating a string, which is necessarily flat.
    // `SerializeWithCachedSizesToArray()` is faster than
    // `SerializeWithCachedSizes()`.
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
  riegeli::ArrayWriter writer(&dest[0], size);
  const absl::Status status =
      SerializeMessageUsingStream(src, writer, options.deterministic(), size);
  RIEGELI_EVAL_ASSERT(writer.Close()) << "ArrayWriter has no reason to fail "
                                         "if the size does not overflow: "
                                      << writer.status();
  return status;
}

absl::Status SerializeMessage(const google::protobuf::MessageLite& src,
                              CompactString& dest,
                              SerializeMessageOptions options) {
  RIEGELI_ASSERT(options.partial() || src.IsInitialized())
      << "Failed to serialize message of type " << src.GetTypeName()
      << " because it is missing required fields: "
      << src.InitializationErrorString();
  const size_t size = options.GetByteSize(src);
  if (ABSL_PREDICT_FALSE(size >
                         uint32_t{std::numeric_limits<int32_t>::max()})) {
    return FailSizeOverflow(src, size);
  }
  char* const data = dest.resize(size, 0);
  if (options.deterministic() == google::protobuf::io::CodedOutputStream::
                                     IsDefaultSerializationDeterministic()) {
    // Creating a string, which is necessarily flat.
    // `SerializeWithCachedSizesToArray()` is faster than
    // `SerializeWithCachedSizes()`.
    char* const cursor = reinterpret_cast<char*>(
        src.SerializeWithCachedSizesToArray(reinterpret_cast<uint8_t*>(data)));
    RIEGELI_ASSERT_EQ(src.ByteSizeLong(), size)
        << src.GetTypeName()
        << " was modified concurrently during serialization";
    RIEGELI_ASSERT_EQ(PtrDistance(data, cursor), size)
        << "Byte size calculation and serialization were inconsistent. This "
           "may indicate a bug in protocol buffers or it may be caused by "
           "concurrent modification of "
        << src.GetTypeName();
    return absl::OkStatus();
  }
  riegeli::ArrayWriter writer(data, size);
  const absl::Status status =
      SerializeMessageUsingStream(src, writer, options.deterministic(), size);
  RIEGELI_EVAL_ASSERT(writer.Close()) << "ArrayWriter has no reason to fail "
                                         "if the size does not overflow: "
                                      << writer.status();
  return status;
}

absl::Status SerializeMessage(const google::protobuf::MessageLite& src,
                              Chain& dest, SerializeMessageOptions options) {
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
  riegeli::ChainWriter writer(&dest);
  writer.SetWriteSizeHint(size);
  const absl::Status status =
      SerializeMessageUsingStream(src, writer, options.deterministic(), size);
  RIEGELI_EVAL_ASSERT(writer.Close())
      << "ChainWriter has no reason to fail: " << writer.status();
  return status;
}

absl::Status SerializeMessage(const google::protobuf::MessageLite& src,
                              absl::Cord& dest,
                              SerializeMessageOptions options) {
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
    absl::CordBuffer buffer = dest.GetAppendBuffer(0, 0);
    dest.Clear();
    if (buffer.capacity() < size) {
      buffer = absl::CordBuffer::CreateWithDefaultLimit(size);
    }
    buffer.SetLength(size);
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
    dest.Append(std::move(buffer));
    return absl::OkStatus();
  }
  riegeli::CordWriter writer(&dest);
  writer.SetWriteSizeHint(size);
  const absl::Status status =
      SerializeMessageUsingStream(src, writer, options.deterministic(), size);
  RIEGELI_EVAL_ASSERT(writer.Close())
      << "CordWriter has no reason to fail: " << writer.status();
  return status;
}

bool WriterOutputStream::Next(void** data, int* size) {
  RIEGELI_ASSERT_NE(dest_, nullptr)
      << "Failed precondition of WriterOutputStream::Next(): "
         "WriterOutputStream not initialized";
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
  RIEGELI_ASSERT_NE(dest_, nullptr)
      << "Failed precondition of WriterOutputStream::BackUp(): "
         "WriterOutputStream not initialized";
  RIEGELI_ASSERT_GE(length, 0)
      << "Failed precondition of ZeroCopyOutputStream::BackUp(): "
         "negative length";
  RIEGELI_ASSERT_LE(IntCast<size_t>(length), dest_->start_to_cursor())
      << "Failed precondition of ZeroCopyOutputStream::BackUp(): "
         "length larger than the amount of buffered data";
  dest_->set_cursor(dest_->cursor() - length);
}

int64_t WriterOutputStream::ByteCount() const {
  RIEGELI_ASSERT_NE(dest_, nullptr)
      << "Failed precondition of WriterOutputStream::ByteCount(): "
         "WriterOutputStream not initialized";
  return SaturatingIntCast<int64_t>(dest_->pos());
}

bool WriterOutputStream::WriteCord(const absl::Cord& src) {
  RIEGELI_ASSERT_NE(dest_, nullptr)
      << "Failed precondition of WriterOutputStream::WriteCord(): "
         "WriterOutputStream not initialized";
  if (ABSL_PREDICT_FALSE(src.size() >
                         Position{std::numeric_limits<int64_t>::max()} -
                             dest_->pos())) {
    return false;
  }
  return dest_->Write(src);
}

}  // namespace riegeli
