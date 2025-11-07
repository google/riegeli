// Copyright 2025 Google LLC
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

#ifndef RIEGELI_MESSAGES_SERIALIZED_MESSAGE_BACKWARD_WRITER_H_
#define RIEGELI_MESSAGES_SERIALIZED_MESSAGE_BACKWARD_WRITER_H_

#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/base/casts.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/any.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/limiting_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/stringify.h"
#include "riegeli/endian/endian_writing.h"
#include "riegeli/messages/map_entry_field.h"  // IWYU pragma: export
#include "riegeli/messages/message_wire_format.h"
#include "riegeli/messages/serialize_message.h"
#include "riegeli/varint/varint_writing.h"

namespace riegeli {

// `SerializedMessageBackwardWriter` builds a serialized proto message,
// specifying contents of particular fields, instead of traversing an in-memory
// message object like in `SerializeMessage()`. Building proceeds back to front,
// using `BackwardWriter`.
//
// Use cases:
//
//  * Processing a subset of fields without the overhead of materializing the
//    message object, i.e. without processing fields contained in submessages
//    which can be processed as a whole, and without keeping the whole parsed
//    message in memory.
//
//  * Processing a message in a way known at runtime, possibly with the schema
//    known at runtime, possibly partially known.
//
//  * Processing messages with so many elements of toplevel repeated fields that
//    the total message size exceeds 2GiB. This is not a great idea in itself,
//    because such messages cannot be processed using native proto parsing and
//    serialization.
//
// `SerializedMessageBackwardWriter` is more efficient than
// `SerializedMessageWriter` in the case of nested messages, because their
// contents can be written directly to the original `BackwardWriter`, with the
// length known and written after building the contents.
//
// Building elements of a repeated field is done in the opposite order than in
// `SerializedMessageWriter`, and if a non-repeated field is written multiple
// times then they are overridden or merged in the opposite order. Otherwise the
// field order does not influence how the message is parsed.
//
// Functions working on strings are applicable to any length-delimited field:
// `string`, `bytes`, submessage, or a packed repeated field.
class SerializedMessageBackwardWriter {
 public:
  // An empty object. It can be associated with a particular message by
  // `set_dest()` or assignment.
  //
  // An empty `SerializedMessageBackwardWriter` is not usable directly.
  SerializedMessageBackwardWriter() = default;

  // Will write to `*dest`, which is not owned and must outlive usages of this
  // object.
  explicit SerializedMessageBackwardWriter(
      BackwardWriter* dest ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : dest_(dest) {}

  SerializedMessageBackwardWriter(
      SerializedMessageBackwardWriter&& that) noexcept;
  SerializedMessageBackwardWriter& operator=(
      SerializedMessageBackwardWriter&& that) noexcept;

  // Returns the original `BackwardWriter` of the root message.
  BackwardWriter* dest() const { return dest_; }

  // Changes the `BackwardWriter` of the root message.
  //
  // This can be called even during building, even when submessages are open,
  // but the position must be the same. It particular this must be called when
  // the original `BackwardWriter` has been moved.
  void set_dest(BackwardWriter* dest);

  // Returns the `BackwardWriter` of the current message or length-delimited
  // field being built.
  //
  // `writer()` is the same as `*dest()`. Functions are separate for consistency
  // with `SerializedMessageWriter`.
  //
  // This can be used to write parts of the message directly, apart from
  // `Write*()` functions which write whole fields.
  //
  // Building elements of a repeated field and writing parts of a single field
  // to `writer()` is done in the opposite order than in
  // `SerializedMessageWriter`.
  BackwardWriter& writer() ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT_NE(dest_, nullptr)
        << "Failed precondition of SerializedMessageBackwardWriter::writer(): "
           "dest() not set";
    return *dest_;
  }

  // Writes the field tag and the field value.
  absl::Status WriteInt32(int field_number, int32_t value);
  absl::Status WriteInt64(int field_number, int64_t value);
  absl::Status WriteUInt32(int field_number, uint32_t value);
  absl::Status WriteUInt64(int field_number, uint64_t value);
  absl::Status WriteSInt32(int field_number, int32_t value);
  absl::Status WriteSInt64(int field_number, int64_t value);
  absl::Status WriteBool(int field_number, bool value);
  absl::Status WriteFixed32(int field_number, uint32_t value);
  absl::Status WriteFixed64(int field_number, uint64_t value);
  absl::Status WriteSFixed32(int field_number, int32_t value);
  absl::Status WriteSFixed64(int field_number, int64_t value);
  absl::Status WriteFloat(int field_number, float value);
  absl::Status WriteDouble(int field_number, double value);
  template <typename EnumType,
            std::enable_if_t<std::disjunction_v<std::is_enum<EnumType>,
                                                std::is_integral<EnumType>>,
                             int> = 0>
  absl::Status WriteEnum(int field_number, EnumType value);
  template <typename... Values
#if !__cpp_concepts
            ,
            std::enable_if_t<IsStringifiable<Values...>::value, int> = 0
#endif
            >
  absl::Status WriteString(int field_number, Values&&... values)
#if __cpp_concepts
      // For conjunctions, `requires` gives better error messages than
      // `std::enable_if_t`, indicating the relevant argument.
    requires(IsStringifiable<Values>::value && ...)
#endif
  ;

  // Writes the field tag of a length-delimited field and copies the field value
  // from a `Reader`.
  absl::Status CopyString(int field_number, AnyRef<Reader*> src);
  template <typename ReaderType>
  absl::Status CopyString(int field_number, ReaderSpan<ReaderType> src);

  // Writes the field tag of a length-delimited field and serializes a message
  // as the field value.
  absl::Status WriteSerializedMessage(
      int field_number, const google::protobuf::MessageLite& message,
      SerializeMessageOptions options = {});

  // Writes an element of a packed repeated field.
  //
  // The field must have been opened with `OpenLengthDelimited()` or
  // `WriteLengthUnchecked()`.
  absl::Status WritePackedInt32(int32_t value);
  absl::Status WritePackedInt64(int64_t value);
  absl::Status WritePackedUInt32(uint32_t value);
  absl::Status WritePackedUInt64(uint64_t value);
  absl::Status WritePackedSInt32(int32_t value);
  absl::Status WritePackedSInt64(int64_t value);
  absl::Status WritePackedBool(bool value);
  absl::Status WritePackedFixed32(uint32_t value);
  absl::Status WritePackedFixed64(uint64_t value);
  absl::Status WritePackedSFixed32(int32_t value);
  absl::Status WritePackedSFixed64(int64_t value);
  absl::Status WritePackedFloat(float value);
  absl::Status WritePackedDouble(double value);
  template <typename EnumType,
            std::enable_if_t<std::disjunction_v<std::is_enum<EnumType>,
                                                std::is_integral<EnumType>>,
                             int> = 0>
  absl::Status WritePackedEnum(EnumType value);

  // Begins accumulating contents of a length-delimited field.
  //
  // `writer().pos()` is remembered, and field contents written to `writer()`
  // are written directly to `dest()`.
  void OpenLengthDelimited();

  // Ends accumulating contents of a length-delimited field, and writes the
  // field tag and length to the parent message.
  //
  // Each `OpenLengthDelimited()` call must be matched with a
  // `CloseLengthDelimited()` or `CloseOptionalLengthDelimited()` call,
  // unless the `SerializedMessageBackwardWriter` and its `dest()` are no longer
  // used.
  absl::Status CloseLengthDelimited(int field_number);

  // Like `CloseLengthDelimited()`, but does not write the field tag and length
  // if its contents turn out to be empty.
  absl::Status CloseOptionalLengthDelimited(int field_number);

  // Writes the field tag and the length of a length-delimited field.
  //
  // The value must have been written beforehand to `writer()`, with exactly
  // `length` bytes, unless the `SerializedMessageBackwardWriter` and its
  // `dest()` are no longer used.
  //
  // Fails if `length` exceeds 2GiB.
  //
  // `WriteLengthUnchecked()` is slightly more efficient than
  // `OpenLengthDelimited()` or `NewLengthDelimited()` with
  // `CloseLengthDelimited()`, but harder to use: the length must be pledged
  // before writing the contents, and its correctness is not checked.
  //
  // Writing the contents and calling `WriteLengthUnchecked()` is done in the
  // opposite order than in `SerializedMessageWriter`.
  absl::Status WriteLengthUnchecked(int field_number, Position length);

  // Writes a group delimiter.
  //
  // Each `OpenGroup()` must be matched with a `CloseGroup()` call, unless the
  // `SerializedMessageBackwardWriter` and its `dest()` are no longer used.
  absl::Status OpenGroup(int field_number);
  absl::Status CloseGroup(int field_number);

 private:
  ABSL_ATTRIBUTE_COLD static absl::Status LengthOverflowError(Position length);
  ABSL_ATTRIBUTE_COLD static absl::Status CopyStringFailed(
      Reader& src, BackwardWriter& dest);

  BackwardWriter* dest_ = nullptr;
  std::vector<Position> submessages_;

  // Invariant: if `!submessages_.empty()` then `dest_ != nullptr`
};

// Implementation details follow.

inline SerializedMessageBackwardWriter::SerializedMessageBackwardWriter(
    SerializedMessageBackwardWriter&& that) noexcept
    : dest_(that.dest_), submessages_(std::exchange(that.submessages_, {})) {}

inline SerializedMessageBackwardWriter&
SerializedMessageBackwardWriter::operator=(
    SerializedMessageBackwardWriter&& that) noexcept {
  dest_ = that.dest_;
  submessages_ = std::exchange(that.submessages_, {});
  return *this;
}

inline void SerializedMessageBackwardWriter::set_dest(BackwardWriter* dest) {
  if (!submessages_.empty()) {
    RIEGELI_ASSERT_NE(dest, nullptr)
        << "Failed precondition of "
           "SerializedMessageBackwardWriter::set_dest(): "
           "null BackwardWriter pointer while writing a submessage";
    RIEGELI_ASSERT_EQ(dest->pos(), dest_->pos())
        << "Failed precondition of "
           "SerializedMessageBackwardWriter::set_dest(): "
           "pos() changes while writing a submessage";
  }
  dest_ = dest;
}

inline absl::Status SerializedMessageBackwardWriter::WriteInt32(
    int field_number, int32_t value) {
  return WriteUInt64(field_number, static_cast<uint64_t>(value));
}

inline absl::Status SerializedMessageBackwardWriter::WriteInt64(
    int field_number, int64_t value) {
  return WriteUInt64(field_number, static_cast<uint64_t>(value));
}

inline absl::Status SerializedMessageBackwardWriter::WriteUInt32(
    int field_number, uint32_t value) {
  const uint32_t tag = MakeTag(field_number, WireType::kVarint);
  const size_t length = LengthVarint32(tag) + LengthVarint32(value);
  if (ABSL_PREDICT_FALSE(!writer().Push(length))) return writer().status();
  writer().move_cursor(length);
  char* const ptr = WriteVarint32(tag, writer().cursor());
  WriteVarint32(value, ptr);
  return absl::OkStatus();
}

inline absl::Status SerializedMessageBackwardWriter::WriteUInt64(
    int field_number, uint64_t value) {
  const uint32_t tag = MakeTag(field_number, WireType::kVarint);
  const size_t length = LengthVarint32(tag) + LengthVarint64(value);
  if (ABSL_PREDICT_FALSE(!writer().Push(length))) return writer().status();
  writer().move_cursor(length);
  char* const ptr = WriteVarint32(tag, writer().cursor());
  WriteVarint64(value, ptr);
  return absl::OkStatus();
}

inline absl::Status SerializedMessageBackwardWriter::WriteSInt32(
    int field_number, int32_t value) {
  return WriteUInt32(field_number, EncodeVarintSigned32(value));
}

inline absl::Status SerializedMessageBackwardWriter::WriteSInt64(
    int field_number, int64_t value) {
  return WriteUInt64(field_number, EncodeVarintSigned64(value));
}

inline absl::Status SerializedMessageBackwardWriter::WriteBool(int field_number,
                                                               bool value) {
  return WriteUInt32(field_number, value ? 1 : 0);
}

inline absl::Status SerializedMessageBackwardWriter::WriteFixed32(
    int field_number, uint32_t value) {
  const uint32_t tag = MakeTag(field_number, WireType::kFixed32);
  const size_t length = LengthVarint32(tag) + sizeof(uint32_t);
  if (ABSL_PREDICT_FALSE(!writer().Push(length))) return writer().status();
  writer().move_cursor(length);
  char* const ptr = WriteVarint32(tag, writer().cursor());
  WriteLittleEndian32(value, ptr);
  return absl::OkStatus();
}

inline absl::Status SerializedMessageBackwardWriter::WriteFixed64(
    int field_number, uint64_t value) {
  const uint32_t tag = MakeTag(field_number, WireType::kFixed64);
  const size_t length = LengthVarint32(tag) + sizeof(uint64_t);
  if (ABSL_PREDICT_FALSE(!writer().Push(length))) return writer().status();
  writer().move_cursor(length);
  char* const ptr = WriteVarint32(tag, writer().cursor());
  WriteLittleEndian64(value, ptr);
  return absl::OkStatus();
}

inline absl::Status SerializedMessageBackwardWriter::WriteSFixed32(
    int field_number, int32_t value) {
  return WriteFixed32(field_number, static_cast<uint32_t>(value));
}

inline absl::Status SerializedMessageBackwardWriter::WriteSFixed64(
    int field_number, int64_t value) {
  return WriteFixed64(field_number, static_cast<uint64_t>(value));
}

inline absl::Status SerializedMessageBackwardWriter::WriteFloat(
    int field_number, float value) {
  return WriteFixed32(field_number, absl::bit_cast<uint32_t>(value));
}

inline absl::Status SerializedMessageBackwardWriter::WriteDouble(
    int field_number, double value) {
  return WriteFixed64(field_number, absl::bit_cast<uint64_t>(value));
}

template <typename EnumType,
          std::enable_if_t<std::disjunction_v<std::is_enum<EnumType>,
                                              std::is_integral<EnumType>>,
                           int>>
inline absl::Status SerializedMessageBackwardWriter::WriteEnum(int field_number,
                                                               EnumType value) {
  return WriteUInt64(field_number, static_cast<uint64_t>(value));
}

inline absl::Status SerializedMessageBackwardWriter::WritePackedInt32(
    int32_t value) {
  return WritePackedUInt64(static_cast<uint64_t>(value));
}

inline absl::Status SerializedMessageBackwardWriter::WritePackedInt64(
    int64_t value) {
  return WritePackedUInt64(static_cast<uint64_t>(value));
}

inline absl::Status SerializedMessageBackwardWriter::WritePackedUInt32(
    uint32_t value) {
  if (ABSL_PREDICT_FALSE(!WriteVarint32(value, writer()))) {
    return writer().status();
  }
  return absl::OkStatus();
}

inline absl::Status SerializedMessageBackwardWriter::WritePackedUInt64(
    uint64_t value) {
  if (ABSL_PREDICT_FALSE(!WriteVarint64(value, writer()))) {
    return writer().status();
  }
  return absl::OkStatus();
}

inline absl::Status SerializedMessageBackwardWriter::WritePackedSInt32(
    int32_t value) {
  return WritePackedUInt32(EncodeVarintSigned32(value));
}

inline absl::Status SerializedMessageBackwardWriter::WritePackedSInt64(
    int64_t value) {
  return WritePackedUInt64(EncodeVarintSigned64(value));
}

inline absl::Status SerializedMessageBackwardWriter::WritePackedBool(
    bool value) {
  return WritePackedUInt32(value ? 1 : 0);
}

inline absl::Status SerializedMessageBackwardWriter::WritePackedFixed32(
    uint32_t value) {
  if (ABSL_PREDICT_FALSE(!WriteLittleEndian32(value, writer()))) {
    return writer().status();
  }
  return absl::OkStatus();
}

inline absl::Status SerializedMessageBackwardWriter::WritePackedFixed64(
    uint64_t value) {
  if (ABSL_PREDICT_FALSE(!WriteLittleEndian64(value, writer()))) {
    return writer().status();
  }
  return absl::OkStatus();
}

inline absl::Status SerializedMessageBackwardWriter::WritePackedSFixed32(
    int32_t value) {
  return WritePackedFixed32(static_cast<uint32_t>(value));
}

inline absl::Status SerializedMessageBackwardWriter::WritePackedSFixed64(
    int64_t value) {
  return WritePackedFixed64(static_cast<uint64_t>(value));
}

inline absl::Status SerializedMessageBackwardWriter::WritePackedFloat(
    float value) {
  return WritePackedFixed32(absl::bit_cast<uint32_t>(value));
}

inline absl::Status SerializedMessageBackwardWriter::WritePackedDouble(
    double value) {
  return WritePackedFixed64(absl::bit_cast<uint64_t>(value));
}

template <typename EnumType,
          std::enable_if_t<std::disjunction_v<std::is_enum<EnumType>,
                                              std::is_integral<EnumType>>,
                           int>>
inline absl::Status SerializedMessageBackwardWriter::WritePackedEnum(
    EnumType value) {
  return WritePackedUInt64(static_cast<uint64_t>(value));
}

template <typename... Values
#if !__cpp_concepts
          ,
          std::enable_if_t<IsStringifiable<Values...>::value, int>
#endif
          >
inline absl::Status SerializedMessageBackwardWriter::WriteString(
    int field_number, Values&&... values)
#if __cpp_concepts
  requires(IsStringifiable<Values>::value && ...)
#endif
{
  const Position pos_before = writer().pos();
  if (ABSL_PREDICT_FALSE(!writer().Write(std::forward<Values>(values)...))) {
    return writer().status();
  }
  RIEGELI_ASSERT_GE(writer().pos(), pos_before)
      << "BackwardWriter::Write() decreased pos()";
  return WriteLengthUnchecked(field_number, writer().pos() - pos_before);
}

inline absl::Status SerializedMessageBackwardWriter::WriteSerializedMessage(
    int field_number, const google::protobuf::MessageLite& message,
    SerializeMessageOptions options) {
  const size_t length = options.GetByteSize(message);
  if (absl::Status status =
          riegeli::SerializeMessage(message, writer(), options);
      ABSL_PREDICT_FALSE(!status.ok())) {
    return status;
  }
  return WriteLengthUnchecked(field_number, length);
}

template <typename ReaderType>
absl::Status SerializedMessageBackwardWriter::CopyString(
    int field_number, ReaderSpan<ReaderType> src) {
  if (ABSL_PREDICT_FALSE(src.length() >
                         uint32_t{std::numeric_limits<int32_t>::max()})) {
    return LengthOverflowError(src.length());
  }
  if (ABSL_PREDICT_FALSE(
          !src.reader().Copy(IntCast<size_t>(src.length()), writer()))) {
    return CopyStringFailed(src.reader(), writer());
  }
  return WriteLengthUnchecked(field_number, src.length());
}

inline absl::Status SerializedMessageBackwardWriter::WriteLengthUnchecked(
    int field_number, Position length) {
  if (ABSL_PREDICT_FALSE(length >
                         uint32_t{std::numeric_limits<int32_t>::max()})) {
    return LengthOverflowError(length);
  }
  const uint32_t tag = MakeTag(field_number, WireType::kLengthDelimited);
  const size_t header_length =
      LengthVarint32(tag) + LengthVarint32(IntCast<uint32_t>(length));
  if (ABSL_PREDICT_FALSE(!writer().Push(header_length))) {
    return writer().status();
  }
  writer().move_cursor(header_length);
  char* const ptr = WriteVarint32(tag, writer().cursor());
  WriteVarint32(IntCast<uint32_t>(length), ptr);
  return absl::OkStatus();
}

inline absl::Status SerializedMessageBackwardWriter::OpenGroup(
    int field_number) {
  if (ABSL_PREDICT_FALSE(!WriteVarint32(
          MakeTag(field_number, WireType::kEndGroup), writer()))) {
    return writer().status();
  }
  return absl::OkStatus();
}

inline absl::Status SerializedMessageBackwardWriter::CloseGroup(
    int field_number) {
  if (ABSL_PREDICT_FALSE(!WriteVarint32(
          MakeTag(field_number, WireType::kStartGroup), writer()))) {
    return writer().status();
  }
  return absl::OkStatus();
}

}  // namespace riegeli

#endif  // RIEGELI_MESSAGES_SERIALIZED_MESSAGE_BACKWARD_WRITER_H_
