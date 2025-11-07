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

#ifndef RIEGELI_MESSAGES_SERIALIZED_MESSAGE_WRITER_H_
#define RIEGELI_MESSAGES_SERIALIZED_MESSAGE_WRITER_H_

#include <stdint.h>

#include <limits>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/base/casts.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/any.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/constexpr.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/cord_writer.h"
#include "riegeli/bytes/limiting_reader.h"
#include "riegeli/bytes/null_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/stringify.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/endian/endian_writing.h"
#include "riegeli/messages/message_wire_format.h"
#include "riegeli/messages/serialize_message.h"
#include "riegeli/varint/varint_writing.h"

namespace riegeli {

// `SerializedMessageWriter` builds a serialized proto message, specifying
// contents of particular fields, instead of traversing an in-memory message
// object like in `SerializeMessage()`.
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
// Functions working on strings are applicable to any length-delimited field:
// `string`, `bytes`, submessage, or a packed repeated field.
class SerializedMessageWriter {
 public:
  // An empty object. It can be associated with a particular message by
  // `set_dest()` or assignment.
  //
  // An empty `SerializedMessageWriter` is not usable directly, except that
  // submessage contents can be accumulated after `OpenLengthDelimited()`
  // if `set_dest()` is called before the matching `CloseLengthDelimited()`
  // or `CloseOptionalLengthDelimited()`.
  SerializedMessageWriter() = default;

  // Will write to `*dest`, which is not owned and must outlive usages of this
  // object.
  explicit SerializedMessageWriter(Writer* dest ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : dest_(dest), writer_(dest) {}

  SerializedMessageWriter(SerializedMessageWriter&& that) noexcept;
  SerializedMessageWriter& operator=(SerializedMessageWriter&& that) noexcept;

  // Returns the original `Writer` of the root message.
  Writer* dest() const { return dest_; }

  // Changes the `Writer` of the root message.
  //
  // This can be called even during building, even when submessages are open.
  // It particular this must be called when the original `Writer` has been
  // moved.
  void set_dest(Writer* dest);

  // Returns the `Writer` of the current message or length-delimited field being
  // built. This can be the original `Writer` of the root message, or the
  // `Writer` of a field.
  //
  // This can be used to write parts of the message directly, apart from
  // `Write*()` functions which write whole fields.
  Writer& writer() ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT_NE(writer_, nullptr)
        << "Failed precondition of SerializedMessageWriter::writer(): "
           "dest() not set while writing the root message";
    return *writer_;
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
  // The field must have been opened with `OpenLengthDelimited()`,
  // `NewLengthDelimited()`, or `WriteLengthUnchecked()`.
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
  // Field contents written to `writer()` are accumulated in memory until
  // `CloseLengthDelimited()` or `CloseOptionalLengthDelimited()` is called.
  //
  // If `OpenLengthDelimited()` is used a lot and building the message back to
  // front is feasible, then `SerializedMessageBackwardWriter` is more
  // efficient.
  void OpenLengthDelimited();

  // Returns a new `SerializedMessageWriter` which accumulates contents of a
  // length-delimited field of this `SerializedMessageWriter`.
  //
  // The contents are written to the parent `SerializedMessageWriter` by the
  // matching `CloseLengthDelimited()` or `CloseOptionalLengthDelimited()` call
  // on the returned `SerializedMessageWriter`. Multiple such
  // `SerializedMessageWriter` objects can be active at the same time.
  SerializedMessageWriter NewLengthDelimited() ABSL_ATTRIBUTE_LIFETIME_BOUND;

  // Ends accumulating contents of a length-delimited field, and writes the
  // field to the parent message.
  //
  // Each `OpenLengthDelimited()` or `NewLengthDelimited()` call must be matched
  // with a `CloseLengthDelimited()` or `CloseOptionalLengthDelimited()` call,
  // unless the `SerializedMessageWriter` is no longer used.
  absl::Status CloseLengthDelimited(int field_number);

  // Like `CloseLengthDelimited()`, but does not write the field if its contents
  // turn out to be empty.
  absl::Status CloseOptionalLengthDelimited(int field_number);

  // Writes the field tag and the length of a length-delimited field.
  //
  // The value must be written afterwards to `writer()`, with exactly `length`
  // bytes, unless the `SerializedMessageWriter` and its `dest()` are no longer
  // used.
  //
  // Fails if `length` exceeds 2GiB.
  //
  // `WriteLengthUnchecked()` is more efficient than `OpenLengthDelimited()`
  // or `NewLengthDelimited()` with `CloseLengthDelimited()`, but harder to use:
  // the length must be pledged before writing the contents, and its correctness
  // is not checked.
  absl::Status WriteLengthUnchecked(int field_number, Position length);

  // Writes a group delimiter.
  //
  // Each `OpenGroup()` must be matched with a `CloseGroup()` call, unless the
  // `SerializedMessageWriter` and its `dest()` are no longer used.
  absl::Status OpenGroup(int field_number);
  absl::Status CloseGroup(int field_number);

  // Returns the length of the field which would be written.
  //
  // This is useful for `WriteLengthUnchecked()`.
  //
  // If writing would fail due to overflow of a length delimited field,
  // an unspecified value is returned.
  static Position LengthOfInt32(int field_number, int32_t value);
  static Position LengthOfInt64(int field_number, int64_t value);
  static Position LengthOfUInt32(int field_number, uint32_t value);
  static Position LengthOfUInt64(int field_number, uint64_t value);
  static Position LengthOfSInt32(int field_number, int32_t value);
  static Position LengthOfSInt64(int field_number, int64_t value);
  static Position LengthOfBool(int field_number);
  static Position LengthOfFixed32(int field_number);
  static Position LengthOfFixed64(int field_number);
  static Position LengthOfSFixed32(int field_number);
  static Position LengthOfSFixed64(int field_number);
  static Position LengthOfFloat(int field_number);
  static Position LengthOfDouble(int field_number);
  template <typename EnumType,
            std::enable_if_t<std::disjunction_v<std::is_enum<EnumType>,
                                                std::is_integral<EnumType>>,
                             int> = 0>
  static Position LengthOfEnum(int field_number, EnumType value);
  template <typename... Values
#if !__cpp_concepts
            ,
            std::enable_if_t<IsStringifiable<Values...>::value, int> = 0
#endif
            >
  static Position LengthOfString(int field_number, const Values&... values)
#if __cpp_concepts
      // For conjunctions, `requires` gives better error messages than
      // `std::enable_if_t`, indicating the relevant argument.
    requires(IsStringifiable<Values>::value && ...)
#endif
  ;
  static Position LengthOfLengthDelimited(int field_number, Position length);
  static Position LengthOfOptionalLengthDelimited(int field_number,
                                                  Position length);
  static Position LengthOfPackedInt32(int32_t value);
  static Position LengthOfPackedInt64(int64_t value);
  static Position LengthOfPackedUInt32(uint32_t value);
  static Position LengthOfPackedUInt64(uint64_t value);
  static Position LengthOfPackedSInt32(int32_t value);
  static Position LengthOfPackedSInt64(int64_t value);
  static Position LengthOfPackedBool();
  static Position LengthOfPackedFixed32();
  static Position LengthOfPackedFixed64();
  static Position LengthOfPackedSFixed32();
  static Position LengthOfPackedSFixed64();
  static Position LengthOfPackedFloat();
  static Position LengthOfPackedDouble();
  template <typename EnumType,
            std::enable_if_t<std::disjunction_v<std::is_enum<EnumType>,
                                                std::is_integral<EnumType>>,
                             int> = 0>
  static Position LengthOfPackedEnum(EnumType value);
  static Position LengthOfOpenPlusCloseGroup(int field_number);

 private:
  ABSL_ATTRIBUTE_COLD static absl::Status LengthOverflowError(Position length);
  ABSL_ATTRIBUTE_COLD static absl::Status CopyStringFailed(Reader& src,
                                                           Writer& dest);

  Writer* dest_ = nullptr;
  std::vector<CordWriter<absl::Cord>> submessages_;
  Writer* writer_ = nullptr;

  // Invariant:
  //   `writer_ == (submessages_.empty() ? dest_ : &submessages_.back())`
};

// Implementation details follow.

inline void SerializedMessageWriter::set_dest(Writer* dest) {
  dest_ = dest;
  if (submessages_.empty()) writer_ = dest;
}

inline SerializedMessageWriter::SerializedMessageWriter(
    SerializedMessageWriter&& that) noexcept
    : dest_(that.dest_),
      submessages_(std::exchange(that.submessages_, {})),
      // This relies on pointer stability when `std::vector` is moved.
      writer_(std::exchange(that.writer_, that.dest_)) {}

inline SerializedMessageWriter& SerializedMessageWriter::operator=(
    SerializedMessageWriter&& that) noexcept {
  dest_ = that.dest_;
  submessages_ = std::exchange(that.submessages_, {});
  // This relies on pointer stability when `std::vector` is moved.
  writer_ = std::exchange(that.writer_, that.dest_);
  return *this;
}

inline absl::Status SerializedMessageWriter::WriteInt32(int field_number,
                                                        int32_t value) {
  return WriteUInt64(field_number, static_cast<uint64_t>(value));
}

inline absl::Status SerializedMessageWriter::WriteInt64(int field_number,
                                                        int64_t value) {
  return WriteUInt64(field_number, static_cast<uint64_t>(value));
}

inline absl::Status SerializedMessageWriter::WriteUInt32(int field_number,
                                                         uint32_t value) {
  const uint32_t tag = MakeTag(field_number, WireType::kVarint);
  if (ABSL_PREDICT_FALSE(!writer().Push(
          (RIEGELI_IS_CONSTANT(tag) ||
                   (RIEGELI_IS_CONSTANT(tag < 0x80) && tag < 0x80)
               ? LengthVarint32(tag)
               : kMaxLengthVarint32) +
          (RIEGELI_IS_CONSTANT(value) ||
                   (RIEGELI_IS_CONSTANT(value < 0x80) && value < 0x80)
               ? LengthVarint32(value)
               : kMaxLengthVarint32)))) {
    return writer().status();
  }
  char* ptr = WriteVarint32(tag, writer().cursor());
  ptr = WriteVarint32(value, ptr);
  writer().set_cursor(ptr);
  return absl::OkStatus();
}

inline absl::Status SerializedMessageWriter::WriteUInt64(int field_number,
                                                         uint64_t value) {
  const uint32_t tag = MakeTag(field_number, WireType::kVarint);
  if (ABSL_PREDICT_FALSE(!writer().Push(
          (RIEGELI_IS_CONSTANT(tag) ||
                   (RIEGELI_IS_CONSTANT(tag < 0x80) && tag < 0x80)
               ? LengthVarint32(tag)
               : kMaxLengthVarint32) +
          (RIEGELI_IS_CONSTANT(value) ||
                   (RIEGELI_IS_CONSTANT(value < 0x80) && value < 0x80)
               ? LengthVarint64(value)
               : kMaxLengthVarint64)))) {
    return writer().status();
  }
  char* ptr = WriteVarint32(tag, writer().cursor());
  ptr = WriteVarint64(value, ptr);
  writer().set_cursor(ptr);
  return absl::OkStatus();
}

inline absl::Status SerializedMessageWriter::WriteSInt32(int field_number,
                                                         int32_t value) {
  return WriteUInt32(field_number, EncodeVarintSigned32(value));
}

inline absl::Status SerializedMessageWriter::WriteSInt64(int field_number,
                                                         int64_t value) {
  return WriteUInt64(field_number, EncodeVarintSigned64(value));
}

inline absl::Status SerializedMessageWriter::WriteBool(int field_number,
                                                       bool value) {
  return WriteUInt32(field_number, value ? 1 : 0);
}

inline absl::Status SerializedMessageWriter::WriteFixed32(int field_number,
                                                          uint32_t value) {
  const uint32_t tag = MakeTag(field_number, WireType::kFixed32);
  if (ABSL_PREDICT_FALSE(!writer().Push(
          (RIEGELI_IS_CONSTANT(tag) ||
                   (RIEGELI_IS_CONSTANT(tag < 0x80) && tag < 0x80)
               ? LengthVarint32(tag)
               : kMaxLengthVarint32) +
          sizeof(uint32_t)))) {
    return writer().status();
  }
  char* ptr = WriteVarint32(tag, writer().cursor());
  WriteLittleEndian32(value, ptr);
  ptr += sizeof(uint32_t);
  writer().set_cursor(ptr);
  return absl::OkStatus();
}

inline absl::Status SerializedMessageWriter::WriteFixed64(int field_number,
                                                          uint64_t value) {
  const uint32_t tag = MakeTag(field_number, WireType::kFixed64);
  if (ABSL_PREDICT_FALSE(!writer().Push(
          (RIEGELI_IS_CONSTANT(tag) ||
                   (RIEGELI_IS_CONSTANT(tag < 0x80) && tag < 0x80)
               ? LengthVarint32(tag)
               : kMaxLengthVarint32) +
          sizeof(uint64_t)))) {
    return writer().status();
  }
  char* ptr = WriteVarint32(tag, writer().cursor());
  WriteLittleEndian64(value, ptr);
  ptr += sizeof(uint64_t);
  writer().set_cursor(ptr);
  return absl::OkStatus();
}

inline absl::Status SerializedMessageWriter::WriteSFixed32(int field_number,
                                                           int32_t value) {
  return WriteFixed32(field_number, static_cast<uint32_t>(value));
}

inline absl::Status SerializedMessageWriter::WriteSFixed64(int field_number,
                                                           int64_t value) {
  return WriteFixed64(field_number, static_cast<uint64_t>(value));
}

inline absl::Status SerializedMessageWriter::WriteFloat(int field_number,
                                                        float value) {
  return WriteFixed32(field_number, absl::bit_cast<uint32_t>(value));
}

inline absl::Status SerializedMessageWriter::WriteDouble(int field_number,
                                                         double value) {
  return WriteFixed64(field_number, absl::bit_cast<uint64_t>(value));
}

template <typename EnumType,
          std::enable_if_t<std::disjunction_v<std::is_enum<EnumType>,
                                              std::is_integral<EnumType>>,
                           int>>
inline absl::Status SerializedMessageWriter::WriteEnum(int field_number,
                                                       EnumType value) {
  return WriteUInt64(field_number, static_cast<uint64_t>(value));
}

inline absl::Status SerializedMessageWriter::WritePackedInt32(int32_t value) {
  return WritePackedUInt64(static_cast<uint64_t>(value));
}

inline absl::Status SerializedMessageWriter::WritePackedInt64(int64_t value) {
  return WritePackedUInt64(static_cast<uint64_t>(value));
}

inline absl::Status SerializedMessageWriter::WritePackedUInt32(uint32_t value) {
  if (ABSL_PREDICT_FALSE(!WriteVarint32(value, writer()))) {
    return writer().status();
  }
  return absl::OkStatus();
}

inline absl::Status SerializedMessageWriter::WritePackedUInt64(uint64_t value) {
  if (ABSL_PREDICT_FALSE(!WriteVarint64(value, writer()))) {
    return writer().status();
  }
  return absl::OkStatus();
}

inline absl::Status SerializedMessageWriter::WritePackedSInt32(int32_t value) {
  return WritePackedUInt32(EncodeVarintSigned32(value));
}

inline absl::Status SerializedMessageWriter::WritePackedSInt64(int64_t value) {
  return WritePackedUInt64(EncodeVarintSigned64(value));
}

inline absl::Status SerializedMessageWriter::WritePackedBool(bool value) {
  return WritePackedUInt32(value ? 1 : 0);
}

inline absl::Status SerializedMessageWriter::WritePackedFixed32(
    uint32_t value) {
  if (ABSL_PREDICT_FALSE(!WriteLittleEndian32(value, writer()))) {
    return writer().status();
  }
  return absl::OkStatus();
}

inline absl::Status SerializedMessageWriter::WritePackedFixed64(
    uint64_t value) {
  if (ABSL_PREDICT_FALSE(!WriteLittleEndian64(value, writer()))) {
    return writer().status();
  }
  return absl::OkStatus();
}

inline absl::Status SerializedMessageWriter::WritePackedSFixed32(
    int32_t value) {
  return WritePackedFixed32(static_cast<uint32_t>(value));
}

inline absl::Status SerializedMessageWriter::WritePackedSFixed64(
    int64_t value) {
  return WritePackedFixed64(static_cast<uint64_t>(value));
}

inline absl::Status SerializedMessageWriter::WritePackedFloat(float value) {
  return WritePackedFixed32(absl::bit_cast<uint32_t>(value));
}

inline absl::Status SerializedMessageWriter::WritePackedDouble(double value) {
  return WritePackedFixed64(absl::bit_cast<uint64_t>(value));
}

template <typename EnumType,
          std::enable_if_t<std::disjunction_v<std::is_enum<EnumType>,
                                              std::is_integral<EnumType>>,
                           int>>
inline absl::Status SerializedMessageWriter::WritePackedEnum(EnumType value) {
  return WritePackedUInt64(static_cast<uint64_t>(value));
}

template <typename... Values
#if !__cpp_concepts
          ,
          std::enable_if_t<IsStringifiable<Values...>::value, int>
#endif
          >
inline absl::Status SerializedMessageWriter::WriteString(int field_number,
                                                         Values&&... values)
#if __cpp_concepts
  requires(IsStringifiable<Values>::value && ...)
#endif
{
  if constexpr (HasStringifiedSize<Values...>::value) {
    if (absl::Status status = WriteLengthUnchecked(
            field_number, riegeli::StringifiedSize(values...));
        ABSL_PREDICT_FALSE(!status.ok())) {
      return status;
    }
    if (ABSL_PREDICT_FALSE(!writer().Write(std::forward<Values>(values)...))) {
      return writer().status();
    }
  } else {
    CordWriter cord_writer;
    if (ABSL_PREDICT_FALSE(
            !cord_writer.Write(std::forward<Values>(values)...) ||
            !cord_writer.Close())) {
      return cord_writer.status();
    }
    if (absl::Status status =
            WriteLengthUnchecked(field_number, cord_writer.dest().size());
        ABSL_PREDICT_FALSE(!status.ok())) {
      return status;
    }
    if (ABSL_PREDICT_FALSE(!writer().Write(std::move(cord_writer.dest())))) {
      return writer().status();
    }
  }
  return absl::OkStatus();
}

template <typename ReaderType>
absl::Status SerializedMessageWriter::CopyString(int field_number,
                                                 ReaderSpan<ReaderType> src) {
  if (absl::Status status = WriteLengthUnchecked(field_number, src.length());
      ABSL_PREDICT_FALSE(!status.ok())) {
    return status;
  }
  if (ABSL_PREDICT_FALSE(!src.reader().Copy(src.length(), writer()))) {
    return CopyStringFailed(src.reader(), writer());
  }
  return absl::OkStatus();
}

inline absl::Status SerializedMessageWriter::WriteSerializedMessage(
    int field_number, const google::protobuf::MessageLite& message,
    SerializeMessageOptions options) {
  if (absl::Status status =
          WriteLengthUnchecked(field_number, options.GetByteSize(message));
      ABSL_PREDICT_FALSE(!status.ok())) {
    return status;
  }
  return riegeli::SerializeMessage(message, writer(), options);
}

inline absl::Status SerializedMessageWriter::WriteLengthUnchecked(
    int field_number, Position length) {
  if (ABSL_PREDICT_FALSE(length >
                         uint32_t{std::numeric_limits<int32_t>::max()})) {
    return LengthOverflowError(length);
  }
  const uint32_t tag = MakeTag(field_number, WireType::kLengthDelimited);
  if (ABSL_PREDICT_FALSE(!writer().Push(
          (RIEGELI_IS_CONSTANT(tag) ||
                   (RIEGELI_IS_CONSTANT(tag < 0x80) && tag < 0x80)
               ? LengthVarint32(tag)
               : kMaxLengthVarint32) +
          (RIEGELI_IS_CONSTANT(length) ||
                   (RIEGELI_IS_CONSTANT(length < 0x80) && length < 0x80)
               ? LengthVarint32(IntCast<uint32_t>(length))
               : kMaxLengthVarint32)))) {
    return writer().status();
  }
  char* ptr = WriteVarint32(tag, writer().cursor());
  ptr = WriteVarint32(IntCast<uint32_t>(length), ptr);
  writer().set_cursor(ptr);
  return absl::OkStatus();
}

inline absl::Status SerializedMessageWriter::OpenGroup(int field_number) {
  if (ABSL_PREDICT_FALSE(!WriteVarint32(
          MakeTag(field_number, WireType::kStartGroup), writer()))) {
    return writer().status();
  }
  return absl::OkStatus();
}

inline absl::Status SerializedMessageWriter::CloseGroup(int field_number) {
  if (ABSL_PREDICT_FALSE(!WriteVarint32(
          MakeTag(field_number, WireType::kEndGroup), writer()))) {
    return writer().status();
  }
  return absl::OkStatus();
}

inline Position SerializedMessageWriter::LengthOfInt32(int field_number,
                                                       int32_t value) {
  return LengthOfUInt64(field_number, static_cast<uint64_t>(value));
}

inline Position SerializedMessageWriter::LengthOfInt64(int field_number,
                                                       int64_t value) {
  return LengthOfUInt64(field_number, static_cast<uint64_t>(value));
}

inline Position SerializedMessageWriter::LengthOfUInt32(int field_number,
                                                        uint32_t value) {
  return LengthVarint32(MakeTag(field_number, WireType::kVarint)) +
         LengthVarint32(value);
}

inline Position SerializedMessageWriter::LengthOfUInt64(int field_number,
                                                        uint64_t value) {
  return LengthVarint32(MakeTag(field_number, WireType::kVarint)) +
         LengthVarint64(value);
}

inline Position SerializedMessageWriter::LengthOfSInt32(int field_number,
                                                        int32_t value) {
  return LengthOfUInt32(field_number, EncodeVarintSigned32(value));
}

inline Position SerializedMessageWriter::LengthOfSInt64(int field_number,
                                                        int64_t value) {
  return LengthOfUInt64(field_number, EncodeVarintSigned64(value));
}

inline Position SerializedMessageWriter::LengthOfBool(int field_number) {
  return LengthVarint32(MakeTag(field_number, WireType::kVarint)) + 1;
}

inline Position SerializedMessageWriter::LengthOfFixed32(int field_number) {
  return LengthVarint32(MakeTag(field_number, WireType::kFixed32)) +
         sizeof(uint32_t);
}

inline Position SerializedMessageWriter::LengthOfFixed64(int field_number) {
  return LengthVarint32(MakeTag(field_number, WireType::kFixed64)) +
         sizeof(uint64_t);
}

inline Position SerializedMessageWriter::LengthOfSFixed32(int field_number) {
  return LengthOfFixed32(field_number);
}

inline Position SerializedMessageWriter::LengthOfSFixed64(int field_number) {
  return LengthOfFixed64(field_number);
}

inline Position SerializedMessageWriter::LengthOfFloat(int field_number) {
  return LengthOfFixed32(field_number);
}

inline Position SerializedMessageWriter::LengthOfDouble(int field_number) {
  return LengthOfFixed64(field_number);
}

template <typename EnumType,
          std::enable_if_t<std::disjunction_v<std::is_enum<EnumType>,
                                              std::is_integral<EnumType>>,
                           int>>
inline Position SerializedMessageWriter::LengthOfEnum(int field_number,
                                                      EnumType value) {
  return LengthOfUInt64(field_number, static_cast<uint64_t>(value));
}

inline Position SerializedMessageWriter::LengthOfPackedInt32(int32_t value) {
  return LengthOfPackedUInt64(static_cast<uint64_t>(value));
}

inline Position SerializedMessageWriter::LengthOfPackedInt64(int64_t value) {
  return LengthOfPackedUInt64(static_cast<uint64_t>(value));
}

inline Position SerializedMessageWriter::LengthOfPackedUInt32(uint32_t value) {
  return LengthVarint32(value);
}

inline Position SerializedMessageWriter::LengthOfPackedUInt64(uint64_t value) {
  return LengthVarint64(value);
}

inline Position SerializedMessageWriter::LengthOfPackedSInt32(int32_t value) {
  return LengthOfPackedUInt32(EncodeVarintSigned32(value));
}

inline Position SerializedMessageWriter::LengthOfPackedSInt64(int64_t value) {
  return LengthOfPackedUInt64(EncodeVarintSigned64(value));
}

inline Position SerializedMessageWriter::LengthOfPackedBool() { return 1; }

inline Position SerializedMessageWriter::LengthOfPackedFixed32() {
  return sizeof(uint32_t);
}

inline Position SerializedMessageWriter::LengthOfPackedFixed64() {
  return sizeof(uint64_t);
}

inline Position SerializedMessageWriter::LengthOfPackedSFixed32() {
  return LengthOfPackedFixed32();
}

inline Position SerializedMessageWriter::LengthOfPackedSFixed64() {
  return LengthOfPackedFixed64();
}

inline Position SerializedMessageWriter::LengthOfPackedFloat() {
  return LengthOfPackedFixed32();
}

inline Position SerializedMessageWriter::LengthOfPackedDouble() {
  return LengthOfPackedFixed64();
}

template <typename EnumType,
          std::enable_if_t<std::disjunction_v<std::is_enum<EnumType>,
                                              std::is_integral<EnumType>>,
                           int>>
inline Position SerializedMessageWriter::LengthOfPackedEnum(EnumType value) {
  return LengthOfPackedUInt64(static_cast<uint64_t>(value));
}

template <typename... Values
#if !__cpp_concepts
          ,
          std::enable_if_t<IsStringifiable<Values...>::value, int>
#endif
          >
inline Position SerializedMessageWriter::LengthOfString(int field_number,
                                                        const Values&... values)
#if __cpp_concepts
  requires(IsStringifiable<Values>::value && ...)
#endif
{
  if constexpr (HasStringifiedSize<Values...>::value) {
    return LengthOfLengthDelimited(field_number,
                                   riegeli::StringifiedSize(values...));
  } else {
    NullWriter null_writer;
    null_writer.Write(values...);
    null_writer.Close();
    return LengthOfLengthDelimited(field_number, null_writer.pos());
  }
}

inline Position SerializedMessageWriter::LengthOfLengthDelimited(
    int field_number, Position length) {
  return LengthVarint32(MakeTag(field_number, WireType::kLengthDelimited)) +
         LengthVarint32(IntCast<uint32_t>(length)) + length;
}

inline Position SerializedMessageWriter::LengthOfOptionalLengthDelimited(
    int field_number, Position length) {
  return length == 0 ? 0 : LengthOfLengthDelimited(field_number, length);
}

inline Position SerializedMessageWriter::LengthOfOpenPlusCloseGroup(
    int field_number) {
  return 2 * LengthVarint32(MakeTag(field_number, WireType::kStartGroup));
}

}  // namespace riegeli

#endif  // RIEGELI_MESSAGES_SERIALIZED_MESSAGE_WRITER_H_
