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

#include "riegeli/messages/parse_message.h"

#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <optional>
#include <string>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/io/zero_copy_stream_impl_lite.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/bytes_ref.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/cord_reader.h"
#include "riegeli/bytes/limiting_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/varint/varint_reading.h"

namespace riegeli {

namespace {

ABSL_ATTRIBUTE_COLD inline absl::Status ParseError(
    google::protobuf::MessageLite& dest) {
  return absl::InvalidArgumentError(
      absl::StrCat("Failed to parse message of type ", dest.GetTypeName()));
}

ABSL_ATTRIBUTE_COLD inline absl::Status ParseError(
    Reader& src, google::protobuf::MessageLite& dest) {
  return src.AnnotateStatus(ParseError(dest));
}

inline absl::Status CheckInitialized(google::protobuf::MessageLite& dest,
                                     ParseMessageOptions options) {
  if (!options.partial() && ABSL_PREDICT_FALSE(!dest.IsInitialized())) {
    return absl::InvalidArgumentError(
        absl::StrCat("Failed to parse message of type ", dest.GetTypeName(),
                     " because it is missing required fields: ",
                     dest.InitializationErrorString()));
  }
  return absl::OkStatus();
}

inline absl::Status CheckInitialized(Reader& src,
                                     google::protobuf::MessageLite& dest,
                                     ParseMessageOptions options) {
  if (!options.partial() && ABSL_PREDICT_FALSE(!dest.IsInitialized())) {
    return src.AnnotateStatus(absl::InvalidArgumentError(
        absl::StrCat("Failed to parse message of type ", dest.GetTypeName(),
                     " because it is missing required fields: ",
                     dest.InitializationErrorString())));
  }
  return absl::OkStatus();
}

template <typename ReaderType>
absl::Status ParseMessageFromSpanImpl(ReaderSpan<ReaderType> src,
                                      google::protobuf::MessageLite& dest,
                                      ParseMessageOptions options) {
  if (!options.merge() &&
      options.recursion_limit() ==
          google::protobuf::io::CodedInputStream::GetDefaultRecursionLimit() &&
      src.length() <= kMaxBytesToCopy) {
    src.reader().Pull();
    if (src.reader().available() >= src.length()) {
      // The data are flat. `ParsePartialFromArray()` is faster than
      // `ParsePartialFromZeroCopyStream()`.
      const bool parse_ok = dest.ParsePartialFromArray(
          src.reader().cursor(), IntCast<int>(src.length()));
      src.reader().move_cursor(IntCast<size_t>(src.length()));
      if (ABSL_PREDICT_FALSE(!parse_ok)) return ParseError(src.reader(), dest);
      return CheckInitialized(src.reader(), dest, options);
    }
  }
  ScopedLimiterOrLimitingReader scoped_limiter(src);
  ReaderInputStream input_stream(&scoped_limiter.reader());
  bool parse_ok;
  if (!options.merge() &&
      options.recursion_limit() ==
          google::protobuf::io::CodedInputStream::GetDefaultRecursionLimit()) {
    parse_ok = dest.ParsePartialFromZeroCopyStream(&input_stream);
  } else {
    if (!options.merge()) dest.Clear();
    google::protobuf::io::CodedInputStream coded_stream(&input_stream);
    coded_stream.SetRecursionLimit(options.recursion_limit());
    parse_ok = dest.MergePartialFromCodedStream(&coded_stream) &&
               coded_stream.ConsumedEntireMessage();
  }
  if (ABSL_PREDICT_FALSE(!scoped_limiter.reader().ok())) {
    return scoped_limiter.reader().status();
  }
  if (ABSL_PREDICT_FALSE(!parse_ok)) {
    return ParseError(scoped_limiter.reader(), dest);
  }
  const absl::Status status =
      CheckInitialized(scoped_limiter.reader(), dest, options);
  RIEGELI_EVAL_ASSERT(scoped_limiter.Close())
      << "LimitingReader with !fail_if_longer() "
         "has no reason to fail only in Close(): "
      << scoped_limiter.reader().status();
  return status;
}

template <typename Src>
absl::Status ParseLengthPrefixedMessageImpl(Src& src,
                                            google::protobuf::MessageLite& dest,
                                            ParseMessageOptions options) {
  uint32_t length;
  if (ABSL_PREDICT_FALSE(!ReadVarint32(src, length) ||
                         length >
                             uint32_t{std::numeric_limits<int32_t>::max()})) {
    return src.StatusOrAnnotate(
        absl::InvalidArgumentError("Failed to parse message length"));
  }
  return parse_message_internal::ParseMessageImpl(
      ReaderSpan(&src, Position{length}), dest, options);
}

}  // namespace

namespace parse_message_internal {

absl::Status ParseMessageImpl(Reader& src, google::protobuf::MessageLite& dest,
                              ParseMessageOptions options) {
  if (!options.merge() &&
      options.recursion_limit() ==
          google::protobuf::io::CodedInputStream::GetDefaultRecursionLimit() &&
      src.SupportsSize()) {
    const std::optional<Position> size = src.Size();
    if (ABSL_PREDICT_FALSE(size == std::nullopt)) return src.status();
    src.Pull();
    if (src.limit_pos() == *size && src.available() <= kMaxBytesToCopy) {
      // The data are flat. `ParsePartialFromArray()` is faster than
      // `ParsePartialFromZeroCopyStream()`.
      const bool parse_ok = dest.ParsePartialFromArray(
          src.cursor(), IntCast<int>(src.available()));
      src.move_cursor(src.available());
      if (ABSL_PREDICT_FALSE(!parse_ok)) return ParseError(src, dest);
      return CheckInitialized(src, dest, options);
    }
  }
  ReaderInputStream input_stream(&src);
  bool parse_ok;
  if (!options.merge() &&
      options.recursion_limit() ==
          google::protobuf::io::CodedInputStream::GetDefaultRecursionLimit()) {
    parse_ok = dest.ParsePartialFromZeroCopyStream(&input_stream);
  } else {
    if (!options.merge()) dest.Clear();
    google::protobuf::io::CodedInputStream coded_stream(&input_stream);
    coded_stream.SetRecursionLimit(options.recursion_limit());
    parse_ok = dest.MergePartialFromCodedStream(&coded_stream) &&
               coded_stream.ConsumedEntireMessage();
  }
  if (ABSL_PREDICT_FALSE(!src.ok())) return src.status();
  if (ABSL_PREDICT_FALSE(!parse_ok)) return ParseError(src, dest);
  return CheckInitialized(src, dest, options);
}

absl::Status ParseMessageImpl(ReaderSpan<Reader> src,
                              google::protobuf::MessageLite& dest,
                              ParseMessageOptions options) {
  return ParseMessageFromSpanImpl(std::move(src), dest, options);
}

absl::Status ParseMessageImpl(ReaderSpan<> src,
                              google::protobuf::MessageLite& dest,
                              ParseMessageOptions options) {
  return ParseMessageFromSpanImpl(std::move(src), dest, options);
}

}  // namespace parse_message_internal

absl::Status ParseLengthPrefixedMessage(Reader& src,
                                        google::protobuf::MessageLite& dest,
                                        ParseMessageOptions options) {
  return ParseLengthPrefixedMessageImpl(src, dest, options);
}

absl::Status ParseLengthPrefixedMessage(LimitingReaderBase& src,
                                        google::protobuf::MessageLite& dest,
                                        ParseMessageOptions options) {
  return ParseLengthPrefixedMessageImpl(src, dest, options);
}

absl::Status ParseMessage(BytesRef src, google::protobuf::MessageLite& dest,
                          ParseMessageOptions options) {
  bool parse_ok;
  if (ABSL_PREDICT_FALSE(src.size() >
                         unsigned{std::numeric_limits<int>::max()})) {
    parse_ok = false;
  } else if (!options.merge() && options.recursion_limit() ==
                                     google::protobuf::io::CodedInputStream::
                                         GetDefaultRecursionLimit()) {
    parse_ok = dest.ParsePartialFromArray(src.data(), IntCast<int>(src.size()));
  } else {
    if (!options.merge()) dest.Clear();
    google::protobuf::io::ArrayInputStream input_stream(
        src.data(), IntCast<int>(src.size()));
    google::protobuf::io::CodedInputStream coded_stream(&input_stream);
    coded_stream.SetRecursionLimit(options.recursion_limit());
    parse_ok = dest.MergePartialFromCodedStream(&coded_stream) &&
               coded_stream.ConsumedEntireMessage();
  }
  if (ABSL_PREDICT_FALSE(!parse_ok)) return ParseError(dest);
  return CheckInitialized(dest, options);
}

absl::Status ParseMessage(const Chain& src, google::protobuf::MessageLite& dest,
                          ParseMessageOptions options) {
  if (!options.merge() &&
      options.recursion_limit() ==
          google::protobuf::io::CodedInputStream::GetDefaultRecursionLimit() &&
      src.size() <= kMaxBytesToCopy) {
    if (const std::optional<absl::string_view> flat = src.TryFlat();
        flat != std::nullopt) {
      // The data are flat. `ParsePartialFromArray()` is faster than
      // `ParsePartialFromZeroCopyStream()`.
      if (ABSL_PREDICT_FALSE(!dest.ParsePartialFromArray(
              flat->data(), IntCast<int>(flat->size())))) {
        return ParseError(dest);
      }
      return CheckInitialized(dest, options);
    }
  }
  ChainReader reader(&src);
  ReaderInputStream input_stream(&reader);
  bool parse_ok;
  if (!options.merge() &&
      options.recursion_limit() ==
          google::protobuf::io::CodedInputStream::GetDefaultRecursionLimit()) {
    parse_ok = dest.ParsePartialFromZeroCopyStream(&input_stream);
  } else {
    if (!options.merge()) dest.Clear();
    google::protobuf::io::CodedInputStream coded_stream(&input_stream);
    coded_stream.SetRecursionLimit(options.recursion_limit());
    parse_ok = dest.MergePartialFromCodedStream(&coded_stream) &&
               coded_stream.ConsumedEntireMessage();
  }
  RIEGELI_ASSERT_OK(reader) << "ChainReader has no reason to fail";
  if (ABSL_PREDICT_FALSE(!parse_ok)) return ParseError(dest);
  return CheckInitialized(dest, options);
}

absl::Status ParseMessage(const absl::Cord& src,
                          google::protobuf::MessageLite& dest,
                          ParseMessageOptions options) {
  if (!options.merge() &&
      options.recursion_limit() ==
          google::protobuf::io::CodedInputStream::GetDefaultRecursionLimit() &&
      src.size() <= kMaxBytesToCopy) {
    if (const std::optional<absl::string_view> flat = src.TryFlat();
        flat != std::nullopt) {
      // The data are flat. `ParsePartialFromArray()` is faster than
      // `ParsePartialFromZeroCopyStream()`.
      if (ABSL_PREDICT_FALSE(!dest.ParsePartialFromArray(
              flat->data(), IntCast<int>(flat->size())))) {
        return ParseError(dest);
      }
      return CheckInitialized(dest, options);
    }
  }
  CordReader reader(&src);
  ReaderInputStream input_stream(&reader);
  bool parse_ok;
  if (!options.merge() &&
      options.recursion_limit() ==
          google::protobuf::io::CodedInputStream::GetDefaultRecursionLimit()) {
    parse_ok = dest.ParsePartialFromZeroCopyStream(&input_stream);
  } else {
    if (!options.merge()) dest.Clear();
    google::protobuf::io::CodedInputStream coded_stream(&input_stream);
    coded_stream.SetRecursionLimit(options.recursion_limit());
    parse_ok = dest.MergePartialFromCodedStream(&coded_stream) &&
               coded_stream.ConsumedEntireMessage();
  }
  RIEGELI_ASSERT_OK(reader) << "CordReader has no reason to fail";
  if (ABSL_PREDICT_FALSE(!parse_ok)) return ParseError(dest);
  return CheckInitialized(dest, options);
}

bool ReaderInputStream::Next(const void** data, int* size) {
  RIEGELI_ASSERT_NE(src_, nullptr)
      << "Failed precondition of ReaderInputStream::Next(): "
         "ReaderInputStream not initialized";
  if (ABSL_PREDICT_FALSE(src_->pos() >=
                         Position{std::numeric_limits<int64_t>::max()})) {
    return false;
  }
  const Position max_length =
      Position{std::numeric_limits<int64_t>::max()} - src_->pos();
  if (ABSL_PREDICT_FALSE(!src_->Pull())) return false;
  *data = src_->cursor();
  *size = SaturatingIntCast<int>(UnsignedMin(src_->available(), max_length));
  src_->move_cursor(IntCast<size_t>(*size));
  return true;
}

void ReaderInputStream::BackUp(int length) {
  RIEGELI_ASSERT_NE(src_, nullptr)
      << "Failed precondition of ReaderInputStream::BackUp(): "
         "ReaderInputStream not initialized";
  RIEGELI_ASSERT_GE(length, 0)
      << "Failed precondition of ZeroCopyInputStream::BackUp(): "
         "negative length";
  RIEGELI_ASSERT_LE(IntCast<size_t>(length), src_->start_to_cursor())
      << "Failed precondition of ZeroCopyInputStream::BackUp(): "
         "length larger than the amount of buffered data";
  src_->set_cursor(src_->cursor() - length);
}

bool ReaderInputStream::Skip(int length) {
  RIEGELI_ASSERT_NE(src_, nullptr)
      << "Failed precondition of ReaderInputStream::Skip(): "
         "ReaderInputStream not initialized";
  RIEGELI_ASSERT_GE(length, 0)
      << "Failed precondition of ZeroCopyInputStream::Skip(): negative length";
  const Position max_length =
      SaturatingSub(Position{std::numeric_limits<int64_t>::max()}, src_->pos());
  const size_t length_to_skip =
      UnsignedMin(IntCast<size_t>(length), max_length);
  return src_->Skip(length_to_skip) &&
         length_to_skip == IntCast<size_t>(length);
}

int64_t ReaderInputStream::ByteCount() const {
  RIEGELI_ASSERT_NE(src_, nullptr)
      << "Failed precondition of ReaderInputStream::ByteCount(): "
         "ReaderInputStream not initialized";
  return SaturatingIntCast<int64_t>(src_->pos());
}

bool ReaderInputStream::ReadCord(absl::Cord* cord, int length) {
  RIEGELI_ASSERT_NE(src_, nullptr)
      << "Failed precondition of ReaderInputStream::ReadCord(): "
         "ReaderInputStream not initialized";
  RIEGELI_ASSERT_GE(length, 0)
      << "Failed precondition of ZeroCopyInputStream::ReadCord(): "
         "negative length";
  const Position max_length =
      SaturatingSub(Position{std::numeric_limits<int64_t>::max()}, src_->pos());
  const size_t length_to_read =
      UnsignedMin(IntCast<size_t>(length), max_length);
  return src_->ReadAndAppend(length_to_read, *cord) &&
         length_to_read == IntCast<size_t>(length);
}

}  // namespace riegeli
