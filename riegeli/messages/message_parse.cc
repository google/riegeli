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

#include "riegeli/messages/message_parse.h"

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
#include "absl/types/optional.h"
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/io/zero_copy_stream_impl_lite.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/cord_reader.h"
#include "riegeli/bytes/limiting_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/varint/varint_reading.h"

namespace riegeli {

namespace {

ABSL_ATTRIBUTE_COLD
inline absl::Status ParseError(google::protobuf::MessageLite& dest) {
  return absl::InvalidArgumentError(
      absl::StrCat("Failed to parse message of type ", dest.GetTypeName()));
}

ABSL_ATTRIBUTE_COLD
inline absl::Status ParseError(Reader& src,
                               google::protobuf::MessageLite& dest) {
  return src.AnnotateStatus(ParseError(dest));
}

inline absl::Status CheckInitialized(google::protobuf::MessageLite& dest,
                                     ParseOptions options) {
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
                                     ParseOptions options) {
  if (!options.partial() && ABSL_PREDICT_FALSE(!dest.IsInitialized())) {
    return src.AnnotateStatus(absl::InvalidArgumentError(
        absl::StrCat("Failed to parse message of type ", dest.GetTypeName(),
                     " because it is missing required fields: ",
                     dest.InitializationErrorString())));
  }
  return absl::OkStatus();
}

}  // namespace

namespace messages_internal {

absl::Status ParseFromReaderImpl(Reader& src,
                                 google::protobuf::MessageLite& dest,
                                 ParseOptions options) {
  if (!options.merge() &&
      options.recursion_limit() ==
          google::protobuf::io::CodedInputStream::GetDefaultRecursionLimit() &&
      src.SupportsSize()) {
    const absl::optional<Position> size = src.Size();
    if (ABSL_PREDICT_FALSE(size == absl::nullopt)) return src.status();
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

}  // namespace messages_internal

absl::Status ParseFromReaderWithLength(Reader& src, size_t length,
                                       google::protobuf::MessageLite& dest,
                                       ParseOptions options) {
  if (!options.merge() &&
      options.recursion_limit() ==
          google::protobuf::io::CodedInputStream::GetDefaultRecursionLimit() &&
      length <= kMaxBytesToCopy) {
    src.Pull();
    if (src.available() >= length) {
      // The data are flat. `ParsePartialFromArray()` is faster than
      // `ParsePartialFromZeroCopyStream()`.
      const bool parse_ok =
          dest.ParsePartialFromArray(src.cursor(), IntCast<int>(length));
      src.move_cursor(length);
      if (ABSL_PREDICT_FALSE(!parse_ok)) return ParseError(src, dest);
      return CheckInitialized(src, dest, options);
    }
  }
  LimitingReader<> reader(
      &src, LimitingReaderBase::Options().set_exact_length(length));
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
  if (ABSL_PREDICT_FALSE(!reader.ok())) return reader.status();
  if (ABSL_PREDICT_FALSE(!parse_ok)) return ParseError(reader, dest);
  const absl::Status status = CheckInitialized(reader, dest, options);
  if (!reader.Close()) {
    RIEGELI_ASSERT_UNREACHABLE() << "A LimitingReader with !fail_if_longer() "
                                    "has no reason to fail only in Close(): "
                                 << reader.status();
  }
  return status;
}

absl::Status ParseLengthPrefixedFromReader(Reader& src,
                                           google::protobuf::MessageLite& dest,
                                           ParseOptions options) {
  uint32_t length;
  if (ABSL_PREDICT_FALSE(!ReadVarint32(src, length)) ||
      ABSL_PREDICT_FALSE(length >
                         uint32_t{std::numeric_limits<int32_t>::max()})) {
    return src.StatusOrAnnotate(
        absl::InvalidArgumentError("Failed to parse message length"));
  }
  return ParseFromReaderWithLength(src, IntCast<size_t>(length), dest,
                                   std::move(options));
}

absl::Status ParseFromString(absl::string_view src,
                             google::protobuf::MessageLite& dest,
                             ParseOptions options) {
  bool parse_ok;
  if (ABSL_PREDICT_FALSE(src.size() >
                         size_t{std::numeric_limits<int>::max()})) {
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

absl::Status ParseFromChain(const Chain& src,
                            google::protobuf::MessageLite& dest,
                            ParseOptions options) {
  if (!options.merge() &&
      options.recursion_limit() ==
          google::protobuf::io::CodedInputStream::GetDefaultRecursionLimit() &&
      src.size() <= kMaxBytesToCopy) {
    if (const absl::optional<absl::string_view> flat = src.TryFlat()) {
      // The data are flat. `ParsePartialFromArray()` is faster than
      // `ParsePartialFromZeroCopyStream()`.
      if (ABSL_PREDICT_FALSE(!dest.ParsePartialFromArray(
              flat->data(), IntCast<int>(flat->size())))) {
        return ParseError(dest);
      }
      return CheckInitialized(dest, options);
    }
  }
  ChainReader<> reader(&src);
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
  RIEGELI_ASSERT(reader.ok())
      << "A ChainReader has no reason to fail: " << reader.status();
  if (ABSL_PREDICT_FALSE(!parse_ok)) return ParseError(dest);
  return CheckInitialized(dest, options);
}

absl::Status ParseFromCord(const absl::Cord& src,
                           google::protobuf::MessageLite& dest,
                           ParseOptions options) {
  if (!options.merge() &&
      options.recursion_limit() ==
          google::protobuf::io::CodedInputStream::GetDefaultRecursionLimit() &&
      src.size() <= kMaxBytesToCopy) {
    if (const absl::optional<absl::string_view> flat = src.TryFlat()) {
      // The data are flat. `ParsePartialFromArray()` is faster than
      // `ParsePartialFromZeroCopyStream()`.
      if (ABSL_PREDICT_FALSE(!dest.ParsePartialFromArray(
              flat->data(), IntCast<int>(flat->size())))) {
        return ParseError(dest);
      }
      return CheckInitialized(dest, options);
    }
  }
  CordReader<> reader(&src);
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
  RIEGELI_ASSERT(reader.ok())
      << "A CordReader has no reason to fail: " << reader.status();
  if (ABSL_PREDICT_FALSE(!parse_ok)) return ParseError(dest);
  return CheckInitialized(dest, options);
}

bool ReaderInputStream::Next(const void** data, int* size) {
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
  RIEGELI_ASSERT_GE(length, 0)
      << "Failed precondition of ZeroCopyInputStream::BackUp(): "
         "negative length";
  RIEGELI_ASSERT_LE(IntCast<size_t>(length), src_->start_to_cursor())
      << "Failed precondition of ZeroCopyInputStream::BackUp(): "
         "length larger than the amount of buffered data";
  src_->set_cursor(src_->cursor() - length);
}

bool ReaderInputStream::Skip(int length) {
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
  return SaturatingIntCast<int64_t>(src_->pos());
}

}  // namespace riegeli
