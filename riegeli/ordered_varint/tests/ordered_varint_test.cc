// Copyright 2021 Google LLC
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

#include <stddef.h>
#include <stdint.h>

#include <string>
#include <tuple>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/string_reader.h"
#include "riegeli/bytes/string_writer.h"
#include "riegeli/bytes/tests/test_utils.h"
#include "riegeli/ordered_varint/ordered_varint_reading.h"
#include "riegeli/ordered_varint/ordered_varint_writing.h"
#include "testing/base/public/gmock.h"
#include "testing/base/public/gunit.h"

namespace riegeli {
namespace tests {
namespace {

using ::testing::Eq;
using ::testing::Ge;
using ::testing::Optional;

class OrderedVarintTest : public testing::TestWithParam<int> {
 public:
  static std::string ParamToNameSuffix(ParamType param) {
    return absl::StrCat(param);
  }

 protected:
  static int num_bits() { return GetParam(); }

  // A value which needs `num_bits()` binary digits.
  static uint64_t sample_value() {
    if (num_bits() == 0) return 0;
    return uint64_t{0xfedbca9876543210} >> (64 - num_bits());
  }

  // Expected length of varint representation of `sample_value()`.
  static size_t sample_length() {
    if (num_bits() == 0) return 1;
    if (num_bits() == 64) return 9;
    return IntCast<size_t>((num_bits() + 6) / 7);
  }

  // OrderedVarint representation of `sample_value()` with the shortest length.
  static std::string SampleRepr() {
    return FixedLengthSampleRepr(sample_length());
  }

  // OrderedVarint representation of `sample_value()` with the given length.
  static std::string FixedLengthSampleRepr(size_t length) {
    uint64_t data = sample_value();
    std::string repr(length, '\0');
    size_t i = length;
    do {
      --i;
      repr[i] = static_cast<char>(data & 0xff);
      data >>= 8;
    } while (i > 0);
    repr[0] |= static_cast<char>(static_cast<uint8_t>(0xff << (9 - length)));
    return repr;
  }
};

TEST_P(OrderedVarintTest, LengthOrderedVarint32) {
  if (num_bits() > 32) GTEST_SKIP();
  EXPECT_THAT(LengthOrderedVarint32(sample_value()), Eq(sample_length()));
}

TEST_P(OrderedVarintTest, LengthOrderedVarint64) {
  EXPECT_THAT(LengthOrderedVarint64(sample_value()), Eq(sample_length()));
}

TEST_P(OrderedVarintTest, WriteOrderedVarint32_FastPath) {
  if (num_bits() > 32) GTEST_SKIP();
  std::string repr;
  repr.reserve(kMaxLengthOrderedVarint32);
  StringWriter<> writer(&repr);
  ASSERT_TRUE(writer.Push());
  ASSERT_THAT(writer.available(), Ge(kMaxLengthOrderedVarint32));
  EXPECT_TRUE(WriteOrderedVarint32(sample_value(), writer)) << writer.status();
  EXPECT_TRUE(writer.Close()) << writer.status();
  EXPECT_THAT(repr, Eq(SampleRepr()));
}

TEST_P(OrderedVarintTest, WriteOrderedVarint32_SlowPath) {
  if (num_bits() > 32) GTEST_SKIP();
  std::string repr;
  SlowWriter<StringWriter<>> writer(std::forward_as_tuple(&repr));
  EXPECT_TRUE(WriteOrderedVarint32(sample_value(), writer)) << writer.status();
  EXPECT_TRUE(writer.Close()) << writer.status();
  EXPECT_THAT(repr, Eq(SampleRepr()));
}

TEST_P(OrderedVarintTest, WriteOrderedVarint64_FastPath) {
  std::string repr;
  repr.reserve(kMaxLengthOrderedVarint64);
  StringWriter<> writer(&repr);
  ASSERT_TRUE(writer.Push());
  ASSERT_THAT(writer.available(), Ge(kMaxLengthOrderedVarint64));
  EXPECT_TRUE(WriteOrderedVarint64(sample_value(), writer)) << writer.status();
  EXPECT_TRUE(writer.Close()) << writer.status();
  EXPECT_THAT(repr, Eq(SampleRepr()));
}

TEST_P(OrderedVarintTest, WriteOrderedVarint64_SlowPath) {
  std::string repr;
  SlowWriter<StringWriter<>> writer(std::forward_as_tuple(&repr));
  EXPECT_TRUE(WriteOrderedVarint64(sample_value(), writer)) << writer.status();
  EXPECT_TRUE(writer.Close()) << writer.status();
  EXPECT_THAT(repr, Eq(SampleRepr()));
}

TEST_P(OrderedVarintTest, ReadOrderedVarint32_FastPath) {
  if (num_bits() > 32) GTEST_SKIP();
  std::string repr = SampleRepr();
  repr.resize(kMaxLengthOrderedVarint32);
  StringReader<> reader(repr);
  ASSERT_THAT(reader.available(), Ge(kMaxLengthOrderedVarint32));
  EXPECT_THAT(ReadOrderedVarint32(reader), Optional(Eq(sample_value())));
  EXPECT_THAT(reader.pos(), Eq(sample_length()));
}

TEST_P(OrderedVarintTest, ReadOrderedVarint32_SlowPath) {
  if (num_bits() > 32) GTEST_SKIP();
  std::string repr = SampleRepr();
  SlowReader<StringReader<>> reader(std::forward_as_tuple(repr));
  EXPECT_THAT(ReadOrderedVarint32(reader), Optional(Eq(sample_value())));
  EXPECT_THAT(reader.pos(), Eq(sample_length()));
}

TEST_P(OrderedVarintTest, ReadOrderedVarint32_FixedLength_FastPath) {
  if (num_bits() > 32) GTEST_SKIP();
  std::string repr = FixedLengthSampleRepr(kMaxLengthOrderedVarint32);
  StringReader<> reader(repr);
  ASSERT_THAT(reader.available(), Ge(kMaxLengthOrderedVarint32));
  EXPECT_THAT(ReadOrderedVarint32(reader), Optional(Eq(sample_value())));
  EXPECT_THAT(reader.pos(), Eq(kMaxLengthOrderedVarint32));
}

TEST_P(OrderedVarintTest, ReadOrderedVarint32_FixedLength_SlowPath) {
  if (num_bits() > 32) GTEST_SKIP();
  std::string repr = FixedLengthSampleRepr(kMaxLengthOrderedVarint32);
  SlowReader<StringReader<>> reader(std::forward_as_tuple(repr));
  EXPECT_THAT(ReadOrderedVarint32(reader), Optional(Eq(sample_value())));
  EXPECT_THAT(reader.pos(), Eq(kMaxLengthOrderedVarint32));
}

TEST_P(OrderedVarintTest, ReadOrderedVarint32_Truncated) {
  if (num_bits() > 32) GTEST_SKIP();
  std::string repr = SampleRepr();
  repr.pop_back();
  StringReader<> reader(repr);
  EXPECT_THAT(ReadOrderedVarint32(reader), Eq(absl::nullopt));
  EXPECT_THAT(reader.pos(), Eq(0u));
}

TEST_P(OrderedVarintTest, ReadOrderedVarint64_FastPath) {
  std::string repr = SampleRepr();
  repr.resize(kMaxLengthOrderedVarint64);
  StringReader<> reader(repr);
  ASSERT_THAT(reader.available(), Ge(kMaxLengthOrderedVarint64));
  EXPECT_THAT(ReadOrderedVarint64(reader), Optional(Eq(sample_value())));
  EXPECT_THAT(reader.pos(), Eq(sample_length()));
}

TEST_P(OrderedVarintTest, ReadOrderedVarint64_SlowPath) {
  std::string repr = SampleRepr();
  SlowReader<StringReader<>> reader(std::forward_as_tuple(repr));
  EXPECT_THAT(ReadOrderedVarint64(reader), Optional(Eq(sample_value())));
  EXPECT_THAT(reader.pos(), Eq(sample_length()));
}

TEST_P(OrderedVarintTest, ReadOrderedVarint64_FixedLength_FastPath) {
  std::string repr = FixedLengthSampleRepr(kMaxLengthOrderedVarint64);
  StringReader<> reader(repr);
  ASSERT_THAT(reader.available(), Ge(kMaxLengthOrderedVarint64));
  EXPECT_THAT(ReadOrderedVarint64(reader), Optional(Eq(sample_value())));
  EXPECT_THAT(reader.pos(), Eq(kMaxLengthOrderedVarint64));
}

TEST_P(OrderedVarintTest, ReadOrderedVarint64_FixedLength_SlowPath) {
  std::string repr = FixedLengthSampleRepr(kMaxLengthOrderedVarint64);
  SlowReader<StringReader<>> reader(std::forward_as_tuple(repr));
  EXPECT_THAT(ReadOrderedVarint64(reader), Optional(Eq(sample_value())));
  EXPECT_THAT(reader.pos(), Eq(kMaxLengthOrderedVarint64));
}

TEST_P(OrderedVarintTest, ReadOrderedVarint64_Truncated) {
  std::string repr = SampleRepr();
  repr.pop_back();
  StringReader<> reader(repr);
  EXPECT_THAT(ReadOrderedVarint64(reader), Eq(absl::nullopt));
  EXPECT_THAT(reader.pos(), Eq(0u));
}

TEST_P(OrderedVarintTest, ReadCanonicalOrderedVarint32_FastPath) {
  if (num_bits() > 32) GTEST_SKIP();
  std::string repr = SampleRepr();
  repr.resize(kMaxLengthOrderedVarint32);
  StringReader<> reader(repr);
  ASSERT_THAT(reader.available(), Ge(kMaxLengthOrderedVarint32));
  EXPECT_THAT(ReadCanonicalOrderedVarint32(reader),
              Optional(Eq(sample_value())));
  EXPECT_THAT(reader.pos(), Eq(sample_length()));
}

TEST_P(OrderedVarintTest, ReadCanonicalOrderedVarint32_SlowPath) {
  if (num_bits() > 32) GTEST_SKIP();
  std::string repr = SampleRepr();
  SlowReader<StringReader<>> reader(std::forward_as_tuple(repr));
  EXPECT_THAT(ReadCanonicalOrderedVarint32(reader),
              Optional(Eq(sample_value())));
  EXPECT_THAT(reader.pos(), Eq(sample_length()));
}

TEST_P(OrderedVarintTest, ReadCanonicalOrderedVarint32_NotCanonical_FastPath) {
  if (sample_length() >= kMaxLengthOrderedVarint32) GTEST_SKIP();
  std::string repr = FixedLengthSampleRepr(kMaxLengthOrderedVarint32);
  StringReader<> reader(repr);
  ASSERT_THAT(reader.available(), Ge(kMaxLengthOrderedVarint32));
  EXPECT_THAT(ReadCanonicalOrderedVarint32(reader), Eq(absl::nullopt));
  EXPECT_THAT(reader.pos(), Eq(0u));
}

TEST_P(OrderedVarintTest, ReadCanonicalOrderedVarint32_NotCanonical_SlowPath) {
  if (sample_length() >= kMaxLengthOrderedVarint32) GTEST_SKIP();
  std::string repr = FixedLengthSampleRepr(kMaxLengthOrderedVarint32);
  SlowReader<StringReader<>> reader(std::forward_as_tuple(repr));
  EXPECT_THAT(ReadCanonicalOrderedVarint32(reader), Eq(absl::nullopt));
  EXPECT_THAT(reader.pos(), Eq(0u));
}

TEST_P(OrderedVarintTest, ReadCanonicalOrderedVarint32_Truncated) {
  if (num_bits() > 32) GTEST_SKIP();
  std::string repr = SampleRepr();
  repr.pop_back();
  StringReader<> reader(repr);
  EXPECT_THAT(ReadCanonicalOrderedVarint32(reader), Eq(absl::nullopt));
  EXPECT_THAT(reader.pos(), Eq(0u));
}

TEST_P(OrderedVarintTest, ReadCanonicalOrderedVarint64_FastPath) {
  std::string repr = SampleRepr();
  repr.resize(kMaxLengthOrderedVarint64);
  StringReader<> reader(repr);
  ASSERT_THAT(reader.available(), Ge(kMaxLengthOrderedVarint64));
  EXPECT_THAT(ReadCanonicalOrderedVarint64(reader),
              Optional(Eq(sample_value())));
  EXPECT_THAT(reader.pos(), Eq(sample_length()));
}

TEST_P(OrderedVarintTest, ReadCanonicalOrderedVarint64_SlowPath) {
  std::string repr = SampleRepr();
  SlowReader<StringReader<>> reader(std::forward_as_tuple(repr));
  EXPECT_THAT(ReadCanonicalOrderedVarint64(reader),
              Optional(Eq(sample_value())));
  EXPECT_THAT(reader.pos(), Eq(sample_length()));
}

TEST_P(OrderedVarintTest, ReadCanonicalOrderedVarint64_NotCanonical_FastPath) {
  if (sample_length() >= kMaxLengthOrderedVarint64) GTEST_SKIP();
  std::string repr = FixedLengthSampleRepr(kMaxLengthOrderedVarint64);
  StringReader<> reader(repr);
  ASSERT_THAT(reader.available(), Ge(kMaxLengthOrderedVarint64));
  EXPECT_THAT(ReadCanonicalOrderedVarint64(reader), Eq(absl::nullopt));
  EXPECT_THAT(reader.pos(), Eq(0u));
}

TEST_P(OrderedVarintTest, ReadCanonicalOrderedVarint64_NotCanonical_SlowPath) {
  if (sample_length() >= kMaxLengthOrderedVarint64) GTEST_SKIP();
  std::string repr = FixedLengthSampleRepr(kMaxLengthOrderedVarint64);
  SlowReader<StringReader<>> reader(std::forward_as_tuple(repr));
  EXPECT_THAT(ReadCanonicalOrderedVarint64(reader), Eq(absl::nullopt));
  EXPECT_THAT(reader.pos(), Eq(0u));
}

TEST_P(OrderedVarintTest, ReadCanonicalOrderedVarint64_Truncated) {
  std::string repr = SampleRepr();
  repr.pop_back();
  StringReader<> reader(repr);
  EXPECT_THAT(ReadCanonicalOrderedVarint64(reader), Eq(absl::nullopt));
  EXPECT_THAT(reader.pos(), Eq(0u));
}

INSTANTIATE_TEST_SUITE_P(
    OrderedVarintTest, OrderedVarintTest, testing::Range(0, 65),
    [](testing::TestParamInfo<OrderedVarintTest::ParamType> info) {
      return OrderedVarintTest::ParamToNameSuffix(info.param);
    });

}  // namespace
}  // namespace tests
}  // namespace riegeli
