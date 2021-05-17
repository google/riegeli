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

#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/string_reader.h"
#include "riegeli/bytes/string_writer.h"
#include "riegeli/ordered_varint/ordered_varint_reading.h"
#include "riegeli/ordered_varint/ordered_varint_writing.h"
#include "riegeli/varint/varint_reading.h"
#include "riegeli/varint/varint_writing.h"
#include "testing/base/public/benchmark.h"
#include "util/coding/prefixvarint.h"

namespace riegeli {
namespace tests {
namespace {

constexpr size_t kBatchSize = size_t{1} << 20;

uint32_t SampleVarint32(size_t length) {
  if (length == 5) return uint32_t{1} << 31;
  return uint32_t{1} << (7 * (length - 1));
}

uint64_t SampleVarint64(size_t length) {
  return uint64_t{1} << (7 * (length - 1));
}

void BM_WriteVarint32(benchmark::State& state) {
  const size_t length = IntCast<size_t>(state.range(0));
  uint32_t value = SampleVarint32(length);
  while (state.KeepRunningBatch(kBatchSize)) {
    std::string encoded;
    StringWriter<> writer(&encoded, StringWriterBase::Options().set_size_hint(
                                        kBatchSize * length));
    for (size_t i = 0; i < kBatchSize; ++i) {
      benchmark::DoNotOptimize(value);
      WriteVarint32(value, writer);
    }
    writer.Close();
    benchmark::DoNotOptimize(encoded);
  }
}
BENCHMARK(BM_WriteVarint32)->DenseRange(1, 5);

void BM_WriteVarint64(benchmark::State& state) {
  const size_t length = IntCast<size_t>(state.range(0));
  uint64_t value = SampleVarint64(length);
  while (state.KeepRunningBatch(kBatchSize)) {
    std::string encoded;
    StringWriter<> writer(&encoded, StringWriterBase::Options().set_size_hint(
                                        kBatchSize * length));
    for (size_t i = 0; i < kBatchSize; ++i) {
      benchmark::DoNotOptimize(value);
      WriteVarint64(value, writer);
    }
    writer.Close();
    benchmark::DoNotOptimize(encoded);
  }
}
BENCHMARK(BM_WriteVarint64)->DenseRange(1, 10);

void BM_WritePrefixVarint32(benchmark::State& state) {
  const size_t length = IntCast<size_t>(state.range(0));
  uint32_t value = SampleVarint32(length);
  while (state.KeepRunningBatch(kBatchSize)) {
    std::string encoded;
    StringWriter<> writer(&encoded, StringWriterBase::Options().set_size_hint(
                                        kBatchSize * length));
    for (size_t i = 0; i < kBatchSize; ++i) {
      benchmark::DoNotOptimize(value);
      PrefixVarint::Write32(value, writer);
    }
    writer.Close();
    benchmark::DoNotOptimize(encoded);
  }
}
BENCHMARK(BM_WritePrefixVarint32)->DenseRange(1, 5);

void BM_WritePrefixVarint64(benchmark::State& state) {
  const size_t length = IntCast<size_t>(state.range(0));
  uint64_t value = SampleVarint64(length);
  while (state.KeepRunningBatch(kBatchSize)) {
    std::string encoded;
    StringWriter<> writer(&encoded, StringWriterBase::Options().set_size_hint(
                                        kBatchSize * length));
    for (size_t i = 0; i < kBatchSize; ++i) {
      benchmark::DoNotOptimize(value);
      PrefixVarint::Write64(value, writer);
    }
    writer.Close();
    benchmark::DoNotOptimize(encoded);
  }
}
BENCHMARK(BM_WritePrefixVarint64)->DenseRange(1, 9);

void BM_WriteOrderedVarint32(benchmark::State& state) {
  const size_t length = IntCast<size_t>(state.range(0));
  uint32_t value = SampleVarint32(length);
  while (state.KeepRunningBatch(kBatchSize)) {
    std::string encoded;
    StringWriter<> writer(&encoded, StringWriterBase::Options().set_size_hint(
                                        kBatchSize * length));
    for (size_t i = 0; i < kBatchSize; ++i) {
      benchmark::DoNotOptimize(value);
      WriteOrderedVarint32(value, writer);
    }
    writer.Close();
    benchmark::DoNotOptimize(encoded);
  }
}
BENCHMARK(BM_WriteOrderedVarint32)->DenseRange(1, 5);

void BM_WriteOrderedVarint64(benchmark::State& state) {
  const size_t length = IntCast<size_t>(state.range(0));
  uint64_t value = SampleVarint64(length);
  while (state.KeepRunningBatch(kBatchSize)) {
    std::string encoded;
    StringWriter<> writer(&encoded, StringWriterBase::Options().set_size_hint(
                                        kBatchSize * length));
    for (size_t i = 0; i < kBatchSize; ++i) {
      benchmark::DoNotOptimize(value);
      WriteOrderedVarint64(value, writer);
    }
    writer.Close();
    benchmark::DoNotOptimize(encoded);
  }
}
BENCHMARK(BM_WriteOrderedVarint64)->DenseRange(1, 9);

void BM_ReadVarint32(benchmark::State& state) {
  const size_t length = IntCast<size_t>(state.range(0));
  const uint32_t value = SampleVarint32(length);
  std::string encoded;
  StringWriter<> writer(
      &encoded, StringWriterBase::Options().set_size_hint(kBatchSize * length));
  for (size_t i = 0; i < kBatchSize; ++i) {
    WriteVarint32(value, writer);
  }
  writer.Close();
  while (state.KeepRunningBatch(kBatchSize)) {
    StringReader<> reader(encoded);
    while (const absl::optional<uint32_t> decoded = ReadVarint32(reader)) {
      benchmark::DoNotOptimize(*decoded);
    }
    reader.Close();
  }
}
BENCHMARK(BM_ReadVarint32)->DenseRange(1, 5);

void BM_ReadVarint64(benchmark::State& state) {
  const size_t length = IntCast<size_t>(state.range(0));
  const uint64_t value = SampleVarint64(length);
  std::string encoded;
  StringWriter<> writer(
      &encoded, StringWriterBase::Options().set_size_hint(kBatchSize * length));
  for (size_t i = 0; i < kBatchSize; ++i) {
    WriteVarint64(value, writer);
  }
  writer.Close();
  while (state.KeepRunningBatch(kBatchSize)) {
    StringReader<> reader(encoded);
    while (const absl::optional<uint64_t> decoded = ReadVarint64(reader)) {
      benchmark::DoNotOptimize(*decoded);
    }
    reader.Close();
  }
}
BENCHMARK(BM_ReadVarint64)->DenseRange(1, 10);

void BM_ReadPrefixVarint32(benchmark::State& state) {
  const size_t length = IntCast<size_t>(state.range(0));
  const uint32_t value = SampleVarint32(length);
  std::string encoded;
  StringWriter<> writer(
      &encoded, StringWriterBase::Options().set_size_hint(kBatchSize * length));
  for (size_t i = 0; i < kBatchSize; ++i) {
    PrefixVarint::Write32(value, writer);
  }
  writer.Close();
  while (state.KeepRunningBatch(kBatchSize)) {
    StringReader<> reader(encoded);
    uint32_t decoded;
    while (PrefixVarint::Read32(reader, decoded)) {
      benchmark::DoNotOptimize(decoded);
    }
    reader.Close();
  }
}
BENCHMARK(BM_ReadPrefixVarint32)->DenseRange(1, 5);

void BM_ReadPrefixVarint64(benchmark::State& state) {
  const size_t length = IntCast<size_t>(state.range(0));
  const uint64_t value = SampleVarint64(length);
  std::string encoded;
  StringWriter<> writer(
      &encoded, StringWriterBase::Options().set_size_hint(kBatchSize * length));
  for (size_t i = 0; i < kBatchSize; ++i) {
    PrefixVarint::Write64(value, writer);
  }
  writer.Close();
  while (state.KeepRunningBatch(kBatchSize)) {
    StringReader<> reader(encoded);
    uint64_t decoded;
    while (PrefixVarint::Read64(reader, decoded)) {
      benchmark::DoNotOptimize(decoded);
    }
    reader.Close();
  }
}
BENCHMARK(BM_ReadPrefixVarint64)->DenseRange(1, 9);

void BM_ReadOrderedVarint32(benchmark::State& state) {
  const size_t length = IntCast<size_t>(state.range(0));
  const uint32_t value = SampleVarint32(length);
  std::string encoded;
  StringWriter<> writer(
      &encoded, StringWriterBase::Options().set_size_hint(kBatchSize * length));
  for (size_t i = 0; i < kBatchSize; ++i) {
    WriteOrderedVarint32(value, writer);
  }
  writer.Close();
  while (state.KeepRunningBatch(kBatchSize)) {
    StringReader<> reader(encoded);
    while (const absl::optional<uint32_t> decoded =
               ReadOrderedVarint32(reader)) {
      benchmark::DoNotOptimize(*decoded);
    }
    reader.Close();
  }
}
BENCHMARK(BM_ReadOrderedVarint32)->DenseRange(1, 5);

void BM_ReadOrderedVarint64(benchmark::State& state) {
  const size_t length = IntCast<size_t>(state.range(0));
  const uint64_t value = SampleVarint64(length);
  std::string encoded;
  StringWriter<> writer(
      &encoded, StringWriterBase::Options().set_size_hint(kBatchSize * length));
  for (size_t i = 0; i < kBatchSize; ++i) {
    WriteOrderedVarint64(value, writer);
  }
  writer.Close();
  while (state.KeepRunningBatch(kBatchSize)) {
    StringReader<> reader(encoded);
    while (const absl::optional<uint64_t> decoded =
               ReadOrderedVarint64(reader)) {
      benchmark::DoNotOptimize(*decoded);
    }
    reader.Close();
  }
}
BENCHMARK(BM_ReadOrderedVarint64)->DenseRange(1, 9);

}  // namespace
}  // namespace tests
}  // namespace riegeli
