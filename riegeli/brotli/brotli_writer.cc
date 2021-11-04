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

#include "riegeli/brotli/brotli_writer.h"

#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <memory>
#include <string>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "brotli/encode.h"
#include "riegeli/base/base.h"
#include "riegeli/base/port.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if __cplusplus < 201703
constexpr int BrotliWriterBase::Options::kMinCompressionLevel;
constexpr int BrotliWriterBase::Options::kMaxCompressionLevel;
constexpr int BrotliWriterBase::Options::kDefaultCompressionLevel;
constexpr int BrotliWriterBase::Options::kMinWindowLog;
constexpr int BrotliWriterBase::Options::kMaxWindowLog;
constexpr int BrotliWriterBase::Options::kDefaultWindowLog;
#endif

namespace {

struct BrotliEncoderDictionaryDeleter {
  void operator()(BrotliEncoderPreparedDictionary* ptr) const {
    BrotliEncoderDestroyPreparedDictionary(ptr);
  }
};

}  // namespace

void BrotliWriterBase::Initialize(Writer* dest, int compression_level,
                                  int window_log,
                                  absl::optional<Position> size_hint) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of BrotliWriter: null Writer pointer";
  if (ABSL_PREDICT_FALSE(!healthy())) return;
  if (ABSL_PREDICT_FALSE(!dest->healthy())) {
    Fail(*dest);
    return;
  }
  compressor_.reset(BrotliEncoderCreateInstance(
      allocator_.alloc_func(), allocator_.free_func(), allocator_.opaque()));
  if (ABSL_PREDICT_FALSE(compressor_ == nullptr)) {
    Fail(absl::InternalError("BrotliEncoderCreateInstance() failed"));
    return;
  }
  if (ABSL_PREDICT_FALSE(
          !BrotliEncoderSetParameter(compressor_.get(), BROTLI_PARAM_QUALITY,
                                     IntCast<uint32_t>(compression_level)))) {
    Fail(absl::InternalError(
        "BrotliEncoderSetParameter(BROTLI_PARAM_QUALITY) failed"));
    return;
  }
  // Reduce `window_log` if `size_hint` indicates that data will be smaller.
  // TODO(eustas): Do this automatically in the Brotli engine.
  if (size_hint != absl::nullopt) {
#if RIEGELI_INTERNAL_HAS_BUILTIN(__builtin_clzll) || \
    RIEGELI_INTERNAL_IS_GCC_VERSION(3, 4)
    const int ceil_log2 =
        __builtin_clzll(1) -
        __builtin_clzll((SaturatingSub(*size_hint, Position{1})) | 1) + 1;
    window_log =
        SignedMin(window_log, SignedMax(ceil_log2, Options::kMinWindowLog));
#else
    while (Position{1} << (window_log - 1) >= *size_hint &&
           window_log > Options::kMinWindowLog) {
      --window_log;
    }
#endif
  }
  if (ABSL_PREDICT_FALSE(!BrotliEncoderSetParameter(
          compressor_.get(), BROTLI_PARAM_LARGE_WINDOW,
          uint32_t{window_log > BROTLI_MAX_WINDOW_BITS}))) {
    Fail(absl::InternalError(
        "BrotliEncoderSetParameter(BROTLI_PARAM_LARGE_WINDOW) failed"));
    return;
  }
  if (ABSL_PREDICT_FALSE(
          !BrotliEncoderSetParameter(compressor_.get(), BROTLI_PARAM_LGWIN,
                                     IntCast<uint32_t>(window_log)))) {
    Fail(absl::InternalError(
        "BrotliEncoderSetParameter(BROTLI_PARAM_LGWIN) failed"));
    return;
  }
  for (const std::shared_ptr<const BrotliDictionary::Chunk>& chunk :
       dictionary_.chunks()) {
    const std::shared_ptr<const BrotliEncoderPreparedDictionary> prepared =
        chunk->PrepareCompressionDictionary();
    if (ABSL_PREDICT_FALSE(prepared == nullptr)) {
      Fail(absl::InternalError("BrotliEncoderPrepareDictionary() failed"));
      return;
    }
    if (ABSL_PREDICT_FALSE(!BrotliEncoderAttachPreparedDictionary(
            compressor_.get(), prepared.get()))) {
      Fail(absl::InternalError(
          "BrotliEncoderAttachPreparedDictionary() failed"));
      return;
    }
  }
  if (size_hint != absl::nullopt) {
    // Ignore errors from tuning.
    BrotliEncoderSetParameter(compressor_.get(), BROTLI_PARAM_SIZE_HINT,
                              SaturatingIntCast<uint32_t>(*size_hint));
  }
}

void BrotliWriterBase::DoneBehindBuffer(absl::string_view src) {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::DoneBehindBuffer():"
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!healthy())) return;
  Writer& dest = *dest_writer();
  WriteInternal(src, dest, BROTLI_OPERATION_FINISH);
}

void BrotliWriterBase::Done() {
  BufferedWriter::Done();
  compressor_.reset();
  dictionary_ = BrotliDictionary();
  allocator_ = BrotliAllocator();
}

void BrotliWriterBase::DefaultAnnotateStatus() {
  RIEGELI_ASSERT(!not_failed())
      << "Failed precondition of Object::DefaultAnnotateStatus(): "
         "Object not failed";
  if (is_open()) AnnotateStatus(absl::StrCat("at uncompressed byte ", pos()));
}

bool BrotliWriterBase::WriteInternal(absl::string_view src) {
  RIEGELI_ASSERT(!src.empty())
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "nothing to write";
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of BufferedWriter::WriteInternal(): " << status();
  Writer& dest = *dest_writer();
  return WriteInternal(src, dest, BROTLI_OPERATION_PROCESS);
}

inline bool BrotliWriterBase::WriteInternal(absl::string_view src, Writer& dest,
                                            BrotliEncoderOperation op) {
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of BrotliWriterBase::WriteInternal(): "
      << status();
  if (ABSL_PREDICT_FALSE(src.size() >
                         std::numeric_limits<Position>::max() - start_pos())) {
    return FailOverflow();
  }
  size_t available_in = src.size();
  const uint8_t* next_in = reinterpret_cast<const uint8_t*>(src.data());
  size_t available_out = 0;
  for (;;) {
    if (ABSL_PREDICT_FALSE(!BrotliEncoderCompressStream(
            compressor_.get(), op, &available_in, &next_in, &available_out,
            nullptr, nullptr))) {
      return Fail(
          Annotate(absl::InternalError("BrotliEncoderCompressStream() failed"),
                   absl::StrCat("at byte ", dest.pos())));
    }
    size_t length = 0;
    const char* const data = reinterpret_cast<const char*>(
        BrotliEncoderTakeOutput(compressor_.get(), &length));
    if (length > 0) {
      if (ABSL_PREDICT_FALSE(!dest.Write(data, length))) {
        return Fail(dest);
      }
    } else if (available_in == 0) {
      move_start_pos(src.size());
      return true;
    }
  }
}

bool BrotliWriterBase::FlushBehindBuffer(absl::string_view src,
                                         FlushType flush_type) {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::FlushBehindBuffer():"
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Writer& dest = *dest_writer();
  return WriteInternal(src, dest, BROTLI_OPERATION_FLUSH);
}

}  // namespace riegeli
