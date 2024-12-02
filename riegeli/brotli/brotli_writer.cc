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
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "brotli/encode.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/shared_ptr.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"
#include "riegeli/brotli/brotli_reader.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if !__cpp_inline_variables
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
                                  int window_log) {
  RIEGELI_ASSERT_NE(dest, nullptr)
      << "Failed precondition of BrotliWriter: null Writer pointer";
  if (ABSL_PREDICT_FALSE(!dest->ok())) {
    FailWithoutAnnotation(AnnotateOverDest(dest->status()));
    return;
  }
  initial_compressed_pos_ = dest->pos();
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
  for (const SharedPtr<const BrotliDictionary::Chunk>& chunk :
       dictionary_.chunks()) {
    const BrotliEncoderPreparedDictionary* const compression_dictionary =
        chunk->PrepareCompressionDictionary();
    if (ABSL_PREDICT_FALSE(compression_dictionary == nullptr)) {
      Fail(absl::InternalError("BrotliEncoderPrepareDictionary() failed"));
      return;
    }
    if (ABSL_PREDICT_FALSE(!BrotliEncoderAttachPreparedDictionary(
            compressor_.get(), compression_dictionary))) {
      Fail(absl::InternalError(
          "BrotliEncoderAttachPreparedDictionary() failed"));
      return;
    }
  }
}

void BrotliWriterBase::DoneBehindBuffer(absl::string_view src) {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::DoneBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!ok())) return;
  Writer& dest = *DestWriter();
  WriteInternal(src, dest, BROTLI_OPERATION_FINISH);
}

void BrotliWriterBase::Done() {
  BufferedWriter::Done();
  compressor_.reset();
  dictionary_ = BrotliDictionary();
  allocator_ = BrotliAllocator();
  associated_reader_.Reset();
}

absl::Status BrotliWriterBase::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) {
    Writer& dest = *DestWriter();
    status = dest.AnnotateStatus(std::move(status));
  }
  // The status might have been annotated by `dest` with the compressed
  // position. Clarify that the current position is the uncompressed position
  // instead of delegating to `BufferedWriter::AnnotateStatusImpl()`.
  return AnnotateOverDest(std::move(status));
}

absl::Status BrotliWriterBase::AnnotateOverDest(absl::Status status) {
  if (is_open()) {
    return Annotate(status, absl::StrCat("at uncompressed byte ", pos()));
  }
  return status;
}

void BrotliWriterBase::SetWriteSizeHintImpl(
    absl::optional<Position> write_size_hint) {
  BufferedWriter::SetWriteSizeHintImpl(write_size_hint);
  if (ABSL_PREDICT_FALSE(!ok())) return;
  // Ignore failure if compression already started.
  BrotliEncoderSetParameter(compressor_.get(), BROTLI_PARAM_SIZE_HINT,
                            write_size_hint == absl::nullopt
                                ? 0
                                : SaturatingIntCast<uint32_t>(
                                      SaturatingAdd(pos(), *write_size_hint)));
}

bool BrotliWriterBase::WriteInternal(absl::string_view src) {
  RIEGELI_ASSERT(!src.empty())
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "nothing to write";
  RIEGELI_ASSERT_OK(*this)
      << "Failed precondition of BufferedWriter::WriteInternal()";
  Writer& dest = *DestWriter();
  return WriteInternal(src, dest, BROTLI_OPERATION_PROCESS);
}

inline bool BrotliWriterBase::WriteInternal(absl::string_view src, Writer& dest,
                                            BrotliEncoderOperation op) {
  RIEGELI_ASSERT_OK(*this)
      << "Failed precondition of BrotliWriterBase::WriteInternal()";
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
      return Fail(absl::InternalError("BrotliEncoderCompressStream() failed"));
    }
    size_t length = 0;
    const char* const data = reinterpret_cast<const char*>(
        BrotliEncoderTakeOutput(compressor_.get(), &length));
    if (length > 0) {
      if (ABSL_PREDICT_FALSE(!dest.Write(absl::string_view(data, length)))) {
        return FailWithoutAnnotation(AnnotateOverDest(dest.status()));
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
      << "Failed precondition of BufferedWriter::FlushBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Writer& dest = *DestWriter();
  return WriteInternal(src, dest, BROTLI_OPERATION_FLUSH);
}

bool BrotliWriterBase::SupportsReadMode() {
  Writer* const dest = DestWriter();
  if (dest != nullptr && dest->SupportsReadMode()) {
    for (const SharedPtr<const BrotliDictionary::Chunk>& chunk :
         dictionary_.chunks()) {
      if (chunk->type() == BrotliDictionary::Type::kNative) return false;
    }
    return true;
  }
  return false;
}

Reader* BrotliWriterBase::ReadModeBehindBuffer(Position initial_pos) {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::ReadModeBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!BrotliWriterBase::FlushBehindBuffer(
          absl::string_view(), FlushType::kFromObject))) {
    return nullptr;
  }
  Writer& dest = *DestWriter();
  Reader* const compressed_reader = dest.ReadMode(initial_compressed_pos_);
  if (ABSL_PREDICT_FALSE(compressed_reader == nullptr)) {
    FailWithoutAnnotation(AnnotateOverDest(dest.status()));
    return nullptr;
  }
  BrotliReader<>* const reader = associated_reader_.ResetReader(
      compressed_reader, BrotliReaderBase::Options()
                             .set_dictionary(dictionary_)
                             .set_allocator(allocator_));
  reader->Seek(initial_pos);
  return reader;
}

}  // namespace riegeli
