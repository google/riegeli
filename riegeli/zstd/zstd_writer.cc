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

// Enables the experimental zstd API:
//  * `ZSTD_c_srcSizeHint`
#define ZSTD_STATIC_LINKING_ONLY

#include "riegeli/zstd/zstd_writer.h"

#include <stddef.h>

#include <limits>
#include <memory>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/recycling_pool.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/zstd/zstd_reader.h"
#include "zstd.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if !__cpp_inline_variables
constexpr int ZstdWriterBase::Options::kMinCompressionLevel;
constexpr int ZstdWriterBase::Options::kMaxCompressionLevel;
constexpr int ZstdWriterBase::Options::kDefaultCompressionLevel;
constexpr int ZstdWriterBase::Options::kMinWindowLog;
constexpr int ZstdWriterBase::Options::kMaxWindowLog;
#endif

void ZstdWriterBase::Initialize(Writer* dest, int compression_level,
                                absl::optional<int> window_log,
                                bool store_checksum) {
  RIEGELI_ASSERT_NE(dest, nullptr)
      << "Failed precondition of ZstdWriter: null Writer pointer";
  if (ABSL_PREDICT_FALSE(!dest->ok())) {
    FailWithoutAnnotation(AnnotateOverDest(dest->status()));
    return;
  }
  initial_compressed_pos_ = dest->pos();
  compressor_ = RecyclingPool<ZSTD_CCtx, ZSTD_CCtxDeleter>::global(
                    recycling_pool_options_)
                    .Get(
                        [] {
                          return std::unique_ptr<ZSTD_CCtx, ZSTD_CCtxDeleter>(
                              ZSTD_createCCtx());
                        },
                        [](ZSTD_CCtx* compressor) {
                          const size_t result = ZSTD_CCtx_reset(
                              compressor, ZSTD_reset_session_and_parameters);
                          RIEGELI_ASSERT(!ZSTD_isError(result))
                              << "ZSTD_CCtx_reset() failed: "
                              << ZSTD_getErrorName(result);
                        });
  if (ABSL_PREDICT_FALSE(compressor_ == nullptr)) {
    Fail(absl::InternalError("ZSTD_createCCtx() failed"));
    return;
  }
  {
    const size_t result = ZSTD_CCtx_setParameter(
        compressor_.get(), ZSTD_c_compressionLevel, compression_level);
    if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
      Fail(absl::InternalError(absl::StrCat(
          "ZSTD_CCtx_setParameter(ZSTD_c_compressionLevel) failed: ",
          ZSTD_getErrorName(result))));
      return;
    }
  }
  if (window_log != absl::nullopt) {
    const size_t result = ZSTD_CCtx_setParameter(compressor_.get(),
                                                 ZSTD_c_windowLog, *window_log);
    if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
      Fail(absl::InternalError(
          absl::StrCat("ZSTD_CCtx_setParameter(ZSTD_c_windowLog) failed: ",
                       ZSTD_getErrorName(result))));
      return;
    }
  }
  {
    const size_t result = ZSTD_CCtx_setParameter(
        compressor_.get(), ZSTD_c_checksumFlag, store_checksum ? 1 : 0);
    if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
      Fail(absl::InternalError(
          absl::StrCat("ZSTD_CCtx_setParameter(ZSTD_c_checksumFlag) failed: ",
                       ZSTD_getErrorName(result))));
      return;
    }
  }
  if (pledged_size_ != absl::nullopt) {
    BufferedWriter::SetWriteSizeHintImpl(*pledged_size_);
    const size_t result = ZSTD_CCtx_setPledgedSrcSize(
        compressor_.get(), IntCast<unsigned long long>(*pledged_size_));
    if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
      Fail(absl::InternalError(
          absl::StrCat("ZSTD_CCtx_setPledgedSrcSize() failed: ",
                       ZSTD_getErrorName(result))));
      return;
    }
  }
  if (!dictionary_.empty()) {
    compression_dictionary_ =
        dictionary_.PrepareCompressionDictionary(compression_level);
    if (ABSL_PREDICT_FALSE(compression_dictionary_ == nullptr)) {
      Fail(absl::InternalError("ZSTD_createCDict_advanced() failed"));
      return;
    }
    const size_t result =
        ZSTD_CCtx_refCDict(compressor_.get(), compression_dictionary_.get());
    if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
      Fail(absl::InternalError(absl::StrCat("ZSTD_CCtx_refCDict() failed: ",
                                            ZSTD_getErrorName(result))));
      return;
    }
  }
}

void ZstdWriterBase::DoneBehindBuffer(absl::string_view src) {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::DoneBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!ok())) return;
  Writer& dest = *DestWriter();
  WriteInternal(src, dest, ZSTD_e_end);
}

void ZstdWriterBase::Done() {
  BufferedWriter::Done();
  compressor_.reset();
  dictionary_ = ZstdDictionary();
  associated_reader_.Reset();
}

absl::Status ZstdWriterBase::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) {
    Writer& dest = *DestWriter();
    status = dest.AnnotateStatus(std::move(status));
  }
  // The status might have been annotated by `dest` with the compressed
  // position. Clarify that the current position is the uncompressed position
  // instead of delegating to `BufferedWriter::AnnotateStatusImpl()`.
  return AnnotateOverDest(std::move(status));
}

absl::Status ZstdWriterBase::AnnotateOverDest(absl::Status status) {
  if (is_open()) {
    return Annotate(status, absl::StrCat("at uncompressed byte ", pos()));
  }
  return status;
}

void ZstdWriterBase::SetWriteSizeHintImpl(
    absl::optional<Position> write_size_hint) {
  BufferedWriter::SetWriteSizeHintImpl(write_size_hint);
  if (ABSL_PREDICT_FALSE(!ok()) || compressor_ == nullptr) return;
  // Ignore failure if compression already started.
  ZSTD_CCtx_setParameter(
      compressor_.get(), ZSTD_c_srcSizeHint,
      write_size_hint == absl::nullopt
          ? 0
          : SaturatingIntCast<int>(SaturatingAdd(pos(), *write_size_hint)));
}

bool ZstdWriterBase::WriteInternal(absl::string_view src) {
  RIEGELI_ASSERT(!src.empty())
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "nothing to write";
  RIEGELI_ASSERT(ok())
      << "Failed precondition of BufferedWriter::WriteInternal(): " << status();
  Writer& dest = *DestWriter();
  return WriteInternal(src, dest, ZSTD_e_continue);
}

inline bool ZstdWriterBase::WriteInternal(absl::string_view src, Writer& dest,
                                          ZSTD_EndDirective end_op) {
  RIEGELI_ASSERT(ok())
      << "Failed precondition of ZstdWriterBase::WriteInternal(): " << status();
  if (ABSL_PREDICT_FALSE(src.size() >
                         std::numeric_limits<Position>::max() - start_pos())) {
    return FailOverflow();
  }
  if (pledged_size_ != absl::nullopt) {
    const Position next_pos = start_pos() + src.size();
    if (compressor_ == nullptr) {
      if (ABSL_PREDICT_FALSE(!src.empty())) {
        return Fail(absl::FailedPreconditionError(
            absl::StrCat("Actual size does not match pledged size: ", next_pos,
                         " > ", *pledged_size_)));
      }
      return true;
    }
    if (next_pos >= *pledged_size_) {
      // Notify `compressor_` that this is the last fragment. This enables
      // optimizations (compressing directly to a long enough output buffer).
      end_op = ZSTD_e_end;
      if (reserve_max_size_ && start_pos() == 0) {
        // Ensure that the output buffer is actually long enough.
        dest.Push(ZSTD_compressBound(*pledged_size_));
      }
    }
    if (end_op == ZSTD_e_end) {
      if (ABSL_PREDICT_FALSE(next_pos != *pledged_size_)) {
        return Fail(absl::FailedPreconditionError(absl::StrCat(
            "Actual size does not match pledged size: ", next_pos,
            next_pos > *pledged_size_ ? " > " : " < ", *pledged_size_)));
      }
    }
  }
  ZSTD_inBuffer input = {src.data(), src.size(), 0};
  for (;;) {
    ZSTD_outBuffer output = {dest.cursor(), dest.available(), 0};
    const size_t result =
        ZSTD_compressStream2(compressor_.get(), &output, &input, end_op);
    dest.set_cursor(static_cast<char*>(output.dst) + output.pos);
    if (result == 0) {
      RIEGELI_ASSERT_EQ(input.pos, input.size)
          << "ZSTD_compressStream2() returned 0 but there are still input data";
      move_start_pos(input.pos);
      if (end_op == ZSTD_e_end) compressor_.reset();
      return true;
    }
    if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
      return Fail(absl::InternalError(absl::StrCat(
          "ZSTD_compressStream2() failed: ", ZSTD_getErrorName(result))));
    }
    if (output.pos < output.size) {
      RIEGELI_ASSERT_EQ(input.pos, input.size)
          << "ZSTD_compressStream2() returned but there are still input data "
             "and output space";
      move_start_pos(input.pos);
      return true;
    }
    if (ABSL_PREDICT_FALSE(!dest.Push(1, result))) {
      return FailWithoutAnnotation(AnnotateOverDest(dest.status()));
    }
  }
}

bool ZstdWriterBase::FlushBehindBuffer(absl::string_view src,
                                       FlushType flush_type) {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::FlushBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Writer& dest = *DestWriter();
  return WriteInternal(src, dest, ZSTD_e_flush);
}

bool ZstdWriterBase::SupportsReadMode() {
  Writer* const dest = DestWriter();
  return dest != nullptr && dest->SupportsReadMode();
}

Reader* ZstdWriterBase::ReadModeBehindBuffer(Position initial_pos) {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::ReadModeBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!ZstdWriterBase::FlushBehindBuffer(
          absl::string_view(), FlushType::kFromObject))) {
    return nullptr;
  }
  Writer& dest = *DestWriter();
  Reader* const compressed_reader = dest.ReadMode(initial_compressed_pos_);
  if (ABSL_PREDICT_FALSE(compressed_reader == nullptr)) {
    FailWithoutAnnotation(AnnotateOverDest(dest.status()));
    return nullptr;
  }
  ZstdReader<>* const reader = associated_reader_.ResetReader(
      compressed_reader,
      ZstdReaderBase::Options()
          .set_dictionary(dictionary_)
          .set_buffer_options(buffer_options())
          .set_recycling_pool_options(recycling_pool_options_));
  reader->Seek(initial_pos);
  return reader;
}

}  // namespace riegeli
