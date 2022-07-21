// Copyright 2022 Google LLC
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

// Enables the experimental lz4 API:
//  * `LZ4F_compressBegin_usingCDict()`
#define LZ4F_STATIC_LINKING_ONLY

#include "riegeli/lz4/lz4_writer.h"

#include <stddef.h>

#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "lz4frame.h"
#include "riegeli/base/base.h"
#include "riegeli/base/recycling_pool.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/lz4/lz4_reader.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if __cplusplus < 201703
constexpr int Lz4WriterBase::Options::kMinCompressionLevel;
constexpr int Lz4WriterBase::Options::kMaxCompressionLevel;
constexpr int Lz4WriterBase::Options::kDefaultCompressionLevel;
constexpr int Lz4WriterBase::Options::kMinWindowLog;
constexpr int Lz4WriterBase::Options::kMaxWindowLog;
constexpr int Lz4WriterBase::Options::kDefaultWindowLog;
#endif

void Lz4WriterBase::Initialize(Writer* dest, int compression_level,
                               int window_log, bool store_content_checksum,
                               bool store_block_checksum) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of Lz4Writer: null Writer pointer";
  if (ABSL_PREDICT_FALSE(!dest->ok())) {
    FailWithoutAnnotation(AnnotateOverDest(dest->status()));
    return;
  }
  initial_compressed_pos_ = dest->pos();

  const LZ4F_CDict* compression_dictionary = nullptr;
  if (!dictionary_.empty()) {
    compression_dictionary = dictionary_.PrepareCompressionDictionary();
    if (ABSL_PREDICT_FALSE(compression_dictionary == nullptr)) {
      Fail(absl::InternalError("LZ4F_createCDict() failed"));
      return;
    }
  }

  {
    LZ4F_errorCode_t result = 0;
    compressor_ =
        RecyclingPool<LZ4F_cctx, LZ4F_cctxDeleter>::global().Get([&result] {
          LZ4F_cctx* compressor = nullptr;
          result = LZ4F_createCompressionContext(&compressor, LZ4F_VERSION);
          return std::unique_ptr<LZ4F_cctx, LZ4F_cctxDeleter>(compressor);
        });
    if (ABSL_PREDICT_FALSE(LZ4F_isError(result))) {
      Fail(absl::InternalError(
          absl::StrCat("LZ4F_createCompressionContext() failed: ",
                       LZ4F_getErrorName(result))));
      return;
    }
  }

  preferences_.compressionLevel = compression_level;
  preferences_.frameInfo.blockSizeID = window_log < 18   ? LZ4F_max64KB
                                       : window_log < 20 ? LZ4F_max256KB
                                       : window_log < 22 ? LZ4F_max1MB
                                                         : LZ4F_max4MB;
  preferences_.frameInfo.contentChecksumFlag = store_content_checksum
                                                   ? LZ4F_contentChecksumEnabled
                                                   : LZ4F_noContentChecksum;
  preferences_.frameInfo.contentSize =
      pledged_size_ != absl::nullopt
          ? IntCast<unsigned long long>(*pledged_size_)
          : 0;
  preferences_.frameInfo.dictID = dictionary_.dict_id();
  preferences_.frameInfo.blockChecksumFlag =
      store_block_checksum ? LZ4F_blockChecksumEnabled : LZ4F_noBlockChecksum;

  BufferedWriter::SetWriteSizeHintImpl(pledged_size_);
  if (ABSL_PREDICT_FALSE(!dest->Push(LZ4F_HEADER_SIZE_MAX))) {
    FailWithoutAnnotation(AnnotateOverDest(dest->status()));
    return;
  }
  const size_t result = LZ4F_compressBegin_usingCDict(
      compressor_.get(), dest->cursor(), dest->available(),
      compression_dictionary, &preferences_);
  if (ABSL_PREDICT_FALSE(LZ4F_isError(result))) {
    Fail(absl::InternalError(
        absl::StrCat("LZ4F_compressBegin_usingCDict() failed: ",
                     LZ4F_getErrorName(result))));
    return;
  }
  dest->move_cursor(result);
}

void Lz4WriterBase::Done() {
  stable_src_ = true;
  BufferedWriter::Done();
  if (ABSL_PREDICT_TRUE(ok())) {
    if (pledged_size_ != absl::nullopt &&
        ABSL_PREDICT_FALSE(start_pos() < *pledged_size_)) {
      Fail(absl::FailedPreconditionError(
          absl::StrCat("Actual size does not match pledged size: ", start_pos(),
                       " < ", *pledged_size_)));
    } else if (compressor_ != nullptr) {
      Writer& dest = *dest_writer();
      DoneCompression(dest);
    }
  }
  compressor_.reset();
  dictionary_ = Lz4Dictionary();
  associated_reader_.Reset();
}

inline bool Lz4WriterBase::DoneCompression(Writer& dest) {
  if (ABSL_PREDICT_FALSE(!dest.Push(LZ4F_compressBound(0, &preferences_)))) {
    return FailWithoutAnnotation(AnnotateOverDest(dest.status()));
  }
  const size_t result = LZ4F_compressEnd(compressor_.get(), dest.cursor(),
                                         dest.available(), nullptr);
  RIEGELI_ASSERT(!LZ4F_isError(result))
      << "LZ4F_compressEnd() failed: " << LZ4F_getErrorName(result);
  dest.move_cursor(result);
  return true;
}

absl::Status Lz4WriterBase::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) {
    Writer& dest = *dest_writer();
    status = dest.AnnotateStatus(std::move(status));
  }
  // The status might have been annotated by `*dest->writer()` with the
  // compressed position. Clarify that the current position is the uncompressed
  // position instead of delegating to `BufferedWriter::AnnotateStatusImpl()`.
  return AnnotateOverDest(std::move(status));
}

absl::Status Lz4WriterBase::AnnotateOverDest(absl::Status status) {
  if (is_open()) {
    return Annotate(status, absl::StrCat("at uncompressed byte ", pos()));
  }
  return status;
}

bool Lz4WriterBase::WriteInternal(absl::string_view src) {
  RIEGELI_ASSERT(!src.empty())
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "nothing to write";
  RIEGELI_ASSERT(ok())
      << "Failed precondition of BufferedWriter::WriteInternal(): " << status();
  Writer& dest = *dest_writer();
  if (ABSL_PREDICT_FALSE(src.size() >
                         std::numeric_limits<Position>::max() - start_pos())) {
    return FailOverflow();
  }
  if (pledged_size_ != absl::nullopt) {
    const Position next_pos = start_pos() + src.size();
    if (next_pos >= *pledged_size_) {
      if (ABSL_PREDICT_FALSE(next_pos > *pledged_size_)) {
        return Fail(absl::FailedPreconditionError(
            absl::StrCat("Actual size does not match pledged size: ", next_pos,
                         " > ", *pledged_size_)));
      }
      stable_src_ = true;
    }
  }
  size_t block_size;
  switch (preferences_.frameInfo.blockSizeID) {
    case LZ4F_max64KB:
      block_size = size_t{64} << 10;
      break;
    case LZ4F_max256KB:
      block_size = size_t{256} << 10;
      break;
    case LZ4F_max1MB:
      block_size = size_t{1} << 20;
      break;
    case LZ4F_max4MB:
      block_size = size_t{4} << 20;
      break;
    default:
      RIEGELI_ASSERT_UNREACHABLE()
          << "Unexpected preferences_.frameInfo.blockSizeID: "
          << preferences_.frameInfo.blockSizeID;
  }
  LZ4F_compressOptions_t compress_options{};
  do {
    size_t src_length = src.size();
    compress_options.stableSrc = stable_src_ ? 1 : 0;
    if (!reserve_max_size_) {
      // Compressing the whole `src` in one step would require asking `dest`
      // for a potentially large flat buffer. To avoid this, split `src` into
      // smaller pieces.
      src_length = UnsignedMin(src_length, block_size - buffered_length_);
      // To reduce data copying, claim `compress_options.stableSrc` also when
      // this piece of `src` will not be needed after `WriteInternal()` returns
      // because at least a full block follows this piece.
      if (src.size() >= src_length + block_size) compress_options.stableSrc = 1;
    }
    if (ABSL_PREDICT_FALSE(
            !dest.Push(LZ4F_compressBound(src_length, &preferences_)))) {
      return FailWithoutAnnotation(AnnotateOverDest(dest.status()));
    }
    const size_t result =
        LZ4F_compressUpdate(compressor_.get(), dest.cursor(), dest.available(),
                            src.data(), src_length, &compress_options);
    if (ABSL_PREDICT_FALSE(LZ4F_isError(result))) {
      return Fail(absl::InternalError(absl::StrCat(
          "LZ4F_compressUpdate() failed: ", LZ4F_getErrorName(result))));
    }
    dest.move_cursor(result);
    move_start_pos(src_length);
    src.remove_prefix(src_length);
    buffered_length_ = (buffered_length_ + src_length) & (block_size - 1);
  } while (!src.empty());
  if (stable_src_) {
    // `LZ4F_compressEnd()` must be called while `src` is still valid.
    if (ABSL_PREDICT_FALSE(!DoneCompression(dest))) return false;
    compressor_.reset();
  }
  return true;
}

bool Lz4WriterBase::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!BufferedWriter::FlushImpl(flush_type))) return false;
  Writer& dest = *dest_writer();
  if (ABSL_PREDICT_FALSE(!dest.Push(LZ4F_compressBound(0, &preferences_)))) {
    return FailWithoutAnnotation(AnnotateOverDest(dest.status()));
  }
  const size_t result =
      LZ4F_flush(compressor_.get(), dest.cursor(), dest.available(), nullptr);
  RIEGELI_ASSERT(!LZ4F_isError(result))
      << "LZ4F_flush() failed: " << LZ4F_getErrorName(result);
  dest.move_cursor(result);
  buffered_length_ = 0;
  return true;
}

bool Lz4WriterBase::SupportsReadMode() {
  Writer* const dest = dest_writer();
  return dest != nullptr && dest->SupportsReadMode();
}

Reader* Lz4WriterBase::ReadModeBehindBuffer(Position initial_pos) {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::ReadModeBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!Lz4WriterBase::FlushImpl(FlushType::kFromObject))) {
    return nullptr;
  }
  Writer& dest = *dest_writer();
  Reader* const compressed_reader = dest.ReadMode(initial_compressed_pos_);
  if (ABSL_PREDICT_FALSE(compressed_reader == nullptr)) {
    FailWithoutAnnotation(AnnotateOverDest(dest.status()));
    return nullptr;
  }
  Lz4Reader<>* const reader = associated_reader_.ResetReader(
      compressed_reader, Lz4ReaderBase::Options()
                             .set_dictionary(dictionary_)
                             .set_buffer_options(buffer_options()));
  reader->Seek(initial_pos);
  return reader;
}

}  // namespace riegeli
