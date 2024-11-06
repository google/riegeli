// Copyright 2023 Google LLC
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

#include "riegeli/xz/xz_writer.h"

#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <memory>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "lzma.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/recycling_pool.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/xz/xz_error.h"
#include "riegeli/xz/xz_reader.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if !__cpp_inline_variables
constexpr int XzWriterBase::Options::kMinCompressionLevel;
constexpr int XzWriterBase::Options::kMaxCompressionLevel;
constexpr int XzWriterBase::Options::kDefaultCompressionLevel;
constexpr XzWriterBase::Container XzWriterBase::Options::kDefaultContainer;
constexpr XzWriterBase::Check XzWriterBase::Options::kDefaultCheck;
#endif

static_assert(static_cast<int>(XzWriterBase::Container::kXz) ==
                  static_cast<int>(XzReaderBase::Container::kXz),
              "Mismatched Container enums");
static_assert(static_cast<int>(XzWriterBase::Container::kLzma) ==
                  static_cast<int>(XzReaderBase::Container::kLzma),
              "Mismatched Container enums");

void XzWriterBase::Initialize(Writer* dest, uint32_t preset, Check check,
                              int parallelism) {
  RIEGELI_ASSERT_NE(dest, nullptr)
      << "Failed precondition of XzWriter: null Writer pointer";
  if (ABSL_PREDICT_FALSE(!dest->ok())) {
    FailWithoutAnnotation(AnnotateOverDest(dest->status()));
    return;
  }
  initial_compressed_pos_ = dest->pos();
  compressor_ =
      KeyedRecyclingPool<lzma_stream, LzmaStreamKey, LzmaStreamDeleter>::global(
          recycling_pool_options_)
          .Get(LzmaStreamKey(container_,
                             container_ == Container::kXz && parallelism > 0,
                             preset),
               [] {
                 return std::unique_ptr<lzma_stream, LzmaStreamDeleter>(
                     new lzma_stream());
               });
  switch (container_) {
    case Container::kXz: {
      if (parallelism == 0) {
        flush_action_ = LZMA_SYNC_FLUSH;
        const lzma_ret liblzma_code = lzma_easy_encoder(
            compressor_.get(), preset, static_cast<lzma_check>(check));
        if (ABSL_PREDICT_FALSE(liblzma_code != LZMA_OK)) {
          FailOperation("lzma_easy_encoder()", liblzma_code);
        }
      } else {
        // `lzma_stream_encoder_mt()` does not support `LZMA_SYNC_FLUSH`.
        flush_action_ = LZMA_FULL_FLUSH;
        lzma_mt mt_options{};
        mt_options.threads = SaturatingIntCast<uint32_t>(parallelism);
        mt_options.preset = preset;
        mt_options.check = static_cast<lzma_check>(check);
        const lzma_ret liblzma_code =
            lzma_stream_encoder_mt(compressor_.get(), &mt_options);
        if (ABSL_PREDICT_FALSE(liblzma_code != LZMA_OK)) {
          FailOperation("lzma_stream_encoder_mt()", liblzma_code);
        }
      }
      return;
    }
    case Container::kLzma: {
      // `lzma_alone_encoder()` does not support `LZMA_SYNC_FLUSH` nor
      // `LZMA_FULL_FLUSH`.
      flush_action_ = LZMA_RUN;
      lzma_options_lzma options;
      if (ABSL_PREDICT_FALSE(lzma_lzma_preset(&options, preset))) {
        FailOperation("lzma_lzma_preset() failed", LZMA_OPTIONS_ERROR);
        return;
      }
      const lzma_ret liblzma_code =
          lzma_alone_encoder(compressor_.get(), &options);
      if (ABSL_PREDICT_FALSE(liblzma_code != LZMA_OK)) {
        FailOperation("lzma_alone_encoder()", liblzma_code);
      }
      return;
    }
  }
  RIEGELI_ASSERT_UNREACHABLE()
      << "Unknown container format: " << static_cast<int>(container_);
}

void XzWriterBase::DoneBehindBuffer(absl::string_view src) {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::DoneBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!ok())) return;
  Writer& dest = *DestWriter();
  WriteInternal(src, dest, LZMA_FINISH);
}

void XzWriterBase::Done() {
  BufferedWriter::Done();
  compressor_.reset();
  associated_reader_.Reset();
}

inline bool XzWriterBase::FailOperation(absl::string_view operation,
                                        lzma_ret liblzma_code) {
  RIEGELI_ASSERT_NE(liblzma_code, LZMA_OK)
      << "Failed precondition of XzWriterBase::FailOperation(): "
         "liblzma error code not failed";
  RIEGELI_ASSERT(is_open())
      << "Failed precondition of XzWriterBase::FailOperation(): "
         "Object closed";
  return Fail(xz_internal::XzErrorToStatus(operation, liblzma_code));
}

absl::Status XzWriterBase::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) {
    Writer& dest = *DestWriter();
    status = dest.AnnotateStatus(std::move(status));
  }
  // The status might have been annotated by `dest` with the compressed
  // position. Clarify that the current position is the uncompressed position
  // instead of delegating to `BufferedWriter::AnnotateStatusImpl()`.
  return AnnotateOverDest(std::move(status));
}

absl::Status XzWriterBase::AnnotateOverDest(absl::Status status) {
  if (is_open()) {
    return Annotate(status, absl::StrCat("at uncompressed byte ", pos()));
  }
  return status;
}

bool XzWriterBase::WriteInternal(absl::string_view src) {
  RIEGELI_ASSERT(!src.empty())
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "nothing to write";
  RIEGELI_ASSERT(ok())
      << "Failed precondition of BufferedWriter::WriteInternal(): " << status();
  Writer& dest = *DestWriter();
  return WriteInternal(src, dest, LZMA_RUN);
}

inline bool XzWriterBase::WriteInternal(absl::string_view src, Writer& dest,
                                        lzma_action flush) {
  RIEGELI_ASSERT(ok())
      << "Failed precondition of XzWriterBase::WriteInternal(): " << status();
  if (ABSL_PREDICT_FALSE(src.size() >
                         std::numeric_limits<Position>::max() - start_pos())) {
    return FailOverflow();
  }
  compressor_->next_in = reinterpret_cast<const uint8_t*>(src.data());
  for (;;) {
    compressor_->avail_in =
        PtrDistance(reinterpret_cast<const char*>(compressor_->next_in),
                    src.data() + src.size());
    compressor_->next_out = reinterpret_cast<uint8_t*>(dest.cursor());
    compressor_->avail_out = dest.available();
    const lzma_ret liblzma_code = lzma_code(compressor_.get(), flush);
    dest.set_cursor(reinterpret_cast<char*>(compressor_->next_out));
    const size_t length_written = PtrDistance(
        src.data(), reinterpret_cast<const char*>(compressor_->next_in));
    switch (liblzma_code) {
      case LZMA_OK:
      case LZMA_BUF_ERROR:
        if (compressor_->avail_out == 0) {
          if (ABSL_PREDICT_FALSE(!dest.Push())) {
            return FailWithoutAnnotation(AnnotateOverDest(dest.status()));
          }
          continue;
        }
        RIEGELI_ASSERT_EQ(compressor_->avail_in, 0u)
            << "lzma_code() returned but there are still input data "
               "and output space";
        break;
      case LZMA_STREAM_END:
        break;
      default:
        return FailOperation("lzma_code()", liblzma_code);
    }
    RIEGELI_ASSERT_EQ(length_written, src.size())
        << "lzma_code() returned but there are still input data";
    move_start_pos(length_written);
    return true;
  }
}

bool XzWriterBase::FlushBehindBuffer(absl::string_view src,
                                     FlushType flush_type) {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::FlushBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (src.empty() && flush_action_ == LZMA_RUN) return true;
  Writer& dest = *DestWriter();
  return WriteInternal(src, dest, flush_action_);
}

bool XzWriterBase::SupportsReadMode() {
  switch (container_) {
    case Container::kXz: {
      Writer* const dest = DestWriter();
      return dest != nullptr && dest->SupportsReadMode();
    }
    case Container::kLzma:
      return false;
  }
  RIEGELI_ASSERT_UNREACHABLE()
      << "Unknown container format: " << static_cast<int>(container_);
}

Reader* XzWriterBase::ReadModeBehindBuffer(Position initial_pos) {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::ReadModeBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!XzWriterBase::FlushBehindBuffer(
          absl::string_view(), FlushType::kFromObject))) {
    return nullptr;
  }
  Writer& dest = *DestWriter();
  Reader* const compressed_reader = dest.ReadMode(initial_compressed_pos_);
  if (ABSL_PREDICT_FALSE(compressed_reader == nullptr)) {
    FailWithoutAnnotation(AnnotateOverDest(dest.status()));
    return nullptr;
  }
  XzReader<>* const reader = associated_reader_.ResetReader(
      compressed_reader,
      XzReaderBase::Options()
          .set_container(static_cast<XzReaderBase::Container>(container_))
          .set_buffer_options(buffer_options())
          .set_recycling_pool_options(recycling_pool_options_));
  reader->Seek(initial_pos);
  return reader;
}

}  // namespace riegeli
