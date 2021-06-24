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
//  * `ZSTD_d_stableOutBuffer`
//  * `ZSTD_createDDict_advanced()`
//  * `ZSTD_dictLoadMethod_e`
//  * `ZSTD_dictContentType_e`
#define ZSTD_STATIC_LINKING_ONLY

#include "riegeli/zstd/zstd_reader.h"

#include <stddef.h>

#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/base/thread_annotations.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/recycling_pool.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/reader.h"
#include "zstd.h"

namespace riegeli {

// Constants are defined as integer literals in zstd_reader.h and asserted here
// to avoid depending on `ZSTD_STATIC_LINKING_ONLY` in zstd_reader.h.
static_assert(
    static_cast<ZSTD_dictContentType_e>(ZstdReaderBase::ContentType::kAuto) ==
            ZSTD_dct_auto &&
        static_cast<ZSTD_dictContentType_e>(
            ZstdReaderBase::ContentType::kRaw) == ZSTD_dct_rawContent &&
        static_cast<ZSTD_dictContentType_e>(
            ZstdReaderBase::ContentType::kSerialized) == ZSTD_dct_fullDict,
    "Enum values of ZstdReaderBase::ContentType disagree with ZSTD_dct "
    "constants");

namespace {

struct ZSTD_DDictDeleter {
  void operator()(ZSTD_DDict* ptr) const { ZSTD_freeDDict(ptr); }
};

}  // namespace

struct ZstdReaderBase::Dictionary::Shared {
  absl::Mutex mutex;
  bool present ABSL_GUARDED_BY(mutex) = false;
  std::shared_ptr<const ZSTD_DDict> prepared_dictionary ABSL_GUARDED_BY(mutex);
};

std::shared_ptr<ZstdReaderBase::Dictionary::Shared>
ZstdReaderBase::Dictionary::EnsureShared() const {
  absl::MutexLock lock(&mutex_);
  if (shared_ == nullptr) shared_ = std::make_shared<Shared>();
  return shared_;
}

inline std::shared_ptr<const ZSTD_DDict>
ZstdReaderBase::Dictionary::PrepareDictionary() const {
  const std::shared_ptr<Shared> shared = EnsureShared();
  {
    absl::MutexLock lock(&shared->mutex);
    if (shared->present) return shared->prepared_dictionary;
  }
  std::unique_ptr<ZSTD_DDict, ZSTD_DDictDeleter> prepared_dictionary(
      ZSTD_createDDict_advanced(
          data().data(), data().size(), ZSTD_dlm_byRef,
          static_cast<ZSTD_dictContentType_e>(content_type()),
          ZSTD_defaultCMem));
  absl::MutexLock lock(&shared->mutex);
  shared->present = true;
  shared->prepared_dictionary = std::move(prepared_dictionary);
  return shared->prepared_dictionary;
}

void ZstdReaderBase::Initialize(Reader* src) {
  RIEGELI_ASSERT(src != nullptr)
      << "Failed precondition of ZstdReader: null Reader pointer";
  if (ABSL_PREDICT_FALSE(!src->healthy()) && src->available() == 0) {
    Fail(*src);
    return;
  }
  decompressor_ = RecyclingPool<ZSTD_DCtx, ZSTD_DCtxDeleter>::global().Get(
      [] {
        return std::unique_ptr<ZSTD_DCtx, ZSTD_DCtxDeleter>(ZSTD_createDCtx());
      },
      [](ZSTD_DCtx* decompressor) {
        {
          const size_t result =
              ZSTD_DCtx_reset(decompressor, ZSTD_reset_session_and_parameters);
          RIEGELI_ASSERT(!ZSTD_isError(result))
              << "ZSTD_DCtx_reset() failed: " << ZSTD_getErrorName(result);
        }
#if ZSTD_VERSION_NUMBER <= 10405
        // Workaround for https://github.com/facebook/zstd/issues/2331
        {
          const size_t result =
              ZSTD_DCtx_setParameter(decompressor, ZSTD_d_stableOutBuffer, 0);
          RIEGELI_ASSERT(!ZSTD_isError(result))
              << "ZSTD_DCtx_setParameter(ZSTD_d_stableOutBuffer) failed: "
              << ZSTD_getErrorName(result);
        }
#endif
      });
  if (ABSL_PREDICT_FALSE(decompressor_ == nullptr)) {
    Fail(absl::InternalError("ZSTD_createDCtx() failed"));
    return;
  }
  {
    // Maximum window size could also be found with
    // `ZSTD_dParam_getBounds(ZSTD_d_windowLogMax)`.
    const size_t result =
        ZSTD_DCtx_setParameter(decompressor_.get(), ZSTD_d_windowLogMax,
                               sizeof(size_t) == 4 ? 30 : 31);
    if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
      Fail(absl::InternalError(
          absl::StrCat("ZSTD_DCtx_setParameter(ZSTD_d_windowLogMax) failed: ",
                       ZSTD_getErrorName(result))));
      return;
    }
  }
  if (!dictionary_.empty()) {
    prepared_dictionary_ = dictionary_.PrepareDictionary();
    if (ABSL_PREDICT_FALSE(prepared_dictionary_ == nullptr)) {
      Fail(absl::InternalError("ZSTD_createDDict_advanced() failed"));
      return;
    }
    const size_t result =
        ZSTD_DCtx_refDDict(decompressor_.get(), prepared_dictionary_.get());
    if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
      Fail(absl::InternalError(absl::StrCat("ZSTD_DCtx_refDDict() failed: ",
                                            ZSTD_getErrorName(result))));
      return;
    }
  }
  uncompressed_size_ = ZstdUncompressedSize(*src);
  if (uncompressed_size_ != absl::nullopt) {
    // If `uncompressed_size_` is 0, set `size_hint` to 1, because the first
    // `Pull()` call will need a non-empty destination buffer before calling the
    // Zstd decoder.
    set_size_hint(UnsignedMax(Position{1}, *uncompressed_size_));
  }
  just_initialized_ = true;
}

void ZstdReaderBase::Done() {
  if (ABSL_PREDICT_FALSE(truncated_)) {
    Reader& src = *src_reader();
    Fail(Annotate(absl::DataLossError("Truncated Zstd-compressed stream"),
                  absl::StrCat("at byte ", src.pos())));
  }
  BufferedReader::Done();
  decompressor_.reset();
}

void ZstdReaderBase::AnnotateFailure(absl::Status& status) {
  RIEGELI_ASSERT(!status.ok())
      << "Failed precondition of Object::AnnotateFailure(): status not failed";
  status = Annotate(status, absl::StrCat("at uncompressed byte ", pos()));
}

bool ZstdReaderBase::PullSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Reader::PullSlow(): "
         "enough data available, use Pull() instead";
  // After all data have been decompressed, skip `BufferedReader::PullSlow()`
  // to avoid allocating the buffer in case it was not allocated yet.
  if (ABSL_PREDICT_FALSE(decompressor_ == nullptr)) return false;
  return BufferedReader::PullSlow(min_length, recommended_length);
}

bool ZstdReaderBase::ReadInternal(size_t min_length, size_t max_length,
                                  char* dest) {
  RIEGELI_ASSERT_GT(min_length, 0u)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "nothing to read";
  RIEGELI_ASSERT_GE(max_length, min_length)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "max_length < min_length";
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of BufferedReader::ReadInternal(): " << status();
  if (ABSL_PREDICT_FALSE(decompressor_ == nullptr)) return false;
  Reader& src = *src_reader();
  truncated_ = false;
  if (ABSL_PREDICT_FALSE(max_length >
                         std::numeric_limits<Position>::max() - limit_pos())) {
    return FailOverflow();
  }
  size_t effective_min_length = min_length;
  if (just_initialized_ && !growing_source_ &&
      uncompressed_size_ != absl::nullopt &&
      max_length >= *uncompressed_size_) {
    // Avoid a memory copy from an internal buffer of the Zstd engine to `dest`
    // by promising to decompress all remaining data to `dest`.
    const size_t result =
        ZSTD_DCtx_setParameter(decompressor_.get(), ZSTD_d_stableOutBuffer, 1);
    if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
      return Fail(absl::InternalError(absl::StrCat(
          "ZSTD_DCtx_setParameter(ZSTD_d_stableOutBuffer) failed: ",
          ZSTD_getErrorName(result))));
    }
    effective_min_length = std::numeric_limits<size_t>::max();
  }
  just_initialized_ = false;
  ZSTD_outBuffer output = {dest, max_length, 0};
  for (;;) {
    ZSTD_inBuffer input = {src.cursor(), src.available(), 0};
    const size_t result =
        ZSTD_decompressStream(decompressor_.get(), &output, &input);
    src.set_cursor(static_cast<const char*>(input.src) + input.pos);
    if (ABSL_PREDICT_FALSE(result == 0)) {
      decompressor_.reset();
      move_limit_pos(output.pos);
      return output.pos >= min_length;
    }
    if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
      Fail(Annotate(
          absl::DataLossError(absl::StrCat("ZSTD_decompressStream() failed: ",
                                           ZSTD_getErrorName(result))),
          absl::StrCat("at byte ", src.pos())));
      move_limit_pos(output.pos);
      return output.pos >= min_length;
    }
    if (output.pos >= effective_min_length) {
      move_limit_pos(output.pos);
      return true;
    }
    RIEGELI_ASSERT_EQ(input.pos, input.size)
        << "ZSTD_decompressStream() returned but there are still input data "
           "and output space";
    if (ABSL_PREDICT_FALSE(!src.Pull(1, result))) {
      move_limit_pos(output.pos);
      if (ABSL_PREDICT_FALSE(!src.healthy())) {
        Fail(src);
      } else if (growing_source_) {
        truncated_ = true;
      } else {
        Fail(Annotate(absl::DataLossError("Truncated Zstd-compressed stream"),
                      absl::StrCat("at byte ", src.pos())));
      }
      return output.pos >= min_length;
    }
  }
}

absl::optional<Position> ZstdReaderBase::SizeImpl() {
  if (ABSL_PREDICT_FALSE(!healthy())) return absl::nullopt;
  if (ABSL_PREDICT_FALSE(uncompressed_size_ == absl::nullopt)) {
    Fail(absl::UnimplementedError(
        "Uncompressed size was not stored in the Zstd-compressed stream"));
    return absl::nullopt;
  }
  return *uncompressed_size_;
}

absl::optional<Position> ZstdUncompressedSize(Reader& src) {
  src.Pull(18 /* `ZSTD_FRAMEHEADERSIZE_MAX` */);
  unsigned long long uncompressed_size =
      ZSTD_getFrameContentSize(src.cursor(), src.available());
  if (uncompressed_size == ZSTD_CONTENTSIZE_UNKNOWN ||
      uncompressed_size == ZSTD_CONTENTSIZE_ERROR) {
    return absl::nullopt;
  }
  return uncompressed_size;
}

}  // namespace riegeli
