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

// Make ZSTD_WINDOWLOG_MIN, ZSTD_WINDOWLOG_MAX, ZSTD_getParams(), and
// ZSTD_initCStream_advanced() available.
#define ZSTD_STATIC_LINKING_ONLY

#include "riegeli/bytes/zstd_writer.h"

#include <stddef.h>
#include <limits>

#include "absl/base/optimization.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/writer.h"
#include "zstd.h"

namespace riegeli {

// These methods are defined here instead of in zstd_writer.h because
// ZSTD_WINDOWLOG_{MIN,MAX} require ZSTD_STATIC_LINKING_ONLY.
int ZstdWriter::Options::kMinWindowLog() { return ZSTD_WINDOWLOG_MIN; }
int ZstdWriter::Options::kMaxWindowLog() { return ZSTD_WINDOWLOG_MAX; }

ZstdWriter& ZstdWriter::operator=(ZstdWriter&& src) noexcept {
  BufferedWriter::operator=(std::move(src));
  owned_dest_ = std::move(src.owned_dest_);
  dest_ = riegeli::exchange(src.dest_, nullptr);
  compression_level_ = riegeli::exchange(src.compression_level_, 0);
  window_log_ = riegeli::exchange(src.window_log_, 0),
  size_hint_ = riegeli::exchange(src.size_hint_, 0);
  if (src.compressor_ != nullptr || ABSL_PREDICT_FALSE(!healthy())) {
    compressor_ = std::move(src.compressor_);
  } else if (compressor_ != nullptr) {
    // Reuse this ZSTD_CStream because if options are the same then reusing it
    // is faster than creating it again.
    InitializeCStream();
  }
  return *this;
}

void ZstdWriter::Done() {
  PushInternal();
  RIEGELI_ASSERT_EQ(written_to_buffer(), 0u)
      << "BufferedWriter::PushInternal() did not empty the buffer";
  if (ABSL_PREDICT_TRUE(healthy())) {
    FlushInternal(ZSTD_endStream, "ZSTD_endStream()");
  }
  if (owned_dest_ != nullptr) {
    if (ABSL_PREDICT_TRUE(healthy())) {
      if (ABSL_PREDICT_FALSE(!owned_dest_->Close())) Fail(*owned_dest_);
    }
    owned_dest_.reset();
  }
  dest_ = nullptr;
  // Do not reset compressor_. It might be reused if a fresh ZstdWriter is
  // assigned to *this.
  BufferedWriter::Done();
}

inline bool ZstdWriter::EnsureCStreamCreated() {
  if (ABSL_PREDICT_FALSE(compressor_ == nullptr)) {
    compressor_.reset(ZSTD_createCStream());
    if (ABSL_PREDICT_FALSE(compressor_ == nullptr)) {
      return Fail("ZSTD_createCStream() failed");
    }
    return InitializeCStream();
  }
  return true;
}

inline bool ZstdWriter::InitializeCStream() {
  ZSTD_parameters params = ZSTD_getParams(
      compression_level_, IntCast<unsigned long long>(size_hint_), 0);
  if (window_log_ >= 0) {
    params.cParams.windowLog = IntCast<unsigned>(window_log_);
  }
  const size_t result = ZSTD_initCStream_advanced(
      compressor_.get(), nullptr, 0, params, ZSTD_CONTENTSIZE_UNKNOWN);
  if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
    return Fail(absl::StrCat("ZSTD_initCStream_advanced() failed: ",
                             ZSTD_getErrorName(result)));
  }
  return true;
}

bool ZstdWriter::Flush(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!PushInternal())) return false;
  RIEGELI_ASSERT_EQ(written_to_buffer(), 0u)
      << "BufferedWriter::PushInternal() did not empty the buffer";
  if (ABSL_PREDICT_FALSE(
          !FlushInternal(ZSTD_flushStream, "ZSTD_flushStream()"))) {
    return false;
  }
  if (ABSL_PREDICT_FALSE(!dest_->Flush(flush_type))) {
    if (dest_->healthy()) return false;
    limit_ = start_;
    return Fail(*dest_);
  }
  return true;
}

bool ZstdWriter::WriteInternal(absl::string_view src) {
  RIEGELI_ASSERT(!src.empty())
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "nothing to write";
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of BufferedWriter::WriteInternal(): "
      << message();
  RIEGELI_ASSERT_EQ(written_to_buffer(), 0u)
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "buffer not cleared";
  if (ABSL_PREDICT_FALSE(src.size() >
                         std::numeric_limits<Position>::max() - limit_pos())) {
    limit_ = start_;
    return FailOverflow();
  }
  if (ABSL_PREDICT_FALSE(!EnsureCStreamCreated())) return false;
  ZSTD_inBuffer input = {src.data(), src.size(), 0};
  for (;;) {
    ZSTD_outBuffer output = {dest_->cursor(), dest_->available(), 0};
    const size_t result =
        ZSTD_compressStream(compressor_.get(), &output, &input);
    dest_->set_cursor(static_cast<char*>(output.dst) + output.pos);
    if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
      limit_ = start_;
      return Fail(absl::StrCat("ZSTD_compressStream() failed: ",
                               ZSTD_getErrorName(result)));
    }
    if (output.pos < output.size) {
      RIEGELI_ASSERT_EQ(input.pos, input.size)
          << "ZSTD_compressStream() returned but there are still input data "
             "and output space";
      start_pos_ += input.pos;
      return true;
    }
    if (ABSL_PREDICT_FALSE(!dest_->Push())) {
      limit_ = start_;
      return Fail(*dest_);
    }
  }
}

template <typename Function>
bool ZstdWriter::FlushInternal(Function function,
                               absl::string_view function_name) {
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of ZstdWriter::FlushInternal(): " << message();
  RIEGELI_ASSERT_EQ(written_to_buffer(), 0u)
      << "Failed precondition of ZstdWriter::FlushInternal(): "
         "buffer not cleared";
  if (ABSL_PREDICT_FALSE(!EnsureCStreamCreated())) return false;
  for (;;) {
    ZSTD_outBuffer output = {dest_->cursor(), dest_->available(), 0};
    const size_t result = function(compressor_.get(), &output);
    dest_->set_cursor(static_cast<char*>(output.dst) + output.pos);
    if (result == 0) return true;
    if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
      limit_ = start_;
      return Fail(
          absl::StrCat(function_name, " failed: ", ZSTD_getErrorName(result)));
    }
    RIEGELI_ASSERT_EQ(output.pos, output.size)
        << function_name << " returned but there is still output space";
    if (ABSL_PREDICT_FALSE(!dest_->Push())) {
      limit_ = start_;
      return Fail(*dest_);
    }
  }
}

}  // namespace riegeli
