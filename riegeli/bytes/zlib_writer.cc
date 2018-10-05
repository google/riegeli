// Copyright 2018 Google LLC
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

#include "riegeli/bytes/zlib_writer.h"

#include <stddef.h>
#include <limits>
#include <string>

#include "absl/base/optimization.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/writer.h"
#include "zconf.h"
#include "zlib.h"

namespace riegeli {

void ZlibWriterBase::Initialize(int compression_level, int window_bits) {
  compressor_.reset(new z_stream());
  if (ABSL_PREDICT_FALSE(deflateInit2(compressor_.get(), compression_level,
                                      Z_DEFLATED, window_bits, 8,
                                      Z_DEFAULT_STRATEGY) != Z_OK)) {
    FailOperation("deflateInit2()");
  }
}

void ZlibWriterBase::Done() {
  if (ABSL_PREDICT_TRUE(healthy())) {
    Writer* const dest = dest_writer();
    const size_t buffered_length = written_to_buffer();
    cursor_ = start_;
    WriteInternal(absl::string_view(start_, buffered_length), dest, Z_FINISH);
  }
  compressor_.reset();
  BufferedWriter::Done();
}

inline bool ZlibWriterBase::FailOperation(absl::string_view operation) {
  std::string message = absl::StrCat(operation, " failed");
  if (compressor_->msg != nullptr) {
    absl::StrAppend(&message, ": ", compressor_->msg);
  }
  return Fail(message);
}

bool ZlibWriterBase::WriteInternal(absl::string_view src) {
  RIEGELI_ASSERT(!src.empty())
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "nothing to write";
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of BufferedWriter::WriteInternal(): "
      << message();
  RIEGELI_ASSERT_EQ(written_to_buffer(), 0u)
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "buffer not empty";
  Writer* const dest = dest_writer();
  return WriteInternal(src, dest, Z_NO_FLUSH);
}

inline bool ZlibWriterBase::WriteInternal(absl::string_view src, Writer* dest,
                                          int flush) {
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of ZlibWriterBase::WriteInternal(): "
      << message();
  RIEGELI_ASSERT_EQ(written_to_buffer(), 0u)
      << "Failed precondition of ZlibWriterBase::WriteInternal(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(src.size() >
                         std::numeric_limits<Position>::max() - limit_pos())) {
    limit_ = start_;
    return FailOverflow();
  }
  compressor_->next_in =
      const_cast<z_const Bytef*>(reinterpret_cast<const Bytef*>(src.data()));
  for (;;) {
    // If compressor_->avail_out == 0 then deflate() returns Z_BUF_ERROR,
    // so dest->Push() first.
    if (ABSL_PREDICT_FALSE(!dest->Push())) {
      limit_ = start_;
      return Fail(*dest);
    }
    size_t avail_in =
        PtrDistance(reinterpret_cast<const char*>(compressor_->next_in),
                    src.data() + src.size());
    int op = flush;
    if (ABSL_PREDICT_FALSE(avail_in > std::numeric_limits<uInt>::max())) {
      avail_in = size_t{std::numeric_limits<uInt>::max()};
      op = Z_NO_FLUSH;
    }
    compressor_->avail_in = IntCast<uInt>(avail_in);
    compressor_->next_out = reinterpret_cast<Bytef*>(dest->cursor());
    compressor_->avail_out =
        UnsignedMin(dest->available(), std::numeric_limits<uInt>::max());
    const int result = deflate(compressor_.get(), op);
    dest->set_cursor(reinterpret_cast<char*>(compressor_->next_out));
    const size_t length_written = PtrDistance(
        src.data(), reinterpret_cast<const char*>(compressor_->next_in));
    switch (result) {
      case Z_OK:
        if (compressor_->avail_out == 0 ||
            ABSL_PREDICT_FALSE(length_written < src.size())) {
          continue;
        }
        break;
      case Z_STREAM_END:
        break;
      case Z_BUF_ERROR:
        RIEGELI_ASSERT_EQ(op, Z_PARTIAL_FLUSH)
            << "deflate() returned an unexpected Z_BUF_ERROR";
        break;
      default:
        limit_ = start_;
        return FailOperation("deflate()");
    }
    RIEGELI_ASSERT_EQ(length_written, src.size())
        << "deflate() returned but there are still input data";
    start_pos_ += length_written;
    return true;
  }
}

bool ZlibWriterBase::Flush(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Writer* const dest = dest_writer();
  const size_t buffered_length = written_to_buffer();
  cursor_ = start_;
  if (ABSL_PREDICT_FALSE(!WriteInternal(
          absl::string_view(start_, buffered_length), dest, Z_PARTIAL_FLUSH))) {
    return false;
  }
  if (ABSL_PREDICT_FALSE(!dest->Flush(flush_type))) {
    if (ABSL_PREDICT_FALSE(!dest->healthy())) {
      limit_ = start_;
      return Fail(*dest);
    }
    return false;
  }
  return true;
}

template class ZlibWriter<Writer*>;
template class ZlibWriter<std::unique_ptr<Writer>>;

}  // namespace riegeli
