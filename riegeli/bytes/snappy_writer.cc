// Copyright 2019 Google LLC
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

#include "riegeli/bytes/snappy_writer.h"

#include <stddef.h>

#include <limits>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/snappy_streams.h"
#include "riegeli/bytes/writer.h"
#include "snappy.h"

namespace riegeli {

void SnappyWriterBase::Done() {
  if (ABSL_PREDICT_TRUE(healthy())) SyncBuffer();
  Writer::Done();
  if (ABSL_PREDICT_TRUE(healthy())) {
    Writer* const dest = dest_writer();
    ChainReader<> reader(&uncompressed_);
    internal::ReaderSnappySource source(&reader);
    internal::WriterSnappySink sink(dest);
    snappy::Compress(&source, &sink);
    if (ABSL_PREDICT_FALSE(!dest->healthy())) {
      Fail(*dest);
    } else if (ABSL_PREDICT_FALSE(!reader.VerifyEndAndClose())) {
      Fail(reader);
    }
  }
}

bool SnappyWriterBase::PushSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_GT(min_length, available())
      << "Failed precondition of Writer::PushSlow(): "
         "length too small, use Push() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(min_length > std::numeric_limits<size_t>::max() -
                                          uncompressed_.size())) {
    return FailOverflow();
  }
  SyncBuffer();
  MakeBuffer(min_length);
  return true;
}

bool SnappyWriterBase::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_GT(src.size(), UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Writer::WriteSlow(Chain): "
         "length too small, use Write(Chain) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  for (Chain::BlockIterator iter = src.blocks().cbegin();
       iter != src.blocks().cend(); ++iter) {
    absl::string_view fragment = *iter;
    const size_t in_first_block = -IntCast<size_t>(pos()) % snappy::kBlockSize;
    if (fragment.size() > in_first_block) {
      if (!Write(fragment.substr(0, in_first_block))) {
        RIEGELI_ASSERT_UNREACHABLE()
            << "SnappyWriterBase::Write() failed: " << status();
      }
      fragment.remove_prefix(in_first_block);
      const size_t in_whole_blocks =
          fragment.size() - fragment.size() % snappy::kBlockSize;
      if (in_whole_blocks > 0) {
        SyncBuffer();
        start_pos_ += in_whole_blocks;
        iter.AppendSubstrTo(fragment.substr(0, in_whole_blocks), &uncompressed_,
                            size_hint_);
        MakeBuffer();
        fragment.remove_prefix(in_whole_blocks);
      }
    }
    if (!Write(fragment)) {
      RIEGELI_ASSERT_UNREACHABLE()
          << "SnappyWriterBase::Write() failed: " << status();
    }
  }
  return true;
}

bool SnappyWriterBase::Flush(FlushType flush_type) { return healthy(); }

inline void SnappyWriterBase::SyncBuffer() {
  start_pos_ = pos();
  uncompressed_.RemoveSuffix(available());
  start_ = nullptr;
  cursor_ = nullptr;
  limit_ = nullptr;
}

inline void SnappyWriterBase::MakeBuffer(size_t min_length) {
  size_t length =
      min_length + -(uncompressed_.size() + min_length) % snappy::kBlockSize;
  if (uncompressed_.size() < size_hint_) {
    length = UnsignedMax(UnsignedMin(length, size_hint_ - uncompressed_.size()),
                         min_length);
  }
  const absl::Span<char> buffer =
      uncompressed_.AppendFixedBuffer(length, size_hint_);
  start_ = buffer.data();
  cursor_ = start_;
  limit_ = start_ + buffer.size();
}

}  // namespace riegeli
