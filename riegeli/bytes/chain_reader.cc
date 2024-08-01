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

#include "riegeli/bytes/chain_reader.h"

#include <stddef.h>

#include <limits>
#include <memory>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/external_ref.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/pullable_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void ChainReaderBase::Done() {
  PullableReader::Done();
  iter_ = Chain::BlockIterator();
}

bool ChainReaderBase::PullBehindScratch(size_t recommended_length) {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of PullableReader::PullBehindScratch(): "
         "enough data available, use Pull() instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::PullBehindScratch(): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  const Chain& src = *iter_.chain();
  RIEGELI_ASSERT_LE(limit_pos(), src.size())
      << "ChainReader source changed unexpectedly";
  if (ABSL_PREDICT_FALSE(iter_ == src.blocks().cend())) return false;
  while (++iter_ != src.blocks().cend()) {
    if (ABSL_PREDICT_TRUE(!iter_->empty())) {
      RIEGELI_ASSERT_LE(iter_->size(), src.size() - limit_pos())
          << "ChainReader source changed unexpectedly";
      set_buffer(iter_->data(), iter_->size());
      move_limit_pos(available());
      return true;
    }
  }
  set_buffer();
  return false;
}

bool ChainReaderBase::ReadBehindScratch(size_t length, Chain& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of PullableReader::ReadBehindScratch(Chain&): "
         "enough data available, use Read(Chain&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of PullableReader::ReadBehindScratch(Chain&): "
         "Chain size overflow";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::ReadBehindScratch(Chain&): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  const Chain& src = *iter_.chain();
  RIEGELI_ASSERT_LE(limit_pos(), src.size())
      << "ChainReader source changed unexpectedly";
  if (length <= available()) {
    dest.Append(ExternalRef(*iter_, absl::string_view(cursor(), length)));
    move_cursor(length);
    return true;
  }
  if (ABSL_PREDICT_FALSE(iter_ == src.blocks().cend())) return false;
  dest.Append(ExternalRef(*iter_, absl::string_view(cursor(), available())));
  length -= available();
  while (++iter_ != src.blocks().cend()) {
    RIEGELI_ASSERT_LE(iter_->size(), src.size() - limit_pos())
        << "ChainReader source changed unexpectedly";
    move_limit_pos(iter_->size());
    if (length <= iter_->size()) {
      set_buffer(iter_->data(), iter_->size(), length);
      dest.Append(ExternalRef(*iter_, absl::string_view(start(), length)));
      return true;
    }
    dest.Append(*iter_);
    length -= iter_->size();
  }
  set_buffer();
  return false;
}

bool ChainReaderBase::ReadBehindScratch(size_t length, absl::Cord& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of PullableReader::ReadBehindScratch(Cord&): "
         "enough data available, use Read(Cord&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of PullableReader::ReadBehindScratch(Cord&): "
         "Cord size overflow";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::ReadBehindScratch(Cord&): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  const Chain& src = *iter_.chain();
  RIEGELI_ASSERT_LE(limit_pos(), src.size())
      << "ChainReader source changed unexpectedly";
  if (length <= available()) {
    ExternalRef(*iter_, absl::string_view(cursor(), length)).AppendTo(dest);
    move_cursor(length);
    return true;
  }
  if (ABSL_PREDICT_FALSE(iter_ == src.blocks().cend())) return false;
  ExternalRef(*iter_, absl::string_view(cursor(), available())).AppendTo(dest);
  length -= available();
  while (++iter_ != src.blocks().cend()) {
    RIEGELI_ASSERT_LE(iter_->size(), src.size() - limit_pos())
        << "ChainReader source changed unexpectedly";
    move_limit_pos(iter_->size());
    if (length <= iter_->size()) {
      set_buffer(iter_->data(), iter_->size(), length);
      ExternalRef(*iter_, absl::string_view(start(), length)).AppendTo(dest);
      return true;
    }
    ExternalRef(*iter_).AppendTo(dest);
    length -= iter_->size();
  }
  set_buffer();
  return false;
}

bool ChainReaderBase::CopyBehindScratch(Position length, Writer& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of PullableReader::CopyBehindScratch(Writer&): "
         "enough data available, use Copy(Writer&) instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::CopyBehindScratch(Writer&): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  const Chain& src = *iter_.chain();
  RIEGELI_ASSERT_LE(limit_pos(), src.size())
      << "ChainReader source changed unexpectedly";
  const size_t length_to_copy =
      UnsignedMin(length, src.size() - IntCast<size_t>(pos()));
  if (length_to_copy == src.size()) {
    if (!Skip(length_to_copy)) {
      RIEGELI_ASSERT_UNREACHABLE() << "ChainReader::Skip() failed";
    }
    if (ABSL_PREDICT_FALSE(!dest.Write(src))) return false;
  } else if (length_to_copy <= kMaxBytesToCopy) {
    if (ABSL_PREDICT_FALSE(!dest.Push(length_to_copy))) return false;
    if (!Read(length_to_copy, dest.cursor())) {
      RIEGELI_ASSERT_UNREACHABLE() << "ChainReader::Read(char*) failed";
    }
    dest.move_cursor(length_to_copy);
  } else {
    Chain data;
    if (!Read(length_to_copy, data)) {
      RIEGELI_ASSERT_UNREACHABLE() << "ChainReader::Read(Chain&) failed";
    }
    if (ABSL_PREDICT_FALSE(!dest.Write(std::move(data)))) return false;
  }
  return length_to_copy == length;
}

bool ChainReaderBase::CopyBehindScratch(size_t length, BackwardWriter& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of "
         "PullableReader::CopyBehindScratch(BackwardWriter&): "
         "enough data available, use Copy(BackwardWriter&) instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of "
         "PullableReader::CopyBehindScratch(BackwardWriter&): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  const Chain& src = *iter_.chain();
  RIEGELI_ASSERT_LE(limit_pos(), src.size())
      << "ChainReader source changed unexpectedly";
  if (ABSL_PREDICT_FALSE(length > src.size() - pos())) {
    if (!Seek(src.size())) {
      RIEGELI_ASSERT_UNREACHABLE() << "ChainReader::Seek() failed";
    }
    return false;
  }
  if (length == src.size()) {
    if (!Skip(length)) {
      RIEGELI_ASSERT_UNREACHABLE() << "ChainReader::Skip() failed";
    }
    return dest.Write(src);
  }
  if (length <= kMaxBytesToCopy) {
    if (ABSL_PREDICT_FALSE(!dest.Push(length))) return false;
    dest.move_cursor(length);
    if (ABSL_PREDICT_FALSE(!ReadBehindScratch(length, dest.cursor()))) {
      dest.set_cursor(dest.cursor() + length);
      return false;
    }
    return true;
  }
  Chain data;
  if (!ReadBehindScratch(length, data)) {
    RIEGELI_ASSERT_UNREACHABLE()
        << "ChainReader::ReadBehindScratch(Chain&) failed";
  }
  return dest.Write(std::move(data));
}

bool ChainReaderBase::SeekBehindScratch(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of PullableReader::SeekBehindScratch(): "
         "position in the buffer, use Seek() instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::SeekBehindScratch(): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  const Chain& src = *iter_.chain();
  RIEGELI_ASSERT_LE(limit_pos(), src.size())
      << "ChainReader source changed unexpectedly";
  if (new_pos >= src.size()) {
    // Source ends.
    iter_ = src.blocks().cend();
    set_limit_pos(src.size());
    set_buffer();
    return new_pos == src.size();
  }
  const Chain::BlockAndChar block_and_char =
      src.BlockAndCharIndex(IntCast<size_t>(new_pos));
  iter_ = block_and_char.block_iter;
  set_buffer(iter_->data(), iter_->size(), block_and_char.char_index);
  set_limit_pos(new_pos + available());
  return true;
}

absl::optional<Position> ChainReaderBase::SizeImpl() {
  if (ABSL_PREDICT_FALSE(!ok())) return absl::nullopt;
  const Chain& src = *iter_.chain();
  return src.size();
}

std::unique_ptr<Reader> ChainReaderBase::NewReaderImpl(Position initial_pos) {
  if (ABSL_PREDICT_FALSE(!ok())) return nullptr;
  // `NewReaderImpl()` is thread-safe from this point.
  const Chain& src = *iter_.chain();
  std::unique_ptr<Reader> reader = std::make_unique<ChainReader<>>(&src);
  reader->Seek(initial_pos);
  return reader;
}

}  // namespace riegeli
