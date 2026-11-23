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
#include <optional>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/log/absl_check.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
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
  ABSL_DCHECK_EQ(available(), 0u)
      << "Failed precondition of PullableReader::PullBehindScratch(): "
         "enough data available, use Pull() instead";
  ABSL_DCHECK(!scratch_used())
      << "Failed precondition of PullableReader::PullBehindScratch(): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  const Chain& src = *iter_.chain();
  ABSL_DCHECK_LE(limit_pos(), src.size())
      << "ChainReader source changed unexpectedly";
  if (ABSL_PREDICT_FALSE(iter_ == src.blocks().cend())) return false;
  while (++iter_ != src.blocks().cend()) {
    if (ABSL_PREDICT_TRUE(!iter_->empty())) {
      ABSL_DCHECK_LE(iter_->size(), src.size() - limit_pos())
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
  ABSL_DCHECK_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of PullableReader::ReadBehindScratch(Chain&): "
         "enough data available, use Read(Chain&) instead";
  ABSL_DCHECK_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of PullableReader::ReadBehindScratch(Chain&): "
         "Chain size overflow";
  ABSL_DCHECK(!scratch_used())
      << "Failed precondition of PullableReader::ReadBehindScratch(Chain&): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  const Chain& src = *iter_.chain();
  ABSL_DCHECK_LE(limit_pos(), src.size())
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
    ABSL_DCHECK_LE(iter_->size(), src.size() - limit_pos())
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
  ABSL_DCHECK_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of PullableReader::ReadBehindScratch(Cord&): "
         "enough data available, use Read(Cord&) instead";
  ABSL_DCHECK_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of PullableReader::ReadBehindScratch(Cord&): "
         "Cord size overflow";
  ABSL_DCHECK(!scratch_used())
      << "Failed precondition of PullableReader::ReadBehindScratch(Cord&): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  const Chain& src = *iter_.chain();
  ABSL_DCHECK_LE(limit_pos(), src.size())
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
    ABSL_DCHECK_LE(iter_->size(), src.size() - limit_pos())
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
  ABSL_DCHECK_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of PullableReader::CopyBehindScratch(Writer&): "
         "enough data available, use Copy(Writer&) instead";
  ABSL_DCHECK(!scratch_used())
      << "Failed precondition of PullableReader::CopyBehindScratch(Writer&): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  const Chain& src = *iter_.chain();
  ABSL_DCHECK_LE(limit_pos(), src.size())
      << "ChainReader source changed unexpectedly";
  const size_t length_to_copy =
      UnsignedMin(length, src.size() - IntCast<size_t>(pos()));
  if (length_to_copy == src.size()) {
    const bool skip_ok = Skip(length_to_copy);
    ABSL_DCHECK(skip_ok);
    if (ABSL_PREDICT_FALSE(!dest.Write(src))) return false;
  } else if (length_to_copy <= kMaxBytesToCopy) {
    if (ABSL_PREDICT_FALSE(!dest.Push(length_to_copy))) return false;
    const bool read_ok = Read(length_to_copy, dest.cursor());
    ABSL_DCHECK(read_ok);
    dest.move_cursor(length_to_copy);
  } else {
    Chain data;
    const bool read_ok = Read(length_to_copy, data);
    ABSL_DCHECK(read_ok);
    if (ABSL_PREDICT_FALSE(!dest.Write(std::move(data)))) return false;
  }
  return length_to_copy == length;
}

bool ChainReaderBase::CopyBehindScratch(size_t length, BackwardWriter& dest) {
  ABSL_DCHECK_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of "
         "PullableReader::CopyBehindScratch(BackwardWriter&): "
         "enough data available, use Copy(BackwardWriter&) instead";
  ABSL_DCHECK(!scratch_used())
      << "Failed precondition of "
         "PullableReader::CopyBehindScratch(BackwardWriter&): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  const Chain& src = *iter_.chain();
  ABSL_DCHECK_LE(limit_pos(), src.size())
      << "ChainReader source changed unexpectedly";
  if (ABSL_PREDICT_FALSE(length > src.size() - pos())) {
    const bool seek_ok = Seek(src.size());
    ABSL_DCHECK(seek_ok);
    return false;
  }
  if (length == src.size()) {
    const bool skip_ok = Skip(length);
    ABSL_DCHECK(skip_ok);
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
  const bool read_ok = ReadBehindScratch(length, data);
  ABSL_DCHECK(read_ok);
  return dest.Write(std::move(data));
}

bool ChainReaderBase::SeekBehindScratch(Position new_pos) {
  ABSL_DCHECK(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of PullableReader::SeekBehindScratch(): "
         "position in the buffer, use Seek() instead";
  ABSL_DCHECK(!scratch_used())
      << "Failed precondition of PullableReader::SeekBehindScratch(): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  const Chain& src = *iter_.chain();
  ABSL_DCHECK_LE(limit_pos(), src.size())
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

std::optional<Position> ChainReaderBase::SizeImpl() {
  if (ABSL_PREDICT_FALSE(!ok())) return std::nullopt;
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
