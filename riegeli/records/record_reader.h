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

#ifndef RIEGELI_RECORDS_RECORD_READER_H_
#define RIEGELI_RECORDS_RECORD_READER_H_

#include <stdint.h>
#include <memory>
#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/chunk_encoding/chunk_decoder.h"
#include "riegeli/chunk_encoding/field_filter.h"
#include "riegeli/records/chunk_reader.h"
#include "riegeli/records/record_position.h"

namespace google {
namespace protobuf {
class MessageLite;
}  // namespace protobuf
}  // namespace google

namespace riegeli {

// RecordReader reads records of a Riegeli/records file. A record is
// conceptually a binary string; usually it is a serialized proto message.
//
// RecordReader supports reading records sequentially, querying for the current
// position, and seeking to continue reading from another position. There are
// two ways of expressing positions, both strictly monotonic:
//  * RecordPosition (a class) - Faster for seeking.
//  * Position (an integer)    - Scaled between 0 and file size.
//
// Working with RecordPosition is recommended, unless it is needed to seek to an
// approximate position interpolated along the file, e.g. for splitting the file
// into shards, or unless the position must be expressed as an integer from the
// range [0, file_size] in order to fit into a preexisting API.
//
// For reading records sequentially, this kind of loop can be used:
//
//   SomeProto record;
//   while (record_reader_.ReadRecord(&record)) {
//     ... Process record.
//   }
//   if (!record_reader_.Close()) {
//     ... Failed with reason: record_reader_.message()
//   }
class RecordReader final : public Object {
 public:
  class Options {
   public:
    // Not defaulted because of a C++ defect:
    // https://stackoverflow.com/questions/17430377
    Options() noexcept {}

    // If true, corrupted regions and unparsable records will be skipped.
    // If false, they will cause reading to fail.
    //
    // Default: false
    Options& set_skip_errors(bool skip_errors) & {
      skip_errors_ = skip_errors;
      return *this;
    }
    Options&& set_skip_errors(bool skip_errors) && {
      return std::move(set_skip_errors(skip_errors));
    }

    // Specifies the set of fields to be included in returned records, allowing
    // to exclude the remaining fields (but does not guarantee that they will be
    // excluded). Excluding data makes reading faster.
    //
    // Filtering is effective if the file has been written with
    // set_transpose(true). Additionally, set_bucket_fraction() with a lower
    // value can make reading with filtering faster.
    Options& set_field_filter(FieldFilter field_filter) & {
      field_filter_ = std::move(field_filter);
      return *this;
    }
    Options&& set_field_filter(FieldFilter field_filter) && {
      return std::move(set_field_filter(std::move(field_filter)));
    }

   private:
    friend class RecordReader;

    bool skip_errors_ = false;
    FieldFilter field_filter_ = FieldFilter::All();
  };

  // Creates a closed RecordReader.
  RecordReader() noexcept;

  // Will read records from the byte Reader which is owned by this RecordReader
  // and will be closed and deleted when the RecordReader is closed.
  explicit RecordReader(std::unique_ptr<Reader> byte_reader,
                        Options options = Options());

  // Will read records from the byte Reader which is not owned by this
  // RecordReader and must be kept alive but not accessed until closing the
  // RecordReader.
  explicit RecordReader(Reader* byte_reader, Options options = Options());

  RecordReader(RecordReader&& src) noexcept;
  RecordReader& operator=(RecordReader&& src) noexcept;

  // Reads the next record.
  //
  // ReadRecord(MessageLite*) parses raw bytes to a proto message after reading.
  // The remaining overloads read raw bytes. For ReadRecord(string_view*) the
  // string_view is valid until the next non-const operation on this
  // RecordReader.
  //
  // If key != nullptr, *key is set to the canonical record position on success.
  //
  // Return values:
  //  * true                    - success (*record is set)
  //  * false (when healthy())  - source ends
  //  * false (when !healthy()) - failure
  bool ReadRecord(google::protobuf::MessageLite* record, RecordPosition* key = nullptr);
  bool ReadRecord(absl::string_view* record, RecordPosition* key = nullptr);
  bool ReadRecord(std::string* record, RecordPosition* key = nullptr);
  bool ReadRecord(Chain* record, RecordPosition* key = nullptr);

  // Returns true if reading from the current position might succeed, possibly
  // after some data is appended to the source. Returns false if reading from
  // the current position will always return false.
  bool HopeForMore() const;

  // Returns the current position.
  //
  // Pos().numeric() returns the position as an integer of type Position.
  //
  // A position returned by Pos() before reading a record is not greater than
  // the canonical position returned by ReadRecord() in *key for that record,
  // but seeking to either position will read the same record.
  RecordPosition Pos() const;

  // Seeks to a position.
  //
  // In Seek(RecordPosition) the position should have been obtained by pos() for
  // the same file.
  //
  // In Seek(Position) the position can be any integer between 0 and file size.
  // If it points between records, it is interpreted as the next record.
  //
  // Return values:
  //  * true                    - success (position is set to new_pos)
  //  * false (when healthy())  - source ends before new_pos (position is set to
  //                              the end) or seeking backwards is not supported
  //                              (position is unchanged)
  //  * false (when !healthy()) - failure
  bool Seek(RecordPosition new_pos);
  bool Seek(Position new_pos);

  // Returns the size of the file, i.e. the position corresponding to its end.
  //
  // Return values:
  //  * true  - success (*size is set, healthy())
  //  * false - failure (healthy() is unchanged)
  bool Size(Position* size) const;

#if 0
  // Searches the region between the current position and end of file for a
  // desired record. What is desired is specified by a function, which should
  // read a record and set the argument pointer to a value < 0, == 0, or > 0,
  // depending on whether the record read is before, among, or after desired
  // records. If it returns false, the search is aborted.
  //
  // If a desired record has been found, the position is left before the first
  // desired record, otherwise it is left at end of file. If found != nullptr,
  // then *found is set to true if the desired record has been found, or false
  // if it has not been found or the search was aborted.
  //
  // TODO: This is not implemented yet.
  bool Search(std::function<bool(int*)>, bool* found);
#endif

  // Returns the number of bytes skipped because of corrupted regions or
  // unparsable records.
  //
  // An unparsable record counts as the number of bytes between the canonical
  // record position of that record and the next record, i.e. it counts as one
  // byte, except that the last record of a chunk counts as the rest of the
  // chunk size.
  Position skipped_bytes() const;

 protected:
  void Done() override;

 private:
  RecordReader(std::unique_ptr<ChunkReader> chunk_reader, Options options);

  // Precondition: chunk_decoder_.index() == chunk_decoder_.num_records()
  bool ReadRecordSlow(google::protobuf::MessageLite* record, RecordPosition* key,
                      uint64_t index_before);
  template <typename Record>
  bool ReadRecordSlow(Record* record, RecordPosition* key);

  // Reads the next chunk from chunk_reader_ and decodes it into chunk_decoder_
  // and chunk_begin_. On failure resets chunk_decoder_.
  bool ReadChunk();

  // Invariant: if healthy() then chunk_reader_ != nullptr
  std::unique_ptr<ChunkReader> chunk_reader_;
  bool skip_errors_ = false;
  // Position of the beginning of the current chunk or end of file, except when
  // Seek(Position) failed to locate the chunk containing the position, in which
  // case this is that position.
  Position chunk_begin_ = 0;
  // Current chunk if a chunk has been read, empty otherwise.
  //
  // Invariants:
  //   if healthy() then chunk_decoder_.healthy()
  //   if !healthy() then chunk_decoder_.index() == chunk_decoder_.num_records()
  ChunkDecoder chunk_decoder_;
  // The number of bytes skipped because of corrupted regions or unparsable
  // records, in addition to chunk_reader_->skipped_bytes().
  Position skipped_bytes_ = 0;
};

// Implementation details follow.

inline bool RecordReader::ReadRecord(google::protobuf::MessageLite* record,
                                     RecordPosition* key) {
  const uint64_t index_before = chunk_decoder_.index();
  if (ABSL_PREDICT_TRUE(chunk_decoder_.ReadRecord(record))) {
    RIEGELI_ASSERT_GT(chunk_decoder_.index(), index_before)
        << "ChunkDecoder::ReadRecord() did not increment record index";
    if (key != nullptr) {
      *key = RecordPosition(chunk_begin_, chunk_decoder_.index() - 1);
    }
    const uint64_t skipped_records = chunk_decoder_.index() - index_before - 1;
    if (ABSL_PREDICT_FALSE(skipped_records > 0)) {
      skipped_bytes_ = SaturatingAdd(skipped_bytes_, skipped_records);
    }
    return true;
  }
  return ReadRecordSlow(record, key, index_before);
}

inline bool RecordReader::ReadRecord(absl::string_view* record,
                                     RecordPosition* key) {
  if (ABSL_PREDICT_TRUE(chunk_decoder_.ReadRecord(record))) {
    if (key != nullptr) {
      RIEGELI_ASSERT_GT(chunk_decoder_.index(), 0u)
          << "ChunkDecoder::ReadRecord() left record index at 0";
      *key = RecordPosition(chunk_begin_, chunk_decoder_.index() - 1);
    }
    return true;
  }
  return ReadRecordSlow(record, key);
}

inline bool RecordReader::ReadRecord(std::string* record, RecordPosition* key) {
  if (ABSL_PREDICT_TRUE(chunk_decoder_.ReadRecord(record))) {
    if (key != nullptr) {
      RIEGELI_ASSERT_GT(chunk_decoder_.index(), 0u)
          << "ChunkDecoder::ReadRecord() left record index at 0";
      *key = RecordPosition(chunk_begin_, chunk_decoder_.index() - 1);
    }
    return true;
  }
  return ReadRecordSlow(record, key);
}

inline bool RecordReader::ReadRecord(Chain* record, RecordPosition* key) {
  if (ABSL_PREDICT_TRUE(chunk_decoder_.ReadRecord(record))) {
    if (key != nullptr) {
      RIEGELI_ASSERT_GT(chunk_decoder_.index(), 0u)
          << "ChunkDecoder::ReadRecord() left record index at 0";
      *key = RecordPosition(chunk_begin_, chunk_decoder_.index() - 1);
    }
    return true;
  }
  return ReadRecordSlow(record, key);
}

inline bool RecordReader::HopeForMore() const {
  return chunk_decoder_.index() < chunk_decoder_.num_records() ||
         (healthy() && chunk_reader_->HopeForMore());
}

inline RecordPosition RecordReader::Pos() const {
  if (ABSL_PREDICT_TRUE(chunk_decoder_.index() <
                        chunk_decoder_.num_records())) {
    return RecordPosition(chunk_begin_, chunk_decoder_.index());
  }
  return RecordPosition(chunk_reader_->pos(), 0);
}

inline bool RecordReader::Size(Position* size) const {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  return chunk_reader_->Size(size);
}

inline Position RecordReader::skipped_bytes() const {
  if (ABSL_PREDICT_FALSE(chunk_reader_ == nullptr)) return 0;
  return SaturatingAdd(chunk_reader_->skipped_bytes(), skipped_bytes_);
}

}  // namespace riegeli

#endif  // RIEGELI_RECORDS_RECORD_READER_H_
