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

#include <memory>
#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/chunk_encoding/chunk_decoder.h"
#include "riegeli/chunk_encoding/field_filter.h"
#include "riegeli/records/chunk_reader.h"
#include "riegeli/records/record_position.h"
#include "riegeli/records/records_metadata.pb.h"

namespace riegeli {

class Chunk;

// Interprets record_type_name and file_descriptor from metadata.
class RecordsMetadataDescriptors : public Object {
 public:
  explicit RecordsMetadataDescriptors(const RecordsMetadata& metadata);

  RecordsMetadataDescriptors(RecordsMetadataDescriptors&& src) noexcept;
  RecordsMetadataDescriptors& operator=(RecordsMetadataDescriptors&& src);

  // Returns message descriptor of the record type, or nullptr if not available.
  //
  // The message descriptor is valid as long as the RecordsMetadataDescriptors
  // object is valid.
  const google::protobuf::Descriptor* descriptor() const;

  // Returns record type full name, or an empty string if not available.
  const std::string& record_type_name() const { return record_type_name_; }

 protected:
  void Done() override;

 private:
  class ErrorCollector;

  std::string record_type_name_;
  std::unique_ptr<google::protobuf::DescriptorPool> pool_;
};

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
//
// For reading records while skipping errors:
//
//   Position skipped_bytes = 0;
//   Position skipped_messages = 0;
//   SomeProto record;
//   for (;;) {
//     if (!record_reader_.ReadRecord(&record)) {
//       if (record_reader_.Recover(&skipped_bytes, &skipped_messages)) {
//         continue;
//       }
//       break;
//     }
//     ... Process record.
//   }
//   if (!record_reader_.Close() && !record_reader_.Recover(&skipped_bytes)) {
//     ... Failed with reason: record_reader_.message()
//   }
class RecordReader final : public Object {
 public:
  class Options {
   public:
    Options() noexcept {}

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

  // Ensures that the file looks like a valid Riegeli/Records file.
  //
  // ReadMetadata() and ReadRecord() already check the file format.
  // CheckFileFormat() can verify the file format before (or instead of)
  // performing other operations.
  //
  // Return values:
  //  * true                    - success
  //  * false (when healthy())  - source ends
  //  * false (when !healthy()) - failure
  bool CheckFileFormat();

  // Returns file metadata.
  //
  // ReadMetadata() must be called while the RecordReader is at the beginning of
  // the file (calling CheckFileFormat() before is allowed).
  //
  // Record type in metadata can be conveniently interpreted by
  // RecordsMetadataDescriptors.
  //
  // Return values:
  //  * true                    - success (*metadata is set)
  //  * false (when healthy())  - source ends
  //  * false (when !healthy()) - failure
  bool ReadMetadata(RecordsMetadata* metadata);

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
  bool ReadRecord(google::protobuf::MessageLite* record,
                  RecordPosition* key = nullptr);
  bool ReadRecord(absl::string_view* record, RecordPosition* key = nullptr);
  bool ReadRecord(std::string* record, RecordPosition* key = nullptr);
  bool ReadRecord(Chain* record, RecordPosition* key = nullptr);

  // If !healthy() and the failure was caused by invalid file contents, then
  // Recover() tries to recover from the failure and allow reading again by
  // skipping over the invalid region.
  //
  // If Close() failed and the failure was caused by truncated file contents,
  // then Recover() increments *skipped_bytes and returns true. The RecordReader
  // remains closed.
  //
  // If healthy(), or if !healthy() but the failure was not caused by invalid
  // file contents, then Recover() returns false.
  //
  // If skipped_bytes != nullptr, *skipped_bytes is incremented by the number of
  // bytes skipped (by the amount of the file which could not be decoded).
  //
  // If skipped_messages != nullptr, *skipped_messages is incremented by the
  // number of records skipped by ReadRecord(MessageLite*) when the proto
  // message could not be parsed (by 0 or 1). Skipped messages do not count
  // towards skipped bytes.
  //
  // Return values:
  //  * true  - success
  //  * false - failure not caused by invalid file contents
  bool Recover(Position* skipped_bytes = nullptr,
               Position* skipped_messages = nullptr);

  // Returns the current position.
  //
  // pos().numeric() returns the position as an integer of type Position.
  //
  // A position returned by pos() before reading a record is not greater than
  // the canonical position returned by ReadRecord() in *key for that record,
  // but seeking to either position will read the same record.
  RecordPosition pos() const;

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

 protected:
  void Done() override;

 private:
  enum class Recoverable {
    kNo,
    kRecoverChunkReader,
    kRecoverChunkDecoder,
    kReportSkippedBytes
  };

  RecordReader(std::unique_ptr<ChunkReader> chunk_reader, Options options);

  bool ParseMetadata(const Chunk& chunk, RecordsMetadata* metadata);

  // Precondition: !chunk_decoder_.healthy() ||
  //               chunk_decoder_.index() == chunk_decoder_.num_records()
  template <typename Record>
  bool ReadRecordSlow(Record* record, RecordPosition* key);

  // Reads the next chunk from chunk_reader_ and decodes it into chunk_decoder_
  // and chunk_begin_. On failure resets chunk_decoder_.
  bool ReadChunk();

  // Invariant: if healthy() then chunk_reader_ != nullptr
  std::unique_ptr<ChunkReader> chunk_reader_;

  // Position of the beginning of the current chunk or end of file, except when
  // Seek(Position) failed to locate the chunk containing the position, in which
  // case this is that position.
  Position chunk_begin_ = 0;

  // Current chunk if a chunk has been read, empty otherwise.
  //
  // Invariants:
  //   if healthy() then chunk_decoder_.healthy()
  //   if !healthy() then !chunk_decoder_.healthy() ||
  //                      chunk_decoder_.index() == chunk_decoder_.num_records()
  ChunkDecoder chunk_decoder_;

  // Whether Recover() is applicable, and if so, how it should be performed:
  //
  //  * Recoverable::kNo                  - Recover() is not applicable
  //  * Recoverable::kRecoverChunkReader  - Recover() tries to recover
  //                                        chunk_reader_
  //  * Recoverable::kRecoverChunkDecoder - Recover() tries to recover
  //                                        chunk_decoder_
  //  * Recoverable::kReportSkippedBytes  - Recover() only reports
  //                                        recoverable_length_ as the number of
  //                                        skipped bytes
  //
  // Invariants:
  //   if healthy() then recoverable_ == Recoverable::kNo
  //   if closed() then recoverable_ == Recoverable::kNo ||
  //                    recoverable_ == Recoverable::kReportSkippedBytes
  Recoverable recoverable_ = Recoverable::kNo;

  // If recoverable_ == Recoverable::kReportSkippedBytes, the number of skipped
  // bytes to report.
  Position recoverable_length_ = 0;
};

// Implementation details follow.

inline RecordsMetadataDescriptors::RecordsMetadataDescriptors(
    RecordsMetadataDescriptors&& src) noexcept
    : Object(std::move(src)),
      record_type_name_(
          riegeli::exchange(src.record_type_name_, std::string())),
      pool_(std::move(src.pool_)) {}

inline RecordsMetadataDescriptors& RecordsMetadataDescriptors::operator=(
    RecordsMetadataDescriptors&& src) {
  Object::operator=(std::move(src));
  record_type_name_ = riegeli::exchange(src.record_type_name_, std::string()),
  pool_ = std::move(src.pool_);
  return *this;
}

inline bool RecordReader::ReadRecord(google::protobuf::MessageLite* record,
                                     RecordPosition* key) {
  if (ABSL_PREDICT_TRUE(chunk_decoder_.ReadRecord(record))) {
    RIEGELI_ASSERT_GT(chunk_decoder_.index(), 0u)
        << "ChunkDecoder::ReadRecord() left record index at 0";
    if (key != nullptr) {
      *key = RecordPosition(chunk_begin_, chunk_decoder_.index() - 1);
    }
    return true;
  }
  return ReadRecordSlow(record, key);
}

inline bool RecordReader::ReadRecord(absl::string_view* record,
                                     RecordPosition* key) {
  if (ABSL_PREDICT_TRUE(chunk_decoder_.ReadRecord(record))) {
    RIEGELI_ASSERT_GT(chunk_decoder_.index(), 0u)
        << "ChunkDecoder::ReadRecord() left record index at 0";
    if (key != nullptr) {
      *key = RecordPosition(chunk_begin_, chunk_decoder_.index() - 1);
    }
    return true;
  }
  return ReadRecordSlow(record, key);
}

inline bool RecordReader::ReadRecord(std::string* record, RecordPosition* key) {
  if (ABSL_PREDICT_TRUE(chunk_decoder_.ReadRecord(record))) {
    RIEGELI_ASSERT_GT(chunk_decoder_.index(), 0u)
        << "ChunkDecoder::ReadRecord() left record index at 0";
    if (key != nullptr) {
      *key = RecordPosition(chunk_begin_, chunk_decoder_.index() - 1);
    }
    return true;
  }
  return ReadRecordSlow(record, key);
}

inline bool RecordReader::ReadRecord(Chain* record, RecordPosition* key) {
  if (ABSL_PREDICT_TRUE(chunk_decoder_.ReadRecord(record))) {
    RIEGELI_ASSERT_GT(chunk_decoder_.index(), 0u)
        << "ChunkDecoder::ReadRecord() left record index at 0";
    if (key != nullptr) {
      *key = RecordPosition(chunk_begin_, chunk_decoder_.index() - 1);
    }
    return true;
  }
  return ReadRecordSlow(record, key);
}

inline RecordPosition RecordReader::pos() const {
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

}  // namespace riegeli

#endif  // RIEGELI_RECORDS_RECORD_READER_H_
