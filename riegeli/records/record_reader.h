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

#include <functional>
#include <memory>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/functional/function_ref.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/compare.h"
#include "absl/types/optional.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/message_lite.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/resetter.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/chunk_encoding/chunk_decoder.h"
#include "riegeli/chunk_encoding/field_projection.h"
#include "riegeli/records/chunk_reader.h"
#include "riegeli/records/chunk_reader_dependency.h"
#include "riegeli/records/record_position.h"
#include "riegeli/records/records_metadata.pb.h"
#include "riegeli/records/skipped_region.h"

namespace riegeli {

// Interprets `record_type_name` and `file_descriptor` from metadata.
class RecordsMetadataDescriptors : public Object {
 public:
  explicit RecordsMetadataDescriptors(const RecordsMetadata& metadata);

  RecordsMetadataDescriptors(RecordsMetadataDescriptors&& that) noexcept;
  RecordsMetadataDescriptors& operator=(RecordsMetadataDescriptors&& that);

  // Returns message descriptor of the record type, or `nullptr` if not
  // available.
  //
  // The message descriptor is valid as long as the `RecordsMetadataDescriptors`
  // object is valid.
  const google::protobuf::Descriptor* descriptor() const;

  // Returns record type full name, or an empty string if not available.
  const std::string& record_type_name() const { return record_type_name_; }

 private:
  class ErrorCollector;

  std::string record_type_name_;
  std::unique_ptr<google::protobuf::DescriptorPool> pool_;
};

// Template parameter independent part of `RecordReader`.
class RecordReaderBase : public Object {
 public:
  class Options {
   public:
    Options() noexcept {}

    // Specifies the set of fields to be included in returned records, allowing
    // to exclude the remaining fields (but does not guarantee that they will be
    // excluded). Excluding data makes reading faster.
    //
    // Projection is effective if the file has been written with
    // `set_transpose(true)`. Additionally, `set_bucket_fraction()` with a lower
    // value can make reading with projection faster.
    //
    // Default: `FieldProjection::All()`.
    Options& set_field_projection(const FieldProjection& field_projection) & {
      field_projection_ = field_projection;
      return *this;
    }
    Options& set_field_projection(FieldProjection&& field_projection) & {
      field_projection_ = std::move(field_projection);
      return *this;
    }
    Options&& set_field_projection(const FieldProjection& field_projection) && {
      return std::move(set_field_projection(field_projection));
    }
    Options&& set_field_projection(FieldProjection&& field_projection) && {
      return std::move(set_field_projection(std::move(field_projection)));
    }
    FieldProjection& field_projection() & { return field_projection_; }
    const FieldProjection& field_projection() const& {
      return field_projection_;
    }
    FieldProjection&& field_projection() && {
      return std::move(field_projection_);
    }
    const FieldProjection&& field_projection() const&& {
      return std::move(field_projection_);
    }

    // Sets the recovery function to be called after skipping over invalid file
    // contents.
    //
    // If the recovery function is set to `nullptr`, then invalid file contents
    // cause `RecordReader` to fail. `Recover()` can be used to skip over the
    // invalid region.
    //
    // If the recovery function is set to a value other than `nullptr`, then
    // invalid file contents cause `RecordReader` to skip over the invalid
    // region and call the recovery function. If the recovery function returns
    // `true`, reading continues. If the recovery function returns `false`,
    // reading ends.
    //
    // If `Close()` is called and file contents were truncated, the recovery
    // function is called if set. The `RecordReader` remains closed.
    //
    // Calling the following functions may cause the recovery function to be
    // called (in the same thread):
    //  * `Close()` - returns `true`, ignores the result of the recovery
    //                function
    //  * `ReadMetadata()` - returns the result of the recovery function
    //  * `ReadSerializedMetadata()` - returns the result of the recovery
    //                                 function
    //  * `ReadRecord()` - retried if the recovery function returns `true`,
    //                     returns `false` if the recovery function returns
    //                     `false`
    //  * `Seek()` - returns the result of the recovery function
    //
    // Default: `nullptr`
    Options& set_recovery(
        const std::function<bool(const SkippedRegion&)>& recovery) & {
      recovery_ = recovery;
      return *this;
    }
    Options& set_recovery(
        std::function<bool(const SkippedRegion&)>&& recovery) & {
      recovery_ = std::move(recovery);
      return *this;
    }
    Options&& set_recovery(
        const std::function<bool(const SkippedRegion&)>& recovery) && {
      return std::move(set_recovery(recovery));
    }
    Options&& set_recovery(
        std::function<bool(const SkippedRegion&)>&& recovery) && {
      return std::move(set_recovery(std::move(recovery)));
    }
    std::function<bool(const SkippedRegion&)>& recovery() & {
      return recovery_;
    }
    const std::function<bool(const SkippedRegion&)>& recovery() const& {
      return recovery_;
    }
    std::function<bool(const SkippedRegion&)>&& recovery() && {
      return std::move(recovery_);
    }
    const std::function<bool(const SkippedRegion&)>&& recovery() const&& {
      return std::move(recovery_);
    }

   private:
    FieldProjection field_projection_ = FieldProjection::All();
    std::function<bool(const SkippedRegion&)> recovery_;
  };

  // Returns the Riegeli/records file being read from. Unchanged by `Close()`.
  virtual ChunkReader* src_chunk_reader() = 0;
  virtual const ChunkReader* src_chunk_reader() const = 0;

  // Ensures that the file looks like a valid Riegeli/Records file.
  //
  // Reading functions already check the file format. `CheckFileFormat()` can
  // verify the file format before (or instead of) performing other operations.
  //
  // This ignores the recovery function. If invalid file contents are skipped,
  // then checking the file format is meaningless: any file can be read.
  //
  // Return values:
  //  * `true`                      - success
  //  * `false` (when `healthy()`)  - source ends
  //  * `false` (when `!healthy()`) - failure
  bool CheckFileFormat();

  // Returns file metadata.
  //
  // `ReadMetadata()` must be called while the `RecordReader` is at the
  // beginning of the file (calling `CheckFileFormat()` before is allowed).
  //
  // Record type in metadata can be conveniently interpreted by
  // `RecordsMetadataDescriptors`.
  //
  // Return values:
  //  * `true`                      - success (`*metadata` is set)
  //  * `false` (when `healthy()`)  - source ends
  //  * `false` (when `!healthy()`) - failure
  bool ReadMetadata(RecordsMetadata& metadata);

  // Like `ReadMetadata()`, but metadata is returned in the serialized form.
  //
  // This is faster if the caller needs metadata already serialized.
  bool ReadSerializedMetadata(Chain& metadata);

  // Reads the next record.
  //
  // `ReadRecord(google::protobuf::MessageLite&)` parses raw bytes to a proto
  // message after reading. The remaining overloads read raw bytes. For
  // `ReadRecord(absl::string_view&)` the `absl::string_view` is valid until the
  // next non-const operation on this `RecordReader`.
  //
  // If `key != nullptr`, `*key` is set to the canonical record position on
  // success.
  //
  // Return values:
  //  * `true`                      - success (`record` is set)
  //  * `false` (when `healthy()`)  - source ends
  //  * `false` (when `!healthy()`) - failure
  bool ReadRecord(google::protobuf::MessageLite& record,
                  RecordPosition* key = nullptr);
  bool ReadRecord(absl::string_view& record, RecordPosition* key = nullptr);
  bool ReadRecord(std::string& record, RecordPosition* key = nullptr);
  bool ReadRecord(Chain& record, RecordPosition* key = nullptr);
  bool ReadRecord(absl::Cord& record, RecordPosition* key = nullptr);

  // Like `Options::set_field_projection()`, but can be done at any time.
  //
  // This may cause reading the current chunk again.
  //
  // Return values:
  //  * `true`  - success (`healthy()`)
  //  * `false` - failure (`!healthy()`)
  bool SetFieldProjection(FieldProjection field_projection);

  // If `!healthy()` and the failure was caused by invalid file contents, then
  // `Recover()` tries to recover from the failure and allow reading again by
  // skipping over the invalid region.
  //
  // If `Close()` failed and the failure was caused by truncated file contents,
  // then `Recover()` returns `true`. The `RecordReader` remains closed.
  //
  // If `healthy()`, or if `!healthy()` but the failure was not caused by
  // invalid file contents, then `Recover()` returns `false`.
  //
  // If `skipped_region != nullptr`, `*skipped_region` is set to the position of
  // the skipped region on success.
  //
  // If a recovery function (`RecordReaderBase::Options::set_recovery()`) is
  // set, then `Recover()` is called automatically. Otherwise `Recover()` can be
  // called after one of the following functions returned `false`, and the
  // function can be assumed to have returned `true` if `Recover()` returns
  // `true`:
  //  * `Close()`
  //  * `ReadMetadata()`
  //  * `ReadSerializedMetadata()`
  //  * `ReadRecord()` - should be retried if `Recover()` returns `true`
  //  * `Seek()`
  //
  // Return values:
  //  * `true`  - success
  //  * `false` - failure not caused by invalid file contents
  bool Recover(SkippedRegion* skipped_region = nullptr);

  // Returns the current position.
  //
  // `pos().numeric()` returns the position as an integer of type `Position`.
  //
  // A position returned by `pos()` before reading a record is not greater than
  // the canonical position returned by `ReadRecord()` in `*key` for that
  // record, but seeking to either position will read the same record.
  //
  // `pos()` is unchanged by `Close()`.
  RecordPosition pos() const;

  // Returns `true` if this `RecordReader` supports `Seek()`, `SeekBack()`,
  // `Size()`, and `Search()`.
  bool SupportsRandomAccess() const;

  // Seeks to a position.
  //
  // In `Seek(RecordPosition)` the position should have been obtained by `pos()`
  // for the same file.
  //
  // In `Seek(Position)` the position can be any integer between 0 and file
  // size. If it points between records, it is interpreted as the next record.
  //
  // Return values:
  //  * `true`  - success (`healthy()`)
  //  * `false` - failure (`!healthy()`)
  bool Seek(RecordPosition new_pos);
  bool Seek(Position new_pos);

  // Seeks back by one record.
  //
  // Return values:
  //  * `true`                      - success (`healthy()`)
  //  * `false` (when `healthy()`)  - beginning of the source reached
  //  * `false` (when `!healthy()`) - failure
  bool SeekBack();

  // Returns the size of the file in bytes, i.e. the position corresponding to
  // its end.
  //
  // Returns `absl::nullopt` on failure (`!healthy()`).
  absl::optional<Position> Size();

  // Searches the file for a desired record, or for a desired position between
  // records, given that it is possible to determine whether a given record is
  // before or after the desired position.
  //
  // The current position before calling `Search()` does not matter.
  //
  // The `test` function takes `this` as a parameter, seeked to some record,
  // and returns `absl::partial_ordering`:
  //  * `less`       - the current record is before the desired position
  //  * `equivalent` - the current record is desired, searching can stop
  //  * `greater`    - the current record is after the desired position
  //  * `unordered`  - it could not be determined which is the case; the current
  //                   record will be skipped
  //
  // Preconditions:
  //  * all `less` records precede all `equivalent` records
  //  * all `less` records precede all `greater` records
  //  * all `equivalent` records precede all `greater` records
  //
  // Return values:
  //  * `true`  - success (`healthy()`)
  //  * `false` - failure (`!healthy()`)
  //
  // On success, if there is some `equivalent` record, `Search()` points to some
  // `equivalent` record. Otherwise, if there is some `greater` record,
  // `Search()` points to earliest `greater` record. Otherwise `Search()` points
  // to the end of file.
  //
  // To find the earliest `equivalent` record instead of an arbitrary one,
  // `test()` can be changed to return `greater` in place of `equivalent`.
  //
  // Further guarantees:
  //  * If a `test()` returns `equivalent`, `Search()` points back to the record
  //    before `test()` and returns.
  //  * If a `test()` returns `less`, `test()` will not be called again at
  //    earlier positions.
  //  * If a `test()` returns `greater`, `test()` will not be called again at
  //    later positions.
  //  * `test()` will not be called again at the same position.
  //
  // It follows that if a `test()` returns `equivalent` or `greater`, `Search()`
  // points to the record before the last `test()` call with one of these
  // results. This allows to communicate additional context of an `equivalent`
  // or `greater` result by a side effect of `test()`.
  //
  // For skipping invalid file regions during `Search()`, a recovery function
  // (`RecordReaderBase::Options::set_recovery()`) can be set, but `Recover()`
  // resumes only simple operations and is not applicable here.
  bool Search(
      absl::FunctionRef<absl::partial_ordering(RecordReaderBase* reader)> test);

  // A variant of `Search()` which reads a record before calling `test()`,
  // instead of letting `test()` read the record.
  //
  // The `Record` type must be supported by `ReadRecord()`, and `test` must be
  // callable with an argument of type `Record&` or `const Record&`, returning
  // `absl::partial_ordering`.
  template <typename Record, typename Test>
  bool Search(Test test);

 protected:
  enum class Recoverable { kNo, kRecoverChunkReader, kRecoverChunkDecoder };

  explicit RecordReaderBase(InitiallyClosed) noexcept;
  explicit RecordReaderBase(InitiallyOpen) noexcept;

  RecordReaderBase(RecordReaderBase&& that) noexcept;
  RecordReaderBase& operator=(RecordReaderBase&& that) noexcept;

  void Reset(InitiallyClosed);
  void Reset(InitiallyOpen);
  void Initialize(ChunkReader* src, Options&& options);

  void Done() override;

  bool TryRecovery();

  // Position of the beginning of the current chunk or end of file, except when
  // `Seek(Position)` failed to locate the chunk containing the position, in
  // which case this is that position.
  Position chunk_begin_ = 0;

  // Current chunk if a chunk has been read, empty otherwise.
  //
  // Invariants:
  //   if `healthy()` then `chunk_decoder_.healthy()`
  //   if `!healthy()` then
  //       `!chunk_decoder_.healthy() ||
  //        chunk_decoder_.index() == chunk_decoder_.num_records()`
  ChunkDecoder chunk_decoder_;

  // Whether `Recover()` is applicable, and if so, how it should be performed:
  //
  //  * `Recoverable::kNo`                  - `Recover()` is not applicable
  //  * `Recoverable::kRecoverChunkReader`  - `Recover()` tries to recover
  //                                          `chunk_reader_`
  //  * `Recoverable::kRecoverChunkDecoder` - `Recover()` tries to recover
  //                                          `chunk_decoder_,` skips the chunk
  //                                          if that failed
  //
  // Invariants:
  //   if `healthy()` then `recoverable_ == Recoverable::kNo`
  //   if `closed()` then `recoverable_ == Recoverable::kNo ||
  //                       recoverable_ == Recoverable::kRecoverChunkReader`
  Recoverable recoverable_ = Recoverable::kNo;

  std::function<bool(const SkippedRegion&)> recovery_;

 private:
  class ChunkSearchTraits;

  bool FailReading(const ChunkReader& src);
  bool FailSeeking(const ChunkReader& src);

  bool ParseMetadata(const Chunk& chunk, Chain& metadata);

  template <typename Record>
  bool ReadRecordImpl(Record& record, RecordPosition* key);

  // Reads the next chunk from `chunk_reader_` and decodes it into
  // `chunk_decoder_` and `chunk_begin_`. On failure resets `chunk_decoder_`.
  //
  // Precondition: `healthy()`
  bool ReadChunk();
};

// `RecordReader` reads records of a Riegeli/records file. A record is
// conceptually a binary string; usually it is a serialized proto message.
//
// `RecordReader` supports reading records sequentially, querying for the
// current position, and seeking to continue reading from another position.
//
// For reading records sequentially, this kind of loop can be used:
// ```
//   SomeProto record;
//   while (record_reader_.ReadRecord(record)) {
//     ... Process record.
//   }
//   if (!record_reader_.Close()) {
//     ... Failed with reason: record_reader_.status()
//   }
// ```
//
// For reading records while skipping errors, pass options like these:
// ```
//       RecordReaderBase::Options().set_recovery(
//           [&skipped_bytes](const SkippedRegion& skipped_region) {
//             skipped_bytes += skipped_region.length();
//             return true;
//           })
// ```
//
// An equivalent lower level implementation, without callbacks:
// ```
//   Position skipped_bytes = 0;
//   SomeProto record;
//   for (;;) {
//     if (!record_reader_.ReadRecord(record)) {
//       SkippedRegion skipped_region;
//       if (record_reader_.Recover(&skipped_region)) {
//         skipped_bytes += skipped_region.length();
//         continue;
//       }
//       break;
//     }
//     ... Process record.
//   }
//   if (!record_reader_.Close()) {
//     SkippedRegion skipped_region;
//     if (record_reader_.Recover(&skipped_region)) {
//       skipped_bytes += skipped_region.length();
//     } else {
//       ... Failed with reason: record_reader_.status()
//     }
//   }
// ```
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the byte `Reader`. `Src` must support
// `Dependency<Reader*, Src>`, e.g. `Reader*` (not owned, default),
// `std::unique_ptr<Reader>` (owned), `ChainReader<>` (owned).
//
// `Src` may also specify a `ChunkReader` instead of a byte `Reader`. In this
// case `Src` must support `Dependency<ChunkReader*, Src>`, e.g.
// `ChunkReader*` (not owned), `std::unique_ptr<ChunkReader>` (owned),
// `DefaultChunkReader<>` (owned).
//
// The byte `Reader` or `ChunkReader` must not be accessed until the
// `RecordReader` is closed or no longer used.
template <typename Src = Reader*>
class RecordReader : public RecordReaderBase {
 public:
  // Creates a closed `RecordReader`.
  RecordReader() noexcept : RecordReaderBase(kInitiallyClosed) {}

  // Will read from the byte `Reader` or `ChunkReader` provided by `src`.
  explicit RecordReader(const Src& src, Options options = Options());
  explicit RecordReader(Src&& src, Options options = Options());

  // Will read from the byte `Reader` or `ChunkReader` provided by a `Src`
  // constructed from elements of `src_args`. This avoids constructing a
  // temporary `Src` and moving from it.
  template <typename... SrcArgs>
  explicit RecordReader(std::tuple<SrcArgs...> src_args,
                        Options options = Options());

  RecordReader(RecordReader&& that) noexcept;
  RecordReader& operator=(RecordReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `RecordReader`. This avoids
  // constructing a temporary `RecordReader` and moving from it.
  void Reset();
  void Reset(const Src& src, Options options = Options());
  void Reset(Src&& src, Options options = Options());
  template <typename... SrcArgs>
  void Reset(std::tuple<SrcArgs...> src_args, Options options = Options());

  // Returns the object providing and possibly owning the byte `Reader` or
  // `ChunkReader`. Unchanged by `Close()`.
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  ChunkReader* src_chunk_reader() override { return src_.get(); }
  const ChunkReader* src_chunk_reader() const override { return src_.get(); }

  // An optimized implementation in a derived class, avoiding a virtual call.
  RecordPosition pos() const;

 protected:
  void Done() override;

 private:
  // The object providing and possibly owning the byte `Reader` or
  // `ChunkReader`.
  Dependency<ChunkReader*, Src> src_;
};

// Support CTAD.
#if __cplusplus >= 201703
template <typename Src>
RecordReader(Src&& src,
             RecordReaderBase::Options options = RecordReaderBase::Options())
    -> RecordReader<std::decay_t<Src>>;
template <typename... SrcArgs>
RecordReader(std::tuple<SrcArgs...> src_args,
             RecordReaderBase::Options options = RecordReaderBase::Options())
    -> RecordReader<void>;  // Delete.
#endif

// Implementation details follow.

inline RecordsMetadataDescriptors::RecordsMetadataDescriptors(
    RecordsMetadataDescriptors&& that) noexcept
    : Object(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      record_type_name_(std::move(that.record_type_name_)),
      pool_(std::move(that.pool_)) {}

inline RecordsMetadataDescriptors& RecordsMetadataDescriptors::operator=(
    RecordsMetadataDescriptors&& that) {
  Object::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  record_type_name_ = std::move(that.record_type_name_),
  pool_ = std::move(that.pool_);
  return *this;
}

inline bool RecordReaderBase::TryRecovery() {
  if (recovery_ == nullptr) return false;
  SkippedRegion skipped_region;
  return Recover(&skipped_region) && recovery_(skipped_region);
}

inline RecordPosition RecordReaderBase::pos() const {
  if (ABSL_PREDICT_TRUE(chunk_decoder_.index() <
                        chunk_decoder_.num_records()) ||
      ABSL_PREDICT_FALSE(recoverable_ == Recoverable::kRecoverChunkDecoder)) {
    return RecordPosition(chunk_begin_, chunk_decoder_.index());
  }
  return RecordPosition(src_chunk_reader()->pos(), 0);
}

template <typename Record, typename Test>
bool RecordReaderBase::Search(Test test) {
  Record record;
  return Search([&](RecordReaderBase* self) {
    if (ABSL_PREDICT_FALSE(!self->ReadRecord(record))) {
      return absl::partial_ordering::unordered;
    }
    return test(record);
  });
}

template <typename Src>
inline RecordReader<Src>::RecordReader(const Src& src, Options options)
    : RecordReaderBase(kInitiallyOpen), src_(src) {
  Initialize(src_.get(), std::move(options));
}

template <typename Src>
inline RecordReader<Src>::RecordReader(Src&& src, Options options)
    : RecordReaderBase(kInitiallyOpen), src_(std::move(src)) {
  Initialize(src_.get(), std::move(options));
}

template <typename Src>
template <typename... SrcArgs>
inline RecordReader<Src>::RecordReader(std::tuple<SrcArgs...> src_args,
                                       Options options)
    : RecordReaderBase(kInitiallyOpen), src_(std::move(src_args)) {
  Initialize(src_.get(), std::move(options));
}

template <typename Src>
inline RecordReader<Src>::RecordReader(RecordReader&& that) noexcept
    : RecordReaderBase(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      src_(std::move(that.src_)) {}

template <typename Src>
inline RecordReader<Src>& RecordReader<Src>::operator=(
    RecordReader&& that) noexcept {
  RecordReaderBase::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  src_ = std::move(that.src_);
  return *this;
}

template <typename Src>
inline void RecordReader<Src>::Reset() {
  RecordReaderBase::Reset(kInitiallyClosed);
  src_.Reset();
}

template <typename Src>
inline void RecordReader<Src>::Reset(const Src& src, Options options) {
  RecordReaderBase::Reset(kInitiallyOpen);
  src_.Reset(src);
  Initialize(src_.get(), std::move(options));
}

template <typename Src>
inline void RecordReader<Src>::Reset(Src&& src, Options options) {
  RecordReaderBase::Reset(kInitiallyOpen);
  src_.Reset(std::move(src));
  Initialize(src_.get(), std::move(options));
}

template <typename Src>
template <typename... SrcArgs>
inline void RecordReader<Src>::Reset(std::tuple<SrcArgs...> src_args,
                                     Options options) {
  RecordReaderBase::Reset(kInitiallyOpen);
  src_.Reset(std::move(src_args));
  Initialize(src_.get(), std::move(options));
}

template <typename Src>
void RecordReader<Src>::Done() {
  RecordReaderBase::Done();
  if (src_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) {
      recoverable_ = Recoverable::kRecoverChunkReader;
      Fail(*src_);
      TryRecovery();
    }
  }
}

template <typename Src>
inline RecordPosition RecordReader<Src>::pos() const {
  if (ABSL_PREDICT_TRUE(chunk_decoder_.index() <
                        chunk_decoder_.num_records()) ||
      ABSL_PREDICT_FALSE(recoverable_ == Recoverable::kRecoverChunkDecoder)) {
    return RecordPosition(chunk_begin_, chunk_decoder_.index());
  }
  return RecordPosition(src_->pos(), 0);
}

template <typename Src>
struct Resetter<RecordReader<Src>> : ResetterByReset<RecordReader<Src>> {};

}  // namespace riegeli

#endif  // RIEGELI_RECORDS_RECORD_READER_H_
