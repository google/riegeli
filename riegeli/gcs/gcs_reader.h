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

#ifndef RIEGELI_GCS_GCS_READER_H_
#define RIEGELI_GCS_GCS_READER_H_

#include <stddef.h>
#include <stdint.h>

#include <functional>
#include <memory>
#include <optional>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "google/cloud/storage/client.h"
#include "google/cloud/storage/download_options.h"
#include "google/cloud/storage/object_read_stream.h"
#include "google/cloud/storage/well_known_parameters.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/object.h"
#include "riegeli/base/reset.h"
#include "riegeli/base/type_traits.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/istream_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/gcs/gcs_internal.h"
#include "riegeli/gcs/gcs_object.h"

namespace riegeli {

// A `Reader` which reads from a Google Cloud Storage object.
class GcsReader
    : public IStreamReader<google::cloud::storage::ObjectReadStream> {
 public:
  class Options : public BufferOptionsBase<Options> {
   public:
    Options() noexcept {}

    static constexpr size_t kDefaultMinBufferSize = size_t{64} << 10;
    static constexpr size_t kDefaultMaxBufferSize = size_t{1} << 20;
  };

  // Creates a closed `GcsReader`.
  explicit GcsReader(Closed) : IStreamReader(kClosed) {}

  // Will read from `object`.
  template <typename... ReadObjectOptions>
  explicit GcsReader(const google::cloud::storage::Client& client,
                     Initializer<GcsObject> object, Options options,
                     ReadObjectOptions&&... read_object_options);
  template <typename... ReadObjectOptions>
  explicit GcsReader(const google::cloud::storage::Client& client,
                     Initializer<GcsObject> object,
                     ReadObjectOptions&&... read_object_options);

  GcsReader(GcsReader&& that) noexcept;
  GcsReader& operator=(GcsReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `GcsReader`. This avoids
  // constructing a temporary `GcsReader` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  template <typename... ReadObjectOptions>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      const google::cloud::storage::Client& client,
      Initializer<GcsObject> object, Options options,
      ReadObjectOptions&&... read_object_options);
  template <typename... ReadObjectOptions>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      const google::cloud::storage::Client& client,
      Initializer<GcsObject> object,
      ReadObjectOptions&&... read_object_options);

  google::cloud::storage::Client& client() {
    RIEGELI_ASSERT(client_ != std::nullopt)
        << "Failed precondition of GcsReader::client(): "
           "default-constructed GcsReader";
    return *client_;
  }
  const GcsObject& object() const { return object_; }

  // Returns the object's generation at the time of the download, if known.
  std::optional<int64_t> generation() const { return src().generation(); }

  // Returns the object's metageneration at the time of the download, if known.
  std::optional<int64_t> metageneration() const {
    return src().metageneration();
  }

  // Returns the object's storage class at the time of the download, if known.
  std::optional<absl::string_view> storage_class() const {
    return src().storage_class();
  }

  bool SupportsRandomAccess() override;
  bool SupportsNewReader() override;

 protected:
  void Done() override;
  absl::Status AnnotateStatusImpl(absl::Status status) override;
  bool ReadInternal(size_t min_length, size_t max_length, char* dest) override;
  bool SeekBehindBuffer(Position new_pos) override;
  std::unique_ptr<Reader> NewReaderImpl(Position initial_pos) override;

 private:
  friend class GcsWriter;  // For `set_exact_size()`.

  struct NewReaderTag {};

  enum class Origin { kBegin, kEnd };
  struct RangeOptions {
    Origin origin;
    int64_t initial_pos;
    std::optional<int64_t> max_size;
  };

  explicit GcsReader(
      NewReaderTag, const google::cloud::storage::Client& client,
      const GcsObject& object,
      const std::function<google::cloud::storage::ObjectReadStream(
          GcsReader&, int64_t)>& read_object,
      BufferOptions buffer_options, Position read_from_offset);

  template <typename... ReadObjectOptions>
  static RangeOptions GetRangeOptions(
      const ReadObjectOptions&... read_object_options);

  template <typename ReadObjectOption>
  struct NewReadObjectOptionPredicate
      : std::negation<std::disjunction<
            std::is_same<std::decay_t<ReadObjectOption>,
                         google::cloud::storage::ReadFromOffset>,
            std::is_same<std::decay_t<ReadObjectOption>,
                         google::cloud::storage::ReadRange>,
            std::is_same<std::decay_t<ReadObjectOption>,
                         google::cloud::storage::ReadLast>>> {};

  template <typename... ReadObjectOptions, size_t... indices,
            typename OffsetOption>
  google::cloud::storage::ObjectReadStream ApplyReadObject(
      const std::tuple<ReadObjectOptions...>& read_object_options,
      std::index_sequence<indices...>, OffsetOption&& offset_option) {
    return object_.generation() == std::nullopt
               ? client_->ReadObject(object_.bucket_name(),
                                     object_.object_name(),
                                     std::get<indices>(read_object_options)...,
                                     std::forward<OffsetOption>(offset_option))
               : client_->ReadObject(
                     object_.bucket_name(), object_.object_name(),
                     google::cloud::storage::Generation(*object_.generation()),
                     std::get<indices>(read_object_options)...,
                     std::forward<OffsetOption>(offset_option));
  }

  template <typename... ReadObjectOptions>
  void Initialize(const Options& options,
                  ReadObjectOptions&&... read_object_options);
  void Initialize(google::cloud::storage::ObjectReadStream stream,
                  BufferOptions buffer_options,
                  const RangeOptions& range_options);
  template <typename... ReadObjectOptions>
  void SetReadObject(const RangeOptions& range_options,
                     const ReadObjectOptions&... read_object_options);
  void PropagateStatus();
  ABSL_ATTRIBUTE_COLD void PropagateStatusSlow();

  std::optional<google::cloud::storage::Client> client_;
  GcsObject object_;
  std::function<google::cloud::storage::ObjectReadStream(GcsReader&, int64_t)>
      read_object_;
};

// Implementation details follow.

template <typename... ReadObjectOptions>
GcsReader::GcsReader(const google::cloud::storage::Client& client,
                     Initializer<GcsObject> object, Options options,
                     ReadObjectOptions&&... read_object_options)
    : IStreamReader(kClosed), client_(client), object_(std::move(object)) {
  Initialize(options, std::forward<ReadObjectOptions>(read_object_options)...);
}

template <typename... ReadObjectOptions>
GcsReader::GcsReader(const google::cloud::storage::Client& client,
                     Initializer<GcsObject> object,
                     ReadObjectOptions&&... read_object_options)
    : GcsReader(client, std::move(object), Options(),
                std::forward<ReadObjectOptions>(read_object_options)...) {}

inline GcsReader::GcsReader(GcsReader&& that) noexcept
    : IStreamReader(static_cast<IStreamReader&&>(that)),
      client_(std::move(that.client_)),
      object_(std::move(that.object_)),
      read_object_(std::move(that.read_object_)) {}

inline GcsReader& GcsReader::operator=(GcsReader&& that) noexcept {
  IStreamReader::operator=(static_cast<IStreamReader&&>(that));
  client_ = std::move(that.client_);
  object_ = std::move(that.object_);
  read_object_ = std::move(that.read_object_);
  return *this;
}

template <typename... ReadObjectOptions>
void GcsReader::Reset(const google::cloud::storage::Client& client,
                      Initializer<GcsObject> object, Options options,
                      ReadObjectOptions&&... read_object_options) {
  client_ = client;
  riegeli::Reset(object_, std::move(object));
  Initialize(options, std::forward<ReadObjectOptions>(read_object_options)...);
}

template <typename... ReadObjectOptions>
void GcsReader::Reset(const google::cloud::storage::Client& client,
                      Initializer<GcsObject> object,
                      ReadObjectOptions&&... read_object_options) {
  Reset(client, std::move(object), Options(),
        std::forward<ReadObjectOptions>(read_object_options)...);
}

template <typename... ReadObjectOptions>
inline void GcsReader::Initialize(const Options& options,
                                  ReadObjectOptions&&... read_object_options) {
  if (ABSL_PREDICT_FALSE(!object_.ok())) {
    Fail(object_.status());
    return;
  }
  const RangeOptions range_options = GetRangeOptions(read_object_options...);
  SetReadObject(range_options, read_object_options...);
  Initialize(
      object_.generation() == std::nullopt
          ? client_->ReadObject(
                object_.bucket_name(), object_.object_name(),
                std::forward<ReadObjectOptions>(read_object_options)...)
          : client_->ReadObject(
                object_.bucket_name(), object_.object_name(),
                google::cloud::storage::Generation(*object_.generation()),
                std::forward<ReadObjectOptions>(read_object_options)...),
      options.buffer_options(), range_options);
}

template <typename... ReadObjectOptions>
inline GcsReader::RangeOptions GcsReader::GetRangeOptions(
    const ReadObjectOptions&... read_object_options) {
  const google::cloud::storage::ReadFromOffset& read_from_offset =
      gcs_internal::GetOption<google::cloud::storage::ReadFromOffset>(
          read_object_options...);
  const google::cloud::storage::ReadRange& read_range =
      gcs_internal::GetOption<google::cloud::storage::ReadRange>(
          read_object_options...);
  if (read_range.has_value()) {
    return RangeOptions{
        Origin::kBegin,
        read_from_offset.has_value()
            ? SignedMax(read_range.value().begin, read_from_offset.value())
            : read_range.value().begin,
        read_range.value().end};
  }
  const google::cloud::storage::ReadLast& read_last =
      gcs_internal::GetOption<google::cloud::storage::ReadLast>(
          read_object_options...);
  if (read_last.has_value()) {
    return RangeOptions{Origin::kEnd, read_last.value(), std::nullopt};
  }
  return RangeOptions{Origin::kBegin, read_from_offset.value_or(0),
                      std::nullopt};
}

template <typename... ReadObjectOptions>
inline void GcsReader::SetReadObject(
    const RangeOptions& range_options,
    const ReadObjectOptions&... read_object_options) {
  if (range_options.max_size == std::nullopt) {
    read_object_ =
        [new_read_object_options = DecayTuple(
             Filter<NewReadObjectOptionPredicate>(read_object_options...))](
            GcsReader& self, int64_t read_from_offset) mutable {
          return self.ApplyReadObject(
              new_read_object_options,
              std::make_index_sequence<
                  std::tuple_size_v<decltype(new_read_object_options)>>(),
              google::cloud::storage::ReadFromOffset(read_from_offset));
        };
  } else {
    read_object_ =
        [new_read_object_options = DecayTuple(
             Filter<NewReadObjectOptionPredicate>(read_object_options...)),
         max_size = *range_options.max_size](GcsReader& self,
                                             int64_t read_from_offset) mutable {
          return self.ApplyReadObject(
              new_read_object_options,
              std::make_index_sequence<
                  std::tuple_size_v<decltype(new_read_object_options)>>(),
              google::cloud::storage::ReadRange(read_from_offset, max_size));
        };
  }
}

}  // namespace riegeli

#endif  // RIEGELI_GCS_GCS_READER_H_
