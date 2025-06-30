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

#ifndef RIEGELI_GCS_GCS_WRITER_H_
#define RIEGELI_GCS_GCS_WRITER_H_

#include <stddef.h>
#include <stdint.h>

#include <functional>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "google/cloud/storage/auto_finalize.h"
#include "google/cloud/storage/client.h"
#include "google/cloud/storage/download_options.h"
#include "google/cloud/storage/object_metadata.h"
#include "google/cloud/storage/object_write_stream.h"
#include "google/cloud/storage/upload_options.h"
#include "google/cloud/storage/user_ip_option.h"
#include "google/cloud/storage/well_known_headers.h"
#include "google/cloud/storage/well_known_parameters.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/object.h"
#include "riegeli/base/reset.h"
#include "riegeli/base/string_ref.h"
#include "riegeli/base/type_traits.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/ostream_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/gcs/gcs_internal.h"
#include "riegeli/gcs/gcs_object.h"
#include "riegeli/gcs/gcs_reader.h"

namespace riegeli {

// A `Writer` which writes to a Google Cloud Storage object.
class GcsWriter
    : public OStreamWriter<google::cloud::storage::ObjectWriteStream> {
 public:
  class Options : public BufferOptionsBase<Options> {
   public:
    Options() noexcept {}

    // The effective buffer size is always a multiple of 256 KiB, and is at
    // least the `UploadBufferSize` setting in the client (which defaults to
    // 8 MiB).
    static constexpr size_t kDefaultMinBufferSize = size_t{8} << 20;
    static constexpr size_t kDefaultMaxBufferSize = size_t{8} << 20;

    // If `std::nullopt`, writing os performed directly to the destination
    // object. `Flush()` is not effective, `append()` and `ReadMode()` are not
    // supported, but `UseResumableUploadSession()` is supported.
    //
    // If not `std::nullopt`, writing is performed either directly to the
    // destination object, or to a temporary object with this name, which is
    // then composed with the destination object and deleted. `Flush()` is
    // effective, `append()` and `ReadMode()` are supported, but
    // `UseResumableUploadSession()` is not supported.
    //
    // Default: `std::nullopt`.
    Options& set_temp_object_name(StringInitializer temp_object_name) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      riegeli::Reset(temp_object_name_, std::move(temp_object_name));
      return *this;
    }
    Options&& set_temp_object_name(StringInitializer temp_object_name) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_temp_object_name(std::move(temp_object_name)));
    }
    std::optional<std::string>& temp_object_name()
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return temp_object_name_;
    }
    const std::optional<std::string>& temp_object_name() const
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return temp_object_name_;
    }

    // If `false`, replaces existing contents of the destination object,
    // clearing it first.
    //
    // If `true`, appends to existing contents of the destination object.
    // This requires `temp_object_name() != std::nullopt`.
    //
    // Default: `false`.
    Options& set_append(bool append) & ABSL_ATTRIBUTE_LIFETIME_BOUND {
      append_ = append;
      return *this;
    }
    Options&& set_append(bool append) && ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_append(append));
    }
    bool append() const { return append_; }

    // Buffer options for `GcsReader` returned by `ReadMode()`.
    //
    // Default: like in `GcsReader::Options`, i.e.
    //   `BufferOptions()
    //        .set_min_buffer_size(size_t{64} << 10)
    //        .set_max_buffer_size(size_t{1} << 20)`.
    Options& set_read_mode_buffer_options(
        BufferOptions read_mode_buffer_options) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      read_mode_buffer_options_ = read_mode_buffer_options;
      return *this;
    }
    Options&& set_read_mode_buffer_options(
        BufferOptions read_mode_buffer_options) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_read_mode_buffer_options(read_mode_buffer_options));
    }
    BufferOptions read_mode_buffer_options() const {
      return read_mode_buffer_options_;
    }

   private:
    std::optional<std::string> temp_object_name_;
    bool append_ = false;
    BufferOptions read_mode_buffer_options_ =
        BufferOptions()
            .set_min_buffer_size(size_t{64} << 10)
            .set_max_buffer_size(size_t{1} << 20);
  };

  // Creates a closed `GcsWriter`.
  explicit GcsWriter(Closed) : OStreamWriter(kClosed) {}

  // Will write to `object`.
  //
  // If an upload session is being resumed, `pos()` corresponds to the next
  // position to write. `GcsWriter` is left closed if the resumed upload session
  // is finalized.
  template <typename... WriteObjectOptions>
  explicit GcsWriter(const google::cloud::storage::Client& client,
                     Initializer<GcsObject> object, Options options,
                     WriteObjectOptions&&... write_object_options);
  template <typename... WriteObjectOptions>
  explicit GcsWriter(const google::cloud::storage::Client& client,
                     Initializer<GcsObject> object,
                     WriteObjectOptions&&... write_object_options);

  GcsWriter(GcsWriter&& that) noexcept;
  GcsWriter& operator=(GcsWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `GcsWriter`. This avoids
  // constructing a temporary `GcsWriter` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  template <typename... WriteObjectOptions>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      const google::cloud::storage::Client& client,
      Initializer<GcsObject> object, Options options,
      WriteObjectOptions&&... write_object_options);
  template <typename... WriteObjectOptions>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      const google::cloud::storage::Client& client,
      Initializer<GcsObject> object,
      WriteObjectOptions&&... write_object_options);

  google::cloud::storage::Client& client() {
    RIEGELI_ASSERT(client_ != std::nullopt)
        << "Failed precondition of GcsWriter::client(): "
           "default-constructed GcsWriter";
    return *client_;
  }
  const GcsObject& object() const { return object_; }
  std::optional<absl::string_view> temp_object_name() const {
    return temp_object_name_;
  }

  // Returns metadata about upload results.
  //
  // Precondition: `Close()` succeeded or `GcsWriter` was created with a resumed
  // upload session which was finalized.
  const google::cloud::storage::ObjectMetadata& metadata() const&;
  google::cloud::storage::ObjectMetadata&& metadata() &&;

  // Returns the resumable upload session id for this upload, or an empty string
  // for uploads that do not use resumable upload session ids.
  absl::string_view resumable_session_id() const {
    return dest().resumable_session_id();
  }

  bool SupportsReadMode() override;

 protected:
  void Done() override;
  absl::Status AnnotateStatusImpl(absl::Status status) override;
  bool WriteInternal(absl::string_view src) override;
  bool FlushImpl(FlushType flush_type) override;
  Reader* ReadModeBehindBuffer(Position initial_pos) override;

 private:
  template <typename WriteObjectOption>
  struct CommonOptionPredicate
      : std::disjunction<std::is_same<std::decay_t<WriteObjectOption>,
                                      google::cloud::storage::QuotaUser>,
                         std::is_same<std::decay_t<WriteObjectOption>,
                                      google::cloud::storage::UserIp>,
                         std::is_same<std::decay_t<WriteObjectOption>,
                                      google::cloud::storage::UserProject>> {};

  template <typename WriteObjectOption>
  struct ComposeObjectOptionPredicate
      : std::disjunction<CommonOptionPredicate<WriteObjectOption>,
                         std::is_same<std::decay_t<WriteObjectOption>,
                                      google::cloud::storage::EncryptionKey>,
                         std::is_same<std::decay_t<WriteObjectOption>,
                                      google::cloud::storage::Fields>,
                         std::is_same<std::decay_t<WriteObjectOption>,
                                      google::cloud::storage::KmsKeyName>> {};

  template <typename WriteObjectOption>
  struct DeleteTempObjectOptionPredicate
      : CommonOptionPredicate<WriteObjectOption> {};

  template <typename WriteObjectOption>
  struct GetObjectSizeOptionPredicate
      : CommonOptionPredicate<WriteObjectOption> {};

  template <typename WriteObjectOption>
  struct MakeGcsReaderOptionPredicate
      : CommonOptionPredicate<WriteObjectOption> {};

  template <typename... ComposeObjectOptions, size_t... indices>
  google::cloud::StatusOr<google::cloud::storage::ObjectMetadata>
  ApplyComposeObject(
      const std::tuple<ComposeObjectOptions...>& compose_object_options,
      std::index_sequence<indices...>) {
    std::vector<google::cloud::storage::ComposeSourceObject> source_objects;
    source_objects.reserve(2);
    source_objects.push_back(google::cloud::storage::ComposeSourceObject{
        object_.object_name(),
        already_composed_ ? std::nullopt : object_.generation(), std::nullopt});
    source_objects.push_back(google::cloud::storage::ComposeSourceObject{
        *temp_object_name_, std::nullopt, std::nullopt});
    already_composed_ = true;
    return client_->ComposeObject(object_.bucket_name(), source_objects,
                                  object_.object_name(),
                                  std::get<indices>(compose_object_options)...);
  }

  template <typename... WriteObjectOptions, size_t... indices,
            typename... ExtraOptions>
  google::cloud::storage::ObjectWriteStream ApplyWriteTempObject(
      const std::tuple<WriteObjectOptions...>& write_object_options,
      std::index_sequence<indices...>, ExtraOptions&&... extra_options) {
    return client_->WriteObject(object_.bucket_name(), *temp_object_name_,
                                std::get<indices>(write_object_options)...,
                                std::forward<ExtraOptions>(extra_options)...,
                                google::cloud::storage::Fields(""),
                                google::cloud::storage::AutoFinalizeDisabled());
  }

  template <typename... DeleteObjectOptions, size_t... indices>
  google::cloud::Status ApplyDeleteTempObject(
      const std::tuple<DeleteObjectOptions...>& delete_object_options,
      std::index_sequence<indices...>) {
    return client_->DeleteObject(object_.bucket_name(), *temp_object_name_,
                                 std::get<indices>(delete_object_options)...,
                                 google::cloud::storage::Fields(""));
  }

  template <typename... GetObjectMetadataOptions, size_t... indices>
  google::cloud::StatusOr<google::cloud::storage::ObjectMetadata>
  ApplyGetObjectSize(const std::tuple<GetObjectMetadataOptions...>&
                         get_object_metadata_options,
                     std::index_sequence<indices...>) {
    return object_.generation() == std::nullopt
               ? client_->GetObjectMetadata(
                     object_.bucket_name(), object_.object_name(),
                     std::get<indices>(get_object_metadata_options)...,
                     google::cloud::storage::Fields("size"))
               : client_->GetObjectMetadata(
                     object_.bucket_name(), object_.object_name(),
                     google::cloud::storage::Generation(*object_.generation()),
                     std::get<indices>(get_object_metadata_options)...,
                     google::cloud::storage::Fields("size"));
  }

  template <typename... ReadObjectOptions, size_t... indices>
  GcsReader* ApplyMakeGcsReader(
      const std::tuple<ReadObjectOptions...>& read_object_options,
      std::index_sequence<indices...>, int64_t read_from_offset) {
    return associated_reader_.ResetReader(
        client(),
        already_composed_
            ? GcsObject(object().bucket_name(), object().object_name())
            : object(),
        GcsReader::Options().set_buffer_options(read_mode_buffer_options_),
        std::get<indices>(read_object_options)...,
        google::cloud::storage::ReadFromOffset(read_from_offset));
  }

  bool CheckPreconditions(bool append, bool resumable_upload_session);
  template <typename... WriteObjectOptions>
  void Initialize(const Options& options,
                  WriteObjectOptions&&... write_object_options);
  template <typename... WriteObjectOptions>
  void SetFunctions(const WriteObjectOptions&... write_object_options);
  void Initialize(google::cloud::storage::ObjectWriteStream stream,
                  BufferOptions buffer_options, bool append);
  void PropagateStatus();
  ABSL_ATTRIBUTE_COLD void PropagateStatusSlow(
      const google::cloud::Status& status);

  std::optional<google::cloud::storage::Client> client_;
  GcsObject object_;
  std::optional<std::string> temp_object_name_;
  BufferOptions read_mode_buffer_options_;
  std::function<google::cloud::storage::ObjectWriteStream(GcsWriter&)>
      write_temp_object_;
  std::function<google::cloud::StatusOr<google::cloud::storage::ObjectMetadata>(
      GcsWriter&)>
      compose_object_;
  std::function<google::cloud::Status(GcsWriter&)> delete_temp_object_;
  std::function<google::cloud::StatusOr<google::cloud::storage::ObjectMetadata>(
      GcsWriter&)>
      get_object_size_;
  std::function<GcsReader*(GcsWriter&, int64_t)> make_gcs_reader_;
  bool writing_temp_object_ = false;
  bool already_composed_ = false;
  Position initial_temp_object_pos_ = 0;
  std::optional<google::cloud::storage::ObjectMetadata> metadata_;

  AssociatedReader<GcsReader> associated_reader_;
};

// Implementation details follow.

template <typename... WriteObjectOptions>
GcsWriter::GcsWriter(const google::cloud::storage::Client& client,
                     Initializer<GcsObject> object, Options options,
                     WriteObjectOptions&&... write_object_options)
    : OStreamWriter(kClosed),
      client_(client),
      object_(std::move(object)),
      temp_object_name_(std::move(options.temp_object_name())),
      read_mode_buffer_options_(options.read_mode_buffer_options()) {
  Initialize(options,
             std::forward<WriteObjectOptions>(write_object_options)...);
}

template <typename... WriteObjectOptions>
GcsWriter::GcsWriter(const google::cloud::storage::Client& client,
                     Initializer<GcsObject> object,
                     WriteObjectOptions&&... write_object_options)
    : GcsWriter(client, std::move(object), Options(),
                std::forward<WriteObjectOptions>(write_object_options)...) {}

inline GcsWriter::GcsWriter(GcsWriter&& that) noexcept
    : OStreamWriter(static_cast<OStreamWriter&&>(that)),
      client_(std::move(that.client_)),
      object_(std::move(that.object_)),
      temp_object_name_(std::exchange(that.temp_object_name_, std::nullopt)),
      read_mode_buffer_options_(that.read_mode_buffer_options_),
      write_temp_object_(std::move(that.write_temp_object_)),
      compose_object_(std::move(that.compose_object_)),
      delete_temp_object_(std::move(that.delete_temp_object_)),
      get_object_size_(std::move(that.get_object_size_)),
      make_gcs_reader_(std::move(that.make_gcs_reader_)),
      writing_temp_object_(that.writing_temp_object_),
      already_composed_(that.already_composed_),
      initial_temp_object_pos_(that.initial_temp_object_pos_),
      metadata_(std::exchange(that.metadata_, std::nullopt)),
      associated_reader_(std::move(that.associated_reader_)) {}

inline GcsWriter& GcsWriter::operator=(GcsWriter&& that) noexcept {
  OStreamWriter::operator=(static_cast<OStreamWriter&&>(that));
  client_ = std::move(that.client_);
  object_ = std::move(that.object_);
  temp_object_name_ = std::exchange(that.temp_object_name_, std::nullopt);
  read_mode_buffer_options_ = that.read_mode_buffer_options_;
  write_temp_object_ = std::move(that.write_temp_object_);
  compose_object_ = std::move(that.compose_object_);
  delete_temp_object_ = std::move(that.delete_temp_object_);
  get_object_size_ = std::move(that.get_object_size_);
  make_gcs_reader_ = std::move(that.make_gcs_reader_);
  writing_temp_object_ = that.writing_temp_object_;
  already_composed_ = that.already_composed_;
  initial_temp_object_pos_ = that.initial_temp_object_pos_;
  metadata_ = std::exchange(that.metadata_, std::nullopt);
  associated_reader_ = std::move(that.associated_reader_);
  return *this;
}

template <typename... WriteObjectOptions>
void GcsWriter::Reset(const google::cloud::storage::Client& client,
                      Initializer<GcsObject> object, Options options,
                      WriteObjectOptions&&... write_object_options) {
  OStreamWriter::Reset(kClosed);
  client_ = client;
  riegeli::Reset(object_, std::move(object));
  temp_object_name_ = std::move(options.temp_object_name());
  read_mode_buffer_options_ = options.read_mode_buffer_options();
  write_temp_object_ = nullptr;
  compose_object_ = nullptr;
  delete_temp_object_ = nullptr;
  get_object_size_ = nullptr;
  make_gcs_reader_ = nullptr;
  writing_temp_object_ = false;
  already_composed_ = false;
  initial_temp_object_pos_ = 0;
  metadata_ = std::nullopt;
  associated_reader_.Reset();
  Initialize(options,
             std::forward<WriteObjectOptions>(write_object_options)...);
}

template <typename... WriteObjectOptions>
void GcsWriter::Reset(const google::cloud::storage::Client& client,
                      Initializer<GcsObject> object,
                      WriteObjectOptions&&... write_object_options) {
  Reset(client, std::move(object), Options(),
        std::forward<WriteObjectOptions>(write_object_options)...);
}

template <typename... WriteObjectOptions>
inline void GcsWriter::Initialize(
    const Options& options, WriteObjectOptions&&... write_object_options) {
  if (ABSL_PREDICT_FALSE(!object_.ok())) {
    Fail(object_.status());
    return;
  }
  if (ABSL_PREDICT_FALSE(!CheckPreconditions(
          options.append(),
          gcs_internal::GetOption<
              google::cloud::storage::UseResumableUploadSession>(
              write_object_options...)
              .has_value()))) {
    return;
  }
  if (temp_object_name_ != std::nullopt) SetFunctions(write_object_options...);
  Initialize(
      options.append()
          ? client_->WriteObject(
                object_.bucket_name(), object_.object_name(),
                std::forward<WriteObjectOptions>(write_object_options)...,
                google::cloud::storage::IfGenerationMatch(0),
                google::cloud::storage::AutoFinalizeDisabled())
          : client_->WriteObject(
                object_.bucket_name(), object_.object_name(),
                std::forward<WriteObjectOptions>(write_object_options)...,
                google::cloud::storage::AutoFinalizeDisabled()),
      options.buffer_options(), options.append());
}

template <typename... WriteObjectOptions>
inline void GcsWriter::SetFunctions(
    const WriteObjectOptions&... write_object_options) {
  write_temp_object_ =
      [write_temp_object_options = std::tuple<WriteObjectOptions...>(
           write_object_options...)](GcsWriter& self) mutable {
        return self.ApplyWriteTempObject(
            write_temp_object_options,
            std::make_index_sequence<
                std::tuple_size_v<decltype(write_temp_object_options)>>());
      };
  compose_object_ =
      [compose_object_options = DecayTuple(Filter<ComposeObjectOptionPredicate>(
           write_object_options...))](GcsWriter& self) mutable {
        return self.ApplyComposeObject(
            compose_object_options,
            std::make_index_sequence<
                std::tuple_size_v<decltype(compose_object_options)>>());
      };
  delete_temp_object_ =
      [delete_temp_object_options = DecayTuple(
           Filter<DeleteTempObjectOptionPredicate>(write_object_options...))](
          GcsWriter& self) mutable {
        return self.ApplyDeleteTempObject(
            delete_temp_object_options,
            std::make_index_sequence<
                std::tuple_size_v<decltype(delete_temp_object_options)>>());
      };
  get_object_size_ =
      [get_object_size_options = DecayTuple(
           Filter<GetObjectSizeOptionPredicate>(write_object_options...))](
          GcsWriter& self) mutable {
        return self.ApplyGetObjectSize(
            get_object_size_options,
            std::make_index_sequence<
                std::tuple_size_v<decltype(get_object_size_options)>>());
      };
  make_gcs_reader_ =
      [gcs_reader_options = DecayTuple(
           Filter<MakeGcsReaderOptionPredicate>(write_object_options...))](
          GcsWriter& self, int64_t read_from_offset) mutable {
        return self.ApplyMakeGcsReader(
            gcs_reader_options,
            std::make_index_sequence<
                std::tuple_size_v<decltype(gcs_reader_options)>>(),
            read_from_offset);
      };
}

inline const google::cloud::storage::ObjectMetadata& GcsWriter::metadata()
    const& {
  if (metadata_ != std::nullopt) return *metadata_;
  const google::cloud::StatusOr<google::cloud::storage::ObjectMetadata>&
      metadata = dest().metadata();
  RIEGELI_ASSERT_OK(metadata)
      << "Failed precondition of GcsWriter::metadata(): "
         "Close() must have succeeded";
  return *metadata;
}

inline google::cloud::storage::ObjectMetadata&& GcsWriter::metadata() && {
  if (metadata_ != std::nullopt) return *std::move(metadata_);
  google::cloud::StatusOr<google::cloud::storage::ObjectMetadata>&& metadata =
      std::move(dest()).metadata();
  RIEGELI_ASSERT_OK(metadata)
      << "Failed precondition of GcsWriter::metadata(): "
         "Close() must have succeeded";
  return *std::move(metadata);
}

}  // namespace riegeli

#endif  // RIEGELI_GCS_GCS_WRITER_H_
