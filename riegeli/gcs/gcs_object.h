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

#ifndef RIEGELI_GCS_GCS_OBJECT_H_
#define RIEGELI_GCS_GCS_OBJECT_H_

#include <stdint.h>

#include <iosfwd>
#include <optional>
#include <string>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/reset.h"
#include "riegeli/base/string_ref.h"
#include "riegeli/bytes/stringify_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Identifies a GCS object by storing the bucket name, object name, and optional
// generation.
//
// It provides parsing and producing URIs of the form
// "gs://bucket_name/object_name#generation" (generation is optional).
//
// The syntax of bucket names is validated against
// https://cloud.google.com/storage/docs/naming#requirements and
// https://datatracker.ietf.org/doc/html/rfc3696#section-2.
// The syntax of object names is validated against
// https://cloud.google.com/storage/docs/objects#naming and
// https://datatracker.ietf.org/doc/html/rfc3629#section-4.
//
// No detailed verification of the domain name is performed in bucket names
// containing dots (e.g. that the top-level domain is currently recognized),
// nor that the bucket name does not begin with "goog" or contain "google"
// (this applies only to bucket creation).
class GcsObject : public WithEqual<GcsObject> {
 public:
  // Constructs a dummy `GcsObject` which is `!ok()`.
  GcsObject() noexcept : status_(DefaultStatus()) {}

  // Constructs `GcsObject` from bucket name, object name, and
  // optional generation.
  explicit GcsObject(StringInitializer bucket_name,
                     StringInitializer object_name,
                     std::optional<int64_t> generation = std::nullopt);

  // Constructs `GcsObject` from a URI of the form
  // "gs://bucket_name/object_name#generation" (generation is optional).
  //
  // If the URI is not in the expected format, `ok()` is `false` and `status()`
  // explains the reason.
  explicit GcsObject(absl::string_view uri);

  GcsObject(const GcsObject& that);
  GcsObject& operator=(const GcsObject& that);

  GcsObject(GcsObject&& that) noexcept;
  GcsObject& operator=(GcsObject&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `GcsObject`. This avoids
  // constructing a temporary `GcsObject` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset();
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      StringInitializer bucket_name, StringInitializer object_name,
      std::optional<int64_t> generation = std::nullopt);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(absl::string_view uri);

  // Returns `true` if the `GcsObject` is valid (the URI was
  // successfully parsed).
  bool ok() const { return status_.ok(); }
  // If `ok()`, returns `absl::OkStatus()`, otherwise returns the status
  // explaining the reason why the `GcsObject` is invalid.
  const absl::Status& status() const { return status_; }

  const std::string& bucket_name() const { return bucket_name_; }
  const std::string& object_name() const { return object_name_; }
  std::optional<int64_t> generation() const { return generation_; }

  // Returns the URI of the form "gs://bucket_name/object_name#generation"
  // (generation is optional), or empty string if `!ok()`.
  std::string uri() const;

  friend bool operator==(const GcsObject& a, const GcsObject& b) {
    return Equal(a, b);
  }

  // Default stringification by `absl::StrCat()` etc.
  //
  // Writes `src.uri()` to `dest`, or "<status_message>" if `!ok()`.
  template <typename Sink>
  friend void AbslStringify(Sink& dest, const GcsObject& src) {
    StringifyWriter<Sink*> writer(&dest);
    src.WriteTo(writer);
    writer.Close();
  }

  // Writes `src.uri()` to `dest`, or "<status_message>" if `!ok()`.
  friend std::ostream& operator<<(std::ostream& dest, const GcsObject& src) {
    src.Output(dest);
    return dest;
  }

 private:
  static const absl::Status& DefaultStatus();
  static const absl::Status& MovedFromStatus();

  void ParseUri(absl::string_view uri);
  ABSL_ATTRIBUTE_COLD bool FailParsing(absl::string_view message,
                                       absl::string_view context);
  void ValidateNames();
  bool ValidateBucketName();
  bool ValidateObjectName();

  static bool Equal(const GcsObject& a, const GcsObject& b);

  void WriteTo(Writer& dest) const;
  void Output(std::ostream& dest) const;

  absl::Status status_;
  std::string bucket_name_;
  std::string object_name_;
  std::optional<int64_t> generation_;
};

// Implementation details follow.

inline GcsObject::GcsObject(const GcsObject& that)
    : status_(that.status()),
      bucket_name_(that.bucket_name()),
      object_name_(that.object_name()),
      generation_(that.generation()) {}

inline GcsObject& GcsObject::operator=(const GcsObject& that) {
  status_ = that.status();
  bucket_name_ = that.bucket_name();
  object_name_ = that.object_name();
  generation_ = that.generation();
  return *this;
}

inline GcsObject::GcsObject(GcsObject&& that) noexcept
    : status_(std::exchange(that.status_, MovedFromStatus())),
      bucket_name_(std::exchange(that.bucket_name_, std::string())),
      object_name_(std::exchange(that.object_name_, std::string())),
      generation_(std::exchange(that.generation_, std::nullopt)) {}

inline GcsObject& GcsObject::operator=(GcsObject&& that) noexcept {
  status_ = std::exchange(that.status_, MovedFromStatus());
  bucket_name_ = std::exchange(that.bucket_name_, std::string());
  object_name_ = std::exchange(that.object_name_, std::string());
  generation_ = std::exchange(that.generation_, std::nullopt);
  return *this;
}

inline GcsObject::GcsObject(StringInitializer bucket_name,
                            StringInitializer object_name,
                            std::optional<int64_t> generation)
    : bucket_name_(std::move(bucket_name)),
      object_name_(std::move(object_name)),
      generation_(generation) {
  ValidateNames();
}

inline GcsObject::GcsObject(absl::string_view uri) { ParseUri(uri); }

inline void GcsObject::Reset() {
  status_ = DefaultStatus();
  bucket_name_.clear();
  object_name_.clear();
  generation_ = std::nullopt;
}

inline void GcsObject::Reset(StringInitializer bucket_name,
                             StringInitializer object_name,
                             std::optional<int64_t> generation) {
  status_ = absl::OkStatus();
  riegeli::Reset(bucket_name_, std::move(bucket_name));
  riegeli::Reset(object_name_, std::move(object_name));
  generation_ = generation;
  ValidateNames();
}

inline void GcsObject::Reset(absl::string_view uri) {
  status_ = absl::OkStatus();
  bucket_name_.clear();
  object_name_.clear();
  generation_ = std::nullopt;
  ParseUri(uri);
}

}  // namespace riegeli

#endif  // RIEGELI_GCS_GCS_OBJECT_H_
