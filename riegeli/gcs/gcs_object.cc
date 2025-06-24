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

#include "riegeli/gcs/gcs_object.h"

#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <optional>
#include <ostream>
#include <string>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/debug.h"
#include "riegeli/base/global.h"
#include "riegeli/bytes/ostream_writer.h"
#include "riegeli/bytes/string_writer.h"
#include "riegeli/bytes/stringify.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

namespace {

constexpr absl::string_view kUriPrefix = "gs://";

inline bool IsUtf8Tail(char ch, unsigned char start = 0x80,
                       unsigned char end = 0xbf) {
  return static_cast<unsigned char>(ch) >= start &&
         static_cast<unsigned char>(ch) <= end;
}

}  // namespace

void GcsObject::ParseUri(absl::string_view uri) {
  if (ABSL_PREDICT_FALSE(!absl::StartsWith(uri, kUriPrefix))) {
    FailParsing("GCS URI does not begin with \"gs://\"", uri);
    return;
  }
  const absl::string_view after_prefix(uri.data() + kUriPrefix.size(),
                                       uri.size() - kUriPrefix.size());

  const size_t slash = after_prefix.find('/');
  if (ABSL_PREDICT_FALSE(slash == absl::string_view::npos)) {
    FailParsing(
        "GCS URI does not include \"/\" "
        "separating bucket name from object name",
        uri);
    return;
  }
  const absl::string_view after_bucket_name(after_prefix.data() + (slash + 1),
                                            after_prefix.size() - (slash + 1));

  size_t hash = after_bucket_name.rfind('#');
  std::optional<int64_t> generation;
  if (hash == absl::string_view::npos) {
    hash = after_bucket_name.size();
  } else {
    const char* ptr = after_bucket_name.data() + (hash + 1);
    const char* const limit =
        after_bucket_name.data() + after_bucket_name.size();
    if (ABSL_PREDICT_FALSE(ptr == limit)) {
      FailParsing("Empty generation number", uri);
      return;
    }
    generation = 0;
    do {
      if (ABSL_PREDICT_FALSE(*ptr < '0' || *ptr > '9')) {
        FailParsing("Invalid generation number", uri);
        return;
      }
      const int64_t digit = *ptr - '0';
      if (ABSL_PREDICT_FALSE(*generation >
                             std::numeric_limits<int64_t>::max() / 10)) {
        FailParsing("Generation number out of range", uri);
        return;
      }
      *generation *= 10;
      if (ABSL_PREDICT_FALSE(*generation >
                             std::numeric_limits<int64_t>::max() - digit)) {
        FailParsing("Generation number out of range", uri);
        return;
      }
      *generation += digit;
      ++ptr;
    } while (ptr != limit);
  }

  bucket_name_.assign(after_prefix.data(), slash);
  object_name_.assign(after_bucket_name.data(), hash);
  generation_ = generation;
  ValidateNames();
}

const absl::Status& GcsObject::DefaultStatus() {
  return Global([] {
    return absl::InvalidArgumentError("Default-constructed GcsObject");
  });
}

const absl::Status& GcsObject::MovedFromStatus() {
  return Global(
      [] { return absl::InvalidArgumentError("Moved-from GcsObject"); });
}

inline bool GcsObject::FailParsing(absl::string_view message,
                                   absl::string_view context) {
  status_ = absl::InvalidArgumentError(
      absl::StrCat(message, ": ", riegeli::Debug(context)));
  return false;
}

void GcsObject::ValidateNames() {
  if (ABSL_PREDICT_FALSE(!ValidateBucketName() || !ValidateObjectName())) {
    bucket_name_ = std::string();
    object_name_ = std::string();
    generation_ = std::nullopt;
  }
}

inline bool GcsObject::ValidateBucketName() {
  const absl::string_view bucket_name = bucket_name_;
  if (ABSL_PREDICT_FALSE(bucket_name.size() < 3)) {
    return FailParsing("Bucket name must not be shorter than 3 characters",
                       bucket_name);
  }
  if (ABSL_PREDICT_FALSE(bucket_name.size() > 222)) {
    return FailParsing("Bucket name must not be longer than 222 characters",
                       bucket_name);
  }

  // Whether there were any '.' characters.
  bool has_dots = false;
  // Whether there were any '_' characters.
  bool has_underscores = false;
  // Whether the current dot-separated component contains characters other than
  // digits.
  bool component_has_non_digits = false;
  // Whether the previous character was a letter or digit.
  bool after_alphanumeric = false;
  // The beginning of the current dot-separated component.
  const char* component_begin = bucket_name.data();

  const char* const limit = bucket_name.data() + bucket_name.size();
  for (const char* cursor = bucket_name.data(); cursor < limit; ++cursor) {
    const char ch = *cursor;
    if (ch >= 'a' && ch <= 'z') {
      component_has_non_digits = true;
      after_alphanumeric = true;
    } else if (ch >= '0' && ch <= '9') {
      after_alphanumeric = true;
    } else {
      if (ABSL_PREDICT_FALSE(cursor == component_begin)) {
        return FailParsing(
            component_begin == bucket_name.data()
                ? absl::string_view(
                      "Bucket name must begin with a letter or digit")
                : absl::string_view("Bucket name dot-separated component must "
                                    "begin with a letter or digit"),
            bucket_name);
      }
      if (ch == '-') {
        component_has_non_digits = true;
        after_alphanumeric = false;
      } else if (ch == '_') {
        has_underscores = true;
        component_has_non_digits = true;
        after_alphanumeric = false;
      } else if (ch == '.') {
        if (ABSL_PREDICT_FALSE(PtrDistance(component_begin, cursor) > 63)) {
          return FailParsing(
              "Bucket name dot-separated component must not "
              "be longer than 63 characters",
              bucket_name);
        }
        if (ABSL_PREDICT_FALSE(!after_alphanumeric)) {
          return FailParsing(
              "Bucket name dot-separated component must "
              "end with a letter or digit",
              bucket_name);
        }
        has_dots = true;
        component_has_non_digits = false;
        after_alphanumeric = false;
        component_begin = cursor + 1;
      } else {
        return FailParsing(
            "Bucket name must consist of "
            "letters, digits, hyphens, underscores, and dots",
            bucket_name);
      }
    }
  }
  if (ABSL_PREDICT_FALSE(PtrDistance(component_begin, limit) > 63)) {
    return FailParsing(
        component_begin == bucket_name.data()
            ? absl::string_view("Bucket name with no dots must not "
                                "be longer than 63 characters")
            : absl::string_view("Bucket name dot-separated component must not "
                                "be longer than 63 characters"),
        bucket_name);
  }
  if (ABSL_PREDICT_FALSE(!after_alphanumeric)) {
    return FailParsing("Bucket name must end with a letter or digit",
                       bucket_name);
  }
  if (has_dots) {
    if (ABSL_PREDICT_FALSE(has_underscores)) {
      return FailParsing("Bucket name with dots must not contain underscores",
                         bucket_name);
    }
    if (ABSL_PREDICT_FALSE(!component_has_non_digits)) {
      return FailParsing(
          "Bucket name last dot-separated component must not "
          "consist of only digits",
          bucket_name);
    }
  }
  return true;
}

inline bool GcsObject::ValidateObjectName() {
  const absl::string_view object_name = object_name_;
  if (ABSL_PREDICT_FALSE(object_name.empty())) {
    return FailParsing("Object name must not be empty", object_name);
  }
  if (ABSL_PREDICT_FALSE(object_name.size() > 1024)) {
    return FailParsing("Object name must not be longer than 1024 bytes",
                       object_name);
  }

  const char* const limit = object_name.data() + object_name.size();
  for (const char* cursor = object_name.data(); cursor < limit;) {
    const unsigned char byte = static_cast<unsigned char>(*cursor);
    if (ABSL_PREDICT_TRUE(byte <= 0x7f)) {
      if (ABSL_PREDICT_FALSE(byte == '\n' || byte == '\r')) {
        return FailParsing("Object name must not contain newlines",
                           object_name);
      }
      ++cursor;
    } else {
      if (ABSL_PREDICT_FALSE(byte < 0xc2 || byte > 0xf4)) {
      invalid:
        return FailParsing("Object name must be valid UTF-8", object_name);
      }
      const size_t remaining = PtrDistance(cursor, limit);
      if (byte <= 0xdf) {
        if (ABSL_PREDICT_FALSE(remaining < 2 || !IsUtf8Tail(cursor[1]))) {
          goto invalid;
        }
        cursor += 2;
      } else if (byte <= 0xef) {
        if (ABSL_PREDICT_FALSE(
                remaining < 3 ||
                (byte == 0xe0   ? !IsUtf8Tail(cursor[1], 0xa0, 0xbf)
                 : byte == 0xed ? !IsUtf8Tail(cursor[1], 0x80, 0x9f)
                                : !IsUtf8Tail(cursor[1])) ||
                !IsUtf8Tail(cursor[2]))) {
          goto invalid;
        }
        cursor += 3;
      } else {
        if (ABSL_PREDICT_FALSE(
                remaining < 4 ||
                (byte == 0xf0   ? !IsUtf8Tail(cursor[1], 0x90, 0xbf)
                 : byte == 0xf4 ? !IsUtf8Tail(cursor[1], 0x80, 0x8f)
                                : !IsUtf8Tail(cursor[1]))) ||
            !IsUtf8Tail(cursor[2]) || !IsUtf8Tail(cursor[3])) {
          goto invalid;
        }
        cursor += 4;
      }
    }
  }

  if (ABSL_PREDICT_FALSE(object_name == ".")) {
    return FailParsing("Object name must not be \".\"", object_name);
  }
  if (ABSL_PREDICT_FALSE(object_name == "..")) {
    return FailParsing("Object name must not be \"..\"", object_name);
  }
  if (ABSL_PREDICT_FALSE(
          absl::StartsWith(object_name, ".well-known/acme-challenge/"))) {
    return FailParsing(
        "Object name must not start with \".well-known/acme-challenge/\"",
        object_name);
  }
  return true;
}

bool GcsObject::Equal(const GcsObject& a, const GcsObject& b) {
  if (ABSL_PREDICT_FALSE(!a.ok())) return a.status() == b.status();
  if (ABSL_PREDICT_FALSE(!b.ok())) return false;
  return a.bucket_name() == b.bucket_name() &&
         a.object_name() == b.object_name() && a.generation() == b.generation();
}

void GcsObject::Output(std::ostream& dest) const {
  OStreamWriter<> writer(&dest);
  WriteTo(writer);
  writer.Close();
}

void GcsObject::WriteTo(Writer& dest) const {
  if (ABSL_PREDICT_FALSE(!ok())) {
    dest.Write('<', status().message(), '>');
    return;
  }
  dest.Write(kUriPrefix, bucket_name(), '/', object_name());
  if (generation() != std::nullopt) dest.Write('#', *generation());
}

std::string GcsObject::uri() const {
  std::string uri;
  if (ABSL_PREDICT_TRUE(ok())) {
    StringWriter<> writer(&uri);
    writer.SetWriteSizeHint(
        kUriPrefix.size() + bucket_name().size() + 1 + object_name().size() +
        (generation() == std::nullopt
             ? 0
             : 1 + riegeli::StringifiedSize(*generation())));
    WriteTo(writer);
    writer.Close();
  }
  return uri;
}

}  // namespace riegeli
