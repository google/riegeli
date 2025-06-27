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

#include "riegeli/digests/digester_handle.h"

#include <stddef.h>

#include <optional>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/byte_fill.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/type_erased_ref.h"
#include "riegeli/base/types.h"

namespace riegeli {

void DigesterBaseHandle::FailedDigestMethodDefault() {
  RIEGELI_CHECK_UNREACHABLE()
      << "DigesterHandle::Digest() called on a default-constructed "
         "DigesterHandle with a non-void DigesterHandle::DigestType";
}

void DigesterBaseHandle::SetWriteSizeHintMethodDefault(
    ABSL_ATTRIBUTE_UNUSED TypeErasedRef target,
    ABSL_ATTRIBUTE_UNUSED std::optional<Position> write_size_hint) {}

bool DigesterBaseHandle::WriteMethodDefault(
    ABSL_ATTRIBUTE_UNUSED TypeErasedRef target,
    ABSL_ATTRIBUTE_UNUSED absl::string_view src) {
  return true;
}

bool DigesterBaseHandle::WriteChainMethodDefault(
    ABSL_ATTRIBUTE_UNUSED TypeErasedRef target,
    ABSL_ATTRIBUTE_UNUSED const Chain& src) {
  return true;
}

bool DigesterBaseHandle::WriteCordMethodDefault(
    ABSL_ATTRIBUTE_UNUSED TypeErasedRef target,
    ABSL_ATTRIBUTE_UNUSED const absl::Cord& src) {
  return true;
}

bool DigesterBaseHandle::WriteByteFillMethodDefault(
    ABSL_ATTRIBUTE_UNUSED TypeErasedRef target,
    ABSL_ATTRIBUTE_UNUSED ByteFill src) {
  return true;
}

bool DigesterBaseHandle::CloseMethodDefault(
    ABSL_ATTRIBUTE_UNUSED TypeErasedRef target) {
  return true;
}

absl::Status DigesterBaseHandle::StatusMethodDefault(
    ABSL_ATTRIBUTE_UNUSED TypeErasedRef target) {
  return absl::OkStatus();
}

bool DigesterBaseHandle::WriteChainFallback(
    TypeErasedRef target, const Chain& src,
    bool (*write)(TypeErasedRef target, absl::string_view src)) {
  for (const absl::string_view fragment : src.blocks()) {
    if (ABSL_PREDICT_FALSE(!write(target, fragment))) return false;
  }
  return true;
}

void DigesterBaseHandle::WriteChainFallback(
    TypeErasedRef target, const Chain& src,
    void (*write)(TypeErasedRef target, absl::string_view src)) {
  for (const absl::string_view fragment : src.blocks()) write(target, fragment);
}

bool DigesterBaseHandle::WriteCordFallback(
    TypeErasedRef target, const absl::Cord& src,
    bool (*write)(TypeErasedRef target, absl::string_view src)) {
  if (const std::optional<absl::string_view> flat = src.TryFlat();
      flat != std::nullopt) {
    return write(target, *flat);
  }
  for (const absl::string_view fragment : src.Chunks()) {
    if (ABSL_PREDICT_FALSE(!write(target, fragment))) return false;
  }
  return true;
}

void DigesterBaseHandle::WriteCordFallback(
    TypeErasedRef target, const absl::Cord& src,
    void (*write)(TypeErasedRef target, absl::string_view src)) {
  if (const std::optional<absl::string_view> flat = src.TryFlat();
      flat != std::nullopt) {
    write(target, *flat);
    return;
  }
  for (const absl::string_view fragment : src.Chunks()) write(target, fragment);
}

bool DigesterBaseHandle::WriteByteFillFallback(
    TypeErasedRef target, ByteFill src,
    bool (*write)(TypeErasedRef target, absl::string_view src)) {
  for (const absl::string_view fragment : src.blocks()) {
    if (ABSL_PREDICT_FALSE(!write(target, fragment))) return false;
  }
  return true;
}

void DigesterBaseHandle::WriteByteFillFallback(
    TypeErasedRef target, ByteFill src,
    void (*write)(TypeErasedRef target, absl::string_view src)) {
  for (const absl::string_view fragment : src.blocks()) write(target, fragment);
}

}  // namespace riegeli
