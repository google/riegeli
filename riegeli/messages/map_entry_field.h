// Copyright 2025 Google LLC
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

#ifndef RIEGELI_MESSAGES_MAP_ENTRY_FIELD_H_
#define RIEGELI_MESSAGES_MAP_ENTRY_FIELD_H_

// IWYU pragma: private, include "riegeli/messages/field_handlers.h"
// IWYU pragma: private, include "riegeli/messages/serialized_message_backward_writer.h"
// IWYU pragma: private, include "riegeli/messages/serialized_message_writer.h"

#include "absl/base/nullability.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

// Specifies a field of synthetic map entry message.
enum MapEntryField { kMapEntryKey = 1, kMapEntryValue = 2 };

}  // namespace riegeli

#endif  // RIEGELI_MESSAGES_MAP_ENTRY_FIELD_H_
