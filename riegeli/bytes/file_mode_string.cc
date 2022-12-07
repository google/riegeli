// Copyright 2022 Google LLC
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

#include "riegeli/bytes/file_mode_string.h"

#ifdef _WIN32
#include <fcntl.h>
#endif
#include <stddef.h>

#include <string>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"

namespace riegeli {
namespace file_internal {

void SetExisting(bool existing, std::string& mode) {
  if (ABSL_PREDICT_FALSE(mode.empty())) mode = "w";
  if (existing) {
    mode[0] = 'r';
    // Add '+' to modifiers unless it already exists there.
    for (size_t i = 1; i < mode.size(); ++i) {
      if (mode[i] == '+') return;
      if (mode[i] == ',') break;
    }
    mode.insert(1, "+");
  } else {
    mode[0] = 'w';
    // Remove '+' from modifiers.
    for (size_t i = 1; i < mode.size(); ++i) {
      if (mode[i] == '+') {
        mode.erase(i, 1);
        --i;
        continue;
      }
      if (mode[i] == ',') break;
    }
  }
}

void SetRead(bool read, std::string& mode) {
  if (ABSL_PREDICT_FALSE(mode.empty())) mode = "w";
  if (read) {
    // Add '+' to modifiers unless it already exists there.
    for (size_t i = 1; i < mode.size(); ++i) {
      if (mode[i] == '+') return;
      if (mode[i] == ',') break;
    }
    mode.insert(1, "+");
  } else {
    if (mode[0] == 'r') return;
    // Remove '+' from modifiers.
    for (size_t i = 1; i < mode.size(); ++i) {
      if (mode[i] == '+') {
        mode.erase(i, 1);
        --i;
        continue;
      }
      if (mode[i] == ',') break;
    }
  }
}

bool GetRead(absl::string_view mode) {
  if (ABSL_PREDICT_FALSE(mode.empty())) return false;
  if (mode[0] == 'r') return true;
  for (size_t i = 1; i < mode.size(); ++i) {
    if (mode[i] == '+') return true;
    if (mode[i] == ',') break;
  }
  return false;
}

namespace {

inline void SetTextImpl(bool text, std::string& mode) {
#ifdef _WIN32
  const char to_remove = text ? 'b' : 't';
  const char to_add[2] = {text ? 't' : 'b', '\0'};
  bool need_to_add = true;
  for (size_t i = 1; i < mode.size(); ++i) {
    if (mode[i] == to_remove) {
      if (need_to_add) {
        mode[i] = to_add[0];
        need_to_add = false;
      } else {
        mode.erase(i, 1);
        --i;
      }
      continue;
    }
    if (mode[i] == to_add[0]) {
      need_to_add = false;
      continue;
    }
    if (mode[i] == ',') break;
  }
  if (need_to_add) {
    mode.insert(mode.size() > 1 && mode[1] == '+' ? 2 : 1, to_add);
  }
#endif
}

}  // namespace

void SetTextReading(bool text, std::string& mode) {
  if (ABSL_PREDICT_FALSE(mode.empty())) mode = "r";
  SetTextImpl(text, mode);
}

void SetTextWriting(bool text, std::string& mode) {
  if (ABSL_PREDICT_FALSE(mode.empty())) mode = "w";
  SetTextImpl(text, mode);
}

#ifdef _WIN32

int GetTextAsFlags(absl::string_view mode) {
  for (size_t i = 1; i < mode.size(); ++i) {
    if (mode[i] == 'b') return _O_BINARY;
    if (mode[i] == 't') return _O_TEXT;
    if (mode[i] == ',') break;
  }
  return 0;
}

#endif

}  // namespace file_internal
}  // namespace riegeli
