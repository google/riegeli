// Copyright 2018 Google LLC
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

#ifndef RIEGELI_BASE_OPTIONS_PARSER_H_
#define RIEGELI_BASE_OPTIONS_PARSER_H_

#include <stdint.h>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "riegeli/base/str_cat.h"
#include "riegeli/base/string_view.h"

namespace riegeli {

// Specifies how to parse the value of a particular option key.
//
// Return values:
//  * true  - success (captured state is changed according to value)
//  * false - failure (*message is set)
using OptionParser = std::function<bool(string_view value, std::string* message)>;

// Parses options from text:
//
//   options ::= option? ("," option?)*
//   option ::= key (":" value)?
//   key ::= (char except ',' and ':')*
//   value ::= (char except ',')*
//
// For each recognized option key, calls the corresponding option parser.
// If ":" with value is absent, string_view() is passed as the value.
//
// If an option parser fails, prepends StrCat("Option ", key, ": ") to *message.
//
// Return values:
//  * true  - success
//  * false - failure (*message is set)
bool ParseOptions(
    const std::vector<std::pair<string_view, OptionParser>>& option_parsers,
    string_view text, std::string* message);

// Option parser for explicitly enumerated valid values.
//
// An empty possible value matches also the case when ":" with value is absent.
//
// Return values:
//  * true  - success (*out is set to the corresponding value)
//  * false - failure (*message is set)
template <typename Enum>
OptionParser EnumOption(
    Enum* out, std::vector<std::pair<string_view, Enum>> possible_values);

// Option parser for integers min_value..max_value.
//
// Return values:
//  * true  - success (*out is set to the corresponding value)
//  * false - failure (*message is set)
OptionParser IntOption(int* out, int min_value, int max_value);

// Option parser for integers expressed as reals with optional suffix
// [BkKMGTPE], min_value..max_value.
//
// Return values:
//  * true  - success (*out is set to the corresponding value)
//  * false - failure (*message is set)
OptionParser BytesOption(uint64_t* out, uint64_t min_value, uint64_t max_value);

// Option parser for reals min_value..max_value.
//
// Return values:
//  * true  - success (*out is set to the corresponding value)
//  * false - failure (*message is set)
OptionParser RealOption(double* out, double min_value, double max_value);

// Implementation details follow.

namespace internal {

// This is a class rather than a lambda to capture possible_values by move.
template <typename Enum>
class EnumOptionParser {
 public:
  EnumOptionParser(Enum* out,
                   std::vector<std::pair<string_view, Enum>> possible_values)
      : out_(out), possible_values_(std::move(possible_values)) {}

  bool operator()(string_view value, std::string* message) const {
    for (const auto& possible_value : possible_values_) {
      if (value == possible_value.first) {
        *out_ = possible_value.second;
        return true;
      }
    }
    *message = StrCat("invalid value: ", value.empty() ? "(empty)" : value,
                      ", valid values: ");
    auto iter = possible_values_.cbegin();
    if (iter != possible_values_.cend()) {
      StrAppend(message, iter->first.empty() ? "(empty)" : iter->first);
      for (++iter; iter != possible_values_.cend(); ++iter) {
        StrAppend(message, ", ", iter->first.empty() ? "(empty)" : iter->first);
      }
    }
    return false;
  }

 private:
  Enum* out_;
  std::vector<std::pair<string_view, Enum>> possible_values_;
};

}  // namespace internal

template <typename Enum>
OptionParser EnumOption(
    Enum* out, std::vector<std::pair<string_view, Enum>> possible_values) {
  return std::move(
      internal::EnumOptionParser<Enum>(out, std::move(possible_values)));
}

};  // namespace riegeli

#endif  // RIEGELI_BASE_OPTIONS_PARSER_H_
