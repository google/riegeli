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

#include <algorithm>
#include <functional>
#include <iterator>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/object.h"

namespace riegeli {

class OptionsParser;

class ValueParser : public Object {
 public:
  // Parser of an option value.
  //
  // Return values:
  //  * `true`  - success (`FailIfSeen()` nor `FailIfAnySeen()` must not have
  //              been called)
  //  * `false` - failure (`InvalidValue()`, `FailIfSeen()`, or
  //              `FailIfAnySeen()` may have been called)
  using Function = std::function<bool(ValueParser&)>;

  ValueParser(const ValueParser&) = delete;
  ValueParser& operator=(const ValueParser&) = delete;

  // Value parser for absent or empty argument.
  template <typename T>
  static Function Empty(T value, T* out);

  // Value parser for explicitly enumerated valid values.
  //
  // An empty possible value matches also the case when ":" with value is
  // absent.
  template <typename T>
  static Function Enum(std::vector<std::pair<std::string, T>> possible_values,
                       T* out);

  // Value parser for integers in the range [`min_value`..`max_value`].
  static Function Int(int min_value, int max_value, int* out);

  // Value parser for integers expressed as reals with optional suffix
  // `[BkKMGTPE]`, in the range [`min_value`..`max_value`].
  static Function Bytes(uint64_t min_value, uint64_t max_value, uint64_t* out);

  // Value parser for reals in the range [`min_value`..`max_value`].
  static Function Real(double min_value, double max_value, double* out);

  // Value parser which tries multiple parsers and returns the result of the
  // first one which succeeds.
  //
  // The parsers must not include `FailIfSeen()` nor `FailIfAnySeen()`.
  // Conflicts with other options should be checked outside the `Or()`.
  static Function Or(Function function1, Function function2);
  template <typename... Functions,
            std::enable_if_t<(sizeof...(Functions) > 0), int> = 0>
  static Function Or(Function function1, Function function2,
                     Functions&&... functions);

  // Value parser which runs multiple parsers and expects all of them to
  // succeed.
  static Function And(Function function1, Function function2);
  template <typename... Functions,
            std::enable_if_t<(sizeof...(Functions) > 0), int> = 0>
  static Function And(Function function1, Function function2,
                      Functions&&... functions);

  // Value parser which appends the option to a separate options string
  // (as comma-separated key:value pairs), to be parsed with a separate
  // `OptionsParser`.
  static Function CopyTo(std::string* text);

  // Value parser which reports a conflict if an option with any of the given
  // keys was seen before this option.
  //
  // Multiple occurrences of the same option are always invalid and do not have
  // to be explicitly checked with `FailIfSeen()`.
  static Function FailIfSeen(absl::string_view key);
  template <typename... Keys, std::enable_if_t<(sizeof...(Keys) > 0), int> = 0>
  static Function FailIfSeen(absl::string_view key, Keys&&... keys);

  // Value parser which reports a conflict if an any option was seen before this
  // option.
  static Function FailIfAnySeen();

  // Returns the key of the option being parsed.
  absl::string_view key() const { return key_; }

  // Returns the value of the option being parsed.
  absl::string_view value() const { return value_; }

  // Reports that the value is invalid, given a human-readable description of
  // values which would have been valid.
  //
  // Multiple descriptions from several `InvalidValue()` calls are joined with
  // commas. This normally happens if all parsers from `Or()` fail.
  //
  // Precondition: `!valid_values.empty()`
  //
  // Always returns `false`.
  bool InvalidValue(absl::string_view valid_values);

 private:
  friend class OptionsParser;

  explicit ValueParser(OptionsParser* options_parser, absl::string_view key,
                       absl::string_view value);

  OptionsParser* options_parser_;
  absl::string_view key_;
  absl::string_view value_;
  // When `InvalidValue()` was called, a human-readable description of valid
  // values, otherwise empty.
  std::string valid_values_;
};

class OptionsParser : public Object {
 public:
  OptionsParser() : Object(kInitiallyOpen) {}

  OptionsParser(const OptionsParser&) = delete;
  OptionsParser& operator=(const OptionsParser&) = delete;

  // Registers an option with the given key. Its value must be accepted by the
  // value parser.
  //
  // The value parser may be implemented explicitly (e.g. as a lambda)
  // or returned by one of functions below (called on this `OptionsParser`).
  void AddOption(std::string key, ValueParser::Function function);

  // Parses options from text. Valid options must have been registered with
  // `AddOptions()`.
  // ```
  //   options ::= option? ("," option?)*
  //   option ::= key (":" value)?
  //   key ::= (char except ',' and ':')*
  //   value ::= (char except ',')*
  // ```
  //
  // For each recognized option key, calls the corresponding value parser.
  // If ":" with value is absent, `absl::string_view()` is passed as the value.
  //
  // Return values:
  //  * `true`  - success (`healthy()`)
  //  * `false` - failure (`!healthy()`)
  bool FromString(absl::string_view text);

 private:
  friend class ValueParser;

  struct Option {
    explicit Option(std::string key, ValueParser::Function function)
        : key(std::move(key)), function(std::move(function)) {}

    std::string key;
    ValueParser::Function function;
    bool seen = false;
  };

  std::vector<Option> options_;
};

// Implementation details follow.

template <typename T>
ValueParser::Function ValueParser::Empty(T value, T* out) {
  return [value = std::move(value), out](ValueParser& value_parser) {
    if (ABSL_PREDICT_TRUE(value_parser.value().empty())) {
      *out = value;
      return true;
    }
    return value_parser.InvalidValue("(empty)");
  };
}

template <typename T>
ValueParser::Function ValueParser::Enum(
    std::vector<std::pair<std::string, T>> possible_values, T* out) {
  return [possible_values = std::move(possible_values),
          out](ValueParser& value_parser) {
    for (const std::pair<std::string, T>& possible_value : possible_values) {
      if (value_parser.value() == possible_value.first) {
        *out = possible_value.second;
        return true;
      }
    }
    for (const std::pair<std::string, T>& possible_value : possible_values) {
      value_parser.InvalidValue(possible_value.first.empty()
                                    ? absl::string_view("(empty)")
                                    : absl::string_view(possible_value.first));
    }
    return false;
  };
}

template <typename... Functions,
          std::enable_if_t<(sizeof...(Functions) > 0), int>>
ValueParser::Function ValueParser::Or(Function function1, Function function2,
                                      Functions&&... functions) {
  return Or(function1, Or(function2, std::forward<Functions>(functions)...));
}

template <typename... Functions,
          std::enable_if_t<(sizeof...(Functions) > 0), int>>
ValueParser::Function ValueParser::And(Function function1, Function function2,
                                       Functions&&... functions) {
  return And(function1, And(function2, std::forward<Functions>(functions)...));
}

template <typename... Keys, std::enable_if_t<(sizeof...(Keys) > 0), int>>
ValueParser::Function ValueParser::FailIfSeen(absl::string_view key,
                                              Keys&&... keys) {
  return And(FailIfSeen(key), FailIfSeen(std::forward<Keys>(keys)...));
}

inline void OptionsParser::AddOption(std::string key,
                                     ValueParser::Function function) {
  RIEGELI_ASSERT(std::find_if(options_.cbegin(), options_.cend(),
                              [&key](const Option& option) {
                                return option.key == key;
                              }) == options_.cend())
      << "Failed precondition of OptionsParser::AddOption(): option " << key
      << "already registered";
  options_.emplace_back(std::move(key), std::move(function));
}

};  // namespace riegeli

#endif  // RIEGELI_BASE_OPTIONS_PARSER_H_
