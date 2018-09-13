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
#include <string>
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
  //  * true  - success (FailIfSeen() nor FailIfAnySeen() must not have been
  //            called)
  //  * false - failure (InvalidValue(), FailIfSeen(), or FailIfAnySeen() may
  //            have been called)
  using Function = std::function<bool(ValueParser*)>;

  ValueParser(const ValueParser&) = delete;
  ValueParser& operator=(const ValueParser&) = delete;

  // Value parser for absent or empty argument.
  template <typename T>
  static Function Empty(T* out, T value);

  // Value parser for explicitly enumerated valid values.
  //
  // An empty possible value matches also the case when ":" with value is
  // absent.
  template <typename T>
  static Function Enum(T* out,
                       std::vector<std::pair<std::string, T>> possible_values);

  // Value parser for integers min_value..max_value.
  static Function Int(int* out, int min_value, int max_value);

  // Value parser for integers expressed as reals with optional suffix
  // [BkKMGTPE], min_value..max_value.
  static Function Bytes(uint64_t* out, uint64_t min_value, uint64_t max_value);

  // Value parser for reals min_value..max_value.
  static Function Real(double* out, double min_value, double max_value);

  // Value parser which tries multiple parsers and returns the result of the
  // first one which succeeds.
  //
  // The parsers must not include FailIfSeen() nor FailIfAnySeen(). Conflicts
  // with other options should be checked outside the Or().
  static Function Or(Function function1, Function function2);
  template <typename... Functions>
  static Function Or(Function function1, Function function2,
                     Functions&&... functions);

  // Value parser which runs multiple parsers and expects all of them to
  // succeed.
  static Function And(Function function1, Function function2);
  template <typename... Functions>
  static Function And(Function function1, Function function2,
                      Functions&&... functions);

  // Value parser which appends the option to a separate options string
  // (as comma-separated key:value pairs), to be parsed with a separate
  // OptionsParser.
  static Function CopyTo(std::string* text);

  // Value parser which reports a conflict if an option with any of the given
  // keys was seen before this option.
  //
  // Multiple occurrences of the same option are always invalid and do not have
  // to be explicitly checked with FailIfSeen().
  static Function FailIfSeen(absl::string_view key);
  template <typename... Keys>
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
  // Multiple descriptions from several InvalidValue() calls are joined with
  // commas. This normally happens if all parsers from Or() fail.
  //
  // Precondition: !valid_values.empty()
  //
  // Always returns false.
  bool InvalidValue(absl::string_view valid_values);

 private:
  friend class OptionsParser;

  explicit ValueParser(OptionsParser* options_parser, absl::string_view key,
                       absl::string_view value);

  OptionsParser* options_parser_;
  absl::string_view key_;
  absl::string_view value_;
  // When InvalidValue() was called, a human-readable description of valid
  // values, otherwise empty.
  std::string valid_values_;
};

class OptionsParser : public Object {
 public:
  OptionsParser() : Object(State::kOpen) {}

  OptionsParser(const OptionsParser&) = delete;
  OptionsParser& operator=(const OptionsParser&) = delete;

  // Registers an option with the given key. Its value must be accepted by the
  // value parser.
  //
  // The value parser may be implemented explicitly (e.g. as a lambda)
  // or returned by one of functions below (called on this OptionsParser).
  void AddOption(std::string key, ValueParser::Function function);

  // Parses options from text. Valid options must have been registered with
  // AddOptions().
  //
  //   options ::= option? ("," option?)*
  //   option ::= key (":" value)?
  //   key ::= (char except ',' and ':')*
  //   value ::= (char except ',')*
  //
  // For each recognized option key, calls the corresponding value parser.
  // If ":" with value is absent, string_view() is passed as the value.
  //
  // Return values:
  //  * true  - success (healthy())
  //  * false - failure (!healthy())
  bool Parse(absl::string_view text);

 protected:
  void Done() override;

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

namespace internal {

// This is a struct rather than a lambda to capture value by move.
template <typename T>
struct EmptyFunction {
  bool operator()(ValueParser* value_parser) const {
    if (ABSL_PREDICT_TRUE(value_parser->value().empty())) {
      *out = value;
      return true;
    }
    return value_parser->InvalidValue("(empty)");
  }

  T* out;
  T value;
};

// This is a struct rather than a lambda to capture possible_values by move.
template <typename T>
struct EnumOptionFunction {
  bool operator()(ValueParser* value_parser) const {
    for (const std::pair<std::string, T>& possible_value : possible_values) {
      if (value_parser->value() == possible_value.first) {
        *out = possible_value.second;
        return true;
      }
    }
    for (const std::pair<std::string, T>& possible_value : possible_values) {
      value_parser->InvalidValue(possible_value.first.empty()
                                     ? absl::string_view("(empty)")
                                     : absl::string_view(possible_value.first));
    }
    return false;
  }

  T* out;
  std::vector<std::pair<std::string, T>> possible_values;
};

}  // namespace internal

template <typename T>
ValueParser::Function ValueParser::Empty(T* out, T value) {
  return internal::EmptyFunction<T>{out, std::move(value)};
}

template <typename T>
ValueParser::Function ValueParser::Enum(
    T* out, std::vector<std::pair<std::string, T>> possible_values) {
  return internal::EnumOptionFunction<T>{out, std::move(possible_values)};
}

template <typename... Functions>
ValueParser::Function ValueParser::Or(Function function1, Function function2,
                                      Functions&&... functions) {
  return Or(function1, Or(function2, std::forward<Functions>(functions)...));
}

template <typename... Functions>
ValueParser::Function ValueParser::And(Function function1, Function function2,
                                       Functions&&... functions) {
  return And(function1, And(function2, std::forward<Functions>(functions)...));
}

template <typename... Keys>
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
