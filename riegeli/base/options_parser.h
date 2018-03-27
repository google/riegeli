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

#include "absl/strings/string_view.h"
#include "riegeli/base/object.h"

namespace riegeli {

class OptionsParser : public Object {
 public:
  // Parser of an option value.
  //
  // Return values:
  //  * true  - success (FailIfSeen() nor FailIfAnySeen() must not have been
  //            called)
  //  * false - failure (InvalidValue(), FailIfSeen(), or FailIfAnySeen() may
  //            have been called)
  using ValueParser = std::function<bool(absl::string_view)>;

  OptionsParser() : Object(State::kOpen) {}

  OptionsParser(const OptionsParser&) = delete;
  OptionsParser& operator=(const OptionsParser&) = delete;

  // Registers an option with the given key. Its value must be accepted by the
  // value parser.
  //
  // The value parser may be implemented explicitly (e.g. as a lambda)
  // or returned by one of functions below (called on this OptionsParser).
  void AddOption(std::string key, ValueParser value_parser);

  // Value parser for absent or empty argument.
  ValueParser Empty(std::function<bool()> present);

  // Value parser for explicitly enumerated valid values.
  //
  // An empty possible value matches also the case when ":" with value is
  // absent.
  template <typename T>
  ValueParser Enum(T* out, std::vector<std::pair<std::string, T>> possible_values);

  // Value parser for integers min_value..max_value.
  ValueParser Int(int* out, int min_value, int max_value);

  // Value parser for integers expressed as reals with optional suffix
  // [BkKMGTPE], min_value..max_value.
  ValueParser Bytes(uint64_t* out, uint64_t min_value, uint64_t max_value);

  // Value parser for reals min_value..max_value.
  ValueParser Real(double* out, double min_value, double max_value);

  // Value parser which tries multiple parsers and returns the result of the
  // first one which succeeds.
  //
  // The parsers must not call FailIfSeen() nor FailIfAnySeen(). Conflicts with
  // other options should be checked outside the Or().
  static ValueParser Or(ValueParser parser1, ValueParser parser2);
  template <typename... ValueParsers>
  static ValueParser Or(ValueParser parser1, ValueParser parser2,
                        ValueParsers&&... parsers);

  // Value parser which appends the option to a separate options string
  // (as comma-separated key:value pairs), to be parsed with a separate
  // OptionsParser.
  ValueParser CopyTo(std::string* text);

  // TODO: Consider splitting the state of parsing an option value to a
  // separate class.

  // Returns the key of the option being parsed.
  //
  // Precondition: an option value is being parsed
  const std::string& current_key() const;

  // Reports that the value is invalid, given a human-readable description of
  // values which would have been valid.
  //
  // Multiple descriptions from several InvalidValue() calls are joined with
  // commas. This normally happens if all parsers from Or() fail.
  //
  // Preconditions:
  //   !valid_values.empty()
  //   an option value is being parsed
  //
  // Always returns false.
  bool InvalidValue(absl::string_view valid_values);

  // Reports a conflict if an option with any of the given keys was seen before
  // this option.
  //
  // Multiple occurrences of the same option are always invalid and do not have
  // to be explicitly checked with FailIfSeen().
  //
  // Precondition: an option value is being parsed
  //
  // Return values:
  //  * true - there was no conflict
  //  * false - there was a conflict
  bool FailIfSeen(absl::string_view key);
  template <typename... Keys>
  bool FailIfSeen(absl::string_view key, Keys&&... keys);

  // Reports a conflict if an any option was seen before this option.
  //
  // Precondition: an option value is being parsed
  //
  // Return values:
  //  * true - there was no conflict
  //  * false - there was a conflict
  bool FailIfAnySeen();

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
  struct Option {
    Option(std::string key, ValueParser value_parser)
        : key(std::move(key)), value_parser(std::move(value_parser)) {}

    std::string key;
    ValueParser value_parser;
    bool seen = false;
  };

  std::vector<Option> options_;
  // When an option value is being parsed, the corresponding option, otherwise
  // nullptr.
  Option* current_option_ = nullptr;
  // When an option value is being parsed and InvalidValue() was called,
  // a human-readable description of valid values, otherwise empty.
  std::string current_valid_values_;
};

// Implementation details follow.

inline void OptionsParser::AddOption(std::string key, ValueParser value_parser) {
  RIEGELI_ASSERT(std::find_if(options_.cbegin(), options_.cend(),
                              [&key](const Option& option) {
                                return option.key == key;
                              }) == options_.cend())
      << "Failed precondition of OptionsParser::AddOption(): option " << key
      << "already registered";
  options_.emplace_back(std::move(key), std::move(value_parser));
}

inline const std::string& OptionsParser::current_key() const {
  RIEGELI_ASSERT(current_option_ != nullptr)
      << "Failed precondition of OptionsParser::current_key(): "
         "no option is being parsed";
  return current_option_->key;
}

namespace internal {

// This is a struct rather than a lambda to capture possible_values by move.
template <typename T>
struct EnumOptionParser {
  bool operator()(absl::string_view value) const {
    for (const auto& possible_value : possible_values) {
      if (value == possible_value.first) {
        *out = possible_value.second;
        return true;
      }
    }
    for (const auto& possible_value : possible_values) {
      parser->InvalidValue(possible_value.first.empty()
                               ? absl::string_view("(empty)")
                               : absl::string_view(possible_value.first));
    }
    return false;
  }

  OptionsParser* parser;
  T* out;
  std::vector<std::pair<std::string, T>> possible_values;
};

}  // namespace internal

template <typename T>
OptionsParser::ValueParser OptionsParser::Enum(
    T* out, std::vector<std::pair<std::string, T>> possible_values) {
  return internal::EnumOptionParser<T>{this, out, std::move(possible_values)};
}

template <typename... ValueParsers>
OptionsParser::ValueParser OptionsParser::Or(ValueParser parser1,
                                             ValueParser parser2,
                                             ValueParsers&&... parsers) {
  return Or(parser1, Or(parser2, std::forward<ValueParsers>(parsers)...));
}

template <typename... Keys>
bool OptionsParser::FailIfSeen(absl::string_view key, Keys&&... keys) {
  return RIEGELI_LIKELY(FailIfSeen(key)) &&
         FailIfSeen(std::forward<Keys>(keys)...);
}

};  // namespace riegeli

#endif  // RIEGELI_BASE_OPTIONS_PARSER_H_
