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

#include "riegeli/base/options_parser.h"

#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/string_ref.h"

namespace riegeli {

ValueParser::ValueParser(OptionsParser* options_parser, absl::string_view key,
                         absl::string_view value)
    : options_parser_(RIEGELI_EVAL_ASSERT_NOTNULL(options_parser)),
      key_(key),
      value_(value) {}

ValueParser::Function ValueParser::Int(int min_value, int max_value, int* out) {
  RIEGELI_ASSERT_LE(min_value, max_value)
      << "Failed precondition of OptionsParser::IntOption(): "
         "bounds in the wrong order";
  return [min_value, max_value, out](ValueParser& value_parser) {
    int int_value;
    if (ABSL_PREDICT_TRUE(absl::SimpleAtoi(value_parser.value(), &int_value) &&
                          int_value >= min_value && int_value <= max_value)) {
      *out = int_value;
      return true;
    }
    return value_parser.InvalidValue(absl::StrCat(
        "integers in the range [", min_value, "..", max_value, "]"));
  };
}

ValueParser::Function ValueParser::Bytes(uint64_t min_value, uint64_t max_value,
                                         uint64_t* out) {
  RIEGELI_ASSERT_LE(min_value, max_value)
      << "Failed precondition of BytesOption(): bounds in the wrong order";
  return [min_value, max_value, out](ValueParser& value_parser) {
    absl::string_view value = value_parser.value();
    double scale = 1.0;
    if (ABSL_PREDICT_TRUE(!value.empty())) {
      switch (value.back()) {
        case 'B':
          break;
        case 'k':
        case 'K':
          scale = static_cast<double>(uint64_t{1} << 10);
          break;
        case 'M':
          scale = static_cast<double>(uint64_t{1} << 20);
          break;
        case 'G':
          scale = static_cast<double>(uint64_t{1} << 30);
          break;
        case 'T':
          scale = static_cast<double>(uint64_t{1} << 40);
          break;
        case 'P':
          scale = static_cast<double>(uint64_t{1} << 50);
          break;
        case 'E':
          scale = static_cast<double>(uint64_t{1} << 60);
          break;
        default:
          goto no_scale;
      }
      value.remove_suffix(1);
    }
  no_scale:
    double double_value;
    if (ABSL_PREDICT_TRUE(absl::SimpleAtod(value, &double_value) &&
                          double_value >= 0.0)) {
      double_value = std::round(double_value * scale);
      const uint64_t uint64_value =
          ABSL_PREDICT_FALSE(
              double_value >=
              static_cast<double>(std::numeric_limits<uint64_t>::max()))
              ? std::numeric_limits<uint64_t>::max()
              : static_cast<uint64_t>(double_value);
      if (ABSL_PREDICT_TRUE(uint64_value >= min_value &&
                            uint64_value <= max_value)) {
        *out = uint64_value;
        return true;
      }
    }
    return value_parser.InvalidValue(
        absl::StrCat("integers expressed as reals with "
                     "optional suffix [BkKMGTPE], in the range [",
                     min_value, "..", max_value, "]"));
  };
}

ValueParser::Function ValueParser::Real(double min_value, double max_value,
                                        double* out) {
  RIEGELI_ASSERT_LE(min_value, max_value)
      << "Failed precondition of IntOption(): bounds in the wrong order";
  return [min_value, max_value, out](ValueParser& value_parser) {
    double double_value;
    if (ABSL_PREDICT_TRUE(
            absl::SimpleAtod(value_parser.value(), &double_value) &&
            double_value >= min_value && double_value <= max_value)) {
      *out = double_value;
      return true;
    }
    return value_parser.InvalidValue(
        absl::StrCat("reals in the range [", min_value, "..", max_value, "]"));
  };
}

ValueParser::Function ValueParser::Or(Initializer<Function> function1,
                                      Initializer<Function> function2) {
  return [function1 = std::move(function1).Construct(),
          function2 =
              std::move(function2).Construct()](ValueParser& value_parser) {
    return function1(value_parser) || function2(value_parser);
  };
}

ValueParser::Function ValueParser::And(Initializer<Function> function1,
                                       Initializer<Function> function2) {
  return [function1 = std::move(function1).Construct(),
          function2 =
              std::move(function2).Construct()](ValueParser& value_parser) {
    return function1(value_parser) && function2(value_parser);
  };
}

ValueParser::Function ValueParser::CopyTo(std::string* text) {
  return [text](ValueParser& value_parser) {
    absl::StrAppend(text, text->empty() ? "" : ",", value_parser.key(),
                    value_parser.value().empty() ? "" : ":",
                    value_parser.value());
    return true;
  };
}

ValueParser::Function ValueParser::FailIfSeen(StringInitializer key) {
  return [key = std::move(key).Construct()](ValueParser& value_parser) {
    for (const OptionsParser::Option& option :
         value_parser.options_parser_->options_) {
      if (option.key == key) {
        if (ABSL_PREDICT_FALSE(option.seen)) {
          return value_parser.Fail(absl::InvalidArgumentError(absl::StrCat(
              "Option ", value_parser.key(), " conflicts with option ", key)));
        }
        return true;
      }
    }
    RIEGELI_ASSUME_UNREACHABLE() << "Unknown option " << key;
  };
}

ValueParser::Function ValueParser::FailIfAnySeen() {
  return [](ValueParser& value_parser) {
    for (const OptionsParser::Option& option :
         value_parser.options_parser_->options_) {
      if (ABSL_PREDICT_FALSE(option.seen)) {
        return value_parser.Fail(absl::InvalidArgumentError(
            absl::StrCat("Option ", value_parser.key(), " must be first")));
      }
    }
    return true;
  };
}

bool ValueParser::InvalidValue(absl::string_view valid_values) {
  RIEGELI_ASSERT(!valid_values.empty())
      << "Failed precondition of OptionsParser::InvalidValue(): "
         "empty valid values";
  absl::StrAppend(&valid_values_, valid_values_.empty() ? "" : ", ",
                  valid_values);
  return false;
}

bool OptionsParser::FromString(absl::string_view text) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  size_t option_begin = 0;
  for (;;) {
    size_t option_end = text.find(',', option_begin);
    if (option_end == absl::string_view::npos) option_end = text.size();
    if (option_begin != option_end) {
      const absl::string_view key_value =
          text.substr(option_begin, option_end - option_begin);
      absl::string_view key;
      absl::string_view value;
      const size_t colon = key_value.find(':');
      if (colon == absl::string_view::npos) {
        key = key_value;
      } else {
        key = key_value.substr(0, colon);
        value = key_value.substr(colon + 1);
      }
      const std::vector<Option>::iterator option = std::find_if(
          options_.begin(), options_.end(),
          [key](const Option& option) { return option.key == key; });
      if (ABSL_PREDICT_FALSE(option == options_.end())) {
        std::string message =
            absl::StrCat("Unknown option ", key, ", valid options: ");
        std::vector<Option>::const_iterator iter = options_.cbegin();
        if (iter != options_.cend()) {
          absl::StrAppend(&message, iter->key);
          for (++iter; iter != options_.cend(); ++iter) {
            absl::StrAppend(&message, ", ", iter->key);
          }
        }
        return Fail(absl::InvalidArgumentError(message));
      }
      if (ABSL_PREDICT_FALSE(option->seen)) {
        return Fail(absl::InvalidArgumentError(
            absl::StrCat("Option ", key, " is present more than once")));
      }
      ValueParser value_parser(this, key, value);
      if (ABSL_PREDICT_FALSE(!option->function(value_parser))) {
        if (!value_parser.ok()) return Fail(value_parser.status());
        return Fail(absl::InvalidArgumentError(absl::StrCat(
            "Option ", key, ": ",
            "invalid value: ", value.empty() ? "(empty)" : value,
            value_parser.valid_values_.empty() ? "" : ", valid values: ",
            value_parser.valid_values_)));
      }
      RIEGELI_ASSERT_OK(value_parser)
          << "Value parser of option " << key
          << " returned true but failed the ValueParser";
      option->seen = true;
    }
    if (option_end == text.size()) break;
    option_begin = option_end + 1;
  }
  return true;
}

}  // namespace riegeli
