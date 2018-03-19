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
#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <functional>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "riegeli/base/base.h"
#include "riegeli/base/str_cat.h"
#include "riegeli/base/string_view.h"

namespace riegeli {

namespace {

// This is a struct rather than a lambda to capture present by move.
struct EmptyParser {
  bool operator()(string_view value) const {
    if (RIEGELI_LIKELY(value.empty())) return present();
    return parser->InvalidValue("(empty)");
  }

  OptionsParser* parser;
  std::function<bool()> present;
};

// This is a struct rather than a lambda to capture parser1 and parser2 by move.
struct OrParser {
  bool operator()(string_view value) const {
    return parser1(value) || parser2(value);
  }

  OptionsParser::ValueParser parser1;
  OptionsParser::ValueParser parser2;
};

}  // namespace

void OptionsParser::Done() {
  options_ = std::vector<Option>();
  current_option_ = nullptr;
  current_valid_values_ = std::string();
}

OptionsParser::ValueParser OptionsParser::Empty(std::function<bool()> present) {
  return EmptyParser{this, std::move(present)};
}

OptionsParser::ValueParser OptionsParser::Int(int* out, int min_value,
                                              int max_value) {
  RIEGELI_ASSERT_LE(min_value, max_value)
      << "Failed precondition of OptionsParser::IntOption(): "
         "bounds in the wrong order";
  return [this, out, min_value, max_value](string_view value) {
    if (RIEGELI_LIKELY(!value.empty())) {
      const std::string string_value(value);
      errno = 0;
      char* end;
      long long_value = std::strtol(string_value.c_str(), &end, 10);
      if (RIEGELI_LIKELY(errno == 0 &&
                         end == string_value.c_str() + string_value.size() &&
                         long_value >= min_value && long_value <= max_value)) {
        *out = IntCast<int>(long_value);
        return true;
      }
    }
    return InvalidValue(StrCat("integers ", min_value, "..", max_value));
  };
}

OptionsParser::ValueParser OptionsParser::Bytes(uint64_t* out,
                                                uint64_t min_value,
                                                uint64_t max_value) {
  RIEGELI_ASSERT_LE(min_value, max_value)
      << "Failed precondition of BytesOption(): bounds in the wrong order";
  return [this, out, min_value, max_value](string_view value) {
    if (RIEGELI_LIKELY(!value.empty())) {
      const std::string string_value(value);
      errno = 0;
      char* end;
      long double long_double_value = std::strtold(string_value.c_str(), &end);
      if (RIEGELI_LIKELY(errno == 0)) {
        if (end != string_value.c_str() + string_value.size()) {
          long double scale;
          switch (*end++) {
            case 'B':
              scale = 1.0L;
              break;
            case 'k':
            case 'K':
              scale = static_cast<long double>(uint64_t{1} << 10);
              break;
            case 'M':
              scale = static_cast<long double>(uint64_t{1} << 20);
              break;
            case 'G':
              scale = static_cast<long double>(uint64_t{1} << 30);
              break;
            case 'T':
              scale = static_cast<long double>(uint64_t{1} << 40);
              break;
            case 'P':
              scale = static_cast<long double>(uint64_t{1} << 50);
              break;
            case 'E':
              scale = static_cast<long double>(uint64_t{1} << 60);
              break;
            default:
              goto error;
          }
          long_double_value *= scale;
        }
        if (RIEGELI_LIKELY(end == string_value.c_str() + string_value.size() &&
                           long_double_value >= 0.0L)) {
          long_double_value = std::round(long_double_value);
          const uint64_t uint64_value =
              RIEGELI_UNLIKELY(long_double_value >=
                               static_cast<long double>(
                                   std::numeric_limits<uint64_t>::max()))
                  ? std::numeric_limits<uint64_t>::max()
                  : static_cast<uint64_t>(long_double_value);
          if (RIEGELI_LIKELY(uint64_value >= min_value &&
                             uint64_value <= max_value)) {
            *out = uint64_value;
            return true;
          }
        }
      }
    }
  error:
    return InvalidValue(
        StrCat("integers expressed as reals with "
               "optional suffix [BkKMGTPE], ",
               min_value, "..", max_value));
  };
}

OptionsParser::ValueParser OptionsParser::Real(double* out, double min_value,
                                               double max_value) {
  RIEGELI_ASSERT_LE(min_value, max_value)
      << "Failed precondition of IntOption(): bounds in the wrong order";
  return [this, out, min_value, max_value](string_view value) {
    if (RIEGELI_LIKELY(!value.empty())) {
      const std::string string_value(value);
      errno = 0;
      char* end;
      double float_value = std::strtod(string_value.c_str(), &end);
      if (RIEGELI_LIKELY(
              errno == 0 && end == string_value.c_str() + string_value.size() &&
              float_value >= min_value && float_value <= max_value)) {
        *out = float_value;
        return true;
      }
    }
    return InvalidValue(StrCat("reals ", min_value, "..", max_value));
  };
}

OptionsParser::ValueParser OptionsParser::Or(ValueParser parser1,
                                             ValueParser parser2) {
  return OrParser{std::move(parser1), std::move(parser2)};
}

OptionsParser::ValueParser OptionsParser::CopyTo(std::string* text) {
  return [this, text](string_view value) {
    StrAppend(text, text->empty() ? "" : ",", current_key(),
              value.empty() ? "" : ":", value);
    return true;
  };
}

bool OptionsParser::InvalidValue(string_view valid_values) {
  RIEGELI_ASSERT(!valid_values.empty())
      << "Failed precondition of OptionsParser::InvalidValue(): "
         "empty valid values";
  RIEGELI_ASSERT(current_option_ != nullptr)
      << "Failed precondition of OptionsParser::InvalidValue(): "
         "no option is being parsed";
  StrAppend(&current_valid_values_, current_valid_values_.empty() ? "" : ", ",
            valid_values);
  return false;
}

bool OptionsParser::FailIfSeen(string_view key) {
  RIEGELI_ASSERT(current_option_ != nullptr)
      << "Failed precondition of OptionsParser::FailIfSeen(): "
         "no option is being parsed";
  const auto option =
      std::find_if(options_.cbegin(), options_.cend(),
                   [key](const Option& option) { return option.key == key; });
  RIEGELI_ASSERT(option != options_.cend()) << "Unknown option " << key;
  if (RIEGELI_UNLIKELY(option->seen)) {
    return Fail(StrCat("Option ", current_option_->key,
                       " conflicts with option ", key));
  }
  return true;
}

bool OptionsParser::FailIfAnySeen() {
  RIEGELI_ASSERT(current_option_ != nullptr)
      << "Failed precondition of OptionsParser::FailIfAnySeen(): "
         "no option is being parsed";
  for (const auto& option : options_) {
    if (RIEGELI_UNLIKELY(option.seen)) {
      return Fail(StrCat("Option ", current_option_->key, " must be first"));
    }
  }
  return true;
}

bool OptionsParser::Parse(string_view text) {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  size_t option_begin = 0;
  for (;;) {
    size_t option_end = text.find(',', option_begin);
    if (option_end == string_view::npos) option_end = text.size();
    if (option_begin != option_end) {
      const string_view key_value =
          text.substr(option_begin, option_end - option_begin);
      string_view key;
      string_view value;
      const size_t colon = key_value.find(':');
      if (colon == string_view::npos) {
        key = key_value;
      } else {
        key = key_value.substr(0, colon);
        value = key_value.substr(colon + 1);
      }
      const auto option = std::find_if(
          options_.begin(), options_.end(),
          [key](const Option& option) { return option.key == key; });
      if (RIEGELI_UNLIKELY(option == options_.end())) {
        std::string message = StrCat("Unknown option ", key, ", valid options: ");
        auto iter = options_.cbegin();
        if (iter != options_.cend()) {
          StrAppend(&message, iter->key);
          for (++iter; iter != options_.cend(); ++iter) {
            StrAppend(&message, ", ", iter->key);
          }
        }
        return Fail(message);
      }
      if (RIEGELI_UNLIKELY(option->seen)) {
        return Fail(StrCat("Option ", key, " is present more than once"));
      }
      current_option_ = &*option;
      RIEGELI_ASSERT_EQ(current_valid_values_, "")
          << "Failed invariant of OptionsParser: "
             "current_valid_values_ not cleared";
      if (RIEGELI_UNLIKELY(!option->value_parser(value))) {
        if (!healthy()) return false;
        return Fail(
            StrCat("Option ", key, ": ",
                   "invalid value: ", value.empty() ? "(empty)" : value,
                   current_valid_values_.empty() ? "" : ", valid values: ",
                   current_valid_values_));
      }
      RIEGELI_ASSERT(healthy())
          << "Value parser of option " << key
          << " failed the OptionsParser but returned true";
      option->seen = true;
      current_option_ = nullptr;
      current_valid_values_.clear();
    }
    if (option_end == text.size()) break;
    option_begin = option_end + 1;
  }
  return true;
}

}  // namespace riegeli
