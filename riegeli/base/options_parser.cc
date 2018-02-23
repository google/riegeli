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
#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "riegeli/base/base.h"
#include "riegeli/base/str_cat.h"
#include "riegeli/base/string_view.h"

namespace riegeli {

namespace {

const OptionParser* FindOption(
    const std::vector<std::pair<string_view, OptionParser>>& option_parsers,
    string_view key) {
  for (const auto& option_parser : option_parsers) {
    if (option_parser.first == key) {
      return &option_parser.second;
    }
  }
  return nullptr;
}

}  // namespace

bool ParseOptions(
    const std::vector<std::pair<string_view, OptionParser>>& option_parsers,
    string_view text, std::string* message) {
  size_t option_begin = 0;
  for (;;) {
    size_t option_end = text.find(',', option_begin);
    if (option_end == string_view::npos) option_end = text.size();
    if (option_begin != option_end) {
      string_view option = text.substr(option_begin, option_end - option_begin);
      string_view value;
      const size_t colon = option.find(':');
      if (colon != string_view::npos) {
        value = option.substr(colon + 1);
        option = option.substr(0, colon);
      }
      const OptionParser* option_parser = FindOption(option_parsers, option);
      if (RIEGELI_UNLIKELY(option_parser == nullptr)) {
        *message = StrCat("Unknown option ", option, ", valid options: ");
        auto iter = option_parsers.cbegin();
        if (iter != option_parsers.cend()) {
          StrAppend(message, iter->first);
          for (++iter; iter != option_parsers.cend(); ++iter) {
            StrAppend(message, ", ", iter->first);
          }
        }
        return false;
      }
      if (RIEGELI_UNLIKELY(!(*option_parser)(value, message))) {
        *message = StrCat("Option ", option, ": ", *message);
        return false;
      }
    }
    if (option_end == text.size()) break;
    option_begin = option_end + 1;
  }
  return true;
}

OptionParser IntOption(int* out, int min_value, int max_value) {
  RIEGELI_ASSERT_LE(min_value, max_value)
      << "Failed precondition of IntOption(): bounds in the wrong order";
  return [out, min_value, max_value](string_view value, std::string* message) {
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
    *message = StrCat("invalid value: ", value.empty() ? "(empty)" : value,
                      ", valid values: integers ", min_value, "..", max_value);
    return false;
  };
}

OptionParser BytesOption(uint64_t* out, uint64_t min_value,
                         uint64_t max_value) {
  RIEGELI_ASSERT_LE(min_value, max_value)
      << "Failed precondition of BytesOption(): bounds in the wrong order";
  return [out, min_value, max_value](string_view value, std::string* message) {
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
    *message = StrCat("invalid value: ", value.empty() ? "(empty)" : value,
                      ", valid values: integers expressed as reals with "
                      "optional suffix [BkKMGTPE], ",
                      min_value, "..", max_value);
    return false;
  };
}

OptionParser RealOption(double* out, double min_value, double max_value) {
  RIEGELI_ASSERT_LE(min_value, max_value)
      << "Failed precondition of IntOption(): bounds in the wrong order";
  return [out, min_value, max_value](string_view value, std::string* message) {
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
    *message = StrCat("invalid value: ", value.empty() ? "(empty)" : value,
                      ", valid values: reals ", min_value, "..", max_value);
    return false;
  };
}

}  // namespace riegeli
