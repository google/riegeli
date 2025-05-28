// Copyright 2024 Google LLC
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

#include "riegeli/base/debug.h"

#include <stdint.h>

#include <cstddef>
#include <optional>

#include "absl/base/attributes.h"
#include "absl/meta/type_traits.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"

namespace riegeli {

namespace {

inline void WriteHex1(uint8_t src, DebugStream& dest) {
  dest.Write(static_cast<char>(src + (src < 10 ? '0' : 'a' - 10)));
}

inline void WriteHex2(uint8_t src, DebugStream& dest) {
  WriteHex1(static_cast<uint8_t>(src >> 4), dest);
  WriteHex1(static_cast<uint8_t>(src & 0x0f), dest);
}

inline void WriteHex4(uint16_t src, DebugStream& dest) {
  WriteHex2(static_cast<uint8_t>(src >> 8), dest);
  WriteHex2(static_cast<uint8_t>(src & 0xff), dest);
}

inline void WriteHex8(uint32_t src, DebugStream& dest) {
  WriteHex4(static_cast<uint16_t>(src >> 16), dest);
  WriteHex4(static_cast<uint16_t>(src & 0xffff), dest);
}

template <char quote, typename IntType, typename CharType>
debug_internal::LastEscape WriteChar(CharType src, DebugStream& dest) {
  if (src >= 32 && src <= 126) {
    if (src == quote || src == '\\') dest.Write('\\');
    dest.Write(static_cast<char>(src));
    return debug_internal::LastEscape::kNormal;
  }
  switch (src) {
    case '\0':
      dest.Write("\\0");
      return debug_internal::LastEscape::kZero;
    case '\t':
      dest.Write("\\t");
      return debug_internal::LastEscape::kNormal;
    case '\n':
      dest.Write("\\n");
      return debug_internal::LastEscape::kNormal;
    case '\r':
      dest.Write("\\r");
      return debug_internal::LastEscape::kNormal;
    default: {
      const auto unsigned_src = static_cast<IntType>(src);
      if (unsigned_src <= 0xff) {
        dest.Write("\\x");
        WriteHex2(static_cast<uint8_t>(unsigned_src), dest);
        return debug_internal::LastEscape::kHex;
      }
      if (unsigned_src <= 0xffff) {
        dest.Write("\\u");
        WriteHex4(static_cast<uint16_t>(unsigned_src), dest);
        return debug_internal::LastEscape::kNormal;
      }
      dest.Write("\\U");
      WriteHex8(unsigned_src, dest);
      return debug_internal::LastEscape::kNormal;
    }
  }
}

template <typename IntType, typename CharType>
debug_internal::LastEscape WriteStringFragment(
    absl::Span<const CharType> src, DebugStream& dest,
    debug_internal::LastEscape last_escape =
        debug_internal::LastEscape::kNormal) {
  for (const CharType ch : src) {
    switch (last_escape) {
      case debug_internal::LastEscape::kNormal:
        break;
      case debug_internal::LastEscape::kZero:
        if (ch >= '0' && ch <= '7') {
          // Ensure that the next character is not interpreted as a part of the
          // previous octal escape sequence.
          dest.Write("00");
        }
        break;
      case debug_internal::LastEscape::kHex:
        if ((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') ||
            (ch >= 'A' && ch <= 'F')) {
          // Ensure that the next character is not interpreted as a part of the
          // previous hex escape sequence.
          dest.Write("\" \"");
        }
        break;
    }
    last_escape = WriteChar<'"', IntType>(ch, dest);
  }
  return last_escape;
}

}  // namespace

void DebugStream::DebugStringFragment(absl::string_view src,
                                      EscapeState& escape_state) {
  escape_state.last_escape_ = WriteStringFragment<uint8_t>(
      absl::MakeConstSpan(src), *this, escape_state.last_escape_);
}

void RiegeliDebug(bool src, DebugStream& dest) {
  dest.Write(src ? absl::string_view("true") : absl::string_view("false"));
}

void RiegeliDebug(char src, DebugStream& dest) {
  dest.Write('\'');
  WriteChar<'\'', uint8_t>(src, dest);
  dest.Write('\'');
}

void RiegeliDebug(wchar_t src, DebugStream& dest) {
  dest.Write('\'');
  WriteChar<'\'',
            absl::conditional_t<sizeof(wchar_t) == 2, uint16_t, uint32_t>>(
      src, dest);
  dest.Write('\'');
}

#if __cpp_char8_t
void RiegeliDebug(char8_t src, DebugStream& dest) {
  dest.Write('\'');
  WriteChar<'\'', uint8_t>(src, dest);
  dest.Write('\'');
}
#endif  // __cpp_char8_t

void RiegeliDebug(char16_t src, DebugStream& dest) {
  dest.Write('\'');
  WriteChar<'\'', uint16_t>(src, dest);
  dest.Write('\'');
}

void RiegeliDebug(char32_t src, DebugStream& dest) {
  dest.Write('\'');
  WriteChar<'\'', uint32_t>(src, dest);
  dest.Write('\'');
}

void RiegeliDebug(absl::string_view src, DebugStream& dest) {
  dest.DebugStringQuote();
  WriteStringFragment<uint8_t>(absl::MakeConstSpan(src), dest);
  dest.DebugStringQuote();
}

#if __cpp_lib_string_view

void RiegeliDebug(std::wstring_view src, DebugStream& dest) {
  dest.DebugStringQuote();
  WriteStringFragment<
      absl::conditional_t<sizeof(wchar_t) == 2, uint16_t, uint32_t>>(
      absl::MakeConstSpan(src), dest);
  dest.DebugStringQuote();
}

#if __cpp_char8_t
void RiegeliDebug(std::u8string_view src, DebugStream& dest) {
  dest.DebugStringQuote();
  WriteStringFragment<uint8_t>(absl::MakeConstSpan(src), dest);
  dest.DebugStringQuote();
}
#endif  // __cpp_char8_t

void RiegeliDebug(std::u16string_view src, DebugStream& dest) {
  dest.DebugStringQuote();
  WriteStringFragment<uint16_t>(absl::MakeConstSpan(src), dest);
  dest.DebugStringQuote();
}

void RiegeliDebug(std::u32string_view src, DebugStream& dest) {
  dest.DebugStringQuote();
  WriteStringFragment<uint32_t>(absl::MakeConstSpan(src), dest);
  dest.DebugStringQuote();
}

#else  // !__cpp_lib_string_view

void RiegeliDebug(const std::wstring& src, DebugStream& dest) {
  dest.DebugStringQuote();
  WriteStringFragment<
      absl::conditional_t<sizeof(wchar_t) == 2, uint16_t, uint32_t>>(
      absl::MakeConstSpan(src), dest);
  dest.DebugStringQuote();
}

#if __cpp_char8_t
void RiegeliDebug(const std::u8string& src, DebugStream& dest) {
  dest.DebugStringQuote();
  WriteStringFragment<uint8_t>(absl::MakeConstSpan(src), dest);
  dest.DebugStringQuote();
}
#endif  // __cpp_char8_t

void RiegeliDebug(const std::u16string& src, DebugStream& dest) {
  dest.DebugStringQuote();
  WriteStringFragment<uint16_t>(absl::MakeConstSpan(src), dest);
  dest.DebugStringQuote();
}

void RiegeliDebug(const std::u32string& src, DebugStream& dest) {
  dest.DebugStringQuote();
  WriteStringFragment<uint32_t>(absl::MakeConstSpan(src), dest);
  dest.DebugStringQuote();
}

#endif  // !__cpp_lib_string_view

void RiegeliDebug(const absl::Cord& src, DebugStream& dest) {
  dest.DebugStringQuote();
  DebugStream::EscapeState escape_state;
  for (const absl::string_view fragment : src.Chunks()) {
    dest.DebugStringFragment(fragment, escape_state);
  }
  dest.DebugStringQuote();
}

void RiegeliDebug(const void* src, DebugStream& dest) {
  if (src == nullptr) {
    dest.Write("nullptr");
  } else {
    dest << src;
  }
}

void RiegeliDebug(ABSL_ATTRIBUTE_UNUSED std::nullptr_t src, DebugStream& dest) {
  dest.Write("nullptr");
}

void RiegeliDebug(ABSL_ATTRIBUTE_UNUSED std::nullopt_t src, DebugStream& dest) {
  dest.Write("nullopt");
}

}  // namespace riegeli
