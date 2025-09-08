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
#include <type_traits>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"

ABSL_POINTERS_DEFAULT_NONNULL

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
void WriteChar(CharType src, DebugStream& dest) {
  if (src >= 32 && src <= 126) {
    if (src == quote || src == '\\') dest.Write('\\');
    dest.Write(static_cast<char>(src));
    return;
  }
  switch (src) {
    case '\t':
      dest.Write("\\t");
      break;
    case '\n':
      dest.Write("\\n");
      break;
    case '\r':
      dest.Write("\\r");
      break;
    default: {
      const auto unsigned_src = static_cast<IntType>(src);
      if (unsigned_src <= 0xff) {
        dest.Write("\\x{");
        WriteHex2(static_cast<uint8_t>(unsigned_src), dest);
      } else {
        dest.Write("\\u{");
        if (unsigned_src <= 0xffff) {
          WriteHex4(static_cast<uint16_t>(unsigned_src), dest);
        } else {
          WriteHex8(unsigned_src, dest);
        }
      }
      dest.Write('}');
      break;
    }
  }
}

template <typename IntType, typename CharType>
void WriteQuotedChar(CharType src, DebugStream& dest) {
  dest.Write('\'');
  WriteChar<'\'', IntType>(src, dest);
  dest.Write('\'');
}

template <typename IntType, typename CharType>
void WriteQuotedString(absl::Span<const CharType> src, DebugStream& dest) {
  dest.Write('"');
  for (const CharType ch : src) {
    WriteChar<'"', IntType>(ch, dest);
  }
  dest.Write('"');
}

}  // namespace

void DebugStream::DebugStringFragment(absl::string_view src) {
  for (const char ch : src) {
    WriteChar<'"', uint8_t>(ch, *this);
  }
}

void RiegeliDebug(bool src, DebugStream& dest) {
  dest.Write(src ? absl::string_view("true") : absl::string_view("false"));
}

void RiegeliDebug(char src, DebugStream& dest) {
  WriteQuotedChar<uint8_t>(src, dest);
}

void RiegeliDebug(wchar_t src, DebugStream& dest) {
  WriteQuotedChar<std::conditional_t<sizeof(wchar_t) == 2, uint16_t, uint32_t>>(
      src, dest);
}

#if __cpp_char8_t
void RiegeliDebug(char8_t src, DebugStream& dest) {
  WriteQuotedChar<uint8_t>(src, dest);
}
#endif  // __cpp_char8_t

void RiegeliDebug(char16_t src, DebugStream& dest) {
  WriteQuotedChar<uint16_t>(src, dest);
}

void RiegeliDebug(char32_t src, DebugStream& dest) {
  WriteQuotedChar<uint32_t>(src, dest);
}

void RiegeliDebug(absl::string_view src, DebugStream& dest) {
  WriteQuotedString<uint8_t>(absl::MakeConstSpan(src), dest);
}

void RiegeliDebug(std::wstring_view src, DebugStream& dest) {
  WriteQuotedString<
      std::conditional_t<sizeof(wchar_t) == 2, uint16_t, uint32_t>>(
      absl::MakeConstSpan(src), dest);
}

#if __cpp_char8_t
void RiegeliDebug(std::u8string_view src, DebugStream& dest) {
  WriteQuotedString<uint8_t>(absl::MakeConstSpan(src), dest);
}
#endif  // __cpp_char8_t

void RiegeliDebug(std::u16string_view src, DebugStream& dest) {
  WriteQuotedString<uint16_t>(absl::MakeConstSpan(src), dest);
}

void RiegeliDebug(std::u32string_view src, DebugStream& dest) {
  WriteQuotedString<uint32_t>(absl::MakeConstSpan(src), dest);
}

void RiegeliDebug(const absl::Cord& src, DebugStream& dest) {
  dest.DebugStringQuote();
  for (const absl::string_view fragment : src.Chunks()) {
    dest.DebugStringFragment(fragment);
  }
  dest.DebugStringQuote();
}

void RiegeliDebug(const void* absl_nullable src, DebugStream& dest) {
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
