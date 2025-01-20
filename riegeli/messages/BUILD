package(
    default_visibility = ["//visibility:public"],
    features = ["header_modules"],
)

licenses(["notice"])

cc_library(
    name = "parse_message",
    srcs = ["parse_message.cc"],
    hdrs = ["parse_message.h"],
    deps = [
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:buffering",
        "//riegeli/base:bytes_ref",
        "//riegeli/base:chain",
        "//riegeli/base:dependency",
        "//riegeli/base:types",
        "//riegeli/bytes:chain_reader",
        "//riegeli/bytes:cord_reader",
        "//riegeli/bytes:limiting_reader",
        "//riegeli/bytes:reader",
        "//riegeli/varint:varint_reading",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:optional",
        "@com_google_protobuf//:protobuf_lite",
    ],
)

cc_library(
    name = "serialize_message",
    srcs = ["serialize_message.cc"],
    hdrs = ["serialize_message.h"],
    deps = [
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:buffering",
        "//riegeli/base:chain",
        "//riegeli/base:compact_string",
        "//riegeli/base:dependency",
        "//riegeli/base:string_utils",
        "//riegeli/base:types",
        "//riegeli/bytes:array_writer",
        "//riegeli/bytes:chain_writer",
        "//riegeli/bytes:cord_writer",
        "//riegeli/bytes:writer",
        "//riegeli/varint:varint_writing",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:span",
        "@com_google_protobuf//:protobuf_lite",
    ],
)

cc_library(
    name = "text_parse_message",
    srcs = ["text_parse_message.cc"],
    hdrs = ["text_parse_message.h"],
    deps = [
        ":parse_message",
        "//riegeli/base:bytes_ref",
        "//riegeli/base:chain",
        "//riegeli/base:dependency",
        "//riegeli/bytes:chain_reader",
        "//riegeli/bytes:cord_reader",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:string_reader",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "text_print_message",
    srcs = ["text_print_message.cc"],
    hdrs = ["text_print_message.h"],
    deps = [
        ":serialize_message",
        "//riegeli/base:assert",
        "//riegeli/base:chain",
        "//riegeli/base:dependency",
        "//riegeli/bytes:chain_writer",
        "//riegeli/bytes:cord_writer",
        "//riegeli/bytes:string_writer",
        "//riegeli/bytes:writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:cord",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "message_wire_format",
    srcs = ["message_wire_format.cc"],
    hdrs = ["message_wire_format.h"],
    deps = [
        "//riegeli/base:arithmetic",
        "//riegeli/base:constexpr",
        "//riegeli/base:object",
        "//riegeli/bytes:backward_writer",
        "//riegeli/bytes:writer",
        "//riegeli/endian:endian_writing",
        "//riegeli/varint:varint_writing",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
    ],
)
