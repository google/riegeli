package(
    default_visibility = ["//visibility:public"],
    features = ["header_modules"],
)

licenses(["notice"])

cc_library(
    name = "message_parse",
    srcs = ["message_parse.cc"],
    hdrs = ["message_parse.h"],
    deps = [
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:buffering",
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
    name = "message_serialize",
    srcs = ["message_serialize.cc"],
    hdrs = ["message_serialize.h"],
    deps = [
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:buffer",
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
    name = "text_parse",
    srcs = ["text_parse.cc"],
    hdrs = ["text_parse.h"],
    deps = [
        ":message_parse",
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
        "@com_google_absl//absl/strings:str_format",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "text_print",
    srcs = ["text_print.cc"],
    hdrs = ["text_print.h"],
    deps = [
        ":message_serialize",
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
