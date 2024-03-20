package(
    default_visibility = ["//visibility:public"],
    features = ["header_modules"],
)

licenses(["notice"])

cc_library(
    name = "newline",
    hdrs = ["newline.h"],
    deps = [
        "//riegeli/base:constexpr",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "line_reading",
    srcs = ["line_reading.cc"],
    hdrs = ["line_reading.h"],
    deps = [
        ":newline",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:chain",
        "//riegeli/bytes:reader",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
    ],
)

cc_library(
    name = "line_writing",
    hdrs = ["line_writing.h"],
    deps = [
        ":newline",
        "//riegeli/base:assert",
        "//riegeli/base:type_traits",
        "//riegeli/bytes:writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/meta:type_traits",
    ],
)

cc_library(
    name = "text_reader",
    srcs = ["text_reader.cc"],
    hdrs = ["text_reader.h"],
    deps = [
        ":newline",
        "//riegeli/base:any_dependency",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:dependency",
        "//riegeli/base:initializer",
        "//riegeli/base:object",
        "//riegeli/base:status",
        "//riegeli/base:types",
        "//riegeli/bytes:buffer_options",
        "//riegeli/bytes:buffered_reader",
        "//riegeli/bytes:prefix_limiting_reader",
        "//riegeli/bytes:reader",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/utility",
    ],
)

cc_library(
    name = "text_writer",
    srcs = ["text_writer.cc"],
    hdrs = ["text_writer.h"],
    deps = [
        ":line_writing",
        ":newline",
        "//riegeli/base:any_dependency",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:dependency",
        "//riegeli/base:initializer",
        "//riegeli/base:object",
        "//riegeli/base:status",
        "//riegeli/base:types",
        "//riegeli/bytes:buffer_options",
        "//riegeli/bytes:buffered_writer",
        "//riegeli/bytes:prefix_limiting_writer",
        "//riegeli/bytes:writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/utility",
    ],
)
