package(
    default_visibility = ["//visibility:public"],
    features = ["header_modules"],
)

licenses(["notice"])

cc_library(
    name = "xz_reader",
    srcs = ["xz_reader.cc"],
    hdrs = ["xz_reader.h"],
    deps = [
        ":xz_error",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:dependency",
        "//riegeli/base:object",
        "//riegeli/base:recycling_pool",
        "//riegeli/base:status",
        "//riegeli/base:types",
        "//riegeli/bytes:buffer_options",
        "//riegeli/bytes:buffered_reader",
        "//riegeli/bytes:reader",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@xz//:liblzma",
    ],
)

cc_library(
    name = "xz_writer",
    srcs = ["xz_writer.cc"],
    hdrs = ["xz_writer.h"],
    deps = [
        ":xz_error",
        ":xz_reader",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:dependency",
        "//riegeli/base:object",
        "//riegeli/base:recycling_pool",
        "//riegeli/base:status",
        "//riegeli/base:types",
        "//riegeli/bytes:buffer_options",
        "//riegeli/bytes:buffered_writer",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@xz//:liblzma",
    ],
)

cc_library(
    name = "xz_error",
    srcs = ["xz_error.cc"],
    hdrs = ["xz_error.h"],
    visibility = ["//visibility:private"],
    deps = [
        "//riegeli/base:assert",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@xz//:liblzma",
    ],
)
