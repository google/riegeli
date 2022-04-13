package(
    default_visibility = ["//visibility:public"],
    features = ["header_modules"],
)

licenses(["notice"])

cc_library(
    name = "snappy_writer",
    srcs = ["snappy_writer.cc"],
    hdrs = ["snappy_writer.h"],
    deps = [
        ":snappy_streams",
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/base:status",
        "//riegeli/bytes:chain_reader",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
        "@snappy",
    ],
)

cc_library(
    name = "snappy_reader",
    srcs = ["snappy_reader.cc"],
    hdrs = ["snappy_reader.h"],
    deps = [
        ":snappy_streams",
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/base:status",
        "//riegeli/bytes:chain_reader",
        "//riegeli/bytes:chain_writer",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@snappy",
    ],
)

cc_library(
    name = "snappy_streams",
    srcs = ["snappy_streams.cc"],
    hdrs = ["snappy_streams.h"],
    visibility = ["//visibility:private"],
    deps = [
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@snappy",
    ],
)
