package(
    default_visibility = ["//visibility:public"],
    features = ["header_modules"],
)

licenses(["notice"])

cc_library(
    name = "bzip2_reader",
    srcs = ["bzip2_reader.cc"],
    hdrs = ["bzip2_reader.h"],
    deps = [
        "//riegeli/base",
        "//riegeli/base:status",
        "//riegeli/bytes:buffer_options",
        "//riegeli/bytes:buffered_reader",
        "//riegeli/bytes:reader",
        "@bzip2//:bz2lib",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "bzip2_writer",
    srcs = ["bzip2_writer.cc"],
    hdrs = ["bzip2_writer.h"],
    deps = [
        "//riegeli/base",
        "//riegeli/base:status",
        "//riegeli/bytes:buffer_options",
        "//riegeli/bytes:buffered_writer",
        "//riegeli/bytes:writer",
        "@bzip2//:bz2lib",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
    ],
)
