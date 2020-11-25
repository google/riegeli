package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "line_writing",
    hdrs = ["line_writing.h"],
    deps = [
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/bytes:writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
    ],
)

cc_library(
    name = "line_reading",
    srcs = ["line_reading.cc"],
    hdrs = ["line_reading.h"],
    deps = [
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/bytes:reader",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
    ],
)
