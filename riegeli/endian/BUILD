package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "endian_writing",
    srcs = ["endian_internal.h"],
    hdrs = ["endian_writing.h"],
    deps = [
        "//riegeli/bytes:backward_writer",
        "//riegeli/bytes:writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "endian_reading",
    srcs = ["endian_internal.h"],
    hdrs = ["endian_reading.h"],
    deps = [
        "//riegeli/bytes:reader",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/types:optional",
    ],
)
