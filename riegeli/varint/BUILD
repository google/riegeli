package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "varint_writing",
    srcs = ["varint_internal.h"],
    hdrs = ["varint_writing.h"],
    deps = [
        "//riegeli/base",
        "//riegeli/bytes:backward_writer",
        "//riegeli/bytes:writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/numeric:bits",
    ],
)

cc_library(
    name = "varint_reading",
    srcs = [
        "varint_internal.h",
        "varint_reading.cc",
    ],
    hdrs = ["varint_reading.h"],
    deps = [
        "//riegeli/base",
        "//riegeli/bytes:reader",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/types:optional",
    ],
)
