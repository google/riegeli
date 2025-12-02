load("@rules_cc//cc:defs.bzl", "cc_library")

package(
    default_visibility = ["//visibility:public"],
    features = ["header_modules"],
)

licenses(["notice"])

cc_library(
    name = "varint_reading",
    srcs = [
        "varint_internal.h",
        "varint_reading.cc",
    ],
    hdrs = ["varint_reading.h"],
    deps = [
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/bytes:reader",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/strings:string_view",
    ],
)

cc_library(
    name = "varint_writing",
    srcs = ["varint_internal.h"],
    hdrs = ["varint_writing.h"],
    deps = [
        "//riegeli/base:arithmetic",
        "//riegeli/base:constexpr",
        "//riegeli/bytes:backward_writer",
        "//riegeli/bytes:writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/numeric:bits",
    ],
)
