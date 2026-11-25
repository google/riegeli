load("@rules_cc//cc:defs.bzl", "cc_library")

package(
    default_visibility = ["//visibility:public"],
    features = ["header_modules"],
)

licenses(["notice"])

cc_library(
    name = "newline",
    hdrs = ["newline.h"],
    deps = ["@com_google_absl//absl/strings:string_view"],
)

cc_library(
    name = "line_reading",
    srcs = ["line_reading.cc"],
    hdrs = ["line_reading.h"],
    deps = [
        ":newline",
        "//riegeli/base:arithmetic",
        "//riegeli/base:chain",
        "//riegeli/bytes:reader",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/log:absl_check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/strings:string_view",
    ],
)

cc_library(
    name = "line_writing",
    hdrs = ["line_writing.h"],
    deps = [
        ":newline",
        "//riegeli/base:type_traits",
        "//riegeli/bytes:stringify",
        "//riegeli/bytes:writer",
        "@com_google_absl//absl/base:core_headers",
    ],
)
