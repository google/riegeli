package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "file_reader",
    srcs = ["file_reader.cc"],
    hdrs = ["file_reader.h"],
    deps = [
        "//riegeli/base",
        "//riegeli/base:buffer",
        "//riegeli/base:chain",
        "//riegeli/base:status",
        "//riegeli/bytes:backward_writer",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
        "@local_config_tf//:tf_header_lib",
    ],
)

cc_library(
    name = "file_writer",
    srcs = ["file_writer.cc"],
    hdrs = ["file_writer.h"],
    deps = [
        "//riegeli/base",
        "//riegeli/base:buffer",
        "//riegeli/base:chain",
        "//riegeli/base:status",
        "//riegeli/bytes:writer",
        "//third_party/tensorflow/core/platform:status",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@local_config_tf//:tf_header_lib",
    ],
)
