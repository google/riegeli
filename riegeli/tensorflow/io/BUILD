load("@rules_cc//cc:defs.bzl", "cc_library")

package(
    default_visibility = ["//visibility:public"],
    features = ["header_modules"],
)

licenses(["notice"])

cc_library(
    name = "file_reader",
    srcs = ["file_reader.cc"],
    hdrs = ["file_reader.h"],
    deps = [
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:buffering",
        "//riegeli/base:chain",
        "//riegeli/base:dependency",
        "//riegeli/base:external_ref",
        "//riegeli/base:initializer",
        "//riegeli/base:null_safe_memcpy",
        "//riegeli/base:object",
        "//riegeli/base:reset",
        "//riegeli/base:sized_shared_buffer",
        "//riegeli/base:status",
        "//riegeli/base:types",
        "//riegeli/bytes:backward_writer",
        "//riegeli/bytes:buffer_options",
        "//riegeli/bytes:path_ref",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:writer",
        # TODO: Riegeli/TensorFlow build is currently broken
        # in open source because TensorFlow does not support bzlmod yet.
    ],
)

cc_library(
    name = "file_writer",
    srcs = ["file_writer.cc"],
    hdrs = ["file_writer.h"],
    deps = [
        ":file_reader",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/strings:cord",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:buffering",
        "//riegeli/base:chain",
        "//riegeli/base:dependency",
        "//riegeli/base:external_ref",
        "//riegeli/base:byte_fill",
        "//riegeli/base:initializer",
        "//riegeli/base:object",
        "//riegeli/base:reset",
        "//riegeli/base:shared_buffer",
        "//riegeli/base:status",
        "//riegeli/base:types",
        "//riegeli/bytes:buffer_options",
        "//riegeli/bytes:path_ref",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:writer",
        # TODO: Riegeli/TensorFlow build is currently broken
        # in open source because TensorFlow does not support bzlmod yet.
    ],
)

cc_library(
    name = "tstring_writer",
    hdrs = ["tstring_writer.h"],
    deps = [
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:dependency",
        "//riegeli/base:initializer",
        "//riegeli/base:object",
        "//riegeli/bytes:resizable_writer",
        # TODO: Riegeli/TensorFlow build is currently broken
        # in open source because TensorFlow does not support bzlmod yet.
    ],
)
