load("@rules_cc//cc:defs.bzl", "cc_library")

package(
    default_visibility = ["//visibility:public"],
    features = ["header_modules"],
)

licenses(["notice"])

cc_library(
    name = "gcs_object",
    srcs = ["gcs_object.cc"],
    hdrs = ["gcs_object.h"],
    deps = [
        "//riegeli/base:arithmetic",
        "//riegeli/base:compare",
        "//riegeli/base:debug",
        "//riegeli/base:global",
        "//riegeli/base:reset",
        "//riegeli/base:string_ref",
        "//riegeli/bytes:ostream_writer",
        "//riegeli/bytes:string_writer",
        "//riegeli/bytes:stringify",
        "//riegeli/bytes:stringify_writer",
        "//riegeli/bytes:writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "gcs_reader",
    srcs = [
        "gcs_internal.h",
        "gcs_reader.cc",
    ],
    hdrs = ["gcs_reader.h"],
    deps = [
        ":gcs_object",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:initializer",
        "//riegeli/base:object",
        "//riegeli/base:reset",
        "//riegeli/base:status",
        "//riegeli/base:type_traits",
        "//riegeli/base:types",
        "//riegeli/bytes:buffer_options",
        "//riegeli/bytes:istream_reader",
        "//riegeli/bytes:reader",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@google_cloud_cpp//:common",
        "@google_cloud_cpp//:storage",
    ],
)

cc_library(
    name = "gcs_writer",
    srcs = [
        "gcs_internal.h",
        "gcs_writer.cc",
    ],
    hdrs = ["gcs_writer.h"],
    deps = [
        ":gcs_object",
        ":gcs_reader",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:initializer",
        "//riegeli/base:object",
        "//riegeli/base:reset",
        "//riegeli/base:status",
        "//riegeli/base:string_ref",
        "//riegeli/base:type_traits",
        "//riegeli/base:types",
        "//riegeli/bytes:buffer_options",
        "//riegeli/bytes:ostream_writer",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@google_cloud_cpp//:common",
        "@google_cloud_cpp//:storage",
    ],
)
