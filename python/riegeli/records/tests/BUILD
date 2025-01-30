load("@com_google_protobuf//bazel:proto_library.bzl", "proto_library")
load("@com_google_protobuf//bazel:py_proto_library.bzl", "py_proto_library")
load("@rules_python//python:defs.bzl", "py_test")

package(
    default_visibility = ["//python/riegeli:__subpackages__"],
    features = ["header_modules"],
)

licenses(["notice"])

py_test(
    name = "records_test",
    srcs = ["records_test.py"],
    deps = [
        ":records_test_py_pb2",
        "//python/riegeli",
        "@absl_py//absl/logging",
        "@absl_py//absl/testing:absltest",
        "@absl_py//absl/testing:parameterized",
        "@com_google_protobuf//:protobuf_python",
    ],
)

proto_library(
    name = "records_test_proto",
    srcs = ["records_test.proto"],
)

py_proto_library(
    name = "records_test_py_pb2",
    deps = ["records_test_proto"],
)
