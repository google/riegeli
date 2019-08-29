load("@com_google_protobuf//:protobuf.bzl", "py_proto_library")
load("@local_config_python//:build_defs.bzl", "python_version")

package(default_visibility = ["//python/riegeli:__subpackages__"])

licenses(["notice"])

py_test(
    name = "records_test",
    srcs = ["records_test.py"],
    python_version = python_version,
    deps = [
        ":records_test_py_pb2",
        "//python/riegeli",
        "@absl_py//absl/logging",
        "@absl_py//absl/testing:absltest",
        "@absl_py//absl/testing:parameterized",
        "@six_archive//:six",
    ],
)

py_proto_library(
    name = "records_test_py_pb2",
    srcs = ["records_test.proto"],
)
