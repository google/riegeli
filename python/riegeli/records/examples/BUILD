load("@com_google_protobuf//:protobuf.bzl", "py_proto_library")

package(default_visibility = ["//python/riegeli:__subpackages__"])

licenses(["notice"])

py_binary(
    name = "write_read_records",
    srcs = ["write_read_records.py"],
    deps = [
        ":records_test_py_pb2",
        "//python/riegeli",
    ],
)

py_proto_library(
    name = "records_test_py_pb2",
    srcs = ["records_test.proto"],
)
