load("@rules_python//python:defs.bzl", "py_binary")
load("//third_party/bazel_rules/rules_python/python:py_binary.bzl", "py_binary")

package(features = ["header_modules"])

licenses(["notice"])

py_binary(
    name = "write_read_records",
    srcs = ["write_read_records.py"],
    type_checking = True,
    deps = [
        "//python/riegeli",
        "//python/riegeli/records/tests:records_test_py_pb2",
    ],
)
