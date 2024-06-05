load("@rules_python//python:defs.bzl", "py_binary")

package(
    default_visibility = ["//python/riegeli:__subpackages__"],
    features = ["header_modules"],
)

licenses(["notice"])

py_binary(
    name = "write_read_records",
    srcs = ["write_read_records.py"],
    srcs_version = "PY3",
    deps = [
        "//python/riegeli",
        "//python/riegeli/records/tests:records_test_py_pb2",
    ],
)
