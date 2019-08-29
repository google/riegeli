load("@local_config_python//:build_defs.bzl", "python_version")

package(default_visibility = ["//python/riegeli:__subpackages__"])

licenses(["notice"])

py_binary(
    name = "write_read_records",
    srcs = ["write_read_records.py"],
    python_version = python_version,
    deps = [
        "//python/riegeli",
        "//python/riegeli/records/tests:records_test_py_pb2",
    ],
)
