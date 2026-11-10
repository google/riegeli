# Riegeli, file format for storing a sequence of records.

load("@rules_python//python:defs.bzl", "py_library")
load("//third_party/bazel_rules/rules_python/python:py_library.bzl", "py_library")

package(
    default_visibility = ["//visibility:public"],
    features = ["header_modules"],
)

licenses(["notice"])

exports_files(["LICENSE"])

py_library(
    name = "riegeli",
    srcs = ["__init__.py"],
    imports = [".."],
    type_checking = True,
    deps = [
        "//python/riegeli/base:riegeli_error",
        "//python/riegeli/records:record_position",
        "//python/riegeli/records:record_reader",
        "//python/riegeli/records:record_writer",
        "//python/riegeli/records:records_metadata_py_pb2",
        "//python/riegeli/records:skipped_region",
    ],
)
