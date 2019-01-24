# Riegeli, efficient and safe storage of protocol buffers.

package(default_visibility = ["//visibility:public"])

licenses(["notice"])  # Apache 2.0

exports_files(["LICENSE"])

py_library(
    name = "python",
    srcs = ["__init__.py"],
    visibility = ["//visibility:public"],
    deps = [
        "//riegeli/base/python:riegeli_error",
        "//riegeli/records:records_metadata_py_pb2",
        "//riegeli/records/python:record_position",
        "//riegeli/records/python:record_reader",
        "//riegeli/records/python:record_writer",
        "//riegeli/records/python:skipped_region",
    ],
)
