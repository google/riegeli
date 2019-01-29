# Riegeli, file format for storing a sequence of records.

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

exports_files(["LICENSE"])

py_library(
    name = "riegeli",
    srcs = ["__init__.py"],
    imports = [".."],
    deps = [
        "//python/riegeli/base:riegeli_error",
        "//python/riegeli/records:record_position",
        "//python/riegeli/records:record_reader",
        "//python/riegeli/records:record_writer",
        "//python/riegeli/records:records_metadata_py_pb2",
        "//python/riegeli/records:skipped_region",
    ],
)
