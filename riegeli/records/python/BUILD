load("//riegeli:python.bzl", "py_extension")

package(default_visibility = ["//riegeli:__subpackages__"])

licenses(["notice"])

py_extension(
    name = "record_writer",
    srcs = ["record_writer.cc"],
    deps = [
        ":record_position_cc",
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/base/python:utils",
        "//riegeli/bytes/python:python_writer",
        "//riegeli/records:record_position",
        "//riegeli/records:record_writer",
        "@com_google_absl//absl/base:core_headers",
        "@local_config_python//:python_headers",
    ],
)

py_extension(
    name = "record_reader",
    srcs = ["record_reader.cc"],
    # Python modules imported from C++.
    data = [
        ":skipped_region",
        "//riegeli/records:records_metadata_py_pb2",
        "@com_google_protobuf//:protobuf_python",
    ],
    deps = [
        ":record_position_cc",
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/base/python:utils",
        "//riegeli/bytes/python:python_reader",
        "//riegeli/chunk_encoding:field_projection",
        "//riegeli/records:record_position",
        "//riegeli/records:record_reader",
        "@com_google_absl//absl/base:core_headers",
        "@local_config_python//:python_headers",
    ],
)

py_extension(
    name = "record_position",
    srcs = ["record_position.cc"],
    hdrs = ["record_position.h"],
    deps = [
        "//riegeli/base",
        "//riegeli/base/python:utils",
        "//riegeli/records:record_position",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/strings",
        "@local_config_python//:python_headers",
    ],
)

py_library(
    name = "skipped_region",
    srcs = ["skipped_region.py"],
)
