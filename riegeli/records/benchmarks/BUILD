package(default_visibility = ["//visibility:public"])

licenses(["notice"])  # Apache 2.0

cc_binary(
    name = "benchmark",
    srcs = ["benchmark.cc"],
    deps = [
        ":tfrecord_recognizer",
        "//riegeli/base",
        "//riegeli/base:options_parser",
        "//riegeli/base:str_error",
        "//riegeli/bytes:fd_reader",
        "//riegeli/bytes:fd_writer",
        "//riegeli/bytes:writer_utils",
        "//riegeli/records:chunk_reader",
        "//riegeli/records:record_reader",
        "//riegeli/records:record_writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@org_tensorflow//tensorflow/core:lib",
    ],
)

cc_library(
    name = "tfrecord_recognizer",
    srcs = ["tfrecord_recognizer.cc"],
    hdrs = ["tfrecord_recognizer.h"],
    deps = [
        "//riegeli/base",
        "//riegeli/base:endian",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:zlib_reader",
        "@com_google_absl//absl/base:core_headers",
        "@org_tensorflow//tensorflow/core:lib",
    ],
)
