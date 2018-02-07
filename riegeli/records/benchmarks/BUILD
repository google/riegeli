package(default_visibility = ["//visibility:public"])

licenses(["notice"])  # Apache 2.0

cc_binary(
    name = "benchmark",
    srcs = ["benchmark.cc"],
    deps = [
        ":tfrecord_recognizer",
        "//riegeli/base",
        "//riegeli/base:str_error",
        "//riegeli/bytes:fd_reader",
        "//riegeli/bytes:fd_writer",
        "//riegeli/bytes:writer_utils",
        "//riegeli/records:chunk_reader",
        "//riegeli/records:record_reader",
        "//riegeli/records:record_writer",
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
        "@org_tensorflow//tensorflow/core:lib",
    ],
)
