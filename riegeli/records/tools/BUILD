package(default_visibility = ["//riegeli:__subpackages__"])

licenses(["notice"])

cc_binary(
    name = "records_benchmark",
    srcs = ["records_benchmark.cc"],
    deps = [
        ":tfrecord_recognizer",
        "//riegeli/base",
        "//riegeli/base:options_parser",
        "//riegeli/base:status",
        "//riegeli/bytes:fd_reader",
        "//riegeli/bytes:fd_writer",
        "//riegeli/bytes:writer_utils",
        "//riegeli/records:chunk_reader",
        "//riegeli/records:record_reader",
        "//riegeli/records:record_writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/flags:parse",
        "@com_google_absl//absl/flags:usage",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@local_config_tf//:libtensorflow_framework",
        "@local_config_tf//:tf_header_lib",
    ],
)

cc_library(
    name = "tfrecord_recognizer",
    srcs = ["tfrecord_recognizer.cc"],
    hdrs = ["tfrecord_recognizer.h"],
    deps = [
        "//riegeli/base",
        "//riegeli/base:endian",
        "//riegeli/base:status",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:zlib_reader",
        "@com_google_absl//absl/base:core_headers",
        "@local_config_tf//:tf_header_lib",
    ],
)
