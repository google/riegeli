load("@rules_proto//proto:defs.bzl", "proto_library")

package(default_visibility = ["//riegeli:__subpackages__"])

licenses(["notice"])

cc_binary(
    name = "describe_riegeli_file",
    srcs = ["describe_riegeli_file.cc"],
    deps = [
        ":riegeli_summary_cc_proto",
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/bytes:chain_backward_writer",
        "//riegeli/bytes:chain_reader",
        "//riegeli/bytes:fd_reader",
        "//riegeli/bytes:limiting_reader",
        "//riegeli/bytes:message_parse",
        "//riegeli/bytes:message_serialize",
        "//riegeli/bytes:null_backward_writer",
        "//riegeli/bytes:reader_utils",
        "//riegeli/bytes:std_io",
        "//riegeli/bytes:varint_reading",
        "//riegeli/bytes:writer",
        "//riegeli/chunk_encoding:chunk",
        "//riegeli/chunk_encoding:constants",
        "//riegeli/chunk_encoding:decompressor",
        "//riegeli/chunk_encoding:field_projection",
        "//riegeli/chunk_encoding:transpose_decoder",
        "//riegeli/records:chunk_reader",
        "//riegeli/records:records_metadata_cc_proto",
        "//riegeli/records:skipped_region",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/flags:parse",
        "@com_google_absl//absl/flags:usage",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:optional",
        "@com_google_protobuf//:protobuf",
    ],
)

proto_library(
    name = "riegeli_summary_proto",
    srcs = ["riegeli_summary.proto"],
    deps = ["//riegeli/records:records_metadata_proto"],
)

cc_proto_library(
    name = "riegeli_summary_cc_proto",
    deps = [":riegeli_summary_proto"],
)

cc_binary(
    name = "records_benchmark",
    srcs = ["records_benchmark.cc"],
    deps = [
        ":tfrecord_recognizer",
        "//riegeli/base",
        "//riegeli/base:options_parser",
        "//riegeli/bytes:fd_reader",
        "//riegeli/bytes:fd_writer",
        "//riegeli/bytes:std_io",
        "//riegeli/bytes:varint_writing",
        "//riegeli/bytes:writer",
        "//riegeli/records:chunk_reader",
        "//riegeli/records:record_reader",
        "//riegeli/records:record_writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/flags:parse",
        "@com_google_absl//absl/flags:usage",
        "@com_google_absl//absl/status",
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
        "//riegeli/bytes:endian_reading",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:zlib_reader",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@local_config_tf//:tf_header_lib",
    ],
)
